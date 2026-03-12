# Perfect Hash Join

## 1. What Problem Does It Solve?

A classic **Hash Join** works in two phases:

1. **Build**: scan the smaller (build) table, hash each key, and insert each tuple into a pointer table.
2. **Probe**: for each row in the larger (probe) table, compute the same hash, locate the matching entry in the pointer table, and follow pointers to retrieve matching tuples.

DuckDB's `JoinHashTable` specifically uses **two collision strategies at two different levels**:

- **Pointer table (linear probing)**: the `entries[]` array is a flat array of `ht_entry_t` values, each storing a salt (upper 16 bits of the hash) and a pointer (lower 48 bits). When two *different* keys hash to the same slot, the later key is displaced to the next empty slot via linear probing — they do not share a slot.
- **Tuple storage (embedded linked list)**: each serialized row has an embedded `NEXT POINTER` field. This linked list handles *duplicate keys* — multiple rows with the same join key value — not general collision resolution.

When probing, the salt stored in each `ht_entry_t` is checked against the upper bits of the probe hash before dereferencing the pointer. This filters most non-matching entries without touching tuple data, reducing cache misses for large tables.

The costs of this approach:
- Linear probing displaces keys to adjacent slots → multiple slot comparisons per lookup
- Salt mismatches are cheap, but a salt match still requires dereferencing the tuple pointer to confirm the full key → cache miss on the tuple data
- Duplicate keys require following the embedded `NEXT POINTER` chain → additional pointer chasing per duplicate

A **Perfect Hash Join** eliminates this entirely. When the build-side keys are **integers within a small range `[min, max]`**, there is no need for a hash function at all. Every key maps to a unique array slot via a single arithmetic operation:

```
slot_index = key - min
```

No hash. No buckets. No collisions. One subtraction, one array access.

---

## 2. When Can It Be Used?

All of the following conditions must hold (from `CanDoPerfectHashJoin`):

| Condition | Reason |
|---|---|
| INNER join only | Outer joins require NULL-filling for non-matches |
| Single equality condition | The index formula works on exactly one key |
| Integer key type | `key - min` is only meaningful for integers |
| No nested types on RHS (STRUCT/LIST/ARRAY) | Dictionary vectors cannot encode these |
| `max - min ≤ 1,048,576` | Bounds the array allocation (~1 MB) |
| `row_count ≤ (max - min)` | Guarantees no duplicate keys → one unique slot per key |
| No residual predicates | Lookup must be purely key-based |

If any condition fails at any point — including mid-build if a duplicate key is detected — the engine silently falls back to the regular hash join.

---

## 3. Data Structures

Instead of a hash table with chained buckets, a Perfect Hash Join uses a **columnar array** indexed directly by key value.

```
perfect_hash_table
├── column[0]: array[build_size]   ← one slot per possible key value
├── column[1]: array[build_size]
└── ...

bitmap_build_idx: ValidityMask[build_size]
                  marks which slots are populated
```

`build_size = (max - min) + 1`

Slot `k` holds the data for the build row whose key equals `min + k`. Empty slots (keys not present in the build table) are marked invalid in the bitmap and set to NULL.

---

## 4. Phase 1 — Build

### Step 1: Allocate arrays
One array of `build_size` slots per output column. All bitmap entries start invalid.

### Step 2: Scan the regular hash table
DuckDB first builds a regular hash table (required for the general case). The perfect hash join then **scans that hash table** to extract all keys and compute their slot positions:

```cpp
// For each build key:
idx_t slot = key - min_value;

// Duplicate check
if (bitmap[slot] is already VALID) → return false (abort, fall back)

bitmap[slot] = VALID;
unique_keys++;
```

Two selection vectors are produced:
- `sel_build`: target slot indices in the perfect hash table (where to write)
- `sel_tuples`: source positions in the hash table scan (what to read)

### Step 3: Populate the columnar arrays
`data_collection.Gather(tuples_addresses, sel_tuples, key_count, col_idx, vector, sel_build)` reads from hash table rows and writes into the correct slots.

After this, empty slots get their validity explicitly cleared:
```cpp
col_mask.Combine(bitmap_build_idx, build_size);
```

### Step 4: Dense detection
```cpp
if (unique_keys == build_size && !ht.has_null) {
    is_build_dense = true;  // every slot is filled, no gaps
}
```
Dense means: every integer in `[min, max]` exists as a build key. This enables a fast path during probe.

### Concrete example

Build table `product_dim`:

| product_id | name         | price |
|---|---|---|
| 3          | Widget       | 9.99  |
| 5          | Gadget       | 24.99 |
| 7          | Doohickey    | 4.99  |
| 8          | Thingamajig  | 14.99 |
| 10         | Gizmo        | 34.99 |

- `min=3`, `max=10`, `build_size=8`, `unique_keys=5`
- `5 ≠ 8` → **not dense** (slots 1, 3, 6 are empty — keys 4, 6, 9 absent)

```
perfect_hash_table[name]:
  slot 0 → "Widget"       (key=3,  3-3=0)
  slot 1 → NULL           (key=4,  absent)
  slot 2 → "Gadget"       (key=5,  5-3=2)
  slot 3 → NULL           (key=6,  absent)
  slot 4 → "Doohickey"    (key=7,  7-3=4)
  slot 5 → "Thingamajig"  (key=8,  8-3=5)
  slot 6 → NULL           (key=9,  absent)
  slot 7 → "Gizmo"        (key=10, 10-3=7)

bitmap: [ V  -  V  -  V  V  -  V ]
          0  1  2  3  4  5  6  7
```

---

## 5. Phase 2 — Probe

For each probe chunk, the engine iterates over probe keys and builds two selection vectors:

```cpp
// For each probe row i with key value k:
if (k < min || k > max)       → skip (out of range, no match possible)
idx_t slot = k - min_value;
if (!bitmap[slot])             → skip (slot empty, key not in build table)

build_sel_vec[sel_idx] = slot; // which build slot to read
probe_sel_vec[sel_idx] = i;    // which probe row matched
probe_sel_count++;
```

### Assembling output

**Probe-side columns:**
```cpp
// Non-dense path: filter rows that didn't match
result.Slice(lhs_output_columns, probe_sel_vec, probe_sel_count);
// → produces a DICTIONARY_VECTOR over the input chunk

// Dense path (all probe keys guaranteed to match):
result.Reference(lhs_output_columns);
// → zero-copy direct reference, no filtering at all
```

**Build-side columns:**
```cpp
result_vector.Dictionary(perfect_hash_table[i], build_sel_vec);
// → zero-copy dictionary vector: pointer to array + index vector
```

### Concrete probe example

Probe chunk (`sales`):

| i | product_id |
|---|---|
| 0 | 3  |
| 1 | 10 |
| 2 | 5  |
| 3 | **12** ← out of range |
| 4 | 7  |
| 5 | 3  |
| 6 | 8  |

Processing:

| i | key | in [3,10]? | slot = key−3 | bitmap[slot]? | Result |
|---|---|---|---|---|---|
| 0 | 3  | yes | 0 | VALID | MATCH |
| 1 | 10 | yes | 7 | VALID | MATCH |
| 2 | 5  | yes | 2 | VALID | MATCH |
| 3 | 12 | **no** | — | — | **SKIP** |
| 4 | 7  | yes | 4 | VALID | MATCH |
| 5 | 3  | yes | 0 | VALID | MATCH |
| 6 | 8  | yes | 5 | VALID | MATCH |

```
probe_sel_vec = [ 0, 1, 2, 4, 5, 6 ]     ← 6 probe rows that matched
build_sel_vec = [ 0, 7, 2, 4, 0, 5 ]     ← slots to read from perfect_hash_table
                              ^  ^
                              index 0 appears twice (product_id=3 matched twice)
```

Final output (6 rows, row i=3 silently dropped):

| sale_id | product_id | quantity | name         | price |
|---|---|---|---|---|
| 1001 | 3  | 2 | Widget       | 9.99  |
| 1002 | 10 | 1 | Gizmo        | 34.99 |
| 1003 | 5  | 5 | Gadget       | 24.99 |
| 1005 | 7  | 1 | Doohickey    | 4.99  |
| 1006 | 3  | 4 | Widget       | 9.99  |
| 1007 | 8  | 2 | Thingamajig  | 14.99 |

---

## 6. Output: Dictionary Vectors

The perfect hash join emits **dictionary vectors** for build-side columns. A dictionary vector has two components:

```
DICTIONARY_VECTOR
├── VectorChildBuffer (the "dictionary")
│     ├── data[]         ← flat array of actual values (the perfect_hash_table slot)
│     ├── size           ← number of entries (= build_size)
│     └── id             ← a UUID assigned once at build time
│
└── DictionaryBuffer (the "indices")
      └── sel_vector[]   ← per-row indices into data[] (= build_sel_vec)
```

**The `dictionary_id` (UUID)** identifies the child data array as a whole — not individual rows. It is unique per `VectorChildBuffer` and is assigned once during `CreateReusableDictionary`. It is used for optimization: if two dictionary vectors share the same `dictionary_id`, the engine can reuse pre-computed hashes of the dictionary entries without recomputation.

**The `sel_vector` entries** are per-row indices and are **not** unique. Multiple probe rows matching the same build key all point to the same slot. In the example above, both `sale_id=1001` and `sale_id=1006` (both `product_id=3`) get `sel_vector[k]=0`. The value `"Widget"` is stored exactly once; both rows just hold an index to it.

This is the **compression benefit**: repeated matches cost nothing extra in the output — only the index is duplicated, not the data.

### What probe-side columns look like

| Scenario | Probe-side output type |
|---|---|
| Dense build, all probe rows match | Flat vector (direct `Reference` — zero copy) |
| Non-dense or some rows filtered | `DICTIONARY_VECTOR` (via `Slice`) |

Downstream operators receive dictionary vectors transparently. They call `ToUnifiedFormat()` which normalizes any vector type (flat, dictionary, constant) into a uniform `data + selection_vector + validity_mask` view.

---

## 7. The Dense Fast Path

When every integer in `[min, max]` exists as a build key (`is_build_dense = true`):

- The bitmap is reset to all-valid (no need to check it per probe row).
- During probe, if **all** probe keys are within `[min, max]` (i.e., `probe_sel_count == keys_count`):

```cpp
result.Reference(lhs_output_columns);  // probe side: zero-copy, no filtering
```

Combined with the dictionary vectors on the build side, **the entire join produces zero data copies**.

---

## 8. Why It's Faster Than Regular Hash Join

| Operation | DuckDB Regular Hash Join | Perfect Hash Join |
|---|---|---|
| Probe lookup | `hash(key) → linear probe until slot matches → dereference pointer` | `key - min → array[slot]` |
| Different-key collisions | Linear probing: hop to next slot, compare salt, repeat | None — every key has a unique slot by construction |
| Duplicate-key resolution | Follow embedded `NEXT POINTER` chain per tuple | None — duplicates are rejected at build time |
| Salt check | Required for large tables (>8192 entries) to avoid pointer dereference on mismatch | Not needed — direct index, no false positives |
| Cache behavior | Displaced keys scatter across the pointer table; tuple data at arbitrary addresses | Sequential columnar array; predictable access pattern |
| Validity check | Full key comparison after pointer dereference | Single bitmap bit test |
| Output construction | Tuple data gathered via pointer | Dictionary vector (zero-copy) |

The subtraction is a single CPU instruction. The bitmap check is one memory read. The dictionary output avoids any data movement. For the right workload — small integer-keyed dimension tables — this is dramatically faster.

---

## 9. Ideal Use Case: Star Schema Joins

```sql
SELECT f.revenue, d.month_name
FROM fact_sales f
JOIN date_dim d ON f.date_key = d.date_key
```

If `date_dim` has `date_key` in range `[1, 365]`:

- `build_range = 364`, `build_size = 365`, all integer keys → perfect hash applicable
- Arrays of 365 slots allocated for `month_name`, etc.
- Each probe row computes `date_key - 1` → direct slot access
- Output emitted as dictionary vectors pointing into those arrays

This is precisely the pattern perfect hash join is designed for: **dimension table joins in analytical (OLAP) workloads**, where dimension keys are small integers and the build side fits entirely in a compact array.

---

## 10. Integration with Physical Hash Join

`PerfectHashJoinExecutor` is not a separate operator. It is an **optimization layer owned by `PhysicalHashJoin`**:

1. The regular hash table is always built first (required for the fallback path).
2. After the build phase, statistics (`min`, `max`) are inspected.
3. `CanDoPerfectHashJoin` checks all conditions.
4. If eligible, `BuildPerfectHashTable` scans the regular hash table and populates the columnar arrays.
5. During probe, `ExecuteInternal` routes to `ProbePerfectHashTable` instead of the regular probe path.
6. If anything fails (range too large, duplicate key found, etc.), `perfect_join_executor` is reset to `nullptr` and the regular hash join continues unchanged.

```
PhysicalHashJoin::ExecuteInternal
    │
    ├─ sink.perfect_join_executor != nullptr?
    │       └─ YES → ProbePerfectHashTable → emit dict vectors
    │
    └─ NO → regular hash probe → emit flat/pointer vectors
```
