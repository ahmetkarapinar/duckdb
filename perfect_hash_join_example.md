# Perfect Hash Join — Step-by-Step Walkthrough

This document traces a Perfect Hash Join end-to-end using a concrete employee/department query. It covers every internal step: eligibility checks, the build phase with exact selection vectors, dense detection, two probe chunk scenarios (dense fast path and non-dense path), and the exact dictionary vector structures emitted downstream.

---

## Query

```sql
SELECT e.emp_id, e.salary, d.dept_name, d.budget
FROM employee e
JOIN department d ON e.dept_id = d.dept_id
```

**`department`** is the build side (smaller table). **`employee`** is the probe side.

---

## Input Data

### Build side: `department`

| dept_id | dept_name   | budget    |
|---------|-------------|-----------|
| 1       | Engineering | 5,000,000 |
| 2       | Marketing   | 2,000,000 |
| 3       | Sales       | 3,000,000 |
| 4       | HR          | 1,000,000 |
| 5       | Finance     | 4,000,000 |

### Probe side: `employee` (two chunks shown below)

---

## Phase 0 — Eligibility Check (`CanDoPerfectHashJoin`)

After the regular hash table is built, DuckDB inspects the build statistics and checks every condition:

| Condition | Value | Pass? |
|-----------|-------|-------|
| Join type == INNER | INNER JOIN | ✓ |
| Single equality condition | `e.dept_id = d.dept_id` — one condition | ✓ |
| Key type is integer | `dept_id` is INTEGER | ✓ |
| No nested types on RHS (STRUCT/LIST/ARRAY) | `dept_name` = VARCHAR, `budget` = BIGINT | ✓ |
| `max - min ≤ 1,048,576` | `5 - 1 = 4 ≤ 1,048,576` | ✓ |
| `row_count ≤ (max - min + 1)` | `5 ≤ 5` | ✓ (no duplicates possible) |
| No residual predicates | no extra filter conditions | ✓ |

All conditions pass. `CanDoPerfectHashJoin` returns `true`.

Computed statistics stored in `PerfectHashJoinStats`:
```
build_min   = 1
build_max   = 5
build_range = 4        (max - min)
build_size  = 5        (build_range + 1)
is_build_small = true
```

---

## Phase 1 — Build (`BuildPerfectHashTable`)

### Step 1a: Allocate columnar arrays

One `VectorChildBuffer` per output column, each with `build_size = 5` slots. All bitmap entries start **invalid**.

```
perfect_hash_table[dept_name]:  [ _ , _ , _ , _ , _ ]
                                  0   1   2   3   4

perfect_hash_table[budget]:     [ _ , _ , _ , _ , _ ]
                                  0   1   2   3   4

bitmap_build_idx:               [ - , - , - , - , - ]   (all invalid)
                                  0   1   2   3   4

unique_keys = 0
```

### Step 1b: Scan the regular hash table (`FullScanHashTable`)

The regular hash table stores rows in hash order, not insertion order. Let's say `FullScanHashTable` produces rows in this order: `dept_id = 3, 1, 5, 2, 4`.

`TemplatedFillSelectionVectorBuild<int32_t>` is called with `min_value = 1`:

```
scan position i=0 → dept_id = 3
  slot = 3 - 1 = 2
  bitmap[2] == INVALID → OK (no duplicate)
  bitmap[2] = VALID
  sel_build[0]  = 2   ← write to slot 2 in perfect_hash_table
  sel_tuples[0] = 0   ← read from scan position 0
  unique_keys = 1

scan position i=1 → dept_id = 1
  slot = 1 - 1 = 0
  bitmap[0] == INVALID → OK
  bitmap[0] = VALID
  sel_build[1]  = 0
  sel_tuples[1] = 1
  unique_keys = 2

scan position i=2 → dept_id = 5
  slot = 5 - 1 = 4
  bitmap[4] == INVALID → OK
  bitmap[4] = VALID
  sel_build[2]  = 4
  sel_tuples[2] = 2
  unique_keys = 3

scan position i=3 → dept_id = 2
  slot = 2 - 1 = 1
  bitmap[1] == INVALID → OK
  bitmap[1] = VALID
  sel_build[3]  = 1
  sel_tuples[3] = 3
  unique_keys = 4

scan position i=4 → dept_id = 4
  slot = 4 - 1 = 3
  bitmap[3] == INVALID → OK
  bitmap[3] = VALID
  sel_build[4]  = 3
  sel_tuples[4] = 4
  unique_keys = 5
```

After the scan:

```
sel_build  = [ 2, 0, 4, 1, 3 ]   ← target slots in perfect_hash_table (where to write)
sel_tuples = [ 0, 1, 2, 3, 4 ]   ← source positions in hash table scan (what to read)

bitmap_build_idx: [ V, V, V, V, V ]   (all valid — every slot has a row)
```

### Step 1c: Populate the columnar arrays (`data_collection.Gather`)

`Gather` reads each source row (via `sel_tuples`) and writes it to the target slot (via `sel_build`):

```
Gather step  sel_tuples[i]  dept_name read  sel_build[i]  slot written
─────────────────────────────────────────────────────────────────────
i=0          pos 0          "Sales"         slot 2         slot 2 ← "Sales"
i=1          pos 1          "Engineering"   slot 0         slot 0 ← "Engineering"
i=2          pos 2          "Finance"       slot 4         slot 4 ← "Finance"
i=3          pos 3          "Marketing"     slot 1         slot 1 ← "Marketing"
i=4          pos 4          "HR"            slot 3         slot 3 ← "HR"
```

Resulting columnar arrays:

```
perfect_hash_table[dept_name]:
  slot 0 → "Engineering"   (key = 1 → 1-1=0)
  slot 1 → "Marketing"     (key = 2 → 2-1=1)
  slot 2 → "Sales"         (key = 3 → 3-1=2)
  slot 3 → "HR"            (key = 4 → 4-1=3)
  slot 4 → "Finance"       (key = 5 → 5-1=4)

perfect_hash_table[budget]:
  slot 0 → 5,000,000
  slot 1 → 2,000,000
  slot 2 → 3,000,000
  slot 3 → 1,000,000
  slot 4 → 4,000,000

bitmap_build_idx: [ V, V, V, V, V ]
                    0  1  2  3  4
```

Every slot is populated. The key `min + slot` maps directly to the stored row:
- `slot 0` = key `1` (Engineering)
- `slot 1` = key `2` (Marketing)
- `slot 2` = key `3` (Sales)
- `slot 3` = key `4` (HR)
- `slot 4` = key `5` (Finance)

### Step 1d: Dense detection

```cpp
if (unique_keys == build_size && !ht.has_null) {
    is_build_dense = true;
}
```

```
unique_keys = 5
build_size  = 5
ht.has_null = false

5 == 5 and no nulls → is_build_dense = true
```

Since `is_build_dense = true`, the bitmap is reset to **all-valid** — the per-row validity check during probe is skipped entirely. Every integer in `[1, 5]` is guaranteed to match a build row.

---

## Phase 2 — Probe, Scenario A: All Probe Keys In Range (Dense Fast Path)

### Probe Chunk A

| i | emp_id | dept_id | salary  |
|---|--------|---------|---------|
| 0 | 101    | 2       | 75,000  |
| 1 | 102    | 5       | 120,000 |
| 2 | 103    | 1       | 95,000  |
| 3 | 104    | 3       | 85,000  |
| 4 | 105    | 4       | 65,000  |
| 5 | 106    | 2       | 80,000  |
| 6 | 107    | 1       | 110,000 |

`keys_count = 7`

### `TemplatedFillSelectionVectorProbe<int32_t>` — row by row

For each probe row, the range check is `k < min (1)` or `k > max (5)`. Since `is_build_dense = true`, the bitmap check is skipped (all slots valid).

```
i=0, key=2:  in [1,5] ✓  slot = 2-1 = 1  → build_sel_vec[0]=1, probe_sel_vec[0]=0
i=1, key=5:  in [1,5] ✓  slot = 5-1 = 4  → build_sel_vec[1]=4, probe_sel_vec[1]=1
i=2, key=1:  in [1,5] ✓  slot = 1-1 = 0  → build_sel_vec[2]=0, probe_sel_vec[2]=2
i=3, key=3:  in [1,5] ✓  slot = 3-1 = 2  → build_sel_vec[3]=2, probe_sel_vec[3]=3
i=4, key=4:  in [1,5] ✓  slot = 4-1 = 3  → build_sel_vec[4]=3, probe_sel_vec[4]=4
i=5, key=2:  in [1,5] ✓  slot = 2-1 = 1  → build_sel_vec[5]=1, probe_sel_vec[5]=5
i=6, key=1:  in [1,5] ✓  slot = 1-1 = 0  → build_sel_vec[6]=0, probe_sel_vec[6]=6
```

```
probe_sel_count = 7
keys_count      = 7

probe_sel_vec = [ 0, 1, 2, 3, 4, 5, 6 ]
build_sel_vec = [ 1, 4, 0, 2, 3, 1, 0 ]
                              ^        ^
                    slot 1 appears twice (dept_id=2 matched rows 0 and 5)
                                       slot 0 appears twice (dept_id=1 matched rows 2 and 6)
```

### Output Assembly — Dense Fast Path

Condition: `is_build_dense == true && probe_sel_count == keys_count` (7 == 7)

**Probe-side columns (`emp_id`, `salary`):**

```cpp
result.Reference(lhs_output_columns);
```

Zero-copy. The output chunk holds a direct reference to the input data — no filtering, no copying, no selection vector overhead.

**Build-side columns (`dept_name`, `budget`):**

```cpp
result_vector.Dictionary(perfect_hash_table[dept_name], build_sel_vec);
result_vector.Dictionary(perfect_hash_table[budget],    build_sel_vec);
```

### Dictionary Vector Structure (dept_name)

```
DICTIONARY_VECTOR for dept_name
│
├── VectorChildBuffer  (the "dictionary" — shared, created once at build time)
│     ├── id:   "550e8400-e29b-41d4-a716-446655440000"   ← UUID, unique per child array
│     ├── size: 5
│     └── data: [ "Engineering", "Marketing", "Sales", "HR", "Finance" ]
│                      slot 0        slot 1     slot 2   slot 3  slot 4
│
└── DictionaryBuffer  (the "indices" — one per output chunk)
      └── sel_vector: [ 1, 4, 0, 2, 3, 1, 0 ]
                        ↑           ↑     ↑
                        slot 1 ("Marketing") appears at rows 0 and 5
                                          slot 0 ("Engineering") appears at rows 2 and 6
```

Reading each output row by following `sel_vector → data[]`:

| output row | probe sel | build sel | emp_id | dept_id | salary  | dept_name   | budget    |
|------------|-----------|-----------|--------|---------|---------|-------------|-----------|
| 0          | row 0     | slot 1    | 101    | 2       | 75,000  | Marketing   | 2,000,000 |
| 1          | row 1     | slot 4    | 102    | 5       | 120,000 | Finance     | 4,000,000 |
| 2          | row 2     | slot 0    | 103    | 1       | 95,000  | Engineering | 5,000,000 |
| 3          | row 3     | slot 2    | 104    | 3       | 85,000  | Sales       | 3,000,000 |
| 4          | row 4     | slot 3    | 105    | 4       | 65,000  | HR          | 1,000,000 |
| 5          | row 5     | slot 1    | 106    | 2       | 80,000  | Marketing   | 2,000,000 |
| 6          | row 6     | slot 0    | 107    | 1       | 110,000 | Engineering | 5,000,000 |

`"Marketing"` and `"Engineering"` are each stored once in the child array. Rows 0+5 and rows 2+6 reference them via index. No data is duplicated.

---

## Phase 2 — Probe, Scenario B: Some Probe Keys Out of Range

### Probe Chunk B

| i | emp_id | dept_id         | salary  |
|---|--------|-----------------|---------|
| 0 | 201    | 3               | 90,000  |
| 1 | 202    | **7** (no dept) | 200,000 |
| 2 | 203    | 1               | 70,000  |
| 3 | 204    | 5               | 130,000 |
| 4 | 205    | **0** (no dept) | 50,000  |

`keys_count = 5`

### `TemplatedFillSelectionVectorProbe<int32_t>` — row by row

```
i=0, key=3:  in [1,5] ✓  slot = 3-1 = 2  → build_sel_vec[0]=2, probe_sel_vec[0]=0
i=1, key=7:  7 > 5  ✗   SKIP (out of range — no match possible)
i=2, key=1:  in [1,5] ✓  slot = 1-1 = 0  → build_sel_vec[1]=0, probe_sel_vec[1]=2
i=3, key=5:  in [1,5] ✓  slot = 5-1 = 4  → build_sel_vec[2]=4, probe_sel_vec[2]=3
i=4, key=0:  0 < 1  ✗   SKIP (out of range — no match possible)
```

```
probe_sel_count = 3
keys_count      = 5

probe_sel_vec = [ 0, 2, 3 ]    ← probe rows that matched (row 1 and row 4 dropped)
build_sel_vec = [ 2, 0, 4 ]    ← corresponding build slots
```

### Output Assembly — Non-Dense Probe Path

Condition: `probe_sel_count (3) ≠ keys_count (5)` — even though `is_build_dense = true`, the probe side has unmatched rows.

**Probe-side columns (`emp_id`, `salary`):**

```cpp
result.Slice(lhs_output_columns, probe_sel_vec, probe_sel_count);
// probe_sel_vec = [0, 2, 3]
```

This produces a `DICTIONARY_VECTOR` over the input chunk:

```
DICTIONARY_VECTOR for emp_id (probe side)
├── VectorChildBuffer
│     └── data: [ 201, 202, 203, 204, 205 ]   ← original input, untouched
│                  0     1    2    3    4
└── DictionaryBuffer
      └── sel_vector: [ 0, 2, 3 ]              ← selects rows 0, 2, 3 from input
```

Rows 1 (emp_id=202, dept_id=7) and 4 (emp_id=205, dept_id=0) are excluded — they had no matching build row.

**Build-side columns (`dept_name`, `budget`):**

```cpp
result_vector.Dictionary(perfect_hash_table[dept_name], build_sel_vec);
// build_sel_vec = [2, 0, 4]
```

```
DICTIONARY_VECTOR for dept_name (build side)
├── VectorChildBuffer
│     ├── id:   "550e8400-e29b-41d4-a716-446655440000"  ← same UUID as Chunk A (reused!)
│     ├── size: 5
│     └── data: [ "Engineering", "Marketing", "Sales", "HR", "Finance" ]
└── DictionaryBuffer
      └── sel_vector: [ 2, 0, 4 ]
```

Final output (3 rows):

| emp_id | dept_id | salary  | dept_name   | budget    |
|--------|---------|---------|-------------|-----------|
| 201    | 3       | 90,000  | Sales       | 3,000,000 |
| 203    | 1       | 70,000  | Engineering | 5,000,000 |
| 204    | 5       | 130,000 | Finance     | 4,000,000 |

Rows for `emp_id=202` (dept 7) and `emp_id=205` (dept 0) are silently dropped — they had no matching department.

---

## The Dictionary ID: Shared Across Chunks

Notice that both Chunk A and Chunk B produce a `VectorChildBuffer` for `dept_name` with the **same UUID** (`"550e8400-e29b-41d4-a716-446655440000"`). This is because:

- `CreateReusableDictionary` is called once during `BuildPerfectHashTable` and assigns the UUID at that time.
- The same `buffer_ptr<VectorChildBuffer>` is stored in `perfect_hash_table[dept_name]` and is reused across all probe chunks.
- Each probe chunk produces a **new** `DictionaryBuffer` (with a new `sel_vector`), but shares the same child array.

This enables a downstream optimization: if a sort or aggregation operator sees the same `dictionary_id` across multiple output chunks, it can reuse pre-computed hashes of the `["Engineering", "Marketing", "Sales", "HR", "Finance"]` array without recomputing them from scratch.

---

## What Happens If a Duplicate Key Appears at Build Time

Suppose `department` had two rows with `dept_id = 3`:

| dept_id | dept_name | budget    |
|---------|-----------|-----------|
| 3       | Sales     | 3,000,000 |
| 3       | Sales-EU  | 1,500,000 |

During `TemplatedFillSelectionVectorBuild`:

```
scan position i=0 → dept_id = 3
  slot = 3 - 1 = 2
  bitmap[2] == INVALID → set VALID, unique_keys++

scan position i=1 → dept_id = 3
  slot = 3 - 1 = 2
  bitmap[2] == VALID → DUPLICATE DETECTED
  → return false
```

`FullScanHashTable` returns `false` → `BuildPerfectHashTable` returns `false` → `perfect_join_executor` is reset to `nullptr`. The regular linear probing hash join continues unchanged, handling duplicates via its embedded `NEXT POINTER` chain.

---

## What Downstream Operators Receive (`ToUnifiedFormat`)

Downstream operators (aggregations, projections, further joins) call `ToUnifiedFormat()` on each vector. This normalizes any vector type into a uniform view:

```
UnifiedVectorFormat {
    data        → pointer to raw values
    sel         → SelectionVector (per-row indices)
    validity    → ValidityMask
}
```

For the dictionary vector `dept_name` from Chunk A:

```
UnifiedVectorFormat for dept_name:
  data     → &VectorChildBuffer.data[0]   ("Engineering", "Marketing", ...)
  sel      → DictionaryBuffer.sel_vector   [1, 4, 0, 2, 3, 1, 0]
  validity → derived from child's validity mask
```

The operator reads `data[sel[i]]` to get the value for row `i`. No copy of the string data is made. The dictionary vector is transparent to all downstream operators.

---

## Summary: Dense Path vs. Non-Dense Path

| | Chunk A (all in range) | Chunk B (some out of range) |
|--|--|--|
| `probe_sel_count` | 7 (= `keys_count`) | 3 (< `keys_count`) |
| Probe side output | Flat vector via `Reference()` — zero copy | `DICTIONARY_VECTOR` via `Slice()` — index into original |
| Build side output | `DICTIONARY_VECTOR` via `Dictionary()` | `DICTIONARY_VECTOR` via `Dictionary()` |
| Rows dropped | 0 | 2 (dept_id=7 and dept_id=0) |
| Data copies | **Zero** | Zero (indices only, no value copy) |
