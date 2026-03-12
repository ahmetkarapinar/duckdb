# DuckDB Hash Join — Deep Dive

This document covers DuckDB's `JoinHashTable` implementation end-to-end: the data structures, the build phase, the probe phase, chain following for duplicates, and a concrete step-by-step example.

---

## 1. Overview

DuckDB's hash join is a **linear probing hash table** (not chaining). It has two tiers of collision handling:

| Level | What it handles | Mechanism |
|-------|----------------|-----------|
| Different-key collision | Two different keys land on the same slot | Linear probing — displace to next slot |
| Duplicate-key collision | Multiple rows share the same join key | Embedded NEXT POINTER chain inside serialized rows |

The hash table (`entries[]`) is a flat array of `ht_entry_t` values. Each `ht_entry_t` packs a **16-bit salt** and a **48-bit pointer** into one 64-bit word. The pointer points to a serialized row in a separate row-oriented storage (`TupleDataCollection`). Each serialized row ends with a NEXT POINTER field that, when non-null, points to the next row with the same key.

---

## 2. Core Data Structures

### 2.1 `ht_entry_t` — The Hash Table Entry

**File:** `src/include/duckdb/execution/ht_entry.hpp`

```
┌──────────────────────────────────────────────────────────────────┐
│                         ht_entry_t (64 bits)                     │
├──────────────────────┬───────────────────────────────────────────┤
│  SALT (upper 16 b)   │          POINTER (lower 48 b)             │
│  bits 63..48         │          bits 47..0                       │
└──────────────────────┴───────────────────────────────────────────┘

SALT_MASK    = 0xFFFF000000000000
POINTER_MASK = 0x0000FFFFFFFFFFFF
```

Key methods:
- `IsOccupied()` — `value != 0`; an all-zero entry is empty
- `GetPointer()` — `value & POINTER_MASK` → dereferences to the row
- `GetSalt()` — `value | POINTER_MASK` → upper 16 bits with lower bits all-1; used for fast comparison
- `ExtractSalt(hash)` — `hash | POINTER_MASK` → same shape from a hash value for comparison
- `IncrementAndWrap(offset, mask)` — `++offset &= mask`; branchless wrap-around for linear probing

**Salt optimization:** Only enabled when `capacity > USE_SALT_THRESHOLD` (8 192 entries). For small tables the whole HT fits in L1/L2 cache, so the extra comparison is unnecessary.

---

### 2.2 Serialized Row Layout

Every build-side row is serialized into a contiguous byte region. The layout is:

```
┌─────────────────┬──────────────────┬──────────────┬──────────────────┐
│  key columns    │  payload columns │  found? bool │  NEXT POINTER    │
│  (eq. predicate)│  (build output)  │  (opt. FULL/ │  (hash_t / 8 B)  │
│                 │                  │   RIGHT join) │                  │
└─────────────────┴──────────────────┴──────────────┴──────────────────┘
 ← tuple_size ──────────────────────────────────────→ ← pointer_offset →
```

Important offsets stored in `JoinHashTable`:
- `tuple_size` — byte offset to the optional found-boolean (or the NEXT POINTER if no outer join flag)
- `pointer_offset` — byte offset to the NEXT POINTER field (always the very last field)
- `entry_size` — total row width in bytes

---

### 2.3 `ScanStructure` — Probe Cursor

`ScanStructure` holds all state needed to resume a probe across multiple output chunks:

| Field | Type | Purpose |
|-------|------|---------|
| `pointers` | `Vector<data_ptr_t>` | Current row pointers being examined |
| `count` | `idx_t` | Number of active pointers |
| `sel_vector` | `SelectionVector` | Indices of valid active pointers |
| `found_match[]` | `bool[]` | Per-probe-row: has any match been found yet? (for outer joins) |
| `chain_match_sel_vector` | `SelectionVector` | Indices that matched in the current chain position |
| `chain_no_match_sel_vector` | `SelectionVector` | Indices that did not match |

---

## 3. Build Phase

### Step 1 — Serialize and Hash (`Build`)

For each input chunk from the build side:

1. **Filter NULLs** — rows with NULL in any join key are excluded (`PrepareKeys`).
2. **Hash the keys** — `Hash(keys, sel, count, hash_values)` writes one hash per row.
3. **Append to row store** — `sink_collection->AppendUnified(...)` serializes each row (keys + payload + optional found-bool + placeholder for NEXT POINTER) into `TupleDataCollection`.

The hash value is embedded in the row at `pointer_offset` during serialization so `Finalize` can retrieve it cheaply.

---

### Step 2 — Insert into Hash Table (`Finalize` → `InsertHashesLoop`)

After all rows are collected, `Finalize` iterates over chunks:

```
for each serialized row:
    hash = Load<hash_t>(row + pointer_offset)   ← read hash stored in row
    InsertHashesLoop(hash, row_ptr)
```

`InsertHashesLoop` is the core insertion algorithm:

#### 2a. Compute initial slot and salt

```
ht_offset = hash & bitmask          ← lower bits → table index
salt      = ht_entry_t::ExtractSalt(hash)   ← upper 16 bits, lower bits all-1
```

#### 2b. Linear probe to find a slot

```
loop:
    entry = entries[ht_offset]
    if !entry.IsOccupied():
        break                        ← found an empty slot
    if entry.GetSalt() == salt:
        break                        ← same salt → need key comparison
    IncrementAndWrap(ht_offset, mask) ← different salt → displace to next slot
```

#### 2c. Insert into the found slot

**Empty slot** — Atomic CAS:
```
StorePointer(nullptr, row + pointer_offset)   ← mark end of chain
entries[ht_offset].compare_exchange(expected=empty, desired={salt, row_ptr})
```
If another thread won the CAS (parallel build), this row falls through to the key-comparison path.

**Occupied slot (salt matched)** — Key comparison:
```
gather keys from existing row at entries[ht_offset].GetPointer()
compare against the row being inserted

if keys MATCH (true duplicate):
    ht.chains_longer_than_one = true
    StorePointer(current_head, row + pointer_offset)  ← new row points to old head
    entries[ht_offset] = {salt, new_row_ptr}           ← new row becomes new head
    → NEXT POINTER chain now has length 2+

if keys DO NOT MATCH (hash collision, different key):
    IncrementAndWrap(ht_offset, mask)   ← linear probe further
    retry with new slot
```

---

### Build Phase Summary: What the Table Looks Like After Build

```
entries[] (the pointer table — one ht_entry_t per slot):
┌─────┬─────────────────────────────────────────────┐
│ idx │ ht_entry_t value                            │
├─────┼─────────────────────────────────────────────┤
│  0  │ 0 (empty)                                   │
│  1  │ salt_A | ptr_to_ROW_A                       │  ← single row, no chain
│  2  │ 0 (empty)                                   │
│  3  │ salt_B | ptr_to_ROW_C                       │  ← chain head (ROW_C is newest)
│  4  │ salt_C | ptr_to_ROW_X                       │  ← displaced from collision at 3
│ ... │ ...                                         │
└─────┴─────────────────────────────────────────────┘

Duplicate-key chain at slot 3:
ROW_C  [key=42][payload=...][NEXT_PTR → ROW_B]
ROW_B  [key=42][payload=...][NEXT_PTR → ROW_A]
ROW_A  [key=42][payload=...][NEXT_PTR → nullptr]
```

---

## 4. Probe Phase

### Step 1 — Initialize (`Probe`)

For each probe chunk:

1. Evaluate join keys from the probe side → `lhs_join_keys`.
2. Hash them → `hashes`.
3. Call `GetRowPointers(...)` — this runs linear probing to find the first matching row for each probe key and populates `scan_structure.pointers[]`.

---

### Step 2 — Linear Probing to Find Initial Pointers (`ProbeForPointersInternal`)

For each probe key:

```
row_hash     = hashes[i]
ht_offset    = row_hash & bitmask          ← start slot

if USE_SALTS:
    loop:
        entry = entries[ht_offset]
        if !entry.IsOccupied():
            break                          ← no match possible
        row_salt = ht_entry_t::ExtractSalt(row_hash)
        if entry.GetSalt() == row_salt:
            → candidate found, add to comparison list
            break
        IncrementAndWrap(ht_offset, mask)  ← linear probe on salt mismatch
else:
    entry = entries[ht_offset]
    if entry.IsOccupied():
        → candidate found
```

For the candidates (salt match), actual **key comparison** is done next:

```
row_matcher.Match(probe_keys, existing_row_keys) →
    key_match_sel    ← indices that truly matched
    key_no_match_sel ← indices that had a false salt match (different key, same salt)
```

For key mismatches (false salt matches): `IncrementAndWrap` and re-probe. This loop repeats until all non-matching probes find an empty slot or a true match.

After this step, `scan_structure.pointers[i]` points to the **first** row that genuinely matches probe key `i` (or is null if no match).

---

### Step 3 — Emit Results and Follow Chains (`NextInnerJoin` / `ScanInnerJoin`)

Results may span multiple output vectors because one probe key can match many build rows (duplicate chain). `ScanStructure` acts as a cursor:

```
while scan_structure.count > 0:
    result_count = ResolvePredicates(keys, probe_data, result)
    if result_count > 0:
        emit result rows
    AdvancePointers()   ← follow NEXT POINTER in each matched row
```

`AdvancePointers`:
```
for each active pointer i:
    ptrs[i] = LoadPointer(ptrs[i] + pointer_offset)   ← read NEXT POINTER from row
    if ptrs[i] != nullptr:
        keep i in sel_vector
    else:
        remove i (chain exhausted for this probe key)
```

**Fast path:** If `!ht.chains_longer_than_one` (set only when a true duplicate was inserted), `AdvancePointers` immediately sets `count = 0` — no chain following needed at all.

---

### Probe Phase Summary

```
Probe key k=42:
    hash(42) → ht_offset = 3, salt = S
    entries[3].GetSalt() == S  → candidate
    key comparison: ROW_C.key == 42  → MATCH
    scan_structure.pointers[i] = ptr_to_ROW_C

    emit row (probe_row_i JOIN ROW_C)
    AdvancePointers: ptrs[i] = ROW_C.NEXT_PTR = ptr_to_ROW_B

    emit row (probe_row_i JOIN ROW_B)
    AdvancePointers: ptrs[i] = ROW_B.NEXT_PTR = ptr_to_ROW_A

    emit row (probe_row_i JOIN ROW_A)
    AdvancePointers: ptrs[i] = ROW_A.NEXT_PTR = nullptr → remove from active set
```

---

## 5. Concrete Step-by-Step Example

### Query

```sql
SELECT o.order_id, o.amount, c.name, c.city
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
```

**Build side: `customers`** (5 rows)

| customer_id | name    | city      |
|-------------|---------|-----------|
| 10          | Alice   | Berlin    |
| 20          | Bob     | Paris     |
| 30          | Charlie | London    |
| 40          | Diana   | Berlin    |
| 50          | Eve     | Tokyo     |

**Probe side: `orders`** (7 rows, two probe chunks shown below)

---

### Phase 1 — Build

#### 1a. Allocate hash table

The planner chooses `customers` as the build side (smaller). Suppose `capacity = 8` (next power-of-two ≥ 5 × load-factor). The `bitmask = 7` (capacity − 1).

```
entries[0..7] = all empty (value = 0)
```

#### 1b. Serialize and hash

Assume these hashes (lower 3 bits determine slot, upper bits form salt):

| customer_id | hash (hex)           | ht_offset (hash & 7) | salt (upper 16 b, lower=1) |
|-------------|----------------------|----------------------|----------------------------|
| 10          | 0xABCD000000000003   | 3                    | 0xABCDFFFFFFFFFFFF         |
| 20          | 0x1111000000000005   | 5                    | 0x1111FFFFFFFFFFFF         |
| 30          | 0x2222000000000001   | 1                    | 0x2222FFFFFFFFFFFF         |
| 40          | 0x3333000000000003   | **3** ← collision!   | 0x3333FFFFFFFFFFFF         |
| 50          | 0x4444000000000006   | 6                    | 0x4444FFFFFFFFFFFF         |

#### 1c. Insert each row

**Insert customer_id=10 (hash → slot 3, salt=0xABCD...):**
```
entries[3] is empty → insert directly
StorePointer(nullptr, ROW_10 + pointer_offset)   ← chain end
entries[3] = {salt=0xABCD..., ptr=ROW_10}
```

**Insert customer_id=20 (hash → slot 5):**
```
entries[5] is empty → insert
entries[5] = {salt=0x1111..., ptr=ROW_20}
```

**Insert customer_id=30 (hash → slot 1):**
```
entries[1] is empty → insert
entries[1] = {salt=0x2222..., ptr=ROW_30}
```

**Insert customer_id=40 (hash → slot 3, salt=0x3333...) — COLLISION:**
```
entries[3] is occupied, salt=0xABCD... ≠ 0x3333... → different key collision
IncrementAndWrap(3, 7) → ht_offset = 4
entries[4] is empty → insert
StorePointer(nullptr, ROW_40 + pointer_offset)
entries[4] = {salt=0x3333..., ptr=ROW_40}
```
> customer_id=40 was displaced to slot 4 by linear probing.

**Insert customer_id=50 (hash → slot 6):**
```
entries[6] is empty → insert
entries[6] = {salt=0x4444..., ptr=ROW_50}
```

#### Final hash table after build:

```
slot │ ht_entry_t                                      │ Notes
─────┼─────────────────────────────────────────────────┼────────────────────────
  0  │ empty                                           │
  1  │ {salt=0x2222..., ptr=ROW_30}                   │ customer_id=30
  2  │ empty                                           │
  3  │ {salt=0xABCD..., ptr=ROW_10}                   │ customer_id=10
  4  │ {salt=0x3333..., ptr=ROW_40}                   │ customer_id=40 (displaced)
  5  │ {salt=0x1111..., ptr=ROW_20}                   │ customer_id=20
  6  │ {salt=0x4444..., ptr=ROW_50}                   │ customer_id=50
  7  │ empty                                           │
```

Serialized rows in TupleDataCollection:
```
ROW_10: [cust_id=10][name="Alice" ][city="Berlin"][NEXT_PTR=nullptr]
ROW_20: [cust_id=20][name="Bob"   ][city="Paris" ][NEXT_PTR=nullptr]
ROW_30: [cust_id=30][name="Charlie"][city="London"][NEXT_PTR=nullptr]
ROW_40: [cust_id=40][name="Diana" ][city="Berlin"][NEXT_PTR=nullptr]
ROW_50: [cust_id=50][name="Eve"   ][city="Tokyo" ][NEXT_PTR=nullptr]
```

No duplicate keys → `chains_longer_than_one = false`. `AdvancePointers` will use the fast path.

---

### Phase 2 — Probe, Chunk A

**Probe Chunk A (orders):**

| i | order_id | customer_id | amount |
|---|----------|-------------|--------|
| 0 | 1001     | 30          | 500    |
| 1 | 1002     | 10          | 200    |
| 2 | 1003     | 99          | 900    |  ← no matching customer
| 3 | 1004     | 40          | 350    |
| 4 | 1005     | 20          | 150    |

#### 2a. Hash probe keys

| i | customer_id | hash               | ht_offset | salt               |
|---|-------------|--------------------|-----------|--------------------|
| 0 | 30          | 0x2222...0001      | 1         | 0x2222FFFFFFFFFFFF |
| 1 | 10          | 0xABCD...0003      | 3         | 0xABCDFFFFFFFFFFFF |
| 2 | 99          | 0x9999...0002      | 2         | 0x9999FFFFFFFFFFFF |
| 3 | 40          | 0x3333...0003      | **3**     | 0x3333FFFFFFFFFFFF |
| 4 | 20          | 0x1111...0005      | 5         | 0x1111FFFFFFFFFFFF |

#### 2b. Linear probe to find initial pointers

**i=0, customer_id=30 → slot 1, salt=0x2222...:**
```
entries[1] = {salt=0x2222..., ptr=ROW_30}
GetSalt() == row_salt → candidate
Key compare: ROW_30.cust_id(30) == probe(30) → MATCH
scan_structure.pointers[0] = ROW_30
```

**i=1, customer_id=10 → slot 3, salt=0xABCD...:**
```
entries[3] = {salt=0xABCD..., ptr=ROW_10}
GetSalt() == row_salt → candidate
Key compare: ROW_10.cust_id(10) == probe(10) → MATCH
scan_structure.pointers[1] = ROW_10
```

**i=2, customer_id=99 → slot 2, salt=0x9999...:**
```
entries[2] = empty → !IsOccupied() → no match possible
scan_structure.pointers[2] = nullptr (excluded from active set)
```

**i=3, customer_id=40 → slot 3, salt=0x3333...:**
```
entries[3] = {salt=0xABCD..., ptr=ROW_10}
GetSalt() = 0xABCDFFFF... ≠ row_salt=0x3333FFFF... → SALT MISMATCH
IncrementAndWrap(3, 7) → slot 4

entries[4] = {salt=0x3333..., ptr=ROW_40}
GetSalt() == row_salt → candidate
Key compare: ROW_40.cust_id(40) == probe(40) → MATCH
scan_structure.pointers[3] = ROW_40
```
> probe key i=3 required 2 slot checks because customer_id=40 was displaced during build.

**i=4, customer_id=20 → slot 5, salt=0x1111...:**
```
entries[5] = {salt=0x1111..., ptr=ROW_20}
GetSalt() == row_salt → MATCH
scan_structure.pointers[4] = ROW_20
```

#### 2c. Active scan_structure after initial probe

```
count = 4 (i=2 excluded — no match)
sel_vector = [0, 1, 3, 4]
pointers   = [ROW_30, ROW_10, ROW_40, ROW_20]
```

#### 2d. Emit results and advance chains

Since `chains_longer_than_one = false`, `AdvancePointers` sets `count = 0` immediately after the first result batch. One result row per match:

| order_id | amount | name    | city    |
|----------|--------|---------|---------|
| 1001     | 500    | Charlie | London  |
| 1002     | 200    | Alice   | Berlin  |
| 1004     | 350    | Diana   | Berlin  |
| 1005     | 150    | Bob     | Paris   |

Row i=2 (order_id=1003, customer_id=99) is silently dropped — no match.

---

### Phase 3 — Build with Duplicate Keys (Extended Example)

Now suppose `customers` had two rows with `customer_id = 10`:

| customer_id | name      | city    |
|-------------|-----------|---------|
| 10          | Alice     | Berlin  |
| 10          | Alice-EU  | Vienna  |  ← duplicate!

**Insert Alice (first):**
```
entries[3] is empty → insert
StorePointer(nullptr, ROW_Alice + pointer_offset)
entries[3] = {salt=0xABCD..., ptr=ROW_Alice}
chains_longer_than_one = false
```

**Insert Alice-EU (second, same key=10, same slot=3, same salt):**
```
entries[3] is occupied, GetSalt() == row_salt=0xABCD... → compare keys
Key compare: ROW_Alice.cust_id(10) == inserting.cust_id(10) → DUPLICATE

ht.chains_longer_than_one = true
StorePointer(ROW_Alice, ROW_Alice_EU + pointer_offset)   ← Alice-EU.NEXT → Alice
entries[3] = {salt=0xABCD..., ptr=ROW_Alice_EU}          ← Alice-EU is new head
```

Chain at slot 3:
```
entries[3] → ROW_Alice_EU → [NEXT_PTR → ROW_Alice → [NEXT_PTR → nullptr]]
```

**Probing customer_id=10 against this chain:**
```
First match:  ROW_Alice_EU → emit (order JOIN Alice-EU)
AdvancePointers: ptr = ROW_Alice_EU.NEXT_PTR = ROW_Alice
Second match: ROW_Alice    → emit (order JOIN Alice)
AdvancePointers: ptr = ROW_Alice.NEXT_PTR = nullptr → remove from active set
```

One probe row for customer_id=10 produces **two** result rows (one per duplicate).

---

## 6. Probe Chunk B — Revisiting the Displaced Key

To reinforce linear probing on the probe side: suppose Probe Chunk B contains only `customer_id = 40`.

```
hash(40) = 0x3333...0003 → ht_offset = 3, salt = 0x3333FFFFFFFFFFFF

entries[3]: salt = 0xABCD... ≠ 0x3333... → SALT MISMATCH → IncrementAndWrap → slot 4
entries[4]: salt = 0x3333... == 0x3333... → candidate
key compare: ROW_40.cust_id(40) == probe(40) → MATCH
```

The probe side has to follow the **same displacement path** as the build side did. This is guaranteed to converge — the row was inserted at slot 4 during build, and the probe will always reach slot 4 by linear probing from slot 3.

---

## 7. Salt Threshold and the `USE_SALT_THRESHOLD` Optimization

```cpp
static constexpr const idx_t USE_SALT_THRESHOLD = 8192;

inline bool JoinHashTable::UseSalt() const {
    return this->capacity > USE_SALT_THRESHOLD;
}
```

For small hash tables (≤ 8 192 slots), the entire `entries[]` array fits in L1/L2 cache. In this case:

- The salt check `entry.GetSalt() == row_salt` is skipped.
- Every occupied slot triggers a full key comparison.
- This is actually faster because the full key is immediately in cache — the salt comparison would add an extra operation with no cache-miss benefit.

For large tables (> 8 192 slots), the salt stored in the upper 16 bits of `ht_entry_t` acts as a bloom filter: if the salt doesn't match, the pointer (lower 48 bits) is never dereferenced — saving a cache miss into the row data.

---

## 8. Outer Join Handling — The Found Boolean

For `FULL OUTER JOIN` and `RIGHT OUTER JOIN`, the serialized row layout includes an extra `bool found` field at offset `tuple_size`:

```
[key columns][payload columns][found: bool = false][NEXT POINTER]
```

During probe, every time a build row is part of a match:
```cpp
Store<bool>(true, ptrs[idx] + ht.tuple_size);   // mark this build row as matched
```

After probing all probe chunks is complete, a final scan of the build-side `TupleDataCollection` emits all rows where `found == false` — these are the unmatched build rows that need a NULL-filled probe side in the output.

---

## 9. Execution Flow in `PhysicalHashJoin`

```
PhysicalHashJoin::Sink (build side):
    for each build chunk:
        JoinHashTable::Build(keys, payload)   ← serialize + hash

PhysicalHashJoin::Finalize:
    JoinHashTable::Finalize()                 ← insert into entries[]
    PerfectHashJoinExecutor::Build()          ← attempt perfect hash join (if eligible)

PhysicalHashJoin::ExecuteInternal (probe side):
    for each probe chunk:
        if scan_structure.is_null:
            evaluate probe keys
            JoinHashTable::Probe(scan_structure, keys)   ← GetRowPointers

        scan_structure.Next(keys, probe_data, result)    ← emit result rows

        if scan_structure.PointersExhausted() && result.size() == 0:
            return NEED_MORE_INPUT   ← done with this probe chunk
        else:
            return HAVE_MORE_OUTPUT  ← more result rows pending (long chains)
```

The `HAVE_MORE_OUTPUT` / `NEED_MORE_INPUT` protocol handles the case where a single probe chunk produces more than `STANDARD_VECTOR_SIZE` (2048) result rows due to long duplicate chains.

---

## 10. Performance Properties

| Property | Value |
|----------|-------|
| Hash function | MurmurHash / DuckDB custom |
| Collision resolution (different keys) | Open addressing (linear probing) |
| Collision resolution (same keys) | Embedded linked list (NEXT POINTER) |
| Load factor | ≤ 0.5 (capacity is always 2× row count) |
| Salt filter | Enabled for capacity > 8 192 |
| Chain fast path | If `!chains_longer_than_one`, `AdvancePointers` is O(1) |
| Parallelism | Parallel build via atomic CAS on `ht_entry_t` |
| Memory layout | Row-oriented (`TupleDataCollection`); entries[] is separate flat array |

---

## 11. Summary: Full Algorithm at a Glance

### Build
```
1. For each build chunk:
   a. Filter NULLs in join keys
   b. Hash join keys → embed hash at pointer_offset in each row
   c. Serialize row (keys + payload + [found_bool] + hash) → TupleDataCollection

2. Finalize:
   for each row in TupleDataCollection:
       hash     = row[pointer_offset]
       offset   = hash & bitmask
       salt     = ExtractSalt(hash)

       loop (linear probe):
           if entries[offset] empty:
               row[pointer_offset] = nullptr          ← chain end
               entries[offset] = {salt, row_ptr}
               break
           if entries[offset].salt == salt:
               compare keys with existing row
               if match (duplicate):
                   row[pointer_offset] = existing_head  ← chain prepend
                   entries[offset] = {salt, row_ptr}
                   chains_longer_than_one = true
                   break
               else (false salt match):
                   IncrementAndWrap(offset, mask)      ← linear probe
           else:
               IncrementAndWrap(offset, mask)          ← linear probe
```

### Probe
```
For each probe chunk:
    hash probe keys → {ht_offset, salt} per key

    GetRowPointers:
        for each key:
            loop (linear probe):
                if entries[offset] empty: no match
                if salt matches:
                    compare keys
                    if match: record pointer → break
                    else (false salt): IncrementAndWrap → continue
                else: IncrementAndWrap → continue

    while scan_structure.count > 0:
        ResolvePredicates → emit result rows
        AdvancePointers:   ptrs[i] = row[i][pointer_offset]
                           if null: remove from active set
```
