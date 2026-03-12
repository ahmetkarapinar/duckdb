# Project 1 — Small Build Side Dictionary Emission: Implementation Roadmap

---

## Running Example

Throughout this roadmap we trace the same example from `project1.md`:

**Build side: `customers` (6 rows)**

| customer_id | name     | city    | tier    |
|-------------|----------|---------|---------|
| C010        | Alice    | Berlin  | Gold    |
| C010        | Alice-EU | Vienna  | Silver  |
| C020        | Bob      | Paris   | Bronze  |
| C030        | Charlie  | London  | Gold    |
| C040        | Diana    | Berlin  | Silver  |
| C050        | Eve      | Tokyo   | Bronze  |

**Probe side: one chunk of `orders`**

| i | order_id | customer_id | amount |
|---|----------|-------------|--------|
| 0 | 1001     | C030        | 500    |
| 1 | 1002     | C010        | 200    |
| 2 | 1003     | C999        | 900    |
| 3 | 1004     | C040        | 350    |

**Query:**
```sql
SELECT o.order_id, o.amount, c.name, c.city, c.tier
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
```

After normal build + Finalize, the TupleDataCollection contains ROW_0..ROW_5.
The hash table `entries[]` has ROW_1 as chain head for key `C010` with `ROW_1.NEXT_PTR → ROW_0`.

---

## Phase 1 — New Data Structures in JoinHashTable

### What to do

Add new member variables to `JoinHashTable` that hold:
1. A flag indicating whether dictionary emission is active
2. The dictionary arrays (one `VectorChildBuffer` per output column, same type as PHJ's `perfect_hash_table`)
3. A row-pointer → dictionary-index mapping (an `unordered_map<data_ptr_t, idx_t>`)

### Where to modify

**File: `src/include/duckdb/execution/join_hashtable.hpp`**

Add new members after the existing public members (after line ~314, near `lhs_output_in_probe`):

```cpp
//===--------------------------------------------------------------------===//
// Small Build Side Dictionary Emission (Project 1)
//===--------------------------------------------------------------------===//
//! Whether dictionary emission is active for this hash table
bool use_dict_emission = false;
//! Dictionary arrays — one VectorChildBuffer per output column, with UUID assigned
//! Indexed in the same order as output_columns
vector<buffer_ptr<VectorChildBuffer>> dict_arrays;
//! Mapping from row pointer (data_ptr_t) in TupleDataCollection to dictionary index
unordered_map<data_ptr_t, idx_t> ptr_to_dict_idx;
```

### Why these types

- `use_dict_emission`: Simple boolean checked during probe. When `false`, the regular `GatherResult` path runs unchanged.
- `dict_arrays`: Exactly mirrors `PerfectHashJoinExecutor::perfect_hash_table` (type alias `vector<buffer_ptr<VectorChildBuffer>>`). Each entry is created via `DictionaryVector::CreateReusableDictionary(type, build_row_count)`, which assigns a UUID and allocates a flat columnar vector.
- `ptr_to_dict_idx`: During probe, `ScanStructure::pointers` contains `data_ptr_t` values pointing into the TupleDataCollection. This map converts each pointer to the corresponding dictionary index in O(1).

### Data structures after Phase 1 (not yet populated)

```
JoinHashTable:
  use_dict_emission = false        (set to true in Phase 2)
  dict_arrays = []                 (populated in Phase 3)
  ptr_to_dict_idx = {}             (populated in Phase 3)

  // Existing members unchanged:
  data_collection = <TupleDataCollection with ROW_0..ROW_5>
  entries[] = <hash table built by Finalize>
  chains_longer_than_one = true    (because C010 has 2 rows)
```

### Required includes

In `join_hashtable.hpp`, add:
```cpp
#include <unordered_map>
```

---

## Phase 2 — Post-Finalize Trigger in PhysicalHashJoin::Finalize

### What to do

After the existing PHJ decision point in `Finalize()`, add a new check: if PHJ was not used AND the build side is small AND the join is not external (not spilling), trigger dictionary emission by calling a new method `BuildDictionaryArrays()` on the hash table.

### Where to modify

**File: `src/execution/operator/join/physical_hash_join.cpp`**
**Function: `PhysicalHashJoin::Finalize` (line 1044)**

The insertion point is at **line 1140**, right where the code currently handles the PHJ fallback. The new check goes inside the `if (!use_perfect_hash)` block, before `sink.ScheduleFinalize(pipeline, event)`.

### Threshold definition

**File: `src/include/duckdb/execution/join_hashtable.hpp`**

Add a constant near the top of the `JoinHashTable` class (after the existing `EXTERNAL_LOAD_FACTOR` constant):

```cpp
//! Maximum build-side row count to enable dictionary emission
static constexpr idx_t DICT_EMISSION_MAX_ROWS = 1048576; // 2^20, same as PHJ's range limit
```

### Modified code in Finalize

Replace the block at lines 1139-1143 of `physical_hash_join.cpp`:

```cpp
// In case of a large build side or duplicates, use regular hash join
if (!use_perfect_hash) {
    sink.perfect_join_executor.reset();
    sink.ScheduleFinalize(pipeline, event);
}
```

With:

```cpp
// In case of a large build side or duplicates, use regular hash join
if (!use_perfect_hash) {
    sink.perfect_join_executor.reset();

    // Project 1: Check if we can use dictionary emission for a small build side
    // Conditions: not external (no spill), build side small enough
    if (!sink.external && ht.Count() > 0 &&
        ht.Count() <= JoinHashTable::DICT_EMISSION_MAX_ROWS) {
        ht.BuildDictionaryArrays(*this);
    }

    sink.ScheduleFinalize(pipeline, event);
}
```

### Interaction with spill detection

- `sink.external` is set at line 1050 (`sink.external = sink.temporary_memory_state->GetReservation() < sink.total_size`).
- If `sink.external == true`, we skip dictionary emission entirely (the external hash join path at line 1074 already resets `perfect_join_executor` and returns before reaching this code).
- The check `!sink.external` at line 1140 ensures we never attempt dictionary emission when data spills to disk.
- `ScheduleFinalize` still runs after `BuildDictionaryArrays` — the regular hash table finalization (pointer table construction) is unchanged and still needed for probe.

### Interaction with PHJ

- If PHJ succeeds (`use_perfect_hash == true`), we never enter the `if (!use_perfect_hash)` block, so dictionary emission is not attempted. PHJ and dictionary emission are mutually exclusive — PHJ is strictly better when applicable.
- If PHJ fails (non-integer keys, duplicates, range too large), we fall through to the dictionary emission check.

### Data structures after Phase 2 (for our example)

```
ht.Count() = 6  ≤  1,048,576  → trigger BuildDictionaryArrays()
sink.external = false          → not spilling

// After BuildDictionaryArrays() returns (Phase 3 fills in the details):
JoinHashTable:
  use_dict_emission = true
  dict_arrays = [name_dict, city_dict, tier_dict]  (populated in Phase 3)
  ptr_to_dict_idx = { ptr(ROW_0)→0, ptr(ROW_1)→1, ..., ptr(ROW_5)→5 }
```

---

## Phase 3 — Scanning TupleDataCollection to Build Dictionary Arrays

### What to do

Add a new method `BuildDictionaryArrays()` to `JoinHashTable`. This method:
1. Scans the `TupleDataCollection` to get all row pointers
2. Creates one `VectorChildBuffer` per output column via `CreateReusableDictionary`
3. Gathers columnar data from the row-format storage into those buffers
4. Populates `ptr_to_dict_idx` mapping each row pointer to its sequential index

### Where to add

**File: `src/include/duckdb/execution/join_hashtable.hpp`**

Declare the new method in the public section (after line ~222, near `FillWithHTOffsets`):

```cpp
//! Build dictionary arrays for small build side emission (Project 1)
void BuildDictionaryArrays(const PhysicalHashJoin &op);
```

**File: `src/execution/join_hashtable.cpp`**

Add the implementation. The required header for the `PhysicalHashJoin` forward reference:

```cpp
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
```

### Implementation

```cpp
void JoinHashTable::BuildDictionaryArrays(const PhysicalHashJoin &op) {
	auto &dc = *data_collection;
	const auto build_count = dc.Count();
	if (build_count == 0) {
		return;
	}

	// Step 1: Scan all row locations from the TupleDataCollection
	Vector row_locations(LogicalType::POINTER, build_count);
	auto row_ptrs = FlatVector::GetData<data_ptr_t>(row_locations);

	JoinHTScanState scan_state(dc, 0, dc.ChunkCount(),
	                           TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	idx_t total = 0;
	auto &iterator = scan_state.iterator;
	const auto loc = iterator.GetRowLocations();
	do {
		const auto count = iterator.GetCurrentChunkCount();
		for (idx_t i = 0; i < count; i++) {
			row_ptrs[total + i] = loc[i];
		}
		total += count;
	} while (iterator.Next());
	D_ASSERT(total == build_count);

	// Step 2: Build the ptr → index mapping
	ptr_to_dict_idx.reserve(build_count);
	for (idx_t i = 0; i < build_count; i++) {
		ptr_to_dict_idx[row_ptrs[i]] = i;
	}

	// Step 3: Create dictionary arrays — one per output column
	const auto &sel = *FlatVector::IncrementalSelectionVector();
	for (idx_t i = 0; i < op.rhs_output_columns.col_types.size(); i++) {
		const auto &type = op.rhs_output_columns.col_types[i];
		auto dict_buf = DictionaryVector::CreateReusableDictionary(type, build_count);

		// Gather column data from row-format into the columnar dictionary vector
		auto &vec = dict_buf->data;
		const auto output_col_idx = output_columns[i];
		D_ASSERT(vec.GetType() == layout_ptr->GetTypes()[output_col_idx]);
		dc.Gather(row_locations, sel, build_count, output_col_idx, vec, sel, nullptr);

		dict_arrays.emplace_back(std::move(dict_buf));
	}

	use_dict_emission = true;
}
```

### Step-by-step trace with the example

**Step 1 — Scan row locations:**

The `TupleDataChunkIterator` iterates through all chunks in the collection. Each `row_locations[i]` is a `data_ptr_t` pointing to the start of `ROW_i` in the TupleDataCollection's memory.

```
row_ptrs[0] = ptr(ROW_0)   →  [C010 | Alice    | Berlin | Gold   | NEXT=null]
row_ptrs[1] = ptr(ROW_1)   →  [C010 | Alice-EU | Vienna | Silver | NEXT=ROW_0]
row_ptrs[2] = ptr(ROW_2)   →  [C020 | Bob      | Paris  | Bronze | NEXT=null]
row_ptrs[3] = ptr(ROW_3)   →  [C030 | Charlie  | London | Gold   | NEXT=null]
row_ptrs[4] = ptr(ROW_4)   →  [C040 | Diana    | Berlin | Silver | NEXT=null]
row_ptrs[5] = ptr(ROW_5)   →  [C050 | Eve      | Tokyo  | Bronze | NEXT=null]
```

**Step 2 — Build ptr→index map:**

```
ptr_to_dict_idx = {
    ptr(ROW_0) → 0,
    ptr(ROW_1) → 1,
    ptr(ROW_2) → 2,
    ptr(ROW_3) → 3,
    ptr(ROW_4) → 4,
    ptr(ROW_5) → 5
}
```

**Step 3 — Create dictionary arrays:**

For each output column (`name`, `city`, `tier`), `CreateReusableDictionary(type, 6)` allocates a flat `Vector` of capacity 6 and assigns a UUID.

Then `Gather()` reads column data from each row pointer into the vector. With `sel = IncrementalSelectionVector` (identity mapping), the gather is straightforward: row i goes to position i.

```
dict_arrays[0] (name, id="uuid-name-AAA"):
  index:  0         1           2       3          4        5
  value: ["Alice", "Alice-EU", "Bob", "Charlie", "Diana", "Eve"]

dict_arrays[1] (city, id="uuid-city-BBB"):
  index:  0         1         2        3         4         5
  value: ["Berlin", "Vienna", "Paris", "London", "Berlin", "Tokyo"]

dict_arrays[2] (tier, id="uuid-tier-CCC"):
  index:  0       1         2         3       4         5
  value: ["Gold", "Silver", "Bronze", "Gold", "Silver", "Bronze"]
```

Each `VectorChildBuffer` has:
- `data`: the flat `Vector` with the columnar values
- `size`: `6` (set by `CreateReusableDictionary`)
- `id`: a unique UUID string (generated by `UUID::GenerateRandomUUID()`)

**Final state after Phase 3:**

```
JoinHashTable:
  use_dict_emission = true
  dict_arrays = [name_buf, city_buf, tier_buf]     ← VectorChildBuffers with UUIDs
  ptr_to_dict_idx = { ptr(ROW_0)→0, ..., ptr(ROW_5)→5 }

  // Existing state unchanged:
  data_collection = <ROW_0..ROW_5 in row format>
  entries[] = <hash table with pointer chains>
  chains_longer_than_one = true
```

---

## Phase 4 — Modified Emit in ScanStructure

### What to do

Modify `NextInnerJoin` (and its two code paths — fast and slow) so that when `ht.use_dict_emission == true`, instead of calling `GatherResult(...)` to copy build-side row data, we:
1. Look up each matched row pointer in `ht.ptr_to_dict_idx` to get the dictionary index
2. Assemble a `build_sel_vec` containing those indices
3. Call `result_vector.Dictionary(ht.dict_arrays[col], build_sel_vec)` for each output column

### Which functions change

**File: `src/execution/join_hashtable.cpp`**

#### 4a. `ScanStructure::NextInnerJoin` — fast path (lines 1130-1150)

The fast path fires when `!ht.chains_longer_than_one` (no chain has more than one element). In this case, each matched probe row has exactly one build-side match, and `chain_match_sel_vector` directly indexes into the `pointers` vector.

**Current code (lines 1130-1150):**
```cpp
if (ht.join_type != JoinType::RIGHT_SEMI && ht.join_type != JoinType::RIGHT_ANTI) {
    // fast path: no chains longer than one
    if (!ht.chains_longer_than_one) {
        for (idx_t i = 0; i < ht.lhs_output_in_probe.size(); i++) {
            idx_t probe_col_idx = ht.lhs_output_in_probe[i];
            result.data[i].Slice(probe_data.data[probe_col_idx], chain_match_sel_vector, result_count);
        }
        result.SetCardinality(result_count);

        for (idx_t i = 0; i < ht.output_columns.size(); i++) {
            auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
            const auto output_col_idx = ht.output_columns[i];
            D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
            GatherResult(vector, chain_match_sel_vector, result_count, output_col_idx);
        }

        AdvancePointers();
        return;
    }
    // ...
```

**Modified code:**
```cpp
if (ht.join_type != JoinType::RIGHT_SEMI && ht.join_type != JoinType::RIGHT_ANTI) {
    // fast path: no chains longer than one
    if (!ht.chains_longer_than_one) {
        for (idx_t i = 0; i < ht.lhs_output_in_probe.size(); i++) {
            idx_t probe_col_idx = ht.lhs_output_in_probe[i];
            result.data[i].Slice(probe_data.data[probe_col_idx], chain_match_sel_vector, result_count);
        }
        result.SetCardinality(result_count);

        if (ht.use_dict_emission) {
            // Project 1: build selection vector of dictionary indices from row pointers
            SelectionVector build_sel_vec(result_count);
            auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
            for (idx_t i = 0; i < result_count; i++) {
                auto idx = chain_match_sel_vector.get_index(i);
                auto it = ht.ptr_to_dict_idx.find(ptrs[idx]);
                D_ASSERT(it != ht.ptr_to_dict_idx.end());
                build_sel_vec.set_index(i, it->second);
            }
            // Emit dictionary vectors instead of gathered flat vectors
            for (idx_t i = 0; i < ht.output_columns.size(); i++) {
                auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
                D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[ht.output_columns[i]]);
                vector.Dictionary(ht.dict_arrays[i], build_sel_vec);
            }
        } else {
            for (idx_t i = 0; i < ht.output_columns.size(); i++) {
                auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
                const auto output_col_idx = ht.output_columns[i];
                D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
                GatherResult(vector, chain_match_sel_vector, result_count, output_col_idx);
            }
        }

        AdvancePointers();
        return;
    }
    // ...
```

#### 4b. `ScanStructure::NextInnerJoin` — slow path (lines 1152-1175)

The slow path fires when `chains_longer_than_one == true`. Multiple chain elements can match a single probe row. Results are buffered via `UpdateCompactionBuffer` which stores `lhs_sel_vector` (probe indices) and copies matched row pointers into `rhs_pointers`.

For dictionary emission, we need to convert the buffered `rhs_pointers` to dictionary indices after the loop completes.

**Current code (lines 1160-1175):**
```cpp
if (base_count > 0) {
    for (idx_t i = 0; i < ht.lhs_output_in_probe.size(); i++) {
        idx_t probe_col_idx = ht.lhs_output_in_probe[i];
        result.data[i].Slice(probe_data.data[probe_col_idx], lhs_sel_vector, base_count);
    }
    result.SetCardinality(base_count);

    for (idx_t i = 0; i < ht.output_columns.size(); i++) {
        auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
        const auto output_col_idx = ht.output_columns[i];
        D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
        GatherResult(vector, base_count, output_col_idx);
    }
}
```

**Modified code:**
```cpp
if (base_count > 0) {
    for (idx_t i = 0; i < ht.lhs_output_in_probe.size(); i++) {
        idx_t probe_col_idx = ht.lhs_output_in_probe[i];
        result.data[i].Slice(probe_data.data[probe_col_idx], lhs_sel_vector, base_count);
    }
    result.SetCardinality(base_count);

    if (ht.use_dict_emission) {
        // Project 1: convert buffered rhs_pointers to dictionary indices
        SelectionVector build_sel_vec(base_count);
        auto rhs_ptrs = FlatVector::GetData<data_ptr_t>(rhs_pointers);
        for (idx_t i = 0; i < base_count; i++) {
            auto it = ht.ptr_to_dict_idx.find(rhs_ptrs[i]);
            D_ASSERT(it != ht.ptr_to_dict_idx.end());
            build_sel_vec.set_index(i, it->second);
        }
        for (idx_t i = 0; i < ht.output_columns.size(); i++) {
            auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
            D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[ht.output_columns[i]]);
            vector.Dictionary(ht.dict_arrays[i], build_sel_vec);
        }
    } else {
        for (idx_t i = 0; i < ht.output_columns.size(); i++) {
            auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
            const auto output_col_idx = ht.output_columns[i];
            D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
            GatherResult(vector, base_count, output_col_idx);
        }
    }
}
```

### Trace through the example

Our example has `chains_longer_than_one = true` (C010 has 2 rows), so the **slow path** is taken.

**Iteration 1: probe row i=0 (C030)**

```
ScanInnerJoin: hash(C030) → entries[1] → ROW_3 matches
chain_match_sel_vector = [0]   (position 0 in pointers vector)
pointers[0] = ptr(ROW_3)

UpdateCompactionBuffer(base_count=0, chain_match_sel_vector, result_count=1):
  lhs_sel_vector[0] = 0          → probe row i=0
  rhs_pointers[0] = ptr(ROW_3)

base_count = 1
AdvancePointers: ROW_3.NEXT_PTR = null → count = 0 → exit loop for this probe row
```

**Iteration 2: probe row i=1 (C010) — chain step 1**

```
ScanInnerJoin: hash(C010) → entries[3] → ROW_1 (chain head) matches
chain_match_sel_vector = [1]
pointers[1] = ptr(ROW_1)

UpdateCompactionBuffer(base_count=1, ...):
  lhs_sel_vector[1] = 1          → probe row i=1
  rhs_pointers[1] = ptr(ROW_1)

base_count = 2
AdvancePointers: ROW_1.NEXT_PTR → ptr(ROW_0) → count > 0 → continue
```

**Iteration 2 continued: probe row i=1 (C010) — chain step 2**

```
ScanInnerJoin: key compare on ROW_0 → C010 matches
chain_match_sel_vector = [1]
pointers[1] = ptr(ROW_0)     ← now points to chain element ROW_0

UpdateCompactionBuffer(base_count=2, ...):
  lhs_sel_vector[2] = 1          → probe row i=1 again
  rhs_pointers[2] = ptr(ROW_0)

base_count = 3
AdvancePointers: ROW_0.NEXT_PTR = null → count = 0 → exit chain
```

**Iteration 3: probe row i=2 (C999)**

```
hash(C999) → entries[2] → empty → no match → not added
```

**Iteration 4: probe row i=3 (C040)**

```
ScanInnerJoin: hash(C040) → entries[4] → ROW_4 matches
chain_match_sel_vector = [3]
pointers[3] = ptr(ROW_4)

UpdateCompactionBuffer(base_count=3, ...):
  lhs_sel_vector[3] = 3          → probe row i=3
  rhs_pointers[3] = ptr(ROW_4)

base_count = 4
AdvancePointers: ROW_4.NEXT_PTR = null → count = 0
```

**After the while loop — base_count = 4:**

```
lhs_sel_vector = [0, 1, 1, 3]
rhs_pointers   = [ptr(ROW_3), ptr(ROW_1), ptr(ROW_0), ptr(ROW_4)]
```

**Dictionary emission (new code):**

```
Convert rhs_pointers to dictionary indices:
  rhs_ptrs[0] = ptr(ROW_3) → ptr_to_dict_idx[ptr(ROW_3)] = 3
  rhs_ptrs[1] = ptr(ROW_1) → ptr_to_dict_idx[ptr(ROW_1)] = 1
  rhs_ptrs[2] = ptr(ROW_0) → ptr_to_dict_idx[ptr(ROW_0)] = 0
  rhs_ptrs[3] = ptr(ROW_4) → ptr_to_dict_idx[ptr(ROW_4)] = 4

build_sel_vec = [3, 1, 0, 4]
```

**Emit dictionary vectors:**

```cpp
// For each build-side output column:
result.data[2].Dictionary(dict_arrays[0] /* name */, build_sel_vec);  // [3,1,0,4]
result.data[3].Dictionary(dict_arrays[1] /* city */, build_sel_vec);  // [3,1,0,4]
result.data[4].Dictionary(dict_arrays[2] /* tier */, build_sel_vec);  // [3,1,0,4]
```

**Resulting output chunk (4 rows):**

| result.data[0] (order_id) | result.data[1] (amount) | result.data[2] (name) | result.data[3] (city) | result.data[4] (tier) |
|---|---|---|---|---|
| 1001 | 500 | dict[3]="Charlie" | dict[3]="London" | dict[3]="Gold" |
| 1002 | 200 | dict[1]="Alice-EU" | dict[1]="Vienna" | dict[1]="Silver" |
| 1002 | 200 | dict[0]="Alice" | dict[0]="Berlin" | dict[0]="Gold" |
| 1004 | 350 | dict[4]="Diana" | dict[4]="Berlin" | dict[4]="Silver" |

Probe columns (order_id, amount) are sliced via `lhs_sel_vector = [0,1,1,3]` — flat vectors referencing probe chunk data.
Build columns (name, city, tier) are dictionary vectors — each sharing the same `build_sel_vec = [3,1,0,4]` but referencing different `dict_arrays[i]` child buffers with distinct UUIDs.

---

## Phase 5 — Handling Edge Cases

### 5a. Duplicate Keys (C010 with two rows)

**What happens:** The NEXT_PTR chain between ROW_1 → ROW_0 is followed by `AdvancePointers` exactly as in the regular join. At each chain step, one output row is produced. The only difference is that instead of calling `GatherResult` to copy the row data, we look up the row pointer in `ptr_to_dict_idx`:

```
Chain step 1: pointers[1] = ptr(ROW_1) → dict_idx = 1  → "Alice-EU", "Vienna", "Silver"
Chain step 2: pointers[1] = ptr(ROW_0) → dict_idx = 0  → "Alice",    "Berlin", "Gold"
```

Both ROW_0 and ROW_1 have their own dictionary index. No deduplication, no special case. The chain following logic in `AdvancePointers` is completely unchanged.

**Extra care needed:** None. The `ptr_to_dict_idx` map contains every row regardless of key uniqueness.

### 5b. Fallback When Build Side Exceeds Threshold or Spills

**What happens:** If `ht.Count() > DICT_EMISSION_MAX_ROWS` or `sink.external == true`, the `BuildDictionaryArrays()` call is skipped entirely. `use_dict_emission` remains `false`. All code paths in `NextInnerJoin` check `if (ht.use_dict_emission)` and fall through to the existing `GatherResult` path when `false`.

**Extra care needed:** Ensure `use_dict_emission` is initialized to `false` in the JoinHashTable constructor (default member initializer already does this). No other changes to the fallback path.

**Code path:** In `physical_hash_join.cpp:Finalize()`:
```cpp
if (!sink.external && ht.Count() > 0 &&
    ht.Count() <= JoinHashTable::DICT_EMISSION_MAX_ROWS) {
    ht.BuildDictionaryArrays(*this);
}
// ScheduleFinalize runs regardless — the regular hash table is always built
sink.ScheduleFinalize(pipeline, event);
```

When the condition is false, `ScheduleFinalize` proceeds normally and the regular probe path is used.

### 5c. RIGHT / FULL OUTER Join (found-bool scan after all probe chunks)

**What happens:** After all probe chunks are processed, `ScanFullOuter()` (line 1522 of `join_hashtable.cpp`) iterates the TupleDataCollection to find unmatched build rows (where the found-bool at offset `tuple_size` is `false`). For those rows, it sets the left (probe) side to NULL and gathers the right (build) side columns via `data_collection->Gather()`.

**The current code (lines 1577-1583):**
```cpp
for (idx_t i = 0; i < output_columns.size(); i++) {
    auto &vector = result.data[left_column_count + i];
    const auto output_col_idx = output_columns[i];
    D_ASSERT(vector.GetType() == layout_ptr->GetTypes()[output_col_idx]);
    data_collection->Gather(addresses, sel_vector, found_entries, output_col_idx, vector, sel_vector, nullptr);
}
```

**Modified code:**
```cpp
for (idx_t i = 0; i < output_columns.size(); i++) {
    auto &vector = result.data[left_column_count + i];
    const auto output_col_idx = output_columns[i];
    D_ASSERT(vector.GetType() == layout_ptr->GetTypes()[output_col_idx]);

    if (use_dict_emission) {
        // Project 1: emit dictionary vectors for unmatched build rows
        SelectionVector build_sel_vec(found_entries);
        auto key_locs = FlatVector::GetData<data_ptr_t>(addresses);
        for (idx_t j = 0; j < found_entries; j++) {
            auto it = ptr_to_dict_idx.find(key_locs[j]);
            D_ASSERT(it != ptr_to_dict_idx.end());
            build_sel_vec.set_index(j, it->second);
        }
        vector.Dictionary(dict_arrays[i], build_sel_vec);
    } else {
        data_collection->Gather(addresses, sel_vector, found_entries, output_col_idx, vector, sel_vector, nullptr);
    }
}
```

**Extra care needed:**
- The `SelectionVector build_sel_vec` construction should be lifted out of the per-column loop since `addresses` is the same for all columns (the same `build_sel_vec` can be shared across all output columns, exactly as in Phase 4).
- The found-bool marking in `NextInnerJoin` (lines 1118-1128) remains unchanged — it writes directly to the row storage via `Store<bool>(true, ptrs[idx] + ht.tuple_size)` regardless of dictionary emission.

**Optimized version (build_sel_vec outside the loop):**
```cpp
if (use_dict_emission && found_entries > 0) {
    SelectionVector build_sel_vec(found_entries);
    auto key_locs = FlatVector::GetData<data_ptr_t>(addresses);
    for (idx_t j = 0; j < found_entries; j++) {
        auto it = ptr_to_dict_idx.find(key_locs[j]);
        D_ASSERT(it != ptr_to_dict_idx.end());
        build_sel_vec.set_index(j, it->second);
    }
    for (idx_t i = 0; i < output_columns.size(); i++) {
        auto &vector = result.data[left_column_count + i];
        D_ASSERT(vector.GetType() == layout_ptr->GetTypes()[output_columns[i]]);
        vector.Dictionary(dict_arrays[i], build_sel_vec);
    }
} else {
    for (idx_t i = 0; i < output_columns.size(); i++) {
        auto &vector = result.data[left_column_count + i];
        const auto output_col_idx = output_columns[i];
        D_ASSERT(vector.GetType() == layout_ptr->GetTypes()[output_col_idx]);
        data_collection->Gather(addresses, sel_vector, found_entries, output_col_idx, vector, sel_vector, nullptr);
    }
}
```

### 5d. The chains_longer_than_one Fast Path in AdvancePointers

**What happens:** When `!ht.chains_longer_than_one`, `AdvancePointers()` immediately sets `count = 0` without following any next pointers (line 1044-1046 of `join_hashtable.cpp`). This means each probe row can match at most one build row.

**Extra care needed:** None. The `AdvancePointers` logic is completely untouched by Project 1. The dictionary emission change is only in how results are *emitted* (dictionary vector vs. GatherResult), not in how chains are *traversed*. The fast path in `NextInnerJoin` (lines 1132-1150) already handles the `!chains_longer_than_one` case separately and the dictionary emission modification handles both paths (fast and slow) independently.

### 5e. LEFT OUTER Join

**What happens:** `NextLeftJoin` (line 1421) calls `NextInnerJoin` first, then emits unmatched probe rows with NULL build columns. The NULL emission for unmatched rows (lines 1447-1452) sets build columns as constant NULL vectors — this path does not call `GatherResult` and needs no modification.

**Extra care needed:** None. The dictionary emission only applies to matched rows (inside `NextInnerJoin`), which is called by `NextLeftJoin` at line 1425.

### 5f. SINGLE Join

**What happens:** `NextSingleJoin` (line 1458) calls `GatherResult` with its own result_sel vector (line 1496):
```cpp
GatherResult(vector, result_sel, result_sel, result_count, output_col_idx);
```

**Modified code:**
```cpp
if (ht.use_dict_emission) {
    SelectionVector build_sel_vec(result_count);
    auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
    for (idx_t i = 0; i < result_count; i++) {
        auto p_idx = result_sel.get_index(i);
        auto it = ht.ptr_to_dict_idx.find(ptrs[p_idx]);
        D_ASSERT(it != ht.ptr_to_dict_idx.end());
        build_sel_vec.set_index(i, it->second);
    }
    for (idx_t i = 0; i < ht.output_columns.size(); i++) {
        auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
        vector.Dictionary(ht.dict_arrays[i], build_sel_vec);
    }
} else {
    for (idx_t i = 0; i < ht.output_columns.size(); i++) {
        auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
        for (idx_t j = 0; j < probe_data.size(); j++) {
            if (!found_match[j]) {
                FlatVector::SetNull(vector, j, true);
            }
        }
        const auto output_col_idx = ht.output_columns[i];
        D_ASSERT(vector.GetType() == ht.layout_ptr->GetTypes()[output_col_idx]);
        GatherResult(vector, result_sel, result_sel, result_count, output_col_idx);
    }
}
```

**Note:** SINGLE join sets unmatched entries to NULL *before* gathering. With dictionary emission, the NULL-setting for unmatched entries must still happen on the result vector. Since `Dictionary()` overwrites the vector type, we need to handle this carefully — the NULL setting should happen after the Dictionary call, or we handle SINGLE join by falling back to the regular path (since SINGLE joins produce at most one match per probe row and the output size is bounded by probe size, the benefit of dictionary emission is limited).

**Recommended approach for SINGLE join:** Skip dictionary emission for SINGLE joins. Add `ht.join_type != JoinType::SINGLE` to the `use_dict_emission` check, or simply leave the existing code unchanged in `NextSingleJoin`.

### 5g. MARK, SEMI, ANTI Joins

**What happens:** These join types do not output build-side payload columns. `NextMarkJoin` produces a boolean vector, `NextSemiJoin` / `NextAntiJoin` only output probe columns. None of them call `GatherResult` for payload columns.

**Extra care needed:** None. Dictionary emission does not apply.

### 5h. RIGHT_SEMI, RIGHT_ANTI Joins

**What happens:** These join types mark build rows during probe (via `NextRightSemiOrAntiJoin`) and emit results during `ScanFullOuter`. The emission uses `data_collection->Gather()` at line 1582.

**Extra care needed:** Handled by the `ScanFullOuter` modification in section 5c above.

---

## Phase 6 — Testing

### 6a. Unit Tests

Create a new test file: `test/sql/join/inner/test_join_dictionary_emission.test`

Use the DuckDB `.test` format (same as `test/sql/join/inner/test_join_perfect_hash.test_slow`).

**Test cases to cover:**

```
# name: test/sql/join/inner/test_join_dictionary_emission.test
# description: Test dictionary emission for small build side hash joins
# group: [inner]

statement ok
PRAGMA enable_verification

# ─── Test 1: Basic string key join (PHJ cannot apply) ───
statement ok
CREATE TABLE customers (customer_id VARCHAR, name VARCHAR, city VARCHAR, tier VARCHAR);

statement ok
INSERT INTO customers VALUES
    ('C010', 'Alice', 'Berlin', 'Gold'),
    ('C010', 'Alice-EU', 'Vienna', 'Silver'),
    ('C020', 'Bob', 'Paris', 'Bronze'),
    ('C030', 'Charlie', 'London', 'Gold'),
    ('C040', 'Diana', 'Berlin', 'Silver'),
    ('C050', 'Eve', 'Tokyo', 'Bronze');

statement ok
CREATE TABLE orders (order_id INTEGER, customer_id VARCHAR, amount INTEGER);

statement ok
INSERT INTO orders VALUES
    (1001, 'C030', 500),
    (1002, 'C010', 200),
    (1003, 'C999', 900),
    (1004, 'C040', 350);

# Inner join with string key, duplicate build key, unmatched probe row
query IIIII
SELECT o.order_id, o.amount, c.name, c.city, c.tier
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_id, c.name;
----
1001	500	Charlie	London	Gold
1002	200	Alice	Berlin	Gold
1002	200	Alice-EU	Vienna	Silver
1004	350	Diana	Berlin	Silver

# ─── Test 2: LEFT JOIN (unmatched probe rows emit NULL build columns) ───
query IIIII
SELECT o.order_id, o.amount, c.name, c.city, c.tier
FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id
ORDER BY o.order_id, c.name;
----
1001	500	Charlie	London	Gold
1002	200	Alice	Berlin	Gold
1002	200	Alice-EU	Vienna	Silver
1003	900	NULL	NULL	NULL
1004	350	Diana	Berlin	Silver

# ─── Test 3: RIGHT JOIN (unmatched build rows emit NULL probe columns) ───
query IIIII
SELECT o.order_id, o.amount, c.name, c.city, c.tier
FROM orders o RIGHT JOIN customers c ON o.customer_id = c.customer_id
ORDER BY c.name, o.order_id;
----
1002	200	Alice	Berlin	Gold
1002	200	Alice-EU	Vienna	Silver
NULL	NULL	Bob	Paris	Bronze
1001	500	Charlie	London	Gold
1004	350	Diana	Berlin	Silver
NULL	NULL	Eve	Tokyo	Bronze

# ─── Test 4: FULL OUTER JOIN ───
query IIIII
SELECT o.order_id, o.amount, c.name, c.city, c.tier
FROM orders o FULL OUTER JOIN customers c ON o.customer_id = c.customer_id
ORDER BY c.name, o.order_id;
----
1002	200	Alice	Berlin	Gold
1002	200	Alice-EU	Vienna	Silver
NULL	NULL	Bob	Paris	Bronze
1001	500	Charlie	London	Gold
1004	350	Diana	Berlin	Silver
NULL	NULL	Eve	Tokyo	Bronze
1003	900	NULL	NULL	NULL

# ─── Test 5: Composite key (multi-column join key) ───
statement ok
CREATE TABLE t1 (a VARCHAR, b INTEGER, payload VARCHAR);

statement ok
INSERT INTO t1 VALUES ('x', 1, 'p1'), ('y', 2, 'p2'), ('z', 3, 'p3');

statement ok
CREATE TABLE t2 (a VARCHAR, b INTEGER, val INTEGER);

statement ok
INSERT INTO t2 VALUES ('x', 1, 100), ('y', 2, 200), ('w', 9, 999);

query IIII
SELECT t2.a, t2.b, t2.val, t1.payload
FROM t2 JOIN t1 ON t2.a = t1.a AND t2.b = t1.b
ORDER BY t2.val;
----
x	1	100	p1
y	2	200	p2

# ─── Test 6: Large probe against small build (many-to-few) ───
statement ok
CREATE TABLE nations (nation_key INTEGER, name VARCHAR);

statement ok
INSERT INTO nations VALUES (1, 'USA'), (2, 'Germany'), (3, 'Japan');

statement ok
CREATE TABLE big_orders AS
SELECT i AS order_id, (i % 3) + 1 AS nation_key, i * 10 AS amount
FROM range(10000) t(i);

query II
SELECT n.name, COUNT(*)
FROM big_orders b JOIN nations n ON b.nation_key = n.nation_key
GROUP BY n.name
ORDER BY n.name;
----
Germany	3333
Japan	3334
USA	3333

# ─── Test 7: Empty build side ───
statement ok
CREATE TABLE empty_build (k VARCHAR, v VARCHAR);

query II
SELECT o.order_id, e.v
FROM orders o JOIN empty_build e ON o.customer_id = e.k;
----

# ─── Test 8: Single row build side ───
statement ok
CREATE TABLE single_build (k VARCHAR, v VARCHAR);

statement ok
INSERT INTO single_build VALUES ('C030', 'only');

query III
SELECT o.order_id, o.amount, s.v
FROM orders o JOIN single_build s ON o.customer_id = s.k;
----
1001	500	only

statement ok
DROP TABLE customers;

statement ok
DROP TABLE orders;

statement ok
DROP TABLE t1;

statement ok
DROP TABLE t2;

statement ok
DROP TABLE nations;

statement ok
DROP TABLE big_orders;

statement ok
DROP TABLE empty_build;

statement ok
DROP TABLE single_build;
```

### 6b. Key Properties to Verify

1. **Correctness:** `PRAGMA enable_verification` enables DuckDB's internal vector verification. This checks that dictionary vectors have valid indices, proper types, and consistent validity masks. This is the most important correctness check.

2. **Dictionary vector output:** To verify that dictionary vectors are actually being emitted (not just correctness), add a debug `DUCKDB_LOG` call inside the dictionary emission path:
   ```cpp
   DUCKDB_LOG(ht.context, PhysicalOperatorLogType, ht.op, "JoinHashTable", "DictEmission",
              {{"rows", to_string(base_count)}});
   ```

3. **Fallback path:** Test with a build side exceeding `DICT_EMISSION_MAX_ROWS` to ensure the regular path is unchanged. The existing test suite (`test/sql/join/`) already covers this implicitly.

### 6c. Existing Test Files as Reference

- `test/sql/join/inner/test_join_perfect_hash.test_slow` — tests PHJ with numeric keys; our tests mirror this pattern for non-numeric keys
- `test/sql/join/hash_join/hash_join_residual_predicates.test` — tests join predicates
- `test/optimizer/perfect_ht.test` — tests PHJ optimizer decisions

### 6d. Microbenchmarks

Create a benchmark: `benchmark/micro/join/dictionary_emission.benchmark`

```
# name: benchmark/micro/join/dictionary_emission.benchmark
# description: Benchmark dictionary emission on small build / large probe joins

name dictionary_emission_string_key
group join

load
CREATE TABLE nations(nation_key INTEGER, name VARCHAR, region VARCHAR);
INSERT INTO nations SELECT i, 'nation_' || i, 'region_' || (i % 5) FROM range(25) t(i);
CREATE TABLE lineitem AS SELECT i AS l_orderkey, (i % 25) AS nation_key, random() * 1000 AS amount FROM range(5000000) t(i);

run
SELECT n.name, n.region, SUM(l.amount) FROM lineitem l JOIN nations n ON l.nation_key = n.nation_key GROUP BY n.name, n.region;
```

Also create a variant with a VARCHAR key to ensure PHJ cannot be used:

```
name dictionary_emission_varchar_key
group join

load
CREATE TABLE nations(nation_code VARCHAR, name VARCHAR, region VARCHAR);
INSERT INTO nations SELECT 'N' || lpad(i::VARCHAR, 3, '0'), 'nation_' || i, 'region_' || (i % 5) FROM range(25) t(i);
CREATE TABLE lineitem AS SELECT i AS l_orderkey, 'N' || lpad((i % 25)::VARCHAR, 3, '0') AS nation_code, random() * 1000 AS amount FROM range(5000000) t(i);

run
SELECT n.name, n.region, SUM(l.amount) FROM lineitem l JOIN nations n ON l.nation_code = n.nation_code GROUP BY n.name, n.region;
```

---

## Summary of All Changes

| # | File | Function/Location | Change |
|---|------|-------------------|--------|
| 1 | `src/include/duckdb/execution/join_hashtable.hpp` | Class members | Add `use_dict_emission`, `dict_arrays`, `ptr_to_dict_idx`, `DICT_EMISSION_MAX_ROWS` |
| 2 | `src/include/duckdb/execution/join_hashtable.hpp` | Public methods | Add `BuildDictionaryArrays(const PhysicalHashJoin &op)` declaration |
| 3 | `src/execution/join_hashtable.cpp` | New method | Implement `BuildDictionaryArrays()` — scan TupleDataCollection, build dict arrays + ptr map |
| 4 | `src/execution/operator/join/physical_hash_join.cpp` | `Finalize()` line ~1140 | Add dict emission trigger after PHJ check fails |
| 5 | `src/execution/join_hashtable.cpp` | `NextInnerJoin()` fast path (line ~1132) | Add `if (ht.use_dict_emission)` branch emitting dictionary vectors |
| 6 | `src/execution/join_hashtable.cpp` | `NextInnerJoin()` slow path (line ~1160) | Add `if (ht.use_dict_emission)` branch converting `rhs_pointers` to dict indices |
| 7 | `src/execution/join_hashtable.cpp` | `ScanFullOuter()` (line ~1577) | Add `if (use_dict_emission)` branch for RIGHT/FULL OUTER |
| 8 | `test/sql/join/inner/test_join_dictionary_emission.test` | New file | Comprehensive test suite |
| 9 | `benchmark/micro/join/dictionary_emission.benchmark` | New file | Microbenchmarks |

### What is NOT changed

- `Sink` / `Scatter` — build phase unchanged
- `JoinHashTable::Finalize` — pointer table construction unchanged
- `Probe` — hash lookup, salt check, key compare unchanged
- `AdvancePointers` — chain following unchanged
- `ResolvePredicates` — predicate evaluation unchanged
- SEMI, ANTI, MARK join paths — no build-side payload to emit
- External hash join path — dictionary emission skipped entirely
- PHJ path — mutually exclusive with dictionary emission

---

## Implementation Order

1. **Phase 1** — Add data structures (header changes only, compiles immediately)
2. **Phase 3** — Implement `BuildDictionaryArrays()` (can unit-test independently)
3. **Phase 2** — Add the trigger in `Finalize()` (activates dictionary array building)
4. **Phase 4** — Modify `NextInnerJoin` emit paths (activates dictionary vector output)
5. **Phase 5** — Handle `ScanFullOuter` for RIGHT/FULL OUTER joins
6. **Phase 6** — Write tests, run with `PRAGMA enable_verification`, benchmark

Start with Phase 1 + 3 + 2, verify the dictionary arrays are built correctly (debug print), then add Phase 4 to activate the probe-side change and run the test suite. Phase 5 (OUTER joins) can be done last since it's independent of the inner join path.
