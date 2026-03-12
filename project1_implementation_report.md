# Project 1: Small Build Side Dictionary Emission — Implementation Report

## Overview

This optimization targets hash joins where the build side (RHS) is small relative to the probe side (LHS). Instead of gathering (copying) build-side column data from row-format storage for every matched probe row, we pre-materialize the build side into columnar **dictionary arrays** once during finalization, then emit **dictionary-encoded vectors** during the probe phase. Downstream operators (GROUP BY, projections, result collection) that process dictionary vectors can skip redundant work by operating on the much smaller dictionary rather than the full result set.

**Activation condition**: In-memory hash join (not external), not a perfect hash join, build side has > 0 and <= 1,048,576 rows, and no STRUCT-typed output columns.

**Threshold constant**: `JoinHashTable::DICT_EMISSION_MAX_ROWS = 1048576` (1M rows), defined in `join_hashtable.hpp:476`.

---

## Files Modified

### 1. `src/include/duckdb/execution/join_hashtable.hpp`

**Forward declaration** (line ~31):
```cpp
class PhysicalHashJoin;
```
Added because `BuildDictionaryArrays` takes a `const PhysicalHashJoin &` parameter and the header previously only forward-declared it implicitly through other includes.

**Include** (line ~23):
```cpp
#include <unordered_map>
```
Required for `std::unordered_map<data_ptr_t, idx_t>` used for the pointer-to-dictionary-index lookup.

**Public method declaration** (line ~226):
```cpp
void BuildDictionaryArrays(const PhysicalHashJoin &op);
```
The method that pre-materializes build-side data into dictionary arrays during finalization.

**Public member variables and types** (lines ~322–354), in a new section titled "Small Build Side Dictionary Emission":

| Member | Type | Purpose |
|--------|------|---------|
| `use_dict_emission` | `bool` (default `false`) | Gate flag checked at every probe emission site |
| `dict_arrays` | `vector<buffer_ptr<VectorChildBuffer>>` | Pre-materialized columnar data for each RHS output column. Each entry is a reusable dictionary buffer created via `DictionaryVector::CreateReusableDictionary`. Indexed in the same order as `output_columns`. |
| `ptr_to_dict_idx` | `unordered_map<data_ptr_t, idx_t>` | Maps each build-side row pointer to its 0-based dictionary index for O(1) lookup during probe |

**Constant** (line ~476):
```cpp
static constexpr idx_t DICT_EMISSION_MAX_ROWS = 1048576;
```

### 2. `src/execution/join_hashtable.cpp`

**New function: `JoinHashTable::BuildDictionaryArrays`** (lines ~1660–1723):

Three-step process:

1. **Scan row locations**: Uses `TupleDataChunkIterator` with `KEEP_EVERYTHING_PINNED` to collect all `data_ptr_t` row pointers from the `TupleDataCollection` into a contiguous `Vector`.

2. **Build pointer-to-index map**: Populates an `unordered_map<data_ptr_t, idx_t>` mapping each row pointer to its 0-based dictionary index. This map is used during probe to convert matched row pointers into selection vector indices for dictionary emission.

3. **Create dictionary arrays**: For each RHS output column, creates a `VectorChildBuffer` via `DictionaryVector::CreateReusableDictionary(type, build_count)`, then gathers the column data from row-format into the columnar vector using `TupleDataCollection::Gather`.

**STRUCT bailout**: If any RHS output column has `PhysicalType::STRUCT`, the function returns early without enabling dictionary emission. DuckDB's `Dictionary()` method on Vector does not support STRUCT types.

**Modified: `ScanStructure::NextInnerJoin`** — two emission sites:

- **Fast path** (lines ~1140–1163, when `!ht.chains_longer_than_one`): When `use_dict_emission` is true, instead of calling `GatherResult` for each output column, we:
  1. Build a `SelectionVector` by looking up `ht.ptr_to_dict_idx.at(ptrs[idx])` for each matched row
  2. Call `vector.Dictionary(ht.dict_arrays[i], build_sel_vec)` for each output column

- **Slow/compaction path** (lines ~1183–1205, when chains are longer than one): Same logic but uses `rhs_pointers` (the compaction buffer) instead of direct `pointers` vector.

**Modified: `JoinHashTable::ScanFullOuter`** (lines ~1607–1627): When `use_dict_emission && found_entries > 0`, converts addresses to dictionary indices via `ptr_to_dict_idx` and emits dictionary vectors instead of calling `data_collection->Gather`.

### 3. `src/execution/operator/join/physical_hash_join.cpp`

**Modified: `PhysicalHashJoin::Finalize`** (lines ~1143–1148):

Added the activation call between the perfect-hash-join rejection and `ScheduleFinalize`:

```cpp
if (!use_perfect_hash) {
    sink.perfect_join_executor.reset();

    // Check if we can use dictionary emission for a small build side
    if (!sink.external && ht.Count() > 0 && ht.Count() <= JoinHashTable::DICT_EMISSION_MAX_ROWS) {
        ht.BuildDictionaryArrays(*this);
    }

    sink.ScheduleFinalize(pipeline, event);
}
```

The three guards ensure:
- `!sink.external`: Only for in-memory joins (external joins partition and rebuild, invalidating pointers)
- `ht.Count() > 0`: No work needed for empty build sides
- `ht.Count() <= DICT_EMISSION_MAX_ROWS`: Cost/benefit threshold — materializing the full dictionary for very large build sides would waste memory and the dictionary compression benefit diminishes

### 4. `test/sql/join/inner/test_join_dictionary_emission.test`

New test file with 10 test cases covering:

| Test | Description |
|------|-------------|
| 1 | Basic VARCHAR-key inner join with duplicate build keys and unmatched probe rows |
| 2 | LEFT JOIN (unmatched probe rows emit NULL build columns) |
| 3 | RIGHT JOIN (unmatched build rows emit NULL probe columns) |
| 4 | FULL OUTER JOIN (both unmatched sides) |
| 5 | Composite key (multi-column join condition) |
| 6 | Large probe against small build (10,000 rows × 3 rows) with GROUP BY aggregation |
| 7 | Empty build side |
| 8 | Single-row build side |
| 9 | VARCHAR key with many duplicate build-side entries (stress duplicate chains) |
| 10 | Integer key with duplicates (forces regular hash join instead of perfect hash join) |

All tests run with `PRAGMA enable_verification` to engage DuckDB's internal result verification.

---

## Architecture and Data Flow

### Build Phase (unchanged)
```
Input chunks → ExpressionExecutor (join keys) → JoinHashTable::Build
    → PartitionedTupleData (row-format storage)
```

### Finalize Phase (modified)
```
Merge local hash tables → Unpartition → Check perfect hash join
    → [NEW] If not perfect and count <= 1M:
        BuildDictionaryArrays:
          1. Scan all row pointers from TupleDataCollection
          2. Build unordered_map (pointer → dictionary index)
          3. Gather each output column into columnar VectorChildBuffer
          4. Set use_dict_emission = true
    → ScheduleFinalize (build pointer table as before)
```

### Probe Phase (modified)
```
For each probe chunk:
    Probe hash table → Find matches → NextInnerJoin:
        For each matched row:
            LHS: Slice probe_data as before
            RHS: [NEW] If use_dict_emission:
                     Convert row pointers → dictionary indices via ptr_to_dict_idx
                     Emit Dictionary vectors referencing dict_arrays
                 Else:
                     GatherResult from row-format (original path)
```

### Downstream Impact
Dictionary vectors propagate through the pipeline. Key beneficiaries:
- **Hash aggregate**: Can operate on the small dictionary instead of the full result
- **Compressed materialization**: Dictionary-encoded columns compress much better
- **Projections**: Column references through dictionary vectors are O(1)

---

## Pointer-to-Index Lookup: Design

### Problem
During probe, matched rows are identified by `data_ptr_t` pointers into the `TupleDataCollection`. We need to convert these to dictionary indices (0-based positions in the pre-materialized dictionary arrays).

### Approach
`unordered_map<data_ptr_t, idx_t> ptr_to_dict_idx` — stores one entry per build-side row. Built during `BuildDictionaryArrays` by iterating all row pointers and assigning sequential indices. During probe, each matched row pointer is looked up in the map to obtain its dictionary index via `ptr_to_dict_idx.at(ptr)`.

- **Build cost**: O(N) hash insertions during finalization
- **Lookup cost**: O(1) amortized hash table lookup per matched probe row
- **Memory**: ~64 bytes per build-side row (hash table overhead)

---

## Known Limitations and Edge Cases

1. **STRUCT columns**: Dictionary emission is skipped if any RHS output column is STRUCT-typed, because `Vector::Dictionary()` does not support STRUCT vectors.

2. **External hash join**: Dictionary emission is disabled for external (disk-spilling) joins because row pointers become invalid when partitions are rebuilt.

3. **Perfect hash join**: Dictionary emission is not attempted when perfect hash join is used — perfect hash join already has its own optimized emission path.

4. **Row count threshold**: The 1M row limit (`DICT_EMISSION_MAX_ROWS`) is a heuristic. For very large build sides, the cost of pre-materializing all columns outweighs the benefit.

5. **Chains longer than one**: The optimization works for both the fast path (no duplicate keys, `chains_longer_than_one == false`) and the slow path (with the compaction buffer). The slow path uses `rhs_pointers` instead of `pointers` for the lookup.

---

## Cleanup Notes

- `src/execution/join_hashtable.cpp` line 14 contains a stray `#include <iostream>` that should be removed. It was likely added during debugging and is not used.
- `benchmark/query.benchmark` was added to the `benchmark/` directory during profiling setup — it is not part of the optimization implementation.
- `benchmark/tpch_bench.cpp` is an empty staged file unrelated to this optimization.
- Minor whitespace changes in `benchmark/CMakeLists.txt` and `benchmark/tpch/CMakeLists.txt` (trailing newline removal) — cosmetic only.
