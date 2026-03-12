# Embedding Dictionary Index Into Row Layout — Analysis

## 1. When and Where the Dictionary Index Is Known

### Build Lifecycle Timeline

```
1. JoinHashTable constructor    — Row layout (TupleDataLayout) is created
2. Build phase (parallel)       — Rows are Scatter'd into PartitionedTupleData
3. Merge local hash tables      — Local HTs merged via Combine()
4. Unpartition()                — sink_collection → data_collection (single TupleDataCollection)
5. BuildDictionaryArrays()      — Scans all row pointers sequentially, builds dict arrays [NEW]
6. ScheduleFinalize()           — Allocates pointer table, inserts hashes, writes NEXT pointers
7. Probe phase                  — Matched row pointers are resolved to dict indices
```

**The dictionary index (sequential 0-based position) is first knowable at step 5** — during the `TupleDataChunkIterator` scan in `BuildDictionaryArrays`. At this point:

- All rows are in a single, merged `TupleDataCollection` (after Unpartition at step 4)
- Row pointers are stable and pinned (`KEEP_EVERYTHING_PINNED`)
- The sequential scan order assigns index 0, 1, 2, ... to each row

**The index is NOT known during the Build phase (step 2)** because:
- Rows are scattered into a `RadixPartitionedTupleData` across multiple partitions
- Multiple threads scatter concurrently — no global ordering exists
- Partitions are later merged and unpartitioned, changing the physical layout

**The index IS stable after step 4 (Unpartition)** because the `TupleDataCollection`'s block structure and row pointers do not change between steps 4–7. `Finalize` (step 6) only reads the hash from each row at `pointer_offset` and overwrites that same field with NEXT pointers — it does not move, reorder, or reallocate rows.

### Why Perfect Hash Join Doesn't Need a Map

PHJ avoids the mapping problem entirely because the join key IS the index: `idx = key_value - min_value`. The key column is a single integer with a known min/max range, so the dictionary is indexed directly by the key's domain position. This works because:

1. The key→index mapping is a pure arithmetic function — no lookup needed
2. The dictionary array is sized to `build_range + 1`, with a validity bitmap for gaps
3. At probe time, the probe key directly computes the dictionary selection vector index

For a general hash join, no such natural mapping exists — row pointers are the only stable row identifiers, hence the need for pointer→index translation.

---

## 2. Existing Row Layout Structure

The row layout is constructed in `JoinHashTable::JoinHashTable()` (join_hashtable.cpp:79–89):

```cpp
vector<LogicalType> layout_types(condition_types);           // [join key columns]
layout_types.insert(layout_types.end(), build_types.begin(), build_types.end()); // [payload columns]
if (PropagatesBuildSide(join_type)) {
    layout_types.emplace_back(LogicalType::BOOLEAN);         // [found_bool] — RIGHT/FULL only
}
layout_types.emplace_back(LogicalType::HASH);                // [hash]
```

Physical row layout after `TupleDataLayout::Initialize`:

```
[validity_flags (flag_width bytes)]
[heap_size (sizeof(idx_t)) — only if variable-width columns exist]
[condition_col_0] [condition_col_1] ... [condition_col_K-1]
[build_col_0] [build_col_1] ... [build_col_M-1]
[found_bool (1 byte)] — only for RIGHT/FULL outer joins
[hash (sizeof(hash_t) = 8 bytes)]
```

Key offsets stored in `JoinHashTable`:

| Field | Value | Meaning |
|-------|-------|---------|
| `tuple_size` | `offsets[condition_types.size() + build_types.size()]` | End of payload data (start of found_bool or hash) |
| `pointer_offset` | `offsets.back()` | Offset of hash field (later overwritten with NEXT pointer) |
| `entry_size` | `layout_ptr->GetRowWidth()` | Total row width including all fields |

After `Finalize()`, the hash field at `pointer_offset` is overwritten with the NEXT pointer for hash chain linking. The NEXT pointer is read during probe by `AdvancePointers()` to follow collision chains.

### Fields and Their Usage During Probe

| Field | Read during probe? | Written during probe? | Notes |
|-------|-------------------|----------------------|-------|
| Validity flags | Yes (by Gather) | No | Required for NULL handling |
| Condition columns | Yes (key matching) | No | Equality + non-equality predicates |
| Build payload columns | Yes (by GatherResult) | No | **Skipped when dict emission active** |
| Found bool | No (inner join) | Yes (RIGHT/FULL only) | Only exists for RIGHT/FULL |
| Hash/NEXT pointer | Yes (chain traversal) | No | Read by AdvancePointers |

**Critical observation**: When dictionary emission is active, the build-side payload columns in each row are **never read during probe**. They are only read once during `BuildDictionaryArrays` to populate the dictionary arrays, and then become dead data.

---

## 3. Embedding Approaches

### Approach A: Overwrite Build-Side Payload Data with Dict Index

**Mechanism**: After `BuildDictionaryArrays` gathers all output column data into columnar dictionary arrays (step 3 of that function), write the `uint32_t` dictionary index back into each row at a known offset within the build-side payload area.

**Offset choice**: `layout_ptr->GetOffsets()[condition_types.size()]` — the offset of the first build-type column. Since all build payload data has already been materialized into dict arrays, overwriting this area is safe.

**When written**: During `BuildDictionaryArrays`, as a new Step 4 after gathering all columns:

```cpp
// Step 4: Write dict index back into each row at build payload offset
const auto dict_idx_offset = layout_ptr->GetOffsets()[condition_types.size()];
for (idx_t i = 0; i < build_count; i++) {
    Store<uint32_t>(static_cast<uint32_t>(i), row_ptrs[i] + dict_idx_offset);
}
dict_idx_row_offset = dict_idx_offset; // store for probe-time use
```

**How read at probe time**: Replace hash map lookup with direct memory read:

```cpp
// Before (hash map lookup):
build_sel_vec.set_index(i, ht.ptr_to_dict_idx.at(ptrs[idx]));

// After (embedded index):
build_sel_vec.set_index(i, Load<uint32_t>(ptrs[idx] + ht.dict_idx_row_offset));
```

**Implications for row layout**: None. The `TupleDataLayout` is unchanged. We are reusing existing bytes in the row that become dead data once dict arrays are built. No layout reinitialization, no change to `row_width`, `entry_size`, or `tuple_size`.

**Correctness concerns**:

1. **Build payload size >= 4 bytes**: We need at least `sizeof(uint32_t) = 4` bytes of build payload area. The build payload area size is `tuple_size - offsets[condition_types.size()]`. If all output columns are tiny (e.g., a single BOOLEAN = 1 byte), this could be < 4. **Mitigation**: Check `(tuple_size - offsets[condition_types.size()]) >= sizeof(uint32_t)` before enabling; fall back to hash map otherwise.

2. **No code reads build payload after BuildDictionaryArrays**: Must verify that between `BuildDictionaryArrays` and probe completion, no other code path reads the build payload from serialized rows. Analysis: `Finalize` only reads the hash at `pointer_offset` — confirmed safe. `ScanFullOuter` uses dict emission when active — confirmed safe. The only remaining reader is `GatherResult`, which is bypassed by the `use_dict_emission` flag.

3. **External hash join**: Already excluded by the `!sink.external` guard — safe.

4. **Overwriting across column boundaries**: Writing a 4-byte `uint32_t` at the first build column may overwrite adjacent build columns' bytes. This is safe because ALL build payload columns are dead data.

5. **Thread safety**: `BuildDictionaryArrays` runs single-threaded before `ScheduleFinalize`. The write-back is single-threaded and happens before any concurrent access to these rows.

6. **Index range**: `uint32_t` supports up to ~4 billion indices. `DICT_EMISSION_MAX_ROWS = 1M`. Safe.

**Feasibility**: **HIGH** — minimal code changes, zero probe overhead, no layout changes, no extra memory.

---

### Approach B: Add a Conditional Dict Index Field to the Row Layout

**Mechanism**: Add an extra `LogicalType::UINTEGER` (or `LogicalType::UBIGINT`) column to the layout types before calling `layout->Initialize()`. Write the sequential index into this field during `BuildDictionaryArrays`.

**When written**: During `BuildDictionaryArrays`, using `TupleDataCollection::Scatter` or direct memory writes at the known offset of the added column.

**How read at probe time**: `Load<uint32_t>(row_ptr + dict_idx_offset)` where `dict_idx_offset` is the layout offset of the added column.

**Implications for row layout**:

- Row width increases by 4 or 8 bytes for EVERY row in the hash table
- This applies to ALL hash joins, not just those with dict emission, unless the field is added conditionally
- `tuple_size`, `pointer_offset`, and `entry_size` all shift, affecting all code that uses these offsets

**Problem — Conditional addition is not feasible**:

The layout is constructed in `JoinHashTable::JoinHashTable()`, which runs at pipeline initialization time. At that point, the build-side row count is unknown (rows haven't been built yet). Dict emission eligibility (`count > 0 && count <= 1M`) is only known after all rows are built and merged.

Options:
1. **Always add the field**: Wastes 4–8 bytes per row for all hash joins. For a 1M-row build with 100-byte rows, this is 4–8 MB of wasted space — modest but nonzero.
2. **Predict at construction time**: Not possible without the row count.
3. **Rebuild the layout after build**: Would require re-scattering all data — prohibitively expensive.

**Correctness concerns**:

- Adding a column changes all offsets after it. The `pointer_offset`, `tuple_size`, and `entry_size` calculations downstream depend on the layout. If the field is always added, this is correct but invasive.
- The hash column's offset changes, affecting `Finalize`, `InsertHashes`, and all code that reads/writes the hash or NEXT pointer.
- The found_bool offset changes for RIGHT/FULL joins.
- Other systems that interact with the layout (external join, partition data, bloom filters) may be affected.

**Feasibility**: **MEDIUM** — correct but invasive. Always-on wastes memory; conditional addition is infeasible without knowing the row count at layout construction time.

---

### Approach C: Separate Contiguous Array with Pointer Arithmetic

**Mechanism**: Allocate a parallel `uint32_t[]` array per row block. During `BuildDictionaryArrays`, compute the block-relative position of each row and store the dict index in the parallel array. At probe time, use pointer arithmetic to map `row_ptr` → block index → `parallel_array[block_relative_offset]`.

**When written**: During `BuildDictionaryArrays`.

**How read at probe time**:

```cpp
auto block_idx = find_block(row_ptr);           // binary search or precomputed map
auto row_in_block = (row_ptr - block_base[block_idx]) / entry_size;
auto dict_idx = parallel_arrays[block_idx][row_in_block];
```

**Implications for row layout**: None — the index is stored externally.

**Correctness concerns**:

- Finding the block for a given pointer requires either a sorted list of block base addresses (binary search) or a precomputed map — both add overhead.
- This is essentially the pointer arithmetic approach already considered in Project 1, with similar complexity.
- Multiple row blocks means multiple parallel arrays to manage.

**Feasibility**: **MEDIUM** — functional but adds per-row overhead (block lookup + array access) that is similar to the existing pointer arithmetic approach. Does not eliminate the extra lookup; merely changes its implementation.

---

### Approach D: Overwrite NEXT Pointer After Finalize (Limited)

**Mechanism**: After `Finalize` completes and `finalized = true` is set, overwrite the NEXT pointer at `pointer_offset` with the dict index. Since dict emission is only used when the build side is small, and the NEXT pointer is only needed during probe for chain traversal, this works IF chains are never followed.

**When written**: After `Finalize`, before first probe.

**How read at probe time**: `Load<uint32_t>(row_ptr + pointer_offset)`.

**Critical problem**: The NEXT pointer IS needed during probe. Even when `chains_longer_than_one == false`, `AdvancePointers()` is called to read the NEXT pointer (which is NULL, causing the scan to stop). Overwriting it with a non-NULL dict index would cause `AdvancePointers` to follow a garbage pointer.

**Possible mitigation**: When `chains_longer_than_one == false` AND dict emission is active, skip the `AdvancePointers()` call entirely and immediately set `count = 0`. This requires modifying probe logic.

**Correctness concerns**:

- Only works on the fast path (`!chains_longer_than_one`). With duplicate keys (chains exist), the NEXT pointer must remain valid. Dict emission still needs to work with duplicates (a build row can match multiple probe rows, and multiple build rows can share a key).
- Modifying probe control flow to skip AdvancePointers is fragile and affects correctness.
- The `pointer_offset` field is 8 bytes (pointer-sized), so storing a `uint32_t` there leaves 4 bytes of potential garbage that could confuse pointer reads.

**Feasibility**: **LOW** — too many correctness risks and limited to the no-chain case.

---

### Approach E: Encode Dict Index in Upper Bits of NEXT Pointer

**Mechanism**: On x86-64, canonical virtual addresses use 48 bits (or 57 with 5-level paging). The upper 16 bits of a pointer are sign-extended and could theoretically store auxiliary data.

**Problem**: `DICT_EMISSION_MAX_ROWS = 1M` requires 20 bits. Only 16 bits are available in the pointer's non-canonical region on standard x86-64. Additionally:

- This is architecture-specific and non-portable (ARM, RISC-V have different address spaces)
- Loading a tagged pointer requires masking before dereferencing — adds overhead everywhere NEXT pointer is read
- DuckDB uses `data_ptr_t` (raw pointer), and tagged pointer tricks would need to be applied consistently across all pointer access sites

**Feasibility**: **LOW** — non-portable, insufficient bits, invasive changes to pointer handling.

---

## 4. Parallel Build Constraints

`BuildDictionaryArrays` is called from `PhysicalHashJoin::Finalize`, which is a single-threaded finalization step (the `Finalize` method of a `SinkOperator` runs once). At this point:

- All build threads have completed and their local hash tables have been merged
- `Unpartition()` has already been called — all data is in a single `TupleDataCollection`
- The data is pinned via `KEEP_EVERYTHING_PINNED`
- No concurrent readers or writers exist for the build-side rows

The subsequent `ScheduleFinalize()` launches parallel `HashJoinFinalizeTask` workers that:

- **Read** the hash from each row at `pointer_offset`
- **Write** the NEXT pointer at `pointer_offset` via compare-and-swap (`InsertHashes`)
- Do **NOT** read or write any payload column data

Therefore, writing dict indices back into build-side payload columns during `BuildDictionaryArrays` (which runs BEFORE `ScheduleFinalize`) has no thread-safety issues. The parallel Finalize workers operate only on the hash/NEXT pointer field, which is separate from the payload area.

---

## 5. Recommendation

**Approach A (Overwrite Build-Side Payload Data)** is the most promising direction.

### Why

1. **Zero probe-time overhead**: The dict index is embedded in the row at a fixed offset. Reading it is a single `Load<uint32_t>` from a memory location that is already in cache (the row pointer is already dereferenced for key matching). This eliminates the hash map lookup (`unordered_map::at`) or any pointer arithmetic entirely.

2. **No layout changes**: The `TupleDataLayout` remains unchanged. No impact on `row_width`, `entry_size`, `tuple_size`, or `pointer_offset`. No impact on non-dict-emission code paths.

3. **No extra memory**: No hash map, no parallel arrays, no additional columns. The dict index overwrites bytes that are already dead data (build payload columns that have been materialized into dict arrays).

4. **Minimal code changes**:
   - Add ~5 lines to `BuildDictionaryArrays` to write back indices
   - Add a `idx_t dict_idx_row_offset` member to `JoinHashTable`
   - Replace `ptr_to_dict_idx.at(ptr)` calls with `Load<uint32_t>(ptr + dict_idx_row_offset)` at the 3 probe sites (fast path, compaction path, full outer)
   - Remove `ptr_to_dict_idx` member entirely

5. **Clean guard**: Add a size check `(tuple_size - layout_ptr->GetOffsets()[condition_types.size()]) >= sizeof(uint32_t)` to the activation condition. This handles the edge case of tiny build payloads gracefully by falling back to the original GatherResult path.

### Comparison with Perfect Hash Join

This approach makes general hash join dict emission nearly as efficient as PHJ:

| | Perfect Hash Join | Dict Emission (Approach A) |
|---|---|---|
| Index computation | `key - min_value` (arithmetic) | `Load<uint32_t>(row_ptr + offset)` (memory read) |
| Extra data structures | None | None |
| Activation cost | O(N) scan + fill | O(N) scan + gather + write-back |
| Per-probe-row cost | 1 subtraction | 1 memory load (L1 cached) |

The memory load is from a row that is already in cache (the row pointer was just used for key matching), so it should resolve in L1 — roughly equivalent to an arithmetic operation.

### Implementation Sketch

```cpp
// In BuildDictionaryArrays, after Step 3 (gathering columns):

// Step 4: Embed dict index into each row's build payload area
const auto build_payload_offset = layout_ptr->GetOffsets()[condition_types.size()];
const auto build_payload_size = tuple_size - build_payload_offset;
if (build_payload_size >= sizeof(uint32_t)) {
    for (idx_t i = 0; i < build_count; i++) {
        Store<uint32_t>(static_cast<uint32_t>(i), row_ptrs[i] + build_payload_offset);
    }
    dict_idx_row_offset = build_payload_offset;
} else {
    // Fallback: keep ptr_to_dict_idx map (or skip dict emission for tiny payloads)
}

// At probe sites, replace:
//   ht.ptr_to_dict_idx.at(ptrs[idx])
// with:
//   Load<uint32_t>(ptrs[idx] + ht.dict_idx_row_offset)
```
