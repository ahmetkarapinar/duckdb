# Fix 1: Pointer Arithmetic Block Directory — Analysis

## TupleDataCollection Memory Layout

### Row Layout Within a TupleDataChunkPart

Rows are **perfectly contiguous at stride `row_width`** within each `TupleDataChunkPart`. Confirmed in `tuple_data_allocator.cpp`:

```cpp
// Line 410-412
const auto base_row_ptr = GetRowPointer(pin_state, part);
for (idx_t i = 0; i < next; i++) {
    row_locations[offset + i] = base_row_ptr + i * row_width;
}
```

Each part stores rows at `base_ptr + i * row_width` where `row_width = layout.GetRowWidth()`.

### Number of Parts for a Small Build Side

A small build side (e.g. 25 rows for TPC-H nation table) will typically have **1-4 parts**. The number depends on:

1. **STANDARD_VECTOR_SIZE (2048)** — maximum rows per chunk
2. **Row block capacity** — a new part is created when the current row block is full (`block_size / row_width` rows per block, where `block_size` is typically 256KB)
3. **Heap block capacity** — for variable-size columns (VARCHAR), heap space may run out before the row block is full
4. **Partitioned build phase** — during the build phase, rows are distributed across hash partitions. After `Unpartition()`, rows from different partitions end up in separate parts, even if the total count is small. This is why a 4-row build side can have 4 parts.

### row_width Availability

`row_width` is fixed and accessible via `layout_ptr->GetRowWidth()` at the point where `BuildDictionaryArrays` runs. It is the same value used by the `TupleDataAllocator` for row stride.

### Pointer Stability After Finalize

Row pointers are stable after `Finalize()` with `KEEP_EVERYTHING_PINNED` pinning. The `TupleDataCollection` pins all row blocks, and no reallocation occurs during the probe phase. The block directory pointers remain valid throughout the query execution.

---

## The Bug: Unsorted Block Directory

### Root Cause

The `TupleDataChunkIterator` returns row pointers in **logical order** (segment → chunk → part), NOT in ascending address order. When rows are distributed across hash partitions during the build phase, the memory blocks for different partitions are allocated at different addresses. After `Unpartition()`, the logical iteration order does not correspond to address order.

For example, with a 4-row build side across 4 partitions:
```
block[0]: base=0xB94000008  start_idx=0   ← higher address
block[1]: base=0xB98000008  start_idx=1   ← highest address
block[2]: base=0xB90000008  start_idx=2   ← lowest address
block[3]: base=0xB92000008  start_idx=3   ← second lowest
```

`std::upper_bound` requires a sorted range. With an unsorted block directory, the binary search produces undefined results — in our case, `PtrToDictIdx(row_ptrs[0])` returned 65539 instead of 0.

### Fix

Sort the block directory by `base_ptr` after building it:

```cpp
std::sort(dict_block_directory.begin(), dict_block_directory.end(),
          [](const DictBlockEntry &a, const DictBlockEntry &b) {
              return a.base_ptr < b.base_ptr;
          });
```

Each `DictBlockEntry` carries both `base_ptr` (for binary search) and `start_idx` (the logical dictionary index). After sorting, the binary search correctly finds the entry for any pointer, and the `start_idx` gives the correct dictionary index offset.

---

## Final Structure

```cpp
struct DictBlockEntry {
    data_ptr_t base_ptr;  // First row pointer in this contiguous run
    idx_t start_idx;      // Dictionary index of the first row in this run
};
vector<DictBlockEntry> dict_block_directory;  // Sorted by base_ptr
idx_t dict_row_width = 0;                     // Cached row stride
```

Lookup via `PtrToDictIdx(data_ptr_t ptr)`:
1. `std::upper_bound` over the block directory (binary search by `base_ptr`)
2. Decrement to find the block containing `ptr`
3. Return `start_idx + (ptr - base_ptr) / row_width`

For a typical small build side with 1-4 blocks, the binary search is 0-2 comparisons — effectively O(1) with zero pointer chasing.

### Why This Structure

- **No hash computation** — unlike `unordered_map`, no `std::hash` call per lookup
- **No pointer chasing** — the entire block directory fits in a single cache line (~4 entries × 16 bytes = 64 bytes)
- **Near-zero memory overhead** — ~4 entries total vs ~64 bytes per build row for `unordered_map`
- **Cache-friendly** — contiguous array, no linked-list nodes

---

## Edge Cases

1. **Non-contiguous rows across parts**: Handled correctly. Each discontinuity in the pointer sequence creates a new block entry. After sorting, the binary search finds the right block regardless of address ordering.

2. **Single row build side**: One block entry, binary search trivially finds it.

3. **All rows in one contiguous block**: One block entry, lookup is `(ptr - base) / row_width` — a single division.

4. **Large build side (up to 1M rows)**: With 256KB blocks and ~32-64 byte rows, there are ~4-8K rows per block, giving ~125-250 blocks for 1M rows. Binary search over 250 entries is ~8 comparisons — still very fast.

---

## Build and Test Results

- Build: compiles cleanly with no warnings
- Dictionary emission test: **183 assertions passed** (all 10 test cases including inner, left, right, full outer, composite key, large probe, empty build, single row, duplicates)
- Full join test suite: **14,264 assertions passed** across 111 test cases (4 skipped for missing TPC-H extension)
