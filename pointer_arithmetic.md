# Pointer-Arithmetic Block Directory: How fix1 Replaces the `unordered_map`

## The Old Approach (`ahmet/thesis` baseline)

The baseline dictionary emission implementation stores a flat hash map:

```cpp
unordered_map<data_ptr_t, idx_t> ptr_to_dict_idx;
```

During `BuildDictionaryArrays`, every row pointer in the `TupleDataCollection` is inserted into this map with its sequential index:

```cpp
for (idx_t i = 0; i < build_count; i++) {
    ptr_to_dict_idx[row_ptrs[i]] = i;
}
```

Then, for every matched probe row inside `NextInnerJoin`, the map is consulted:

```cpp
build_sel_vec.set_index(i, ht.ptr_to_dict_idx.at(ptrs[idx]));
```

### Why this is slow

Each `.at()` call performs:

1. **Hash the 8-byte pointer** — compute a hash of the `data_ptr_t` value.
2. **Index into the bucket array** — one random memory access.
3. **Traverse the bucket chain** — `std::unordered_map` uses separate chaining with heap-allocated nodes. Each node is a separate allocation, so following the chain means pointer-chasing through unrelated memory locations.
4. **Compare the key** — compare the 8-byte pointer against each candidate.

For a 100K-row build side, the map consumes roughly 3–5 MB of heap memory (bucket array + one heap-allocated node per entry). This doesn't fit in L1/L2 cache, so each lookup triggers one or more cache misses (~100+ cycles each). Since this is called for every matched probe row — potentially millions of times in a large join — the overhead dominates the probe hot path.

---

## The New Data Structure

The fix replaces the `unordered_map` with three fields on `JoinHashTable`:

```cpp
struct DictBlockEntry {
    data_ptr_t base_ptr;  // first row pointer in this contiguous run
    idx_t start_idx;      // dictionary index of that first row
};
vector<DictBlockEntry> dict_block_directory;  // sorted by base_ptr
idx_t dict_row_width = 0;                    // cached row width in bytes
```

And an inline lookup method:

```cpp
inline idx_t PtrToDictIdx(data_ptr_t ptr) const;
```

### Key insight: rows are contiguous within each `TupleDataChunkPart`

DuckDB's `TupleDataCollection` stores rows in fixed-size memory blocks. Within each `TupleDataChunkPart`, rows are laid out contiguously at a fixed stride of `row_width` bytes:

```
Block memory:
┌──────────┬──────────┬──────────┬──────────┬───┐
│  Row 0   │  Row 1   │  Row 2   │  Row 3   │...│
│(row_width)│(row_width)│(row_width)│(row_width)│   │
└──────────┴──────────┴──────────┴──────────┴───┘
^           ^           ^
base_ptr    base_ptr    base_ptr
            + row_width + 2*row_width
```

This means that given a pointer `ptr` to any row within a block, the row's position within that block is:

```
position = (ptr - base_ptr) / row_width
```

No hashing. No pointer chasing. Just subtraction and division.

The only complication is that there are multiple blocks (one per `TupleDataChunkPart` or when a block boundary is crossed). The block directory records where each contiguous run starts, so a binary search identifies the right block.

---

## How the Block Directory Is Built

In `BuildDictionaryArrays`, after scanning all row pointers from the `TupleDataCollection` into a flat array `row_ptrs[0..build_count-1]`:

**Step 1: Detect contiguous runs.**

Walk the pointer array sequentially. Two consecutive pointers belong to the same contiguous run if and only if they are exactly `row_width` bytes apart:

```cpp
dict_row_width = layout_ptr->GetRowWidth();
dict_block_directory.push_back({row_ptrs[0], 0});
for (idx_t i = 1; i < build_count; i++) {
    if (row_ptrs[i] != row_ptrs[i - 1] + dict_row_width) {
        dict_block_directory.push_back({row_ptrs[i], i});
    }
}
```

Each time a discontinuity is found (different memory block, different chunk part, or a gap), a new `DictBlockEntry` is created recording:
- `base_ptr`: the pointer to the first row of this run
- `start_idx`: the dictionary index (i.e., position in the sequential scan) of that first row

**Step 2: Sort by `base_ptr`.**

The `TupleDataChunkIterator` returns pointers in logical order (chunk-by-chunk, part-by-part), which is **not** necessarily ascending by memory address. Different partitions are allocated from different memory regions. Binary search requires ascending order, so the directory is sorted:

```cpp
std::sort(dict_block_directory.begin(), dict_block_directory.end(),
          [](const DictBlockEntry &a, const DictBlockEntry &b) {
              return a.base_ptr < b.base_ptr;
          });
```

### Typical directory size

For a 1M-row build side with 256KB blocks and 32-byte rows, each block holds ~8,192 rows. That produces ~122 blocks — 122 directory entries at 16 bytes each = **~2 KB total**. In practice, small build sides (hundreds to thousands of rows) produce only 1–4 entries. The entire directory fits in a single cache line.

---

## How the Lookup Works at Probe Time

The `PtrToDictIdx` method:

```cpp
inline idx_t PtrToDictIdx(data_ptr_t ptr) const {
    // 1. Binary search: find the last directory entry whose base_ptr <= ptr
    auto it = std::upper_bound(
        dict_block_directory.begin(), dict_block_directory.end(), ptr,
        [](data_ptr_t p, const DictBlockEntry &e) { return p < e.base_ptr; });
    --it;

    // 2. Pointer arithmetic: compute offset within that block
    return it->start_idx + static_cast<idx_t>(ptr - it->base_ptr) / dict_row_width;
}
```

### Step-by-step example

Suppose we have a build side with 10 rows across 2 memory blocks:

```
Block A (base = 0x1000, 6 rows, row_width = 40):
  Row 0 @ 0x1000  →  dict index 0
  Row 1 @ 0x1028  →  dict index 1
  Row 2 @ 0x1050  →  dict index 2
  Row 3 @ 0x1078  →  dict index 3
  Row 4 @ 0x10A0  →  dict index 4
  Row 5 @ 0x10C8  →  dict index 5

Block B (base = 0x5000, 4 rows, row_width = 40):
  Row 6 @ 0x5000  →  dict index 6
  Row 7 @ 0x5028  →  dict index 7
  Row 8 @ 0x5050  →  dict index 8
  Row 9 @ 0x5078  →  dict index 9
```

The block directory (sorted by `base_ptr`) is:

| Entry | `base_ptr` | `start_idx` |
|-------|-----------|-------------|
| 0     | `0x1000`  | 0           |
| 1     | `0x5000`  | 6           |

`dict_row_width = 40`.

**Now resolve `ptr = 0x5050` (Row 8):**

1. `std::upper_bound` with `ptr = 0x5050` finds the first entry where `0x5050 < entry.base_ptr`. Entry 0 has `base_ptr = 0x1000` (not greater), entry 1 has `base_ptr = 0x5000` (not greater), so `upper_bound` returns `end()`.
2. Decrement: `--it` points to entry 1 (`base_ptr = 0x5000`, `start_idx = 6`).
3. Compute: `(0x5050 - 0x5000) / 40 = 0x50 / 40 = 80 / 40 = 2`.
4. Result: `6 + 2 = 8`. Correct — this is Row 8.

**Resolve `ptr = 0x1078` (Row 3):**

1. `upper_bound` finds entry 1 (`base_ptr = 0x5000 > 0x1078`).
2. Decrement: entry 0 (`base_ptr = 0x1000`, `start_idx = 0`).
3. Compute: `(0x1078 - 0x1000) / 40 = 0x78 / 40 = 120 / 40 = 3`.
4. Result: `0 + 3 = 3`. Correct.

---

## Why This Is Faster

| Operation | `unordered_map` | Block directory |
|-----------|----------------|-----------------|
| **Hash computation** | ~5–10 cycles (hash a 64-bit pointer) | Not needed |
| **Bucket array access** | 1 random load (~4 cycles L1, ~100+ cycles L2/L3 miss) | Not needed |
| **Chain traversal** | 1+ pointer-chasing loads (~100+ cycles each on miss) | Not needed |
| **Key comparison** | 1+ 8-byte comparisons | Not needed |
| **Binary search** | Not needed | ~2 comparisons for 4 entries (~6 cycles) |
| **Subtraction** | Not needed | 1 subtraction (~1 cycle) |
| **Division** | Not needed | 1 integer division (~4–6 cycles on Apple Silicon, ~35–90 on x86) |
| **Total per lookup** | ~120–300+ cycles (cache-miss dominated) | ~10–15 cycles (fully cache-resident) |

The critical difference is **cache behaviour**:

- The `unordered_map` has one heap-allocated node per build-side row. For 100K rows, these nodes are scattered across ~3–5 MB of heap, guaranteeing L1/L2 cache misses on nearly every lookup.
- The block directory is typically 1–4 entries (16–64 bytes total). It fits in a **single cache line** and stays hot in L1 across the entire probe phase.

The speedup scales with the number of probe-side matches: more matches means more lookups, and each lookup that would have been a cache miss is now a cache hit.

---

## Limitations and Edge Cases

### Multiple `TupleDataChunkPart`s and block boundaries

The `TupleDataCollection` allocates rows into multiple `TupleDataChunkPart`s, each backed by a position in a `TupleDataBlock`. When a chunk part is full or a new block is allocated, the next row will be at a non-contiguous address. The directory construction detects this as a discontinuity (`row_ptrs[i] != row_ptrs[i-1] + row_width`) and creates a new entry. This correctly handles:

- Multiple chunk parts within one block (one entry per part)
- Multiple blocks (one entry per contiguous run)
- Blocks allocated from different memory regions (the sort step handles non-monotonic addresses)

### Sorting requirement

Row pointers from the `TupleDataChunkIterator` are in logical order, not address order. If the `TupleDataCollection` was built from multiple partitions (as happens during the hash table build), blocks from different partitions interleave in the logical scan order but may have non-monotonic addresses. The `std::sort` after construction is essential — without it, `std::upper_bound` would return wrong results.

### Row width must be non-zero

The division by `dict_row_width` requires it to be positive. This is guaranteed by `D_ASSERT(dict_row_width > 0)` — a `TupleDataLayout` with zero row width cannot hold any rows.

### Exact divisibility

The byte offset `(ptr - base_ptr)` must be an exact multiple of `row_width`. This is guaranteed by construction — rows are allocated at stride `row_width` within each chunk part. The assertion `D_ASSERT((ptr - it->base_ptr) % dict_row_width == 0)` guards against bugs in the allocation logic or corruption.

### No extra per-row storage

Unlike approaches that embed the dictionary index inside each row, this approach stores no per-row metadata. The only overhead is the directory itself (typically < 1 KB) plus the cached `dict_row_width` (8 bytes).
