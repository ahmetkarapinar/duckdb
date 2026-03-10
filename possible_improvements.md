# Possible Improvements to Pointer → Dictionary Index Mapping

## Why the `unordered_map` Approach Is Slow

The original baseline (on `ahmet/thesis`) used `std::unordered_map<data_ptr_t, idx_t>` to map each row pointer to its dictionary index. This is called once per matched probe row inside `NextInnerJoin`. For a workload with millions of probe-side matches, this means millions of hash-map lookups. Each lookup involves:

1. **Hashing the pointer** — a 64-bit hash computation
2. **Bucket lookup** — an indirect memory access into the bucket array
3. **Pointer chasing** — following the chain within the bucket (std nodes are heap-allocated, each node a separate allocation)
4. **Key comparison** — comparing the 8-byte pointer

The memory access pattern is terrible: each lookup touches at least 2–3 cache lines in random locations (bucket array entry → node → possibly next node). For a 100K-row build side, the map itself consumes ~3–5 MB of heap memory (nodes + bucket array), which doesn't fit in L1/L2 and causes frequent cache misses. This per-row overhead dominates the probe hot path and causes a regression compared to the original `GatherResult` path it replaces.

---

## Why the Pointer-Arithmetic Approach Still Leaves Room for Improvement

The fix on `project1-fix1-pointer-arithmetic` replaced the map with a sorted array of `DictBlockEntry` structs (typically 1–10 entries, ~16 bytes each) and computes the index via:

```
upper_bound(block_directory, ptr)  →  decrement  →  start_idx + (ptr - base_ptr) / row_width
```

This is dramatically better: the entire directory fits in one cache line, and lookup is O(log B) where B ≈ 4. However, two costs remain in the per-row hot loop:

1. **Integer division** (`(ptr - base_ptr) / dict_row_width`): On x86, a 64-bit unsigned division (`div` instruction) has a latency of 35–90 cycles depending on microarchitecture. This is called for every matched row. With ~2048 matches per chunk (STANDARD_VECTOR_SIZE), that's 70K–180K cycles per chunk just for divisions.

2. **Binary search branch mispredictions**: Even with only 2 comparisons for 4 blocks, the branch predictor may mispredict when probe-side access patterns are random (which they are for hash joins). Each misprediction costs ~15–20 cycles.

3. **Per-row function call overhead**: `PtrToDictIdx` is called individually for each row in a scalar loop. There is no vectorized/batch processing.

The 25% speedup over DuckDB main confirms the approach works, but the integer division alone likely accounts for a significant fraction of the remaining overhead.

---

## Alternative Approaches

### Approach 1: Eliminate Division via Power-of-Two Row Width Padding

**Key insight**: If `row_width` is rounded up to the next power of two, the integer division becomes a right-shift, which is a single-cycle operation.

**How it works**:
- At build time, pad each row to the next power-of-two size (e.g., row_width=48 → padded to 64).
- Store the shift amount: `dict_row_shift = __builtin_ctz(padded_row_width)`.
- Lookup becomes: `start_idx + ((ptr - base_ptr) >> dict_row_shift)`.

**Trade-offs**:
- **Pro**: Eliminates the 35–90 cycle division entirely, replacing it with a 1-cycle shift.
- **Con**: Requires a separate padded copy of the row data in the `TupleDataCollection`, or requires modifying the `TupleDataAllocator` to pad rows. This is invasive — the row layout is shared across the entire system.
- **Con**: Wastes memory. For row_width=33, padding to 64 wastes 48% of row memory. For a 1M-row build side, this could mean tens of MB of wasted memory.
- **Correctness**: No issues with duplicates or NEXT_PTR chains — the mapping is the same, just with different arithmetic.
- **Cache behaviour**: Same as current approach — directory fits in one cache line.
- **Implementation complexity**: Medium-high. Requires either a parallel padded allocation or changes to `TupleDataAllocator`. Risky due to layout assumptions elsewhere.

**Verdict**: The benefit is real but the implementation cost is high and the memory waste is unappealing. Not recommended as the first attempt.

---

### Approach 2: Replace Division with Multiplication by Magic Constant (Compiler-Style)

**Key insight**: The compiler can't optimize the division because `dict_row_width` is a runtime value. But since it's constant for a given hash table, we can precompute the "magic number" multiplicative inverse at build time and use it at probe time.

**How it works**:
- At build time, compute the magic constant and shift for `dict_row_width` using the standard algorithm (same as what compilers use for compile-time-known divisors):
  ```cpp
  // Precompute at build time:
  uint64_t magic;
  unsigned shift;
  compute_magic_for_division(dict_row_width, magic, shift);

  // At probe time (replaces division):
  idx_t offset = static_cast<idx_t>(__uint128_t(ptr - base_ptr) * magic >> 64) >> shift;
  ```
- This replaces a `div` (35–90 cycles) with a `mul` + two shifts (~4–6 cycles total on modern x86).

**Trade-offs**:
- **Pro**: 6–15x faster than integer division per row. No memory layout changes needed. Drop-in replacement within `PtrToDictIdx`.
- **Pro**: Zero additional memory overhead (just two extra fields on `JoinHashTable`: `dict_magic` and `dict_shift`).
- **Pro**: Completely correct — mathematically equivalent to exact division when the dividend is known to be exactly divisible (which it is, since `(ptr - base_ptr) % row_width == 0` is already asserted).
- **Con**: Requires implementing or pulling in the magic-number computation. This is well-known algorithm (libdivide, Hacker's Delight Chapter 10) but adds ~20 lines of code.
- **Correctness**: Identical to current approach. No issues with duplicates or NEXT_PTR chains.
- **Cache behaviour**: Identical to current approach.
- **Implementation complexity**: **Low**. Only changes `PtrToDictIdx` and adds a precomputation step in `BuildDictionaryArrays`. No changes to memory layout, no changes to the probe loop structure.

**Verdict**: Strongly recommended. Minimal risk, significant per-row speedup, trivial to implement.

---

### Approach 3: Embed Dictionary Index Directly in Each Row (Inline Index)

**Key insight**: The `TupleDataCollection` row format has a fixed layout where the hash value is stored at `pointer_offset` during build, then overwritten with the NEXT_PTR during finalization. After finalization, there may be unused bytes in the row that could hold a dictionary index. Alternatively, we could store the dictionary index in a separate side array indexed by the same pointer-arithmetic scheme.

**How it works (variant A — steal hash slot after finalization)**:
- After `BuildDictionaryArrays` scans all row locations and knows the dictionary index for each row, write the index back into each row at a known offset (e.g., overwrite the hash field, which is no longer needed after probing is set up).
- At probe time: `dict_idx = Load<idx_t>(ptr + dict_idx_offset)` — a single memory load, no arithmetic at all.

**How it works (variant B — separate index array)**:
- Allocate a flat `idx_t[]` array of size `build_count`.
- For each row, use the current pointer-arithmetic to compute its dictionary index and store it in the array.
- At probe time, still use pointer arithmetic to find the position in the array, then load the precomputed index.
- This variant doesn't help — it has the same cost as the current approach plus an extra indirection.

**Trade-offs (variant A)**:
- **Pro**: Lookup becomes a single aligned memory load (~4 cycles if cache-hot, ~100+ cycles if cache-cold). No arithmetic at all.
- **Pro**: The row is already being accessed during key comparison in the probe phase, so the cache line containing the row is likely warm. This means the dictionary index load would likely be a cache hit.
- **Con**: Requires identifying safe bytes in the row to overwrite. The hash field at `pointer_offset` is used during `Finalize` (InsertHashes reads it) but after finalization it's overwritten with the NEXT_PTR. We cannot overwrite the NEXT_PTR because it's used during chain traversal in the probe phase. We would need *additional* bytes.
- **Con**: The row layout is fixed by `TupleDataLayout` and is shared across the system. Adding a field requires modifying the layout, which affects all tuple data operations.
- **Con**: For the "steal hash slot" variant: the hash value offset and NEXT_PTR offset overlap (`pointer_offset`). After finalization, the NEXT_PTR is at `pointer_offset` and is actively used during probing. There is no free slot to steal without expanding the row.
- **Correctness**: Correct for duplicates — each row in the chain has its own index. NEXT_PTR chains are unaffected since we'd use a different offset.
- **Cache behaviour**: Excellent — the data is co-located with the row that's already being accessed.
- **Implementation complexity**: **High**. Requires modifying `TupleDataLayout` to reserve extra bytes when dictionary emission is anticipated, which means the decision must be made before build starts (currently it's made at finalization time). This is a significant architectural change.

**Verdict**: The cache-locality benefit is compelling, but the implementation complexity and invasiveness are too high for a first attempt. Could be a future optimization if division elimination alone is insufficient.

---

### Approach 4: Precomputed Flat Lookup Table (Pointer → Index Array via Page-Aligned Hashing)

**Key insight**: If we know the address range of all row blocks, we can create a compact lookup structure indexed by `(ptr >> some_shift) & mask` that directly yields the dictionary index block, avoiding binary search entirely.

**How it works**:
- At build time, find the minimum and maximum row pointer addresses across all blocks.
- Divide the address range into fixed-size "pages" (e.g., 4KB aligned, matching OS page size).
- Create an array indexed by `(ptr - min_addr) >> page_shift` that stores the `start_idx` and `base_ptr` for the block containing that page.
- At probe time: one shift + one subtraction → array index → load block entry → pointer arithmetic within block.

**Trade-offs**:
- **Pro**: Eliminates binary search entirely — O(1) lookup with one shift and one array access.
- **Pro**: The array is small: for a 1M-row build side with 32-byte rows, total row memory is ~32MB, which is ~8K pages at 4KB granularity → 8K × 16 bytes = 128KB. Fits in L2 cache.
- **Con**: Complexity in handling the address space. Row blocks may be allocated from different memory regions (via BufferManager), so the address range could be sparse. This could make the array very large if allocations are spread across the address space.
- **Con**: Still requires the integer division within each block entry (though this can be combined with Approach 2's magic-number trick).
- **Correctness**: No issues with duplicates or chains.
- **Cache behaviour**: Good for the first level (page table). Second level (within-block arithmetic) same as current.
- **Implementation complexity**: **Medium**. Requires address-range analysis at build time and careful handling of sparse allocations.

**Verdict**: Interesting but overly complex for the marginal benefit over Approach 2. The binary search over 4 entries is already ~2 comparisons; eliminating it saves perhaps 5–10 cycles while the division elimination saves 30–85 cycles.

---

### Approach 5: Batch/Vectorized PtrToDictIdx with Prefetching

**Key insight**: Currently, `PtrToDictIdx` is called in a scalar loop for each matched row. Even with Approach 2's fast division, the loop processes one row at a time. We could process all rows in a batch with software prefetching to hide memory latency.

**How it works**:
- Instead of calling `PtrToDictIdx` per row, process all `result_count` rows in a single vectorized pass.
- In the first pass, prefetch the cache lines for the block directory (already hot, so this is free).
- More importantly, this approach enables the compiler to auto-vectorize the arithmetic (subtraction + multiply-shift for magic division) using SIMD if the data is laid out in a flat array.
- Could also be combined with manual SIMD intrinsics for the subtraction and shift operations.

**Trade-offs**:
- **Pro**: Better instruction-level parallelism — the CPU can pipeline multiple independent multiplications.
- **Pro**: Easy to combine with Approach 2 (magic-number division).
- **Con**: The binary search per row is data-dependent and hard to vectorize. However, with only ~4 block entries, a branchless SIMD comparison could check all entries simultaneously.
- **Con**: Marginal benefit if the directory is already in L1 cache and the main bottleneck was the division.
- **Correctness**: Identical — just restructuring the computation.
- **Cache behaviour**: Slightly better due to prefetching and sequential access patterns.
- **Implementation complexity**: **Low-Medium**. Refactoring the scalar loop into a batch function is straightforward. SIMD would be more complex.

**Verdict**: Worth combining with Approach 2 as a secondary optimization. The batch structure is already there (we iterate `result_count` rows), so it's mainly about ensuring the compiler can optimize the inner loop.

---

### Approach 6: Store Dictionary Index in the NEXT_PTR's Upper Bits

**Key insight**: The `ht_entry_t` structure already packs a 16-bit salt and a 48-bit pointer into a single 64-bit value. The NEXT_PTR stored in each row at `pointer_offset` is a full 64-bit field but only uses 48 bits for the actual pointer (on x86-64, user-space pointers are 48 bits). The upper 16 bits could encode a dictionary index.

**How it works**:
- After `BuildDictionaryArrays`, iterate over all rows and pack the dictionary index into the upper 16 bits of the NEXT_PTR field.
- At probe time, extract the dictionary index from the NEXT_PTR: `dict_idx = Load<uint64_t>(ptr + pointer_offset) >> 48`.
- The NEXT_PTR itself is extracted with a mask: `next_ptr = Load<uint64_t>(ptr + pointer_offset) & 0x0000FFFFFFFFFFFF`.

**Trade-offs**:
- **Pro**: Zero additional memory — reuses existing bits in the row.
- **Pro**: The NEXT_PTR is already loaded during `AdvancePointers` (line 1054: `ptrs[idx] = LoadPointer(ptrs[idx] + ht.pointer_offset)`), so the dictionary index comes "for free" in the same cache line and same load.
- **Con**: 16 bits limits the dictionary to 65,536 unique indices. The current `DICT_EMISSION_MAX_ROWS` is 1M (1,048,576), which exceeds 16 bits. We would need to either lower the threshold to 64K or use more bits (e.g., 20 bits from the pointer, limiting to 44-bit pointers — risky on some platforms).
- **Con**: Requires modifying `AdvancePointers` and `LoadPointer` to mask out the index bits, which affects the non-dictionary-emission code path (or requires branching on `use_dict_emission`).
- **Con**: The `ht_entry_t` salt mechanism already uses the upper 16 bits of the initial hash table entry. If the NEXT_PTR within rows uses a different convention, this could cause confusion. Need to verify that `LoadPointer` only reads 48 bits.
- **Correctness**: Each row in a duplicate chain has its own NEXT_PTR, so each gets its own dictionary index. Correct for duplicates.
- **Cache behaviour**: Optimal — index is co-located with data already being accessed.
- **Implementation complexity**: **Medium**. Requires careful bit-packing and testing on multiple platforms. The 16-bit limit on dictionary size is a significant constraint.

**Verdict**: Elegant but the 16-bit limit is a deal-breaker at the current 1M-row threshold. Could work if the threshold is lowered, but that reduces the applicability of the optimization.

---

## Recommendation

**Try Approach 2 (Magic-Number Division) first.** Rationale:

1. **Highest ROI**: The integer division is the single most expensive operation in the per-row hot loop (35–90 cycles). Replacing it with a multiply+shift (4–6 cycles) gives a 6–15x speedup on that operation. For a workload doing ~2048 lookups per chunk, this saves ~60K–170K cycles per chunk.

2. **Lowest risk**: No changes to memory layout, no changes to the probe loop structure, no platform-specific concerns. The magic-number technique is mathematically proven correct for exact division (which is guaranteed by the `D_ASSERT` on line 351).

3. **Minimal code change**: ~20 lines for the magic-number precomputation, ~3 lines to change `PtrToDictIdx`. The existing binary search stays in place.

4. **Composable**: Can later be combined with Approach 5 (batched/vectorized processing) for additional gains, or with Approach 4 (page-aligned lookup) if the binary search becomes a bottleneck with many blocks.

**Concrete implementation sketch**:
```cpp
// New fields on JoinHashTable:
uint64_t dict_magic = 0;    // multiplicative inverse of row_width
unsigned dict_shift = 0;    // post-multiplication shift

// In BuildDictionaryArrays, after setting dict_row_width:
compute_unsigned_magic(dict_row_width, dict_magic, dict_shift);

// Updated PtrToDictIdx:
inline idx_t PtrToDictIdx(data_ptr_t ptr) const {
    auto it = std::upper_bound(...);  // same binary search
    --it;
    uint64_t byte_offset = static_cast<uint64_t>(ptr - it->base_ptr);
    idx_t row_offset = static_cast<idx_t>((__uint128_t(byte_offset) * dict_magic) >> 64) >> dict_shift;
    return it->start_idx + row_offset;
}
```

As a **secondary follow-up**, consider Approach 5 (batch processing with prefetching) to improve instruction-level parallelism across the chunk. This is compatible with Approach 2 and requires minimal additional effort.
