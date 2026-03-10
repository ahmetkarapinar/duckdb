# fix1 Optimization Opportunities

Analysis of the `project1-fix1-pointer-arithmetic` branch, focusing on concrete inefficiencies in the existing code that can be addressed without changing the fundamental approach.

---

## 1. Per-call `SelectionVector` heap allocation in the hot path

**Location**: `join_hashtable.cpp`, `NextInnerJoin` fast path (line ~1142) and slow path (line ~1185), `ScanFullOuter` (line ~1608).

**What happens**: Every time the dictionary emission path executes, it constructs a fresh `SelectionVector`:

```cpp
SelectionVector build_sel_vec(result_count);
```

The `SelectionVector(idx_t count)` constructor calls `make_shared_ptr<SelectionData>(count)`, which performs a **heap allocation** (`new sel_t[count]`) plus reference-count bookkeeping on every invocation. In the fast path, this runs once per probe chunk — for a 15M-row probe side at 2048 rows/chunk, that's ~7,300 heap allocations.

**Optimized version**: Add a persistent `SelectionVector build_sel_vec` member to `ScanStructure`, initialized once in the constructor with `STANDARD_VECTOR_SIZE` capacity. Reuse it across all `NextInnerJoin` calls — just write into it without reallocating:

```cpp
// In ScanStructure constructor:
build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);

// In NextInnerJoin, fast path:
// (no allocation — just use this->build_sel_vec directly)
```

For `ScanFullOuter`, the same pattern applies: add a member to `JoinHashTable` or pass a pre-allocated buffer.

**Impact**: **Medium**. Each `malloc`/`free` pair costs ~50–100 ns. At 7,300 chunks that's ~0.4–0.7 ms — small relative to the 15 ms total, but it's a free win that also reduces heap fragmentation and improves cache locality.

---

## 2. Integer division by runtime-variable `dict_row_width`

**Location**: `join_hashtable.hpp`, `PtrToDictIdx` (line 352).

```cpp
return it->start_idx + static_cast<idx_t>(ptr - it->base_ptr) / dict_row_width;
```

**What happens**: `dict_row_width` is a runtime value (set once in `BuildDictionaryArrays`, constant thereafter). The compiler cannot optimize the 64-bit unsigned division because it doesn't know the divisor at compile time. On x86-64, `div r64` has a latency of **35–90 cycles** depending on the microarchitecture. On Apple Silicon, `udiv` is faster (~4–6 cycles), but this still runs for every matched row.

**Optimized version**: Precompute the "magic number" multiplicative inverse at build time using the standard algorithm from Hacker's Delight Chapter 10 / libdivide:

```cpp
// Build time (once):
uint64_t dict_magic;
unsigned dict_shift;
compute_magic(dict_row_width, dict_magic, dict_shift);

// Probe time (per row):
auto byte_offset = static_cast<uint64_t>(ptr - it->base_ptr);
auto row_offset = (__uint128_t(byte_offset) * dict_magic >> 64) >> dict_shift;
```

This replaces a `div` with a `mul` + two shifts — ~4–6 cycles total on both x86 and ARM.

**Impact**: **High on x86, Low on Apple Silicon**. For 15M probe rows on x86, saving 30–85 cycles per row would save ~450M–1.3B cycles, or roughly 150–400 ms at 3 GHz. On Apple Silicon, the saving per row is ~0–2 cycles, so the impact is negligible for this specific benchmark but becomes meaningful at larger build-side sizes where total lookup count is higher.

---

## 3. `std::upper_bound` overhead for single-block build sides

**Location**: `join_hashtable.hpp`, `PtrToDictIdx` (lines 345–349).

```cpp
auto it = std::upper_bound(
    dict_block_directory.begin(), dict_block_directory.end(), ptr,
    [](data_ptr_t p, const DictBlockEntry &e) { return p < e.base_ptr; });
--it;
```

**What happens**: For the benchmark workload (nation table, 25 rows), the block directory has **exactly 1 entry**. Yet every call still executes:

1. `dict_block_directory.begin()` — load the `data()` pointer from the vector
2. `dict_block_directory.end()` — load `data()` + `size()` and compute the end pointer
3. `std::upper_bound` — even for 1 element, the library function checks bounds, does one comparison, and returns
4. Decrement `--it`

For a 1-entry directory, the answer is always `&dict_block_directory[0]`. The binary search is wasted work.

**Optimized version**: Special-case the common single-block case:

```cpp
inline idx_t PtrToDictIdx(data_ptr_t ptr) const {
    const DictBlockEntry *block;
    if (dict_block_directory.size() == 1) {
        block = &dict_block_directory[0];
    } else {
        auto it = std::upper_bound(...);
        --it;
        block = &*it;
    }
    return block->start_idx + (ptr - block->base_ptr) / dict_row_width;
}
```

Even better — cache the data pointer and size as raw fields to avoid `vector` accessor overhead in the hot path:

```cpp
// Members:
const DictBlockEntry *dict_blocks_ptr = nullptr;
idx_t dict_blocks_count = 0;

// Set once in BuildDictionaryArrays:
dict_blocks_ptr = dict_block_directory.data();
dict_blocks_count = dict_block_directory.size();
```

**Impact**: **Low-Medium**. Saves ~5–10 cycles per call by eliminating vector accessor overhead and the trivial binary search. Over 15M rows this is ~75–150M cycles (~25–50 ms at 3 GHz). More importantly, reducing branches in the hot path helps the CPU's branch predictor and instruction scheduler.

---

## 4. Redundant `FlatVector::GetData<data_ptr_t>(pointers)` calls

**Location**: `join_hashtable.cpp`, `NextInnerJoin` fast path (line 1143).

```cpp
if (ht.use_dict_emission) {
    SelectionVector build_sel_vec(result_count);
    auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
    for (idx_t i = 0; i < result_count; i++) {
```

**What happens**: `FlatVector::GetData<data_ptr_t>(pointers)` extracts the raw data pointer from the `pointers` Vector. But this exact same call already happened a few lines earlier in the `PropagatesBuildSide` block (line 1120):

```cpp
auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);
```

Both `ptrs` variables are in separate scoped blocks so the compiler cannot CSE them, but even if it could, the second call is redundant. The pointer doesn't change between line 1120 and line 1143.

**Optimized version**: Hoist the `ptrs` extraction above both blocks:

```cpp
auto ptrs = FlatVector::GetData<data_ptr_t>(pointers);

if (PropagatesBuildSide(ht.join_type)) {
    for (idx_t i = 0; i < result_count; i++) {
        auto idx = chain_match_sel_vector.get_index(i);
        Store<bool>(true, ptrs[idx] + ht.tuple_size);
    }
}

// ... later ...
if (ht.use_dict_emission) {
    SelectionVector build_sel_vec(result_count);
    for (idx_t i = 0; i < result_count; i++) {
        auto idx = chain_match_sel_vector.get_index(i);
        build_sel_vec.set_index(i, ht.PtrToDictIdx(ptrs[idx]));
    }
```

**Impact**: **Negligible**. `FlatVector::GetData` is a simple inline accessor (loads a pointer from the Vector's data buffer). The compiler likely optimizes this away. Listed for code cleanliness.

---

## 5. Lambda capture in `std::upper_bound` — potential missed inlining

**Location**: `join_hashtable.hpp`, `PtrToDictIdx` (lines 345–347).

```cpp
auto it = std::upper_bound(
    dict_block_directory.begin(), dict_block_directory.end(), ptr,
    [](data_ptr_t p, const DictBlockEntry &e) { return p < e.base_ptr; });
```

**What happens**: The comparator is a stateless lambda. In optimized builds, the compiler will inline this. However, `std::upper_bound` is a template in `<algorithm>` that may pull in significant code. At -O2 the lambda should be fully inlined, but at -O1 or in debug builds it might not be. More importantly, the `std::upper_bound` template instantiation brings in bounds-checking code and iterator arithmetic that, for very small arrays (1–4 elements), produces more instruction cache pressure than a hand-rolled loop.

**Optimized version**: Replace with a raw-pointer linear scan for small directories:

```cpp
inline idx_t PtrToDictIdx(data_ptr_t ptr) const {
    // Linear scan — directory is typically 1-4 entries
    const auto *blocks = dict_blocks_ptr;
    const auto n = dict_blocks_count;
    idx_t block_idx = 0;
    for (idx_t j = 1; j < n; j++) {
        if (blocks[j].base_ptr <= ptr) {
            block_idx = j;
        } else {
            break;
        }
    }
    auto byte_offset = static_cast<uint64_t>(ptr - blocks[block_idx].base_ptr);
    return blocks[block_idx].start_idx + byte_offset / dict_row_width;
}
```

Or even a branchless approach using CMOV:

```cpp
for (idx_t j = 1; j < n; j++) {
    block_idx = (blocks[j].base_ptr <= ptr) ? j : block_idx;
}
```

**Impact**: **Low**. The `std::upper_bound` is already fast for small arrays. The main benefit is reduced instruction count and fewer branches, which matters at the margin when this runs millions of times.

---

## 6. The two dict-emission code paths (fast and slow) are nearly identical

**Location**: `join_hashtable.cpp`, fast path (lines 1140–1152) and slow path (lines 1183–1194).

**What happens**: Both paths do exactly the same thing — build a `SelectionVector` of dictionary indices and call `vector.Dictionary(...)` for each output column. The only difference is the source of pointers: `FlatVector::GetData<data_ptr_t>(pointers)` with `chain_match_sel_vector` indexing (fast path) vs `FlatVector::GetData<data_ptr_t>(rhs_pointers)` with sequential indexing (slow path).

This is code duplication. If a bug is fixed or an optimization is applied in one path, it must be manually replicated in the other.

**Optimized version**: Extract a shared helper:

```cpp
void ScanStructure::EmitDictionaryVectors(DataChunk &result, const data_ptr_t *ptrs,
                                          const SelectionVector *sel, idx_t count) {
    for (idx_t i = 0; i < count; i++) {
        auto p = sel ? ptrs[sel->get_index(i)] : ptrs[i];
        build_sel_vec.set_index(i, ht.PtrToDictIdx(p));
    }
    for (idx_t i = 0; i < ht.output_columns.size(); i++) {
        auto &vector = result.data[ht.lhs_output_in_probe.size() + i];
        vector.Dictionary(ht.dict_arrays[i], build_sel_vec);
    }
}
```

**Impact**: **None on performance** (the compiler will generate the same code). This is purely a maintainability improvement that makes future optimizations easier to apply consistently.

---

## Summary and Priority

| # | Opportunity | Impact | Effort |
|---|------------|--------|--------|
| 2 | Magic-number division | **High** (x86) / Low (ARM) | Low (~20 lines) |
| 3 | Single-block fast path / raw pointer caching | **Low-Medium** | Low (~10 lines) |
| 1 | Reuse `SelectionVector` allocation | **Medium** | Low (~5 lines) |
| 5 | Linear scan instead of `upper_bound` | **Low** | Low (~10 lines) |
| 6 | Extract shared helper | None (maintainability) | Low (~15 lines) |
| 4 | Hoist `FlatVector::GetData` | Negligible | Trivial |

**Recommended order**: Start with #2 (magic-number division) — it has the largest potential per-row improvement and the lowest implementation risk. Then #1 (reuse selection vector) and #3 (single-block fast path) as quick follow-ups. #5 and #6 are polish.
