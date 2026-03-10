# ExecuteInternal Performance Regression Analysis

## Execution Path Under the Profiler

`ExecuteInternal` → `scan_structure.Next()` → `NextInnerJoin()`. There is **no post-processing** of the result chunk after `Next()` returns — the chunk passes directly downstream. So the entire regression is inside `NextInnerJoin`, which the profiler attributes to its caller `ExecuteInternal`.

---

## Root Cause 1: `unordered_map` Lookup Per Matched Row (Primary Cause)

In the original code path, RHS columns are emitted via `GatherResult`, which calls `TupleDataCollection::Gather`. For each matched row this does:

```
load row_ptr[sel[i]]  →  add column_offset  →  load value  →  store to result
```

Two dependent loads, both into memory that is **already hot in cache** from the probe phase (the same pointers were just dereferenced to compare join keys).

The dictionary path replaces this with `ht.ptr_to_dict_idx.at(ptrs[idx])` per matched row. Even though the map has only 25 entries (nation table) and fits in L1, each `.at()` call involves:

1. **Hash computation** on a 64-bit pointer (`std::hash<uint8_t*>`)
2. **Bucket lookup** — load bucket pointer from the bucket array
3. **Node traversal** — follow the linked-list node pointer (libstdc++/libc++ use a single-linked-list-with-sentinel design, so even a single-entry bucket involves a pointer chase)
4. **Key comparison** — compare the 64-bit pointer
5. **Exception readiness** — `.at()` must check for key-not-found and be ready to throw `std::out_of_range`, which inhibits certain compiler optimizations around the call

That's 3–4 dependent loads with pointer chasing, compared to 2 dependent loads into already-cached memory for `Gather`. The per-lookup overhead is small (maybe 5–10ns), but it's executed **once per matched probe row**. At SF100, that's ~15M lookups, adding 75–150ms of pure overhead.

Critically, **this overhead replaces per-column Gather work but is itself per-row and column-count-independent**. The tradeoff is:

| Path | Per-row work | Per-column work |
|------|-------------|-----------------|
| Original (Gather) | — | N reads from row-format per column |
| Dictionary | N hash map lookups (column-independent) | 1 `vector.Dictionary()` call (O(1)) |

With K output columns, the original does K×N row reads; the dictionary path does N lookups + K constant-time setups. **When K=1** (this query has a single RHS output column `n.n_name`), the dictionary path may actually do *more* work in the join than the original, because one hash map lookup is more expensive than one row-format read from warm cache.

---

## Root Cause 2: Heap Allocation Per `NextInnerJoin` Call (Minor Contributor)

```cpp
SelectionVector build_sel_vec(result_count);
```

This calls `make_buffer<SelectionData>(count)`, which heap-allocates a `SelectionData` object (with refcount overhead) plus the underlying `sel_t[]` array. That's **2 heap allocations and 2 deallocations per call**. With ~15M rows / 2048 per chunk ≈ 7,300 calls, that's ~29,000 malloc/free operations. At ~50–100ns each, this adds ~1.5–3ms. Not the dominant cost, but it's pure overhead that doesn't exist in the original path.

---

## Root Cause 3: Atomic Refcount Operations (Minor Contributor)

Each `vector.Dictionary(ht.dict_arrays[i], build_sel_vec)` takes `buffer_ptr<VectorChildBuffer>` **by value**, which copies the shared pointer (atomic increment on construction), then moves it into `auxiliary`. When the function parameter is destroyed, that's an atomic decrement. That's 2 atomic operations per output column per call — ~14,600 atomics total. On ARM (M-series Mac), atomics use `ldxr/stxr` pairs which are relatively cheap but not free, especially under contention from multiple probe threads sharing the same `dict_arrays`.

---

## Why the Regression Appears in ExecuteInternal Specifically

The profiler attributes callee time to the caller. `ExecuteInternal` → `Next` → `NextInnerJoin` is the call chain, and all three Project 1 changes (hash map lookup, SelectionVector allocation, Dictionary vector setup) happen inside `NextInnerJoin`. The downstream improvements (hash aggregate processing dictionary vectors more efficiently, compressed materialization benefiting from dictionary encoding) are in **different operators** with different call stacks, so they appear as separate improvements elsewhere in the flame chart.

In short: the regression and the improvement are in different functions. The profiler correctly shows ExecuteInternal getting slower (more work per row in the join) and the aggregate getting faster (less work per group due to dictionary vectors).

---

## Fix Directions

| Cause | Fix Direction |
|-------|--------------|
| **unordered_map lookups** | Replace with pointer arithmetic using a block directory approach (binary search over ~4 entries + integer division — 2 comparisons, no pointer chasing, no hash computation). The `TupleDataCollection` stores rows contiguously within each `TupleDataChunkPart` at stride `row_width`, so a small sorted array of `{base_ptr, start_idx}` entries enables lookup via `std::upper_bound` + `(ptr - base_ptr) / row_width`. |
| **Per-call SelectionVector allocation** | Pre-allocate a `SelectionVector` as a member of `ScanStructure` (or the dictionary emission state) sized to `STANDARD_VECTOR_SIZE`. Reuse it across calls by just overwriting entries. Eliminates all heap allocation from the hot path. |
| **Atomic refcount on buffer_ptr** | Cache the raw `VectorChildBuffer*` pointer and use a lower-level Dictionary setup that avoids the by-value `buffer_ptr` copy, or restructure so that `dict_arrays` entries are passed by reference rather than copied. Alternatively, since all probe threads only read the `dict_arrays`, the atomics could potentially be avoided with a non-atomic shared ownership scheme, though this would require DuckDB infrastructure changes. |

The most impactful fix is #1 (eliminating the hash map), followed by #2 (eliminating the allocation). Fix #3 is minor and likely not worth the complexity.
