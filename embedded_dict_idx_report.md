# Embedded Dictionary Index (Approach A) — Implementation Report

## Overview

This implementation replaces the `unordered_map<data_ptr_t, idx_t> ptr_to_dict_idx` hash map used for pointer-to-dictionary-index translation with an **embedded `uint32_t` dictionary index** written directly into the build-side payload area of each serialized row. At probe time, the index is read via a single `Load<uint32_t>(row_ptr + offset)` — eliminating the hash map entirely.

---

## Files Modified

### 1. `src/include/duckdb/execution/join_hashtable.hpp`

- **Removed** `#include <unordered_map>` — no longer needed.
- **Replaced** `unordered_map<data_ptr_t, idx_t> ptr_to_dict_idx` with `idx_t dict_idx_row_offset = 0` — a single offset value storing where the embedded dict index lives within each serialized row.

### 2. `src/execution/join_hashtable.cpp`

- **Removed** `#include <iostream>` — stray debug include from Project 1.
- **`BuildDictionaryArrays`**: Removed Step 2 (hash map construction). Added new Step 3 that writes the dict index into each row at offset `layout_ptr->GetOffsets()[condition_types.size()]` using `Store<uint32_t>`. Added a safety comment explaining why the overwrite is correct under the activation conditions.
- **`NextInnerJoin` fast path** (no chains): Replaced `ht.ptr_to_dict_idx.at(ptrs[idx])` with `Load<uint32_t>(ptrs[idx] + ht.dict_idx_row_offset)`.
- **`NextInnerJoin` compaction path** (chains longer than one): Replaced `ht.ptr_to_dict_idx.at(rhs_ptrs[i])` with `Load<uint32_t>(rhs_ptrs[i] + ht.dict_idx_row_offset)`.
- **`ScanFullOuter`**: Replaced `ptr_to_dict_idx.at(key_locs[j])` with `Load<uint32_t>(key_locs[j] + dict_idx_row_offset)`.

### 3. `src/execution/operator/join/physical_hash_join.cpp`

- **`PhysicalHashJoin::Finalize`**: Extended the activation guard from 3 conditions to 6 conditions (see Activation Conditions below). Added a comment explaining each guard.

---

## How the Embedding Works

### Offset

The dict index is stored at `layout_ptr->GetOffsets()[condition_types.size()]` — the byte offset of the first build-side payload column in the row layout:

```
[validity_flags][heap_size?][cond_col_0]...[cond_col_K-1][dict_idx HERE][...remaining build payload...]
                                                          ^-- dict_idx_row_offset
```

This is the start of the build payload area. The `uint32_t` (4 bytes) may span across multiple logical column boundaries, but this is safe because all build payload bytes are dead data at this point.

### When Write-Back Happens

During `BuildDictionaryArrays`, after Step 2 (gathering all output columns into columnar `dict_arrays`), Step 3 writes the index:

```cpp
for (idx_t i = 0; i < build_count; i++) {
    Store<uint32_t>(static_cast<uint32_t>(i), row_ptrs[i] + dict_idx_row_offset);
}
```

This runs single-threaded during finalization, before `ScheduleFinalize` launches parallel workers. The parallel Finalize workers only touch the hash/NEXT pointer field at `pointer_offset`, which is separate from the build payload area.

### Why It Is Safe

After `BuildDictionaryArrays` gathers all output columns into `dict_arrays`, the build payload bytes in the serialized rows are dead data — no code path reads them. This is guaranteed by the activation conditions:

1. **`join_type != SINGLE`** — `NextSingleJoin` calls `GatherResult` on build payload columns for correlated scalar subquery emission. Excluding SINGLE joins ensures this path is unreachable.
2. **`!residual_predicate`** — `ApplyResidualPredicate` gathers build payload columns to evaluate residual filter expressions (e.g., `l.amount + r.budget > 1100`). Excluding joins with residual predicates ensures this path is unreachable.
3. **The three guarded probe sites** (`NextInnerJoin` fast/slow, `ScanFullOuter`) use `dict_arrays` via dictionary vectors when `use_dict_emission` is true, bypassing `GatherResult`.
4. **`ScheduleFinalize`** only reads/writes the hash/NEXT pointer at `pointer_offset` — separate from build payload.
5. **`PerformKeyComparison`** and **`RowMatcher::Match`** only read condition columns — separate from build payload.

---

## Activation Conditions

All six must hold for dict emission with embedded index to activate:

| Condition | Rationale |
|-----------|-----------|
| `!sink.external` | External joins partition and rebuild, invalidating row pointers |
| `ht.Count() > 0` | No work needed for empty build sides |
| `ht.Count() <= DICT_EMISSION_MAX_ROWS` | Cost/benefit threshold (1M rows) |
| `ht.join_type != JoinType::SINGLE` | NextSingleJoin reads build payload via GatherResult |
| `!ht.residual_predicate` | ApplyResidualPredicate gathers build payload columns |
| `build_payload_size >= sizeof(uint32_t)` | Need at least 4 bytes in the build payload area |

If any condition fails, the system falls back to the regular `GatherResult` path — no dict emission at all.

---

## Benchmark Results

Query: `SELECT n.n_name, COUNT(*) as cnt FROM nation n JOIN customer c ON hash(n.n_nationkey) = hash(c.c_nationkey) GROUP BY n.n_name` on TPC-H SF100 (25 build rows, 15M probe rows).

Five runs, each with 5 iterations (median of 5 shown):

| Run | Median (s) |
|-----|-----------|
| 1   | 0.01140   |
| 2   | 0.01135   |
| 3   | 0.01126   |
| 4   | 0.01093   |
| 5   | 0.01131   |

**Overall median: ~11.3 ms.** Performance is consistent and matches expectations — the embedded index read is effectively free since the row pointer is already in L1 cache from key matching.

---

## Deviations from Analysis Documents

1. **`embed_dict_index_analysis.md` Section 2 ("Critical observation")** claimed that build payload columns are "never read during probe" when dict emission is active. The safety analysis (`approach_a_safety_analysis.md`) identified two code paths that violate this claim: `ApplyResidualPredicate` and `NextSingleJoin`. This implementation adds activation-time guards for both.

2. **No fallback to pointer arithmetic or hash map** — the task requirements specify a clean fallback to the regular `GatherResult` path (no dict emission) when activation conditions fail, rather than falling back to an alternative dict emission mechanism.

3. **Step numbering** in `BuildDictionaryArrays` changed: the original had Steps 1–3 (scan, map, gather). The new version has Steps 1–3 (scan, gather, embed). The hash map construction step was removed entirely.

---

## Follow-Up Note

The current implementation conservatively disables dict emission whenever `residual_predicate` is non-null. A more precise check is possible: inspect `residual_info->build_input_to_layout_map` and allow Approach A if all referenced build columns have `layout_col < condition_types.size()` (i.e., the residual only reads condition columns, which are not overwritten). This would enable dict emission for joins like `ON a = b AND a > 10` where the residual only references condition column `a`. This optimization is excluded for simplicity and can be added as a future improvement.
