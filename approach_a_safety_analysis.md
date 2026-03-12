# Approach A Safety Analysis — Overwriting Build-Side Payload After BuildDictionaryArrays

## Executive Summary

Approach A proposes overwriting the build-side payload area in serialized rows with a `uint32_t` dictionary index after `BuildDictionaryArrays` has gathered all output column data into columnar dictionary arrays. The original analysis claimed these bytes are "dead data" that are never read again. **This claim is incorrect.** There are at least two code paths that read build-side payload columns from serialized rows after `BuildDictionaryArrays` runs. However, both are **guardable** with well-defined fallback conditions, making Approach A viable under specific constraints.

---

## Methodology

Traced every `GatherResult` and `data_collection->Gather` call site in `src/execution/join_hashtable.cpp` and `src/execution/operator/join/physical_hash_join.cpp` that can execute after `BuildDictionaryArrays` runs. For each site, determined whether it reads condition columns only (safe) or build payload columns (unsafe).

### Row Layout Reminder

```
[validity_flags][heap_size?][cond_col_0]...[cond_col_K-1][build_col_0]...[build_col_M-1][found_bool?][hash/NEXT]
                                           ^--- condition_types.size()    ^--- tuple_size
```

Approach A overwrites bytes starting at offset `layout->GetOffsets()[condition_types.size()]` (the start of `build_col_0`) through to `tuple_size`. Condition columns are NOT overwritten.

---

## Code Path Analysis

### 1. ApplyResidualPredicate — UNSAFE

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::ApplyResidualPredicate` (line 973)
**Read site**: Line 991–995

```cpp
for (const auto &entry : ht.residual_info->build_input_to_layout_map) {
    idx_t col_with_offset = entry.first;
    idx_t layout_col = entry.second;
    auto &target_vector = residual_state->eval_chunk.data[col_with_offset];
    GatherResult(target_vector, match_sel, match_count, layout_col);
}
```

**What it reads**: Build-side columns referenced by the residual predicate expression. These are gathered from serialized rows using their layout column index. The `build_input_to_layout_map` maps original table column indices to layout positions. When a residual predicate references a build column that is NOT a join condition column, `layout_col >= condition_types.size()` — pointing into the build payload area that Approach A would overwrite.

**When triggered**: When a hash join has a `residual_predicate` (i.e., `ht.residual_predicate && ht.residual_info`). This occurs when the join has a non-equality filter expression that references columns beyond the equality join keys. Example: `SELECT * FROM l JOIN r ON l.a = r.a AND l.amount + r.budget > 1100` — here `r.budget` is a build payload column read by the residual predicate.

**Who calls it**: `ResolvePredicates` (line 963), which is called by:
- `ScanInnerJoin` (line 1023) — for INNER, RIGHT, LEFT, OUTER joins
- `ScanKeyMatches` (line 1217) — for SEMI, ANTI, MARK joins
- `NextRightSemiOrAntiJoin` (line 1283) — for RIGHT_SEMI, RIGHT_ANTI
- `NextSingleJoin` (line 1492, 1538) — for SINGLE (correlated subqueries)

**Impact**: If Approach A overwrites build payload, and the residual predicate references a column in that area, `GatherResult` reads the `uint32_t` dictionary index bytes (and potentially garbage in adjacent bytes) instead of the original column values. This produces incorrect predicate evaluation results.

**Can it be guarded?**: **YES.** Check `ht.residual_predicate != nullptr` and whether any entry in `residual_info->build_input_to_layout_map` has `layout_col >= condition_types.size()`. If so, the residual predicate reads build payload columns — disable Approach A (set `use_dict_emission = false` or fall back to the pointer arithmetic approach). More conservatively: disable Approach A entirely whenever `residual_predicate` is non-null.

**Note on condition-only residual columns**: If the residual predicate references ONLY build columns that are also join condition columns (i.e., all `layout_col < condition_types.size()`), then `ApplyResidualPredicate` only reads condition columns, which are NOT overwritten. In this case Approach A is safe. However, this requires checking the `build_input_to_layout_map` at activation time.

---

### 2. NextSingleJoin GatherResult — UNSAFE

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextSingleJoin` (line 1487)
**Read site**: Line 1514–1525

```cpp
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
```

**What it reads**: All RHS output columns (`ht.output_columns`), including build payload columns. The `output_columns` vector contains layout column indices that can point into the build payload area.

**When triggered**: When `join_type == JoinType::SINGLE`. This join type is used for correlated scalar subqueries (e.g., `SELECT a, (SELECT test.a), c FROM test, test2 WHERE test.b = test2.b`).

**Impact**: If Approach A overwrites build payload, the `GatherResult` call reads the `uint32_t` dictionary index instead of actual column data. For the test case `test_join.test:43`, column values like `11, 12` were replaced by dictionary indices like `0, 1`.

**Can it be guarded?**: **YES.** Two options:
1. **Disable Approach A for SINGLE joins**: Check `join_type != JoinType::SINGLE` at activation time.
2. **Add dict emission support to NextSingleJoin**: Add a `use_dict_emission` guard in `NextSingleJoin` (similar to the one in `NextInnerJoin`) that uses `vector.Dictionary(dict_arrays[i], build_sel_vec)` instead of `GatherResult`. This is more complex because `NextSingleJoin` has special NULL-filling logic for unmatched rows.

Option 1 is simpler and sufficient: disable Approach A for SINGLE joins.

---

### 3. NextInnerJoin Fast Path (no chains) — SAFE (already guarded)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextInnerJoin` (line 1093)
**Read site**: Lines 1140–1161

Already has `if (ht.use_dict_emission)` guard. When dict emission is active, uses `vector.Dictionary(dict_arrays[i], build_sel_vec)` instead of `GatherResult`. When dict emission is inactive, falls through to `GatherResult` — but Approach A would only be active when `use_dict_emission = true`.

**Verdict**: **SAFE** — correctly guarded.

---

### 4. NextInnerJoin Compaction Path (chains longer than one) — SAFE (already guarded)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextInnerJoin` (line 1175)
**Read site**: Lines 1183–1203

Already has `if (ht.use_dict_emission)` guard. Same logic as the fast path but using `rhs_pointers`.

**Verdict**: **SAFE** — correctly guarded.

---

### 5. ScanFullOuter — SAFE (already guarded)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `JoinHashTable::ScanFullOuter` (line 1551)
**Read site**: Lines 1607–1624

Already has `if (use_dict_emission && found_entries > 0)` guard. When dict emission is active, uses `vector.Dictionary(dict_arrays[i], build_sel_vec)`. The else path at line 1618–1624 uses `data_collection->Gather`, but this only executes when `use_dict_emission` is false.

**Additional read at line 1573**: `Load<bool>(row_locations[i] + tuple_size)` reads the `found_bool` at offset `tuple_size`. This is OUTSIDE the build payload area (it's after the last build column). **Not affected by Approach A.**

**Verdict**: **SAFE** — correctly guarded. The `found_bool` read is at a different offset.

---

### 6. PerformKeyComparison Gather — SAFE

**File**: `src/execution/join_hashtable.cpp`
**Function**: `PerformKeyComparison` (line 535)
**Read site**: Line 544

```cpp
data_collection.Gather(row_locations, state.keys_to_compare_sel, count,
                       ht.equality_predicate_columns, state.lhs_data, ...);
```

Gathers only `equality_predicate_columns`, which are condition columns (indices < `condition_types.size()`). These are NOT in the build payload area.

**Verdict**: **SAFE** — reads only condition columns.

---

### 7. RowMatcher::Match (non-equality predicates) — SAFE

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::ResolvePredicates` (line 940)
**Read site**: Line 957

```cpp
result_count = matcher->Match(keys, key_state.vector_data, match_sel, this->count,
                               pointers, no_match_sel, no_match_count);
```

The `RowMatcher` is initialized with `non_equality_predicate_columns` (line 96), which are indices into `condition_types` — i.e., condition columns only. The `Match` function reads directly from row pointers at condition column offsets using the layout. It does NOT access build payload columns.

**Important distinction**: Non-equality predicates (e.g., `a > b` in `ON a = x AND a > b`) reference **condition columns** only. Residual predicates (e.g., `l.amount + r.budget > 1100`) are arbitrary expressions that can reference **any** column. They are handled by a completely different code path (`ApplyResidualPredicate`).

**Verdict**: **SAFE** — reads only condition columns.

---

### 8. ScanKeyColumn — SAFE

**File**: `src/execution/join_hashtable.cpp`
**Function**: `JoinHashTable::ScanKeyColumn` (line 1646)
**Read site**: Line 1659

Called by filter pushdown (`physical_hash_join.cpp:876`) with `build_idx = join_condition[filter_idx]`, which is a condition column index. Also called by perfect hash join executor on column 0.

**Verdict**: **SAFE** — reads only condition columns.

---

### 9. BuildDictionaryArrays Gather — SAFE (temporal ordering)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `JoinHashTable::BuildDictionaryArrays` (line 1663)
**Read site**: Line 1711

```cpp
dc.Gather(row_locations, sel, build_count, output_col_idx, vec, sel, nullptr);
```

This reads build payload columns — but it executes in Step 3 of `BuildDictionaryArrays`, BEFORE the proposed Step 4 (Approach A overwrite). By the time the overwrite would happen, the data has already been gathered into columnar dict arrays.

**Verdict**: **SAFE** — temporal ordering ensures data is read before overwrite.

---

### 10. NextLeftJoin — SAFE (delegates to NextInnerJoin)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextLeftJoin` (line 1450)

Calls `NextInnerJoin` for matched rows (already guarded). For unmatched rows (lines 1468–1482), sets build columns to `CONSTANT NULL` — does NOT read from serialized rows.

**Verdict**: **SAFE**.

---

### 11. NextSemiOrAntiJoin — SAFE (no build column emission)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextSemiOrAntiJoin` (line 1230)

Only emits probe-side columns. Does NOT call `GatherResult` at all. (But `ScanKeyMatches` → `ResolvePredicates` → `ApplyResidualPredicate` can still read build payload — covered under item #1.)

**Verdict**: **SAFE** for emission. **See item #1** for residual predicate concern.

---

### 12. NextRightSemiOrAntiJoin — SAFE (no payload emission)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextRightSemiOrAntiJoin` (line 1279)

Calls `ResolvePredicates` (line 1283) — **see item #1 for residual predicate concern**. But does not emit build payload columns; only writes `found_bool` and follows NEXT pointers.

Line 1290: `Load<bool>(ptr + ht.tuple_size)` — reads `found_bool`, not build payload. Safe.
Line 1301: `LoadPointer(ptr + ht.pointer_offset)` — reads NEXT pointer, not build payload. Safe.

**Verdict**: **SAFE** for direct reads. **See item #1** for residual predicate concern.

---

### 13. NextMarkJoin — SAFE (no build payload emission)

**File**: `src/execution/join_hashtable.cpp`
**Function**: `ScanStructure::NextMarkJoin` (line 1371)

Calls `ScanKeyMatches` (which can trigger `ApplyResidualPredicate` — **see item #1**). Constructs a boolean result vector — does NOT call `GatherResult` or emit build payload columns.

**Verdict**: **SAFE** for emission. **See item #1** for residual predicate concern.

---

### 14. Parallel Probe Threads — SAFE (read-only after overwrite)

After `BuildDictionaryArrays` writes the dictionary indices (single-threaded, before `ScheduleFinalize`), the parallel Finalize workers only access the `pointer_offset` field (hash → NEXT pointer) via `InsertHashes`. They do NOT read build payload bytes.

During probe, multiple threads may read the same row's build payload area concurrently (when multiple probe rows match the same build row). With Approach A, they would all read the same `uint32_t` dictionary index — this is a read-only access with no data race.

**Verdict**: **SAFE** — no write contention after the single-threaded overwrite.

---

### 15. External Hash Join — NOT APPLICABLE

External hash joins are excluded by the `!sink.external` guard at the activation site (`physical_hash_join.cpp:1143`). `BuildDictionaryArrays` is never called for external joins.

**Verdict**: **N/A** — already excluded.

---

### 16. Verification Infrastructure (PRAGMA enable_verification) — SAFE

DuckDB's verification system re-executes the entire query from scratch through the full pipeline. It does NOT directly inspect hash table row storage. Each re-execution builds its own hash table with its own data. Therefore, overwriting build payload in one execution does not affect verification re-executions.

**Verdict**: **SAFE** — verification uses independent pipeline executions.

---

## Summary Table

| # | Code Path | Reads Build Payload? | Unsafe with Approach A? | Guardable? |
|---|-----------|---------------------|------------------------|------------|
| 1 | `ApplyResidualPredicate` | YES — residual predicate columns | **YES** | YES — disable when `residual_predicate != nullptr` and map contains payload column |
| 2 | `NextSingleJoin::GatherResult` | YES — all output columns | **YES** | YES — disable when `join_type == SINGLE` |
| 3 | `NextInnerJoin` fast path | YES but guarded | SAFE | Already guarded |
| 4 | `NextInnerJoin` compaction path | YES but guarded | SAFE | Already guarded |
| 5 | `ScanFullOuter` | YES but guarded | SAFE | Already guarded |
| 6 | `PerformKeyComparison` | Condition cols only | SAFE | N/A |
| 7 | `RowMatcher::Match` | Condition cols only | SAFE | N/A |
| 8 | `ScanKeyColumn` | Condition cols only | SAFE | N/A |
| 9 | `BuildDictionaryArrays` Gather | YES but before overwrite | SAFE | Temporal ordering |
| 10 | `NextLeftJoin` (unmatched) | No — sets NULL | SAFE | N/A |
| 11 | `NextSemiOrAntiJoin` | No payload emission | SAFE* | *See #1 |
| 12 | `NextRightSemiOrAntiJoin` | No payload emission | SAFE* | *See #1 |
| 13 | `NextMarkJoin` | No payload emission | SAFE* | *See #1 |
| 14 | Parallel probe threads | Read-only | SAFE | N/A |
| 15 | External hash join | N/A — excluded | SAFE | N/A |
| 16 | Verification (PRAGMA) | Independent execution | SAFE | N/A |

---

## Conclusion

**Approach A is viable with two additional fallback conditions.** The build-side payload area is NOT unconditionally dead data — two code paths read from it after `BuildDictionaryArrays`:

1. **`ApplyResidualPredicate`** reads build payload columns referenced by residual filter expressions.
2. **`NextSingleJoin`** reads all build output columns for correlated scalar subquery emission.

Both can be guarded by adding activation-time checks. The complete set of activation conditions for Approach A would be:

```cpp
bool can_use_approach_a =
    !sink.external &&                           // not external join
    ht.Count() > 0 &&                           // non-empty build side
    ht.Count() <= DICT_EMISSION_MAX_ROWS &&     // within threshold
    join_type != JoinType::SINGLE &&            // not a correlated subquery join
    !has_residual_reading_payload(ht);          // no residual predicate reads payload cols
```

Where `has_residual_reading_payload` checks:

```cpp
bool has_residual_reading_payload(const JoinHashTable &ht) {
    if (!ht.residual_predicate || !ht.residual_info) {
        return false;  // no residual predicate — safe
    }
    for (const auto &entry : ht.residual_info->build_input_to_layout_map) {
        if (entry.second >= ht.condition_types.size()) {
            return true;  // residual reads a build payload column — unsafe
        }
    }
    return false;  // residual only reads condition columns — safe
}
```

### Conservative Alternative

A simpler but more conservative activation condition:

```cpp
bool can_use_approach_a =
    !sink.external &&
    ht.Count() > 0 &&
    ht.Count() <= DICT_EMISSION_MAX_ROWS &&
    join_type != JoinType::SINGLE &&
    !ht.residual_predicate;                     // no residual predicate at all
```

This is easier to reason about and covers all residual predicate cases, at the cost of disabling Approach A for any join with a residual filter — even when the residual only references condition columns.

### Impact of Fallback Conditions

- **SINGLE join exclusion**: Correlated scalar subqueries are relatively rare. Most hash joins are INNER, LEFT, RIGHT, or OUTER. This exclusion has minimal impact.
- **Residual predicate exclusion**: Residual predicates occur when the join has filter conditions beyond simple equality (e.g., `ON a = b AND x + y > 100`). These are less common than pure equality joins but not rare. The conservative exclusion (any residual → disable) may exclude some joins unnecessarily. The precise exclusion (check which columns the residual reads) is more targeted but requires inspecting `build_input_to_layout_map` at activation time.

### When Approach A Cannot Apply

For joins excluded by these conditions, the system should fall back to the existing pointer arithmetic approach (as implemented on the `project1-fix1-pointer-arithmetic` branch). The pointer arithmetic approach (`PtrToDictIdx` via block directory binary search + division) works universally and does not depend on the overwrite being safe.

### Recommendation

Implement Approach A with the fallback conditions above, using the pointer arithmetic approach as the fallback for excluded cases. This gives optimal probe-time performance (single `Load<uint32_t>`) for the common case (pure equality INNER/LEFT/RIGHT/OUTER joins without residual predicates), while maintaining correctness for all edge cases via the pointer arithmetic fallback.
