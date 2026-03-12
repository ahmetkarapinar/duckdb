# Salt Overwrite Feasibility Analysis

## Idea

After `Finalize()`, overwrite the upper 16 salt bits of each occupied `ht_entry_t` slot with the dictionary index of the row it points to. At probe time, after a key match, read the dict index directly from the salt bits — no payload read, no map, no arithmetic.

---

## 1. Is the Salt Still Read During Probe?

**Yes — the salt is actively used during every probe operation.**

The dispatch in `JoinHashTable::GetRowPointers` (`join_hashtable.cpp:344`) calls:

```cpp
if (UseSalt()) {
    GetRowPointersInternal<true>(...);  // USE_SALTS = true
} else {
    GetRowPointersInternal<false>(...); // USE_SALTS = false
}
```

`UseSalt()` returns `this->capacity > USE_SALT_THRESHOLD` (line 338).

- `USE_SALT_THRESHOLD = 8192` (line 87 of `join_hashtable.hpp`)
- `MINIMUM_CAPACITY = 16384` (line 455 of `join_hashtable.hpp`)
- `PointerTableCapacity` returns `max(NextPowerOfTwo(count * load_factor), MINIMUM_CAPACITY)` with `load_factor = 2.0`

**Since MINIMUM_CAPACITY (16,384) > USE_SALT_THRESHOLD (8,192), the salt is ALWAYS active**, regardless of build-side size. Even a 1-row build side gets capacity 16,384, which exceeds 8,192.

The salt is read in `ProbeForPointersInternal<true>` (lines 205–228):

```cpp
while (true) {
    const ht_entry_t entry = entries[row_ht_offset];
    if (!entry.IsOccupied()) break;

    const hash_t row_salt = ht_entry_t::ExtractSalt(row_hash);
    const bool salt_match = entry.GetSalt() == row_salt;
    if (salt_match) {
        // salt matches -> compare the keys
        AddPointerToCompare(...);
        break;
    }
    // salt mismatch -> continue linear probing
    IncrementAndWrap(row_ht_offset, ht.bitmask);
}
```

The probe loop uses the salt to decide whether to compare keys or skip a slot. This is not a hint or optional optimization — it is the core probe-skip mechanism for the linear probing scheme.

---

## 2. What Do `GetSalt()` and `ExtractSalt()` Do?

From `ht_entry.hpp`:

```cpp
static inline hash_t ExtractSalt(const hash_t &hash) {
    return hash | POINTER_MASK;  // 0x0000FFFFFFFFFFFF
}

inline hash_t GetSalt() const {
    return ExtractSalt(value);   // value | POINTER_MASK
}
```

Both operations set the lower 48 bits to all 1s, preserving only the upper 16 bits for comparison. The comparison `entry.GetSalt() == row_salt` is effectively:

```
(entry_value | 0x0000FFFFFFFFFFFF) == (probe_hash | 0x0000FFFFFFFFFFFF)
```

This compares **only the upper 16 bits** of the entry against the upper 16 bits of the probe hash.

**If the salt bits are overwritten with a dict index**, `GetSalt()` would return `(dict_index_shifted_to_upper_16 | POINTER_MASK)`. This would NOT match `ExtractSalt(probe_hash)` unless the dict index accidentally equals the hash's upper 16 bits. Probe would skip nearly every occupied slot, producing incorrect empty results.

---

## 3. Is the Salt Check Skipped for Small Hash Tables?

**No — the salt check is never skipped for any JoinHashTable.**

As shown above, `MINIMUM_CAPACITY = 16,384 > USE_SALT_THRESHOLD = 8,192`, so `UseSalt()` always returns `true`. The `USE_SALTS=false` template instantiation is dead code for all practical JoinHashTable configurations.

This is the critical blocker: there is no size regime where salt checking is disabled and the salt bits become "free" for alternative use.

---

## 4. Duplicate Key Chains

In DuckDB's join hash table, duplicate keys (rows with identical join keys) are chained via `NEXT` pointers embedded in the row data at `pointer_offset`. Only the **chain head** occupies an `ht_entry_t` slot. Subsequent chain members are linked through in-row pointers and never appear in the `ht_entry_t` array.

If salt were overwritten with a dict index, only the chain head's index would be stored. The probe phase follows NEXT pointers to walk the chain (`NextInnerJoin` slow path), so chain members' dict indices would need a separate mechanism anyway. This is a secondary problem — the chain head's dict index alone is insufficient for joins with duplicate build keys.

---

## 5. Displaced Keys (Linear Probe Collisions)

When two **different** keys hash to the same slot, one is displaced via linear probing to the next free slot. During probe, the salt mismatch is what allows the probe to **skip** a displaced entry that doesn't belong to the probed key:

```
Slot 7: [salt_A | ptr_A]  ← Key A's home slot
Slot 8: [salt_B | ptr_B]  ← Key B displaced here (different salt)
```

When probing for Key A at slot 7, salt matches → compare keys. When probing for Key B at slot 7, `salt_B ≠ salt_A` → skip slot 7, advance to slot 8 → salt matches → compare keys.

**If both salts are overwritten with dict indices**, the probe loop would compare `dict_index_A` against `ExtractSalt(probe_hash_B)`. Since dict indices are sequential integers (0, 1, 2, ...) and hash salts are pseudo-random, almost no salt comparisons would match. The probe would:
1. Fail to find entries that ARE present (false negatives), or
2. Probe past the correct entry and find wrong entries (correctness violation)

This fundamentally breaks the linear probing invariant.

---

## 6. Thread Safety Assessment

The proposed overwrite timing would be:

```
BuildDictionaryArrays()     ← single-threaded, pre-finalize
ScheduleFinalize()          ← parallel workers write ht_entry_t slots
[proposed salt overwrite]   ← single-threaded, post-finalize
```

If the salt overwrite were to happen **after** all `HashJoinFinalizeTask` workers complete (i.e., in a post-finalize event), it would be safe from a concurrency perspective — no concurrent readers or writers exist at that point. `ScheduleFinalize` launches tasks via `SetTasks()` which are waited on by the pipeline event system before probe begins.

However, `BuildDictionaryArrays` currently runs **before** `ScheduleFinalize`. The `ht_entry_t` slots do not exist yet at that point (they are allocated and populated during `ScheduleFinalize`). The salt overwrite would need to be a separate post-finalize step.

**Thread safety is NOT a blocker**, but the implementation would need a new post-finalize hook.

---

## 7. 16-Bit Limit

The salt field is exactly 16 bits (bits 63–48). A `uint16_t` can hold values 0–65,535. To use this space for a dict index:

- `DICT_EMISSION_MAX_ROWS` would need to be capped at **65,535** (`2^16 - 1`)
- The current threshold is **1,048,576** (1M rows)
- This is a **16× reduction** in the applicable range

For the supervisor's stated use case (small build sides), 65k rows is still generous — most star-schema dimension tables (nation, region, supplier) have far fewer rows. But it would exclude medium-sized dimension tables that the current approach handles.

---

## Conclusion: NOT FEASIBLE Without Fundamental Probe Changes

**The salt overwrite approach is not viable as a drop-in optimization.** The salt bits are actively used during probe to implement the linear probing skip logic. Overwriting them would break probe correctness for any hash table with collisions (i.e., virtually all hash tables).

### Specific Blockers

| Issue | Severity | Can It Be Worked Around? |
|-------|----------|--------------------------|
| Salt used during probe to skip non-matching slots | **Fatal** | Would require rewriting the probe loop to not use salt, falling back to full key comparison at every occupied slot — negating the performance benefit |
| `MINIMUM_CAPACITY > USE_SALT_THRESHOLD` means salt is always active | **Fatal** | Could lower `MINIMUM_CAPACITY` or raise `USE_SALT_THRESHOLD`, but this would degrade probe performance for ALL hash joins, not just dict-emission ones |
| Only chain head in `ht_entry_t` — chain members have no slot | **Moderate** | Would still need NEXT-pointer traversal for duplicates, limiting the benefit |
| 16-bit limit reduces applicable range by 16× | **Minor** | Acceptable for the target use case |

### What Would Be Needed to Make It Work

1. **Disable salt checking** when dict emission is active, by adding a flag like `skip_salt_in_probe` that forces `GetRowPointersInternal<false>` regardless of capacity. This means every occupied slot during probe triggers a full key comparison — which is more expensive for tables with collisions.

2. **Add a post-finalize step** to overwrite salt bits after `ScheduleFinalize` completes.

3. **Handle duplicate chains** separately — the salt only stores the chain head's index; chain members still need NEXT-pointer traversal with `Load<uint32_t>` from the row payload or a similar mechanism.

4. **Cap `DICT_EMISSION_MAX_ROWS` at 65,535.**

### Cost-Benefit

Disabling salt to store dict indices trades **probe-phase skip efficiency** (salt-based filtering) for **post-match dict index locality** (reading from `ht_entry_t` instead of row payload). For small build sides where the hash table fits in cache, the salt check provides little benefit anyway (few collisions, table in L1/L2). However, the current embedded-payload approach (`Load<uint32_t>(ptr + offset)`) already achieves near-zero overhead since the row pointer is in cache from key matching. The marginal gain from reading from `ht_entry_t` instead of the row is negligible.

**The embedded payload approach (current `embedded_dict_idx` branch) is strictly superior**: it works with any build-side size up to 1M rows, doesn't require disabling salt, doesn't reduce the applicable range, handles duplicate chains naturally (each row has its own embedded index), and has no impact on probe correctness.
