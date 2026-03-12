# DuckDB Hash Aggregate — Deep Dive

---

## 0. Big Picture: What Does a Hash Aggregate Do?

Consider this query:

```sql
SELECT city, SUM(amount), COUNT(*)
FROM orders
GROUP BY city
```

With input rows:

```
┌────────────┬────────┐
│    city    │ amount │
├────────────┼────────┤
│  Berlin    │  100   │
│  Paris     │  200   │
│  Berlin    │  300   │
│  London    │  150   │
│  Paris     │   50   │
└────────────┴────────┘
```

Goal: **one output row per unique city**, with running totals accumulated in place:

```
┌────────────┬─────┬───────┐
│    city    │ SUM │ COUNT │
├────────────┼─────┼───────┤
│  Berlin    │ 400 │   2   │
│  Paris     │ 250 │   2   │
│  London    │ 150 │   1   │
└────────────┴─────┴───────┘
```

DuckDB does this with a **hash table** where every distinct group key lives in exactly one slot, and each slot carries the running aggregate state. The process has three major parts:

```
    INPUT ROWS
        │
        ▼
┌─────────────────────┐
│  1. HASH the group  │  hash("Berlin") → slot 2
│     key(s)          │  hash("Paris")  → slot 5
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  2. FIND OR CREATE  │  slot 2 empty? → create new group row
│     a group row     │  slot 2 taken? → follow to existing row
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  3. ACCUMULATE      │  row_Berlin.sum += 100
│     into that row   │  row_Berlin.sum += 300 (second Berlin row)
└─────────────────────┘
```

---

## 1. Core Data Structures

### 1.1 Two Separate Memory Regions

The aggregate HT keeps two distinct memory areas:

```
REGION A — Pointer table (entries[])
─────────────────────────────────────────────────────────────────
  Flat array of ht_entry_t values, one per slot.
  Each is 8 bytes: [SALT 16b | POINTER 48b]
  Indexed by: hash(group_key) & bitmask

   slot 0  slot 1  slot 2  slot 3  slot 4  slot 5  slot 6  slot 7
  ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
  │empty │empty │ A+p  │empty │empty │ B+p  │empty │ C+p  │
  └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘
                    │                    │               │
                    ▼                    ▼               ▼
REGION B — Row storage (PartitionedTupleData)
─────────────────────────────────────────────────────────────────
  Contiguous serialized rows, one per distinct group.
  Each row has three sections:

  ┌─────────────────┬──────────────────────┬──────────────┐
  │  GROUP COLUMNS  │   AGGREGATE STATE    │  HASH (8 B)  │
  │  city="Berlin"  │  SUM=400, COUNT=2    │ 0xAAAA...    │
  └─────────────────┴──────────────────────┴──────────────┘
  ← data_width ────→ ← aggr_width ─────── → ← 8 bytes ──→
                      ↑ GetAggrOffset()       ↑ hash_offset
```

The pointer table is tiny and cache-hot. The row storage holds all the real data and is accessed only after a pointer is resolved.

---

### 1.2 The `ht_entry_t` in Detail

```
63        48 47                                              0
┌──────────┬─────────────────────────────────────────────────┐
│  SALT    │                  POINTER                        │
│ (16 bits)│              (48 bits)                          │
└──────────┴─────────────────────────────────────────────────┘

SALT    = upper 16 bits of the hash, with the lower 48 bits
          all forced to 1 (so salt comparisons use "entry | POINTER_MASK")
POINTER = address of the serialized group row in Region B

Special value: 0x0000000000000000 = EMPTY SLOT
```

**Why store salt?**

When probing for a group key, we first compare just the 16-bit salt. If salt doesn't match, we skip the full key comparison (which would require dereferencing the pointer and comparing the actual group column bytes). Salt acts as a cheap bloom filter.

---

### 1.3 A Single Serialized Group Row

For the query `SELECT city, SUM(amount), COUNT(*) GROUP BY city`:

```
Byte layout of one row (example: city="Berlin", SUM=400, COUNT=2):

offset 0                  offset A             offset H
│                         │                    │
▼                         ▼                    ▼
┌─────────────────────────┬────────┬────────┬──────────────┐
│  city  (varchar, 6 B)   │  SUM   │ COUNT  │  HASH (8 B)  │
│  "Berlin"               │  400   │   2    │ 0xAAAA...    │
└─────────────────────────┴────────┴────────┴──────────────┘

A = GetAggrOffset()  ← pointer is advanced here before UpdateAggregates
H = hash_offset      ← stored so Resize() can re-read it cheaply
```

---

### 1.4 The `addresses[]` Vector — the Glue Between Regions

Throughout execution, the central working array is `state.addresses[]`:

```
After FindOrCreateGroups:            After AddInPlace(aggr_offset):
  addresses[] points to              addresses[] points to
  START of each group row            AGGREGATE STATE of each group row

  input row 0 → ptr to ROW_Berlin    input row 0 → ptr to ROW_Berlin + A
  input row 1 → ptr to ROW_Paris     input row 1 → ptr to ROW_Paris  + A
  input row 2 → ptr to ROW_Berlin    input row 2 → ptr to ROW_Berlin + A
  input row 3 → ptr to ROW_London    input row 3 → ptr to ROW_London + A
  input row 4 → ptr to ROW_Paris     input row 4 → ptr to ROW_Paris  + A
```

`UpdateAggregates` reads `payload[i]` and writes into `addresses[i]` directly — no extra indirection.

---

## 2. `AddChunk` — Decision Flow

When a chunk of input rows arrives, `AddChunk` decides which path to take:

```
AddChunk(groups, payload, filter)
           │
           ├─── Is groups column CONSTANT_VECTOR?
           │         YES → TryAddConstantGroups
           │               (1 HT lookup, broadcast address to all N rows)
           │
           ├─── Is groups column DICTIONARY_VECTOR?
           │         YES → TryAddDictionaryGroups
           │               (D HT lookups for D distinct codes, scatter to N rows)
           │
           └─── Otherwise: Regular path
                    │
                    ├── 1. groups.Hash(hashes)
                    ├── 2. FindOrCreateGroups(groups, hashes, addresses, new_groups)
                    ├── 3. AddInPlace(addresses, aggr_offset, N)
                    └── 4. UpdateAggregates(payload, filter)
```

---

## 3. Salt-Based Linear Probing

When the HT has a collision (two different keys land on the same slot), it probes to the next slot. Unlike the join HT (which always does `+1`), the aggregate HT uses a **variable odd stride**:

```
stride = (top 5 bits of salt) | 1

Why odd? Because:
  table size is always a power of 2 (e.g. 8 = 0b1000)
  any ODD number is coprime with every power of 2
  → stride N visits ALL slots before repeating (it's a permutation)

Example with table size 8, starting at slot 2, stride 3:
  2 → 5 → 0 → 3 → 6 → 1 → 4 → 7 → (back to 2)
  Every slot visited exactly once ✓
```

Different keys get different strides (from their hash), so they "spread out" independently — less clustering than the always-+1 approach.

```
hash=0xAAAA...  →  stride = (0xAAAA >> 59) | 1 = (10101₂) | 1 = 21
hash=0xBBBB...  →  stride = (0xBBBB >> 59) | 1 = (10111₂) | 1 = 23
hash=0xCCCC...  →  stride = (0xCCCC >> 59) | 1 = (11001₂) | 1 = 25
```

---

## 4. `FindOrCreateGroups` — The Core Loop, Step by Step

**File:** `src/execution/aggregate_hashtable.cpp:641`

### Inputs and outputs

```
INPUT:
  groups      — DataChunk with the GROUP BY column(s), N rows
  hashes      — Vector<hash_t>, one hash per row

OUTPUT:
  addresses[] — Vector<pointer>, one row-pointer per input row
                (pointing to the group row in Region B)
  return value — count of newly created groups
```

---

### Step 1 — Compute initial slot and salt for every row

```
for r in [0, N):
    ht_offsets[r] = hashes[r] & bitmask    ← lower bits of hash = initial slot
    hash_salts[r] = ExtractSalt(hashes[r]) ← upper 16 bits, lower forced to 1
```

Example with N=5, bitmask=7:

```
  row │ group    │  hash              │ ht_offset │  salt
 ─────┼──────────┼────────────────────┼───────────┼────────────────────
   0  │ Berlin   │ 0xAAAA000000000002 │     2     │ 0xAAAAFFFFFFFFFFFF
   1  │ Paris    │ 0xBBBB000000000005 │     5     │ 0xBBBBFFFFFFFFFFFF
   2  │ Berlin   │ 0xAAAA000000000002 │     2     │ 0xAAAAFFFFFFFFFFFF
   3  │ London   │ 0xCCCC000000000007 │     7     │ 0xCCCCFFFFFFFFFFFF
   4  │ Paris    │ 0xBBBB000000000005 │     5     │ 0xBBBBFFFFFFFFFFFF
```

A second warm-up pass reads `entries[ht_offsets[r]]` for each row. This is a branchless no-op for the result, but it pre-fetches those cache lines before the main loop needs them.

---

### Step 2 — Inner loop: classify each row

For each row still being processed, the inner loop checks the slot at `ht_offsets[i]` and routes the row to one of three outcomes:

```
┌─────────────────────────────────────────────────────────────────────┐
│  For row i:                                                          │
│                                                                      │
│  entry = entries[ht_offsets[i]]                                      │
│                                                                      │
│  ┌─── entry is EMPTY? ──────────────────────────────────────────┐   │
│  │  YES → claim it: entry.SetSalt(salt[i])                      │   │
│  │         add i to  empty_vector[]                             │   │
│  │         DONE for row i this iteration                        │   │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌─── entry OCCUPIED, GetSalt() == salt[i]? ────────────────────┐   │
│  │  YES → possible match: add i to  compare_vector[]            │   │
│  │         DONE for row i this iteration                        │   │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌─── entry OCCUPIED, salt MISMATCH ────────────────────────────┐   │
│  │  → SaltIncrementAndWrap(ht_offsets[i], salt[i], mask)        │   │
│  │     row i stays in remaining set → retried next iteration    │   │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

Traced for our 5 rows (HT starts empty):

```
BEFORE iteration 1:
  entries[]: all empty
  remaining = {0,1,2,3,4}

Processing row 0 (Berlin, slot=2):
  entries[2] → EMPTY
  → SetSalt(0xAAAA...) at slot 2
  → empty_vector += 0

Processing row 1 (Paris, slot=5):
  entries[5] → EMPTY
  → SetSalt(0xBBBB...) at slot 5
  → empty_vector += 1

Processing row 2 (Berlin, slot=2):
  entries[2] → OCCUPIED, salt=0xAAAA... == 0xAAAA...  ← same salt as row 0
  → compare_vector += 2

Processing row 3 (London, slot=7):
  entries[7] → EMPTY
  → SetSalt(0xCCCC...) at slot 7
  → empty_vector += 3

Processing row 4 (Paris, slot=5):
  entries[5] → OCCUPIED, salt=0xBBBB... == 0xBBBB...  ← same salt as row 1
  → compare_vector += 4

AFTER classification:
  empty_vector   = [0, 1, 3]     ← 3 new groups
  compare_vector = [2, 4]        ← 2 need key comparison
  remaining_no_match = []        ← 0 salt mismatches
```

---

### Step 3 — Handle `empty_vector` (new groups)

Rows in `empty_vector` claimed an empty slot. Now we:
1. Append a new row to Region B (group key + zeroed aggregate state + hash)
2. Write the pointer back into the slot
3. Record the row address in `addresses[]`

```
BEFORE (Region B is empty, entries[] has only salts, no pointers yet):

  entries[]:
  slot 0  slot 1  slot 2        slot 3  slot 4  slot 5        slot 6  slot 7
 ┌──────┬──────┬──────────────┬──────┬──────┬──────────────┬──────┬──────────────┐
 │empty │empty │ salt=0xAAAA  │empty │empty │ salt=0xBBBB  │empty │ salt=0xCCCC  │
 │      │      │ ptr=NULL     │      │      │ ptr=NULL     │      │ ptr=NULL     │
 └──────┴──────┴──────────────┴──────┴──────┴──────────────┴──────┴──────────────┘
  (salts were set in Step 2, but pointers are still empty)

ACTION for empty_vector = [0, 1, 3]:

  AppendUnified → allocate 3 rows in Region B:
    ROW_Berlin @ 0x1000: ["Berlin" | SUM=0 | COUNT=0 | hash=0xAAAA...]
    ROW_Paris  @ 0x1100: ["Paris"  | SUM=0 | COUNT=0 | hash=0xBBBB...]
    ROW_London @ 0x1200: ["London" | SUM=0 | COUNT=0 | hash=0xCCCC...]

  RowOperations::InitializeStates → SUM=0, COUNT=0 already set above

  Write pointers into entries[]:
    entries[2].SetPointer(0x1000)
    entries[5].SetPointer(0x1100)
    entries[7].SetPointer(0x1200)

  Record in addresses[]:
    addresses[0] = 0x1000
    addresses[1] = 0x1100
    addresses[3] = 0x1200

AFTER (Region B and entries[] both updated):

  entries[]:
  slot 2                   slot 5                   slot 7
 ┌──────────────────────┬──────────────────────┬──────────────────────┐
 │ salt=0xAAAA          │ salt=0xBBBB          │ salt=0xCCCC          │
 │ ptr ──────────────┐  │ ptr ──────────────┐  │ ptr ──────────────┐  │
 └───────────────────│──┴───────────────────│──┴───────────────────│──┘
                     │                      │                      │
                     ▼                      ▼                      ▼
                 ROW_Berlin             ROW_Paris              ROW_London
              [Berlin|0|0|0xAA]     [Paris|0|0|0xBB]      [London|0|0|0xCC]
```

---

### Step 4 — Handle `compare_vector` (key comparison)

Rows in `compare_vector` reached a slot that is already occupied with a matching salt. We now **compare the actual key bytes** to confirm whether it's the same group or just a hash salt collision.

```
compare_vector = [2, 4]

For row 2 (Berlin):
  addresses[2] = entries[2].GetPointer() = 0x1000  (ROW_Berlin)
  Compare: input[2].city "Berlin" == ROW_Berlin.city "Berlin"  → MATCH ✓
  addresses[2] = 0x1000  ← confirmed, no change needed

For row 4 (Paris):
  addresses[4] = entries[5].GetPointer() = 0x1100  (ROW_Paris)
  Compare: input[4].city "Paris" == ROW_Paris.city "Paris"     → MATCH ✓
  addresses[4] = 0x1100  ← confirmed

no_match_count = 0  (no false salt hits in this example)
```

If there had been a false salt match (two different cities with the same salt), those rows would go into `no_match_vector` and probe to the next slot in the next iteration.

---

### Step 5 — End of loop (remaining = 0)

```
remaining_entries = 0 → loop exits

Final addresses[] state:
  ┌───┬──────────┬─────────────────────────────────────────────────────────┐
  │ i │  group   │  addresses[i]                                           │
  ├───┼──────────┼─────────────────────────────────────────────────────────┤
  │ 0 │ Berlin   │ 0x1000  → ROW_Berlin  (new group, created in Step 3)    │
  │ 1 │ Paris    │ 0x1100  → ROW_Paris   (new group, created in Step 3)    │
  │ 2 │ Berlin   │ 0x1000  → ROW_Berlin  (existing group, matched Step 4)  │
  │ 3 │ London   │ 0x1200  → ROW_London  (new group, created in Step 3)    │
  │ 4 │ Paris    │ 0x1100  → ROW_Paris   (existing group, matched Step 4)  │
  └───┴──────────┴─────────────────────────────────────────────────────────┘

Note: rows 0 and 2 share addresses[0]==addresses[2]==0x1000 → SAME row in Region B.
      rows 1 and 4 share addresses[1]==addresses[4]==0x1100 → SAME row in Region B.
```

---

### Step 6 — Advance addresses to the aggregate region

`addresses[]` currently points to the **beginning** of each row (the group-column region). Before calling `UpdateAggregates`, they must be shifted forward to the **aggregate state region**:

```
BEFORE AddInPlace:

  addresses[0] → 0x1000
                 │
                 ▼
  ┌──────────────┬───────────────────┬──────────────┐
  │ city=Berlin  │  SUM=0  COUNT=0   │  hash=0xAAAA │
  │ ← data_width→│← aggr starts here │              │
  └──────────────┴───────────────────┴──────────────┘
  ^
  addresses[0] points here

AFTER AddInPlace(addresses, aggr_offset, 5):

  addresses[0] → 0x1000 + aggr_offset
                           │
                           ▼
  ┌──────────────┬─────────┴─────────┬──────────────┐
  │ city=Berlin  │  SUM=0  COUNT=0   │  hash=0xAAAA │
  └──────────────┴───────────────────┴──────────────┘
                  ^
                  addresses[0] points here now
```

All five `addresses[]` are shifted by the same `aggr_offset` in one vectorized operation.

---

### Step 7 — `UpdateAggregates`: accumulate payload into rows

```
UpdateAggregates processes each aggregate in order.

For SUM(amount):
  RowOperations::UpdateStates reads payload[i].amount and adds to *addresses[i]

  i=0: *addresses[0] = ROW_Berlin.sum += 100   → ROW_Berlin.sum = 100
  i=1: *addresses[1] = ROW_Paris.sum  += 200   → ROW_Paris.sum  = 200
  i=2: *addresses[2] = ROW_Berlin.sum += 300   → ROW_Berlin.sum = 400  ← same address!
  i=3: *addresses[3] = ROW_London.sum += 150   → ROW_London.sum = 150
  i=4: *addresses[4] = ROW_Paris.sum  +=  50   → ROW_Paris.sum  = 250  ← same address!

  AddInPlace(addresses, SUM_payload_size, 5)  ← advance past SUM state to COUNT state

For COUNT(*):
  i=0: ROW_Berlin.cnt += 1  → 1
  i=1: ROW_Paris.cnt  += 1  → 1
  i=2: ROW_Berlin.cnt += 1  → 2
  i=3: ROW_London.cnt += 1  → 1
  i=4: ROW_Paris.cnt  += 1  → 2

FINAL Region B state:

  ROW_Berlin @ 0x1000:
  ┌─────────┬─────┬───────┬────────────┐
  │ Berlin  │ 400 │   2   │ 0xAAAA...  │
  └─────────┴─────┴───────┴────────────┘

  ROW_Paris @ 0x1100:
  ┌─────────┬─────┬───────┬────────────┐
  │ Paris   │ 250 │   2   │ 0xBBBB...  │
  └─────────┴─────┴───────┴────────────┘

  ROW_London @ 0x1200:
  ┌─────────┬─────┬───────┬────────────┐
  │ London  │ 150 │   1   │ 0xCCCC...  │
  └─────────┴─────┴───────┴────────────┘
```

---

## 5. Collision Example (Different Keys, Same Slot)

Suppose after the first chunk, a second chunk arrives with two cities that **both hash to slot 2**:

```
Row codes (new chunk):
  i=0  city="Berlin"  hash=0xAAAA...0002  →  slot 2, salt=0xAAAA...
  i=1  city="Tokyo"   hash=0xDDDD...0002  →  slot 2, salt=0xDDDD...
```

**Iteration 1:**

```
HT state (slot 2 already holds ROW_Berlin, salt=0xAAAA...):

  slot 2: [salt=0xAAAA... | ptr=ROW_Berlin]

Row i=0 (Berlin):
  entries[2].GetSalt() = 0xAAAA... == salt[0] = 0xAAAA...  → compare_vector

Row i=1 (Tokyo):
  entries[2].GetSalt() = 0xAAAA... ≠ salt[1] = 0xDDDD...  → SALT MISMATCH
  SaltIncrementAndWrap: ht_offsets[1] = (2 + 29) & 7 = 31 & 7 = 7
  → row i=1 goes back into remaining set with new offset=7
```

**compare_vector for i=0:**

```
addresses[0] = ROW_Berlin
row_matcher: ROW_Berlin.city "Berlin" == input "Berlin"  → MATCH ✓
no_match: empty
```

**Iteration 2 (only row i=1 remaining, now at slot 7):**

```
entries[7]: occupied with ROW_London, salt=0xCCCC... ≠ 0xDDDD... → SALT MISMATCH
SaltIncrementAndWrap: ht_offsets[1] = (7 + 29) & 7 = 36 & 7 = 4

entries[4]: EMPTY → claim it
  empty_vector += i=1
```

**Row i=1 (Tokyo) is a new group, inserted at slot 4:**

```
HT state after second chunk:
  slot 2: [salt=0xAAAA... | ptr=ROW_Berlin]   ← unchanged
  slot 4: [salt=0xDDDD... | ptr=ROW_Tokyo ]   ← new, displaced twice
  slot 5: [salt=0xBBBB... | ptr=ROW_Paris ]   ← unchanged
  slot 7: [salt=0xCCCC... | ptr=ROW_London]   ← unchanged

  (Tokyo needed 2 probes: slot 2 → slot 7 → slot 4)
```

The probe path always mirrors the insert path, so future lookups for "Tokyo" will follow the same displacement.

---

## 6. Resize

Resize triggers when: `Count() > capacity × (1/LOAD_FACTOR) = capacity × 0.667`

```
Before resize (capacity=8, count=5, threshold=5):

  entries[8]:
  ┌──────┬──────┬──────┬──────┬──────┬──────┬──────┬──────┐
  │empty │empty │ Bln  │empty │empty │ Par  │empty │ Lon  │
  └──────┴──────┴──────┴──────┴──────┴──────┴──────┴──────┘

After resize (capacity=16, bitmask=15):
  new entries[16] = all zeros

  Re-insert by reading stored hash from each row (hash_offset):

    ROW_Berlin  hash=0xAAAA...002  → 0x002 & 15 = 2
    ROW_Paris   hash=0xBBBB...005  → 0x005 & 15 = 5
    ROW_London  hash=0xCCCC...007  → 0x007 & 15 = 7

  New entries[16]:
  ┌──┬──┬────┬──┬──┬────┬──┬────┬──┬──┬──┬──┬──┬──┬──┬──┐
  │  │  │Bln │  │  │Par │  │Lon │  │  │  │  │  │  │  │  │
  └──┴──┴────┴──┴──┴────┴──┴────┴──┴──┴──┴──┴──┴──┴──┴──┘
   0  1   2   3  4   5   6   7   8  9  10 11 12 13 14 15

NOTE: Row data (Region B) is NOT moved. Only the pointer table is rebuilt.
```

---

## 7. `TryAddDictionaryGroups` — The Dictionary Fast Path

### 7.1 What Is a Dictionary Vector?

When DuckDB scans a column from storage, low-cardinality string columns are often stored as **dictionary-compressed vectors**:

```
Instead of:                  Dictionary Vector stores:
"Berlin"                     ┌───────────────────────┐
"Paris"                      │  CHILD (dictionary)   │
"Berlin"          ═══════►   │  code 0: "Berlin"     │
"London"                     │  code 1: "Paris"      │
"Paris"                      │  code 2: "London"     │
"Berlin"                     └───────────────────────┘
"Paris"
                             ┌───────────────────────┐
                             │  OFFSETS (codes)      │
                             │  [0, 1, 0, 2, 1, 0, 1]│
                             └───────────────────────┘
```

The full string values are stored **once** in the child. The offsets array is just integers.

---

### 7.2 Why This Matters for GROUP BY

```
Regular path for 7 rows:
  7 hashes computed  →  7 HT lookups  →  7 aggregate updates
  (even though there are only 3 distinct groups)

Dictionary fast path for 7 rows with 3 distinct codes:
  3 hashes computed  →  3 HT lookups  →  7 aggregate updates
  (save 4 HT lookups; HT lookup is ~10× more expensive than array lookup)

For a 1 000 000 row scan with 25 distinct values (e.g. TPC-H nation):
  Regular:    1 000 000 HT lookups
  Dict path:  25 HT lookups  +  1 000 000 array lookups
  ≈ 40 000× fewer HT lookups
```

---

### 7.3 Eligibility Checks

```
TryAddDictionaryGroups runs ONLY when ALL of these hold:

1. groups has exactly 1 column (single GROUP BY column)
2. That column is VectorType::DICTIONARY_VECTOR
3. The column type is NOT a STRUCT (structs have special handling)
4. DictionaryVector::DictionarySize() returns a valid value
   (only dictionaries from storage scans have a known size)
5a. If dictionary_id is EMPTY (no UUID assigned):
      dict_size × 2 < chunk_size   (at least 2× compression ratio)
5b. If dictionary_id is SET (UUID assigned, cross-chunk caching enabled):
      dict_size < 20 000           (any reasonable cardinality)
```

The `dictionary_id` is a UUID assigned once when the dictionary is created. If the same dictionary buffer survives across multiple chunks (as it does during a column scan), this ID stays the same and cross-chunk caching kicks in.

---

### 7.4 The `AggregateDictionaryState` — What It Remembers

```
struct AggregateDictionaryState {

  dictionary_id        ← UUID of the current dictionary
  ┌───────────────────────────────────────────────────────┐
  │  found_entry[dict_size]  (bool array)                 │
  │    found_entry[0] = true/false  ("Berlin" seen yet?)  │
  │    found_entry[1] = true/false  ("Paris" seen yet?)   │
  │    found_entry[2] = true/false  ("London" seen yet?)  │
  └───────────────────────────────────────────────────────┘
  ┌───────────────────────────────────────────────────────┐
  │  dictionary_addresses[dict_size]  (pointer array)     │
  │    dictionary_addresses[0] = ptr to ROW_Berlin aggr   │
  │    dictionary_addresses[1] = ptr to ROW_Paris  aggr   │
  │    dictionary_addresses[2] = ptr to ROW_London aggr   │
  └───────────────────────────────────────────────────────┘

  Both arrays are indexed by dictionary CODE (0, 1, 2, ...)
  Both persist across chunks as long as dictionary_id is unchanged.
}
```

---

### 7.5 Algorithm, Visualized Step by Step

**Input:**

```
Dictionary (child vector, dict_size=3):
  code 0: "Berlin"
  code 1: "Paris"
  code 2: "London"

Chunk 1 — 7 rows:
  row │ code │ city    │ amount
 ─────┼──────┼─────────┼───────
   0  │  0   │ Berlin  │  100
   1  │  1   │ Paris   │  200
   2  │  0   │ Berlin  │  300
   3  │  2   │ London  │  150
   4  │  1   │ Paris   │   50
   5  │  0   │ Berlin  │  400
   6  │  1   │ Paris   │  250
```

---

**STEP A — Check and initialize state:**

```
dictionary_id in chunk == dict_state.dictionary_id?
  → First time: NO
  → memset(found_entry, 0, 3)
  → dict_state.dictionary_id = chunk's dictionary_id

found_entry[] before:   [false, false, false]
dictionary_addresses[]: [???,   ???,   ???  ]  ← uninitialized
```

---

**STEP B — Scan row codes, collect first-seen codes:**

```
Row  Code  found_entry[code]?  Action                         unique_count
 0    0      false             unique_entries[0]=0            → 1
                               found_entry[0] = true
 1    1      false             unique_entries[1]=1            → 2
                               found_entry[1] = true
 2    0      true              SKIP (already seen)               2
 3    2      false             unique_entries[2]=2            → 3
                               found_entry[2] = true
 4    1      true              SKIP                              3
 5    0      true              SKIP                              3
 6    1      true              SKIP                              3

found_entry[] after:    [true,  true,  true ]
unique_entries = [0, 1, 2],  unique_count = 3
```

The key insight: we scanned all 7 rows but **only collected 3 unique codes** for HT lookup.

---

**STEP C — Slice dictionary to get only the unique values:**

```
unique_entries = [0, 1, 2]  (codes of first-seen entries)

unique_values.Slice(child_vector, unique_entries, 3):
  unique_values[0] = child[0] = "Berlin"
  unique_values[1] = child[1] = "Paris"
  unique_values[2] = child[2] = "London"

(This creates a view into the child vector — no data is copied)
```

---

**STEP D — Hash and call `FindOrCreateGroups` with only 3 rows:**

```
unique_values.Hash(hashes):
  hashes[0] = hash("Berlin") = 0xAAAA...
  hashes[1] = hash("Paris")  = 0xBBBB...
  hashes[2] = hash("London") = 0xCCCC...

FindOrCreateGroups(unique_values[0..2], hashes, new_dictionary_pointers, ...):
  → 3 HT lookups (not 7)
  → Creates 3 new group rows in Region B

new_dictionary_pointers:
  [0] = 0x1000  (ROW_Berlin)
  [1] = 0x1100  (ROW_Paris)
  [2] = 0x1200  (ROW_London)
```

---

**STEP E — Cache returned pointers by dictionary code:**

```
for i in [0, 3):
  dict_idx = unique_entries[i]
  dictionary_addresses[dict_idx] = new_dictionary_pointers[i] + aggr_offset

Result:
  dictionary_addresses[0] = 0x1000 + aggr_offset  ← points inside ROW_Berlin
  dictionary_addresses[1] = 0x1100 + aggr_offset  ← points inside ROW_Paris
  dictionary_addresses[2] = 0x1200 + aggr_offset  ← points inside ROW_London

Visual:
  code:                  0            1            2
  dictionary_addresses: [0x1000+A]   [0x1100+A]   [0x1200+A]
                              │             │             │
                              ▼             ▼             ▼
                         ROW_Berlin    ROW_Paris    ROW_London
                          [Bln|0|0|.] [Par|0|0|.] [Lon|0|0|.]
```

---

**STEP F — Scatter: fan out addresses to ALL 7 rows:**

```
for i in [0, 7):
  addresses[i] = dictionary_addresses[ offsets[i] ]

Row  Code  dictionary_addresses[code]   →  addresses[i]
 0    0     0x1000+A                       0x1000+A  → ROW_Berlin aggr
 1    1     0x1100+A                       0x1100+A  → ROW_Paris  aggr
 2    0     0x1000+A                       0x1000+A  → ROW_Berlin aggr  ← same as row 0
 3    2     0x1200+A                       0x1200+A  → ROW_London aggr
 4    1     0x1100+A                       0x1100+A  → ROW_Paris  aggr  ← same as row 1
 5    0     0x1000+A                       0x1000+A  → ROW_Berlin aggr  ← same as row 0
 6    1     0x1100+A                       0x1100+A  → ROW_Paris  aggr  ← same as row 1

This is just an array lookup — no hashing, no probing.
```

---

**STEP G — `UpdateAggregates` for all 7 rows:**

```
SUM(amount) accumulated:

  i=0: ROW_Berlin.sum +=  100  →  100
  i=1: ROW_Paris.sum  +=  200  →  200
  i=2: ROW_Berlin.sum +=  300  →  400  ← accumulated into SAME row
  i=3: ROW_London.sum +=  150  →  150
  i=4: ROW_Paris.sum  +=   50  →  250
  i=5: ROW_Berlin.sum +=  400  →  800
  i=6: ROW_Paris.sum  +=  250  →  500

Final state after Chunk 1:
  ROW_Berlin: SUM= 800
  ROW_Paris:  SUM= 500
  ROW_London: SUM= 150
```

**Summary: 7 input rows → 3 HT lookups + 7 array lookups (instead of 7 HT lookups).**

---

### 7.6 Cross-Chunk Caching: Chunk 2 with Same Dictionary

When the next chunk arrives with the **same dictionary buffer** (`dictionary_id` unchanged), `found_entry[]` is NOT reset:

```
found_entry[] carried over from Chunk 1: [true, true, true]

Chunk 2 — 5 rows:
  row │ code │ city    │ amount
 ─────┼──────┼─────────┼───────
   0  │  2   │ London  │   75
   1  │  0   │ Berlin  │  500
   2  │  1   │ Paris   │  350
   3  │  2   │ London  │   25
   4  │  0   │ Berlin  │  200

STEP B (collect unique codes):
  Row 0: code=2, found_entry[2]=true → SKIP
  Row 1: code=0, found_entry[0]=true → SKIP
  Row 2: code=1, found_entry[1]=true → SKIP
  Row 3: code=2, found_entry[2]=true → SKIP
  Row 4: code=0, found_entry[0]=true → SKIP

  unique_count = 0  ← ALL codes already known!
```

Because `unique_count == 0`, Steps C, D, and E are **entirely skipped**. `FindOrCreateGroups` is never called.

```
STEP F (scatter using cached addresses):
  Row 0: addresses[0] = dictionary_addresses[2] → ROW_London aggr
  Row 1: addresses[1] = dictionary_addresses[0] → ROW_Berlin aggr
  Row 2: addresses[2] = dictionary_addresses[1] → ROW_Paris  aggr
  Row 3: addresses[3] = dictionary_addresses[2] → ROW_London aggr
  Row 4: addresses[4] = dictionary_addresses[0] → ROW_Berlin aggr

STEP G (UpdateAggregates):
  ROW_London.sum +=  75  →  225
  ROW_Berlin.sum += 500  → 1300
  ROW_Paris.sum  += 350  →  850
  ROW_London.sum +=  25  →  250
  ROW_Berlin.sum += 200  → 1500

Final totals after Chunk 1 + Chunk 2:
  ROW_Berlin: SUM=1500
  ROW_Paris:  SUM= 850
  ROW_London: SUM= 250
```

**Chunk 2: 0 HT lookups. Only 5 array lookups + 5 aggregate updates.**

---

### 7.7 Side-by-Side Cost Comparison

```
                   Regular path    Dict path (no caching)   Dict path (cached)
                   ─────────────   ──────────────────────   ──────────────────
HT lookups          N (= 7)            D (= 3)                    0
Array lookups        0                 N (= 7)                N (= 5)
Hash computations    N (= 7)           D (= 3)                    0
Aggregate updates    N (= 7)           N (= 7)                N (= 5)

For TPC-H Q1/Q5 (25 nations, millions of rows):
  Regular:    1 000 000 HT lookups
  Dict cached:          25 HT lookups (once ever) + 1 000 000 array lookups
```

---

## 8. Full Execution Flow in `PhysicalHashAggregate`

```
SINK phase (one thread per pipeline):
  ┌────────────────────────────────────────────────────────────┐
  │  for each input chunk:                                     │
  │    ExpressionExecutor.Execute → groups, payload vectors    │
  │    local_ht.AddChunk(groups, payload, filter)              │
  │      ↳ TryAddDictionaryGroups  or  regular path           │
  └────────────────────────────────────────────────────────────┘
  Each thread has its OWN local HT → no locking needed

  Thread 0 local HT:          Thread 1 local HT:
  [Berlin:300] [Paris:200]    [Berlin:500] [London:150]

COMBINE phase (single-threaded merge):
  ┌────────────────────────────────────────────────────────────┐
  │  for each local_ht:                                        │
  │    global_ht.Combine(local_ht):                           │
  │      for each row in local_ht:                            │
  │        FindOrCreateGroups(row.group_keys, ...)            │
  │        CombineStates(local_row, global_row)               │
  │          e.g. global.sum += local.sum                     │
  └────────────────────────────────────────────────────────────┘

  After combining Thread 0 into global:
    [Berlin:300] [Paris:200]

  After combining Thread 1 into global:
    [Berlin:800] [Paris:200] [London:150]

GETDATA phase (source operator):
  ┌────────────────────────────────────────────────────────────┐
  │  Scan TupleDataCollection partition by partition          │
  │  FinalizeStates:                                          │
  │    AVG → output sum/count                                 │
  │    COUNT → output count as-is                             │
  │  Emit result chunk                                        │
  └────────────────────────────────────────────────────────────┘

Output:
  ┌────────────┬─────┬───────┐
  │    city    │ SUM │ COUNT │
  ├────────────┼─────┼───────┤
  │  Berlin    │ 800 │  ...  │
  │  Paris     │ 200 │  ...  │
  │  London    │ 150 │  ...  │
  └────────────┴─────┴───────┘
```

---

## 9. Complete Visual Summary

### Regular path (one chunk, one input row, tracing all pointers)

```
INPUT CHUNK
  ┌──────────┬────────┐
  │ city     │ amount │
  │ "Berlin" │  300   │  ← row i=2 (second Berlin row in chunk)
  └──────────┴────────┘

STEP 1: Hash
  hash("Berlin") = 0xAAAA000000000002
  ht_offset = 2 & 7 = 2
  salt      = 0xAAAAFFFFFFFFFFFF

STEP 2: Inner loop
  entries[2] → occupied, salt matches 0xAAAA... → compare_vector

STEP 3: Key compare
  entries[2].GetPointer() = 0x1000 (ROW_Berlin, from previous row)
  ROW_Berlin.city == "Berlin" → MATCH
  addresses[2] = 0x1000

STEP 4: AddInPlace
  addresses[2] = 0x1000 + aggr_offset = 0x1020  (hypothetical aggr_offset=32)

STEP 5: UpdateAggregates
  ROW_Berlin.sum (at 0x1020) += 300
  ROW_Berlin.sum: was 100, now 400

MEMORY STATE:
  entries[2]
  ┌─────────────────────────┐
  │ salt=0xAAAA             │
  │ ptr=0x1000 ─────────────┼──────────────────────────────────────────┐
  └─────────────────────────┘                                          │
                                                                        ▼
                                                     0x1000            0x1020
                                                     ┌──────────────┬──┴──────────────────┬──────────────┐
                                                     │ city=Berlin  │  SUM=400  COUNT=2   │  hash=0xAAAA │
                                                     └──────────────┴────────────────────┴──────────────┘
                                                                        ^
                                                                        addresses[2] points here after AddInPlace
```

---

### Dictionary path summary diagram

```
DICTIONARY VECTOR (city column)
  ┌───────────────────┐    ┌──────────────────────────────────┐
  │ CHILD (3 strings) │    │ OFFSETS (N integers)             │
  │  0: "Berlin"      │    │  [0, 1, 0, 2, 1, 0, 1, ...]     │
  │  1: "Paris"       │    │   ↑  ↑  ↑  ↑  ↑  ↑  ↑          │
  │  2: "London"      │    │  row indices into CHILD          │
  └──────┬────────────┘    └──────────────────────────────────┘
         │
         │  STEP C: Slice only unique codes [0,1,2]
         ▼
  unique_values = ["Berlin", "Paris", "London"]  (3 rows)
         │
         │  STEP D: FindOrCreateGroups (3 HT lookups)
         ▼
  new_dictionary_pointers = [ROW_Berlin, ROW_Paris, ROW_London]
         │
         │  STEP E: Cache pointers by code
         ▼
  dictionary_addresses[]:
    code 0 → ROW_Berlin+aggr_offset
    code 1 → ROW_Paris +aggr_offset
    code 2 → ROW_London+aggr_offset
         │
         │  STEP F: Scatter (N array lookups)
         ▼
  addresses[0] = dict_addresses[ offsets[0] ] = dict_addresses[0] = ROW_Berlin
  addresses[1] = dict_addresses[ offsets[1] ] = dict_addresses[1] = ROW_Paris
  addresses[2] = dict_addresses[ offsets[2] ] = dict_addresses[0] = ROW_Berlin
  addresses[3] = dict_addresses[ offsets[3] ] = dict_addresses[2] = ROW_London
  ...
         │
         │  STEP G: UpdateAggregates (N aggregate updates)
         ▼
  ROW_Berlin.sum += payload[0], payload[2], payload[5], ...
  ROW_Paris.sum  += payload[1], payload[4], payload[6], ...
  ROW_London.sum += payload[3], ...
```

---

## 10. Performance Properties

| Property | Value |
|----------|-------|
| HT collision resolution | Open addressing with salt-derived odd stride |
| Probe stride | `(top 5 bits of salt) \| 1` → odd, range [1..31] |
| Load factor at resize | 1 / 1.5 ≈ 0.667 |
| Duplicate group keys | Impossible — exactly one slot per distinct group |
| Dict path savings (no id) | Saves `(N - D)` HT lookups per chunk (D = distinct codes) |
| Dict path savings (with id) | Saves all `N` HT lookups for every chunk after the first |
| Parallelism | One local HT per thread; merged with `CombineStates` |
| Row data movement on resize | None — only the pointer table is rebuilt |
