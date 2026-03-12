# Compressed Materialization in DuckDB

## 1. What It Is

**Compressed Materialization (CM)** is a query optimizer pass in DuckDB that reduces the memory footprint of materializing operators by compressing their input columns to smaller types before materialization, then decompressing the output afterward.

### The Problem It Solves

Materializing operators — `AGGREGATE/GROUP BY`, `COMPARISON_JOIN` (hash join), `DISTINCT`, and `ORDER BY` — build in-memory data structures (hash tables, sort buffers) over their input columns. If those columns have a wide declared type (e.g., `BIGINT`, `VARCHAR`) but a narrow actual value range (e.g., integers in [1000, 1255] or strings ≤ 1 byte long), the memory and CPU work are dominated by the declared width, not the actual data density.

CM exploits per-column statistics gathered during the statistics propagation phase to shrink values to the smallest type that can faithfully represent the observed range. For example:
- A `BIGINT` column with range [1000, 1255] → range = 255 → compresses to `UTINYINT` via `value - 1000`
- A `VARCHAR` column whose strings are all ≤ 1 byte → compresses to `UTINYINT` via a reversible byte-packing

The compressed columns enter the materializing operator. The operator sees fewer bytes per tuple, yielding better cache utilization, cheaper hashing, and smaller memory footprints.

### High-Level Structure

CM inserts **two projections** around a materializing operator:

```
DECOMPRESS PROJECTION   ← new LogicalProjection (on top)
  AGGREGATE / JOIN / DISTINCT / ORDER_BY
    COMPRESS PROJECTION ← new LogicalProjection (below, on each qualifying child)
      <original child subtree>
```

---

## 2. How It Works: Full Execution Flow

### Step 0 — Optimizer Pipeline Placement

CM is not a standalone optimizer pass. It runs **inside** the `StatisticsPropagator`, which is invoked as part of the normal optimizer pipeline:

```cpp
// src/optimizer/optimizer.cpp:292-298
column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map;
RunOptimizer(OptimizerType::STATISTICS_PROPAGATION, [&]() {
    StatisticsPropagator propagator(*this, *plan);
    propagator.PropagateStatistics(plan);
    statistics_map = propagator.GetStatisticsMap();
});
```

`StatisticsPropagator::PropagateStatistics` traverses the plan **bottom-up**. After processing each node, it calls:

```cpp
// src/optimizer/statistics_propagator.cpp:84-88
if (!optimizer.OptimizerDisabled(OptimizerType::COMPRESSED_MATERIALIZATION)) {
    CompressedMaterialization compressed_materialization(optimizer, *root, statistics_map);
    compressed_materialization.Compress(node_ptr);
}
```

Because the traversal is bottom-up, by the time CM runs on a node, all child statistics are already populated in `statistics_map`.

### Step 1 — Dispatch in `CompressedMaterialization::Compress`

```cpp
// src/optimizer/compressed_materialization.cpp:72-105
void CompressedMaterialization::Compress(unique_ptr<LogicalOperator> &op) {
    if (TopN::CanOptimize(*op)) { return; }  // Don't interfere with TopN optimizer

    switch (op->type) {
    case LOGICAL_AGGREGATE_AND_GROUP_BY: CompressAggregate(op); break;
    case LOGICAL_COMPARISON_JOIN:        CompressComparisonJoin(op); break;
    case LOGICAL_DISTINCT:               CompressDistinct(op); break;
    case LOGICAL_ORDER_BY:               CompressOrder(op); break;
    default: return;
    }
}
```

### Step 2 — Operator-Specific Entry Points

Each entry point:
1. Identifies which bindings are **eligible for compression** (group-by columns, join keys, etc.)
2. Marks bindings that must NOT be compressed (those appearing in non-colref expressions or aggregate function arguments)
3. Builds a `CompressedMaterializationInfo` structure mapping input bindings to output bindings
4. Calls `CreateProjections`

#### Example: `CompressAggregate` (`compress_aggregate.cpp:8-112`)

```cpp
void CompressedMaterialization::CompressAggregate(unique_ptr<LogicalOperator> &op) {
    auto &aggregate = op->Cast<LogicalAggregate>();
    // ...
    // Mark bindings referenced in aggregate expressions as non-compressible
    column_binding_set_t referenced_bindings;
    for (auto &aggr_expr : aggregate.expressions) {
        for (auto &child : aggr_expr.Cast<BoundAggregateExpression>().children)
            GetReferencedBindings(*child, referenced_bindings);
    }

    // Create info — child_idx={0} = only the single child of the aggregate
    CompressedMaterializationInfo info(*op, {0}, referenced_bindings);

    // Build binding_map: GROUP column binding → CMBindingInfo(output binding, original type)
    for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
        info.binding_map.emplace(group_bindings[group_idx],
                                 CMBindingInfo(bindings_out[group_idx], types[group_idx]));
    }
    CreateProjections(op, info);
    UpdateAggregateStats(op);
}
```

The join variant (`compress_comparison_join.cpp`) additionally enforces that both sides of an equi-join condition are compressed with **merged statistics** so the comparison remains valid after compression.

### Step 3 — `TryCompressChild` and `GetCompressExpression`

```cpp
// src/optimizer/compressed_materialization.cpp:128-157
bool CompressedMaterialization::TryCompressChild(...) {
    for (idx_t i = 0; i < child_info.bindings_before.size(); i++) {
        auto compress_expr = GetCompressExpression(binding, type, can_compress);
        if (compress_expr) {   // successfully compressed → mark for decompression
            compress_exprs.emplace_back(std::move(compress_expr));
            compressed = true;
        } else {               // not compressed → pass through as plain column ref
            compress_exprs.emplace_back(make_uniq<CompressExpression>(colref_expr, colref_stats));
        }
        UpdateBindingInfo(info, binding, compressed);
    }
    return compressed_anything;
}
```

`GetCompressExpression` checks the type and dispatches:

```cpp
// src/optimizer/compressed_materialization.cpp:310-322
unique_ptr<CompressExpression>
CompressedMaterialization::GetCompressExpression(unique_ptr<Expression> input, const BaseStatistics &stats) {
    if (type.IsIntegral())               return GetIntegralCompress(std::move(input), stats);
    else if (type.id() == VARCHAR)       return GetStringCompress(std::move(input), stats);
    return nullptr;
}
```

#### Integral Compression (`GetIntegralCompress`, `compressed_materialization.cpp:345-406`)

1. Compute `range = max - min` (using the statistics).
2. Pick the **smallest unsigned integer type** that holds the range:
   - range ≤ 255 → `UTINYINT`
   - range ≤ 65535 → `USMALLINT`
   - range ≤ 2³²−1 → `UINTEGER`
   - range ≤ 2⁶⁴−1 → `UBIGINT`
3. If the target type is the same size as the source, skip (no benefit).
4. Build a `BoundFunctionExpression` for `__internal_compress_integral_<type>(col, min)`.
5. Return the expression together with updated statistics (range [0, range]).

```cpp
// src/optimizer/compressed_materialization.cpp:393-405
auto compress_function = CMIntegralCompressFun::GetFunction(type, cast_type);
vector<unique_ptr<Expression>> arguments;
arguments.emplace_back(std::move(input));               // the column ref
arguments.emplace_back(make_uniq<BoundConstantExpression>(min));  // constant min
auto compress_expr = make_uniq<BoundFunctionExpression>(cast_type, compress_function,
                                                        std::move(arguments), nullptr);
```

The actual scalar function subtracts the minimum:
```cpp
// src/function/scalar/compressed_materialization/compress_integral.cpp:19-22
template <class INPUT_TYPE, class RESULT_TYPE>
struct TemplatedIntegralCompress {
    static inline RESULT_TYPE Operation(const INPUT_TYPE &input, const INPUT_TYPE &min_val) {
        return UnsafeNumericCast<RESULT_TYPE>(input - min_val);  // offset encoding
    }
};
```

#### String Compression (`GetStringCompress`, `compressed_materialization.cpp:408-467`)

1. Read `StringStats::MaxStringLength`.
2. Pick the smallest unsigned integer type whose **byte size** exceeds `max_string_length`.
   - For strings ≤ 1 byte → `UTINYINT`
   - For strings ≤ 2 bytes → `USMALLINT`
   - For strings ≤ 4 bytes → `UINTEGER`
   - For strings ≤ 8 bytes → `UBIGINT`
   - For strings ≤ 16 bytes → `UHUGEINT`
3. Build a `BoundFunctionExpression` for `__internal_compress_string_<type>(col)`.

String compression packs the raw bytes of the string into an integer (with the length encoded in the high byte):
```cpp
// src/function/scalar/compressed_materialization/compress_string.cpp:31-48
template <class RESULT_TYPE>
inline RESULT_TYPE StringCompressInternal(const string_t &input) {
    RESULT_TYPE result;
    const auto result_ptr = data_ptr_cast(&result);
    // Copy string bytes in reverse (big-endian style), first byte = length
    ReverseMemCpy(result_ptr + remainder, data_ptr_cast(input.GetPointer()), size);
    result_ptr[0] = UnsafeNumericCast<data_t>(input.GetSize());  // length in high byte
    return BSwapIfBE(result);  // ensure little-endian on all platforms
}
```

### Step 4 — `CreateCompressProjection`

```cpp
// src/optimizer/compressed_materialization.cpp:159-226
void CompressedMaterialization::CreateCompressProjection(...) {
    // Build projection expressions from compress_exprs
    auto compress_projection = make_uniq<LogicalProjection>(table_index, std::move(projections));
    compress_projection->children.emplace_back(std::move(child_op));
    child_op = std::move(compress_projection);    // Replace child with the new projection

    // Use ColumnBindingReplacer to update all references in the plan to the new (compressed) bindings
    ColumnBindingReplacer replacer;
    replacer.stop_operator = child_op.get();      // Stop at compress_projection itself
    replacer.VisitOperator(*root);                 // Fix up rest of plan

    // Store updated statistics under new bindings
    for (auto &binding : child_info.bindings_after)
        statistics_map.emplace(binding, compress_exprs[col_idx]->stats);
}
```

### Step 5 — `CreateDecompressProjection`

After the materializing operator outputs compressed data, a decompression projection restores the original types:

```cpp
// src/optimizer/compressed_materialization.cpp:228-296
void CompressedMaterialization::CreateDecompressProjection(...) {
    for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
        auto decompress_expr = make_uniq<BoundColumnRefExpression>(types[col_idx], binding);
        if (binding_info.needs_decompression) {
            decompress_expr = GetDecompressExpression(std::move(decompress_expr),
                                                      binding_info.type, *stats);
        }
        decompress_exprs.emplace_back(std::move(decompress_expr));
    }
    // Wrap the original operator with a new projection node
    auto decompress_projection = make_uniq<LogicalProjection>(table_index, std::move(decompress_exprs));
    decompress_projection->children.emplace_back(std::move(op));
    op = std::move(decompress_projection);
}
```

The decompression is the inverse:
```cpp
// Integral: output = min_val + UnsafeNumericCast<RESULT_TYPE>(input)
// String:   reverse the byte-pack back to a string_t
```

---

## 3. Concrete Example — Integral Compression

**Schema:**
```sql
CREATE TABLE orders(dept_id BIGINT, amount BIGINT);
-- statistics: dept_id ∈ [1000, 1004], amount ∈ [0, 9999999]
```

**Query:**
```sql
SELECT dept_id, SUM(amount) FROM orders GROUP BY dept_id;
```

**Without CM:**

```
Aggregate(groupby=[dept_id:BIGINT], agg=[SUM(amount:BIGINT)])
  TableScan(orders)
```

`dept_id` is hashed as an 8-byte `BIGINT`. Each hash table slot stores 8 bytes for the group key.

**With CM (range = 1004 - 1000 = 4, fits in UTINYINT):**

```
DecompressProjection [__internal_decompress_integral_bigint(dept_id_c, 1000) → dept_id]
  Aggregate(groupby=[dept_id_c:UTINYINT], agg=[SUM(amount:BIGINT)])
    CompressProjection [__internal_compress_integral_utinyint(dept_id, 1000) → dept_id_c]
      TableScan(orders)
```

**Vector state at each stage:**

| Stage | Column | Type | Sample values (row 0–4) |
|-------|--------|------|------------------------|
| TableScan output | `dept_id` | `BIGINT` | `1002, 1000, 1004, 1001, 1003` |
| After compress projection | `dept_id_c` | `UTINYINT` | `2, 0, 4, 1, 3` |
| After GROUP BY (internal) | `dept_id_c` | `UTINYINT` | `0, 1, 2, 3, 4` (one per group) |
| After decompress projection | `dept_id` | `BIGINT` | `1000, 1001, 1002, 1003, 1004` |

Hash table entry size drops from 8 bytes → 1 byte per group key.

---

## 4. When CM Activates

### All materializing operators

CM fires if:
- The column has **statistics** in `statistics_map` (populated during bottom-up statistics propagation)
- The type is `INTEGRAL` or `VARCHAR`
- For integral: stats have `HasMinMax` AND the value **fits in a strictly smaller unsigned type**
  - e.g., `BIGINT` with range 0–255 → compress to `UTINYINT`
  - Current type is already 1 byte (`UTINYINT`/`TINYINT`/`BOOLEAN`) → skip
- For string: stats have `HasMaxStringLength` AND `max_string_length < sizeof(target_integer_type)`

### Additional Threshold for Joins (`compress_comparison_join.cpp:32-48`)

In release mode, CM on joins only fires when:
```cpp
static constexpr idx_t JOIN_BUILD_CARDINALITY_THRESHOLD = 1048576;    // 1M
static constexpr idx_t JOIN_BUILD_COLUMN_COUNT_THRESHOLD = 20;
static constexpr double JOIN_CARDINALITY_RATIO_THRESHOLD = 8;

// Fire if build cardinality ≥ 1M AND
//   (columns ≥ 20 OR join_cardinality / build_cardinality ≤ 8)
```

In debug builds, CM for joins fires unconditionally (for test coverage).

### When CM Falls Back (Does Nothing)

- No statistics for the binding
- Column is already 1 byte wide
- Type is not `INTEGRAL` or `VARCHAR`
- Integral range doesn't fit in any smaller type than the current one (e.g., `USMALLINT` with range > 65535)
- String `max_string_length` doesn't fit in any supported integer type (> 16 bytes)
- Multiple grouping sets on an aggregate (`aggregate.grouping_sets.size() > 1`)
- Duplicate group bindings
- Join type is `MARK` join
- The binding appears in a non-colref expression in the grouping or join condition (it's added to `referenced_bindings` and excluded from projection-level compression, though it may still be compressed inline in the expression itself)
- `TopN::CanOptimize(*op)` is true (CM defers to TopN optimizer)

---

## 5. How Downstream Operators Consume CM Output

### What CM Produces

The **compress projection** is a `LogicalProjection` that compiles to a `PhysicalProjection`. It uses `ExpressionExecutor` to evaluate the compress scalar function on each incoming chunk:

```cpp
// src/execution/operator/projection/physical_projection.cpp:28-33
OperatorResultType PhysicalProjection::Execute(..., DataChunk &input, DataChunk &chunk, ...) const {
    state.executor.Execute(input, chunk);
    return OperatorResultType::NEED_MORE_INPUT;
}
```

The output chunk contains **flat vectors** (for most cases — see Section 6 for the dictionary edge case) of the compressed type.

### What Downstream Operators See

- **Hash Join (build side):** The compress projection is between the table scan and the join. The build-side hash table stores compressed keys. Memory per tuple shrinks proportionally.
- **Hash Join (probe side):** The join condition uses compressed types on both sides (with merged statistics), so comparisons still work.
- **GROUP BY / Aggregate:** The aggregate's hash table groups on the compressed type. Since both the key type and the number of distinct values remain unchanged (compression is lossless), the aggregate produces exactly the same groups — just using fewer bytes.
- **ORDER BY:** The sort buffer holds compressed values. Sort comparisons work correctly because the offset encoding preserves order: `compress(a) < compress(b) ↔ a < b`.
- **DISTINCT:** Similar to ORDER BY — order preservation means deduplication still works.

The **decompress projection** (also a `PhysicalProjection`) sits above the materializing operator in the **same pipeline or a pipeline boundary**. It applies the inverse function to restore original types before the results are consumed by further operators or returned to the client.

---

## 6. Why Dictionary Vectors Are Flattened After PHJ and Before GROUP BY

This section traces the precise execution path and explains why the Perfect Hash Join (PHJ)'s dictionary-encoded output vectors are converted to flat vectors before reaching the `GroupedAggregateHashTable`, preventing `TryAddDictionaryGroups` from being exploited.

### Background: What PHJ Emits

When a `PhysicalHashJoin` uses the perfect hash join path (enabled for inner joins with a single integer equality condition and a build key range ≤ 1,048,576), it stores the entire build-side table in a **flat array** of fixed size (`build_range + 1`), indexed by `key - min_key`. Each build-side output column is stored as a "reusable dictionary":

```cpp
// src/execution/operator/join/perfect_hash_join_executor.cpp:133-137
const auto build_size = perfect_join_statistics.build_range + 1;
for (const auto &type : join.rhs_output_columns.col_types) {
    perfect_hash_table.emplace_back(
        DictionaryVector::CreateReusableDictionary(type, build_size));  // flat array + UUID
}
```

`DictionaryVector::CreateReusableDictionary` allocates a `VectorChildBuffer` with a flat array and a fresh UUID:
```cpp
// src/common/types/vector.cpp:2079-2084
buffer_ptr<VectorChildBuffer> DictionaryVector::CreateReusableDictionary(const LogicalType &type, const idx_t &size) {
    auto res = make_buffer<VectorChildBuffer>(Vector(type, size));
    res->size = size;
    res->id = UUID::ToString(UUID::GenerateRandomUUID());  // permanent identity
    return res;
}
```

During probe, each RHS output column is emitted as a **dictionary vector** that references this flat array with a selection vector:

```cpp
// src/execution/operator/join/perfect_hash_join_executor.cpp:299-303
for (idx_t i = 0; i < join.rhs_output_columns.col_types.size(); i++) {
    auto &result_vector = result.data[lhs_output_columns.ColumnCount() + i];
    result_vector.Dictionary(perfect_hash_table[i], state.build_sel_vec);
    // ^ DICTIONARY_VECTOR: child = perfect_hash_table[i], sel = build_sel_vec
    //   dict_size = build_range + 1, dict_id = UUID (non-empty)
}
```

### Background: What `TryAddDictionaryGroups` Needs

```cpp
// src/execution/aggregate_hashtable.cpp:506-517
optional_idx GroupedAggregateHashTable::TryAddCompressedGroups(DataChunk &groups, ...) {
    if (groups.ColumnCount() == 1 &&
        groups.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR &&
        groups.data[0].GetType().InternalType() != PhysicalType::STRUCT) {
        return TryAddDictionaryGroups(groups, payload, filter);
    }
    return optional_idx();  // not a dictionary → fall through to regular hashing
}
```

`TryAddDictionaryGroups` requires exactly one group column of type `DICTIONARY_VECTOR`. It exploits the dictionary by hashing only the unique dictionary entries (at most `dict_size` entries) rather than every incoming row, caching group pointers for subsequent chunks with the same dictionary ID.

```cpp
// src/execution/aggregate_hashtable.cpp:359-503 (abridged)
optional_idx GroupedAggregateHashTable::TryAddDictionaryGroups(DataChunk &groups, ...) {
    auto &dict_col = groups.data[0];
    auto opt_dict_size = DictionaryVector::DictionarySize(dict_col);
    if (!opt_dict_size.IsValid()) return optional_idx();     // no known size
    idx_t dict_size = opt_dict_size.GetIndex();

    auto &dictionary_id = DictionaryVector::DictionaryId(dict_col);
    if (dictionary_id.empty()) {
        // No ID: only worthwhile if dict is small vs. chunk size
        if (dict_size * 2 >= groups.size()) return optional_idx();
    } else {
        // Has ID: can cache across chunks, larger threshold
        if (dict_size >= MAX_DICTIONARY_SIZE_THRESHOLD /*20000*/) return optional_idx();
    }
    // ... hash unique entries, cache pointers, update aggregates
}
```

### The CM Compress Projection: The Flattening Agent

When CM is applied to a `GROUP BY` whose group column is a PHJ RHS column, CM inserts a `CompressProjection` **between** the PHJ and the GROUP BY:

```
Physical Pipeline:
  PHJ Execute → CM CompressProjection Execute → GROUP BY Sink
```

The compress projection is a `PhysicalProjection` that calls `ExpressionExecutor::Execute`, which evaluates `__internal_compress_integral_utinyint(col, min)` on the PHJ-emitted dictionary vector.

#### Path 1: `ExecuteFunctionState::TryExecuteDictionaryExpression`

The expression executor first attempts a dictionary-aware optimization:

```cpp
// src/execution/expression_executor/execute_function.cpp:48-117
bool ExecuteFunctionState::TryExecuteDictionaryExpression(...) {
    // ...
    const auto input_dictionary_size_opt = DictionaryVector::DictionarySize(unary_input);
    const auto &input_dictionary_id = DictionaryVector::DictionaryId(unary_input);
    if (!input_dictionary_size_opt.IsValid() || input_dictionary_id.empty())
        return false;   // Not a storage/reusable dictionary

    const auto input_dictionary_size = input_dictionary_size_opt.GetIndex();
    if (input_dictionary_size >= MAX_DICTIONARY_SIZE_THRESHOLD /*20000*/)
        return false;   // ← FAILS HERE for PHJ with build_range ≥ 20000
    // ...
}
```

For a PHJ with build range ≥ 20,000 (the common case — remember PHJ allows up to 1,048,576), this returns `false`.

#### Path 2: Regular Callback → `UnaryExecutor::ExecuteStandard`

After `TryExecuteDictionaryExpression` fails, the raw function callback is invoked:

```cpp
// src/execution/expression_executor/execute_function.cpp:196-197
if (!execute_function_state.TryExecuteDictionaryExpression(expr, arguments, *state, result)) {
    expr.function.GetFunctionCallback()(arguments, *state, result);   // IntegralCompressFunction
}
```

Inside `IntegralCompressFunction`:
```cpp
// src/function/scalar/compressed_materialization/compress_integral.cpp:42-56
void IntegralCompressFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    const auto min_val = ConstantVector::GetData<INPUT_TYPE>(args.data[1])[0];
    UnaryExecutor::Execute<INPUT_TYPE, RESULT_TYPE>(
        args.data[0], result, args.size(), lambda, FunctionErrors::CANNOT_ERROR);
}
```

`UnaryExecutor::ExecuteStandard` has a special case for `DICTIONARY_VECTOR`:

```cpp
// src/include/duckdb/common/vector_operations/unary_executor.hpp:170-196
case VectorType::DICTIONARY_VECTOR: {
    if (errors == FunctionErrors::CANNOT_ERROR) {
        static constexpr idx_t DICTIONARY_THRESHOLD = 2;
        auto dict_size = DictionaryVector::DictionarySize(input);
        if (dict_size.IsValid() && dict_size.GetIndex() * DICTIONARY_THRESHOLD <= count) {
            // dict_size * 2 ≤ chunk_size: operate on dictionary only, preserve structure
            auto &dictionary_values = DictionaryVector::Child(input);
            if (dictionary_values.GetVectorType() == VectorType::FLAT_VECTOR) {
                // execute compress on the dict entries, then re-slice with original sel
                result.Dictionary(result, dict_size.GetIndex(), offsets, count);  // still DICT
                break;
            }
        }
    }
    DUCKDB_EXPLICIT_FALLTHROUGH;   // ← FALLS THROUGH for large dictionaries
}
default: {
    UnifiedVectorFormat vdata;
    input.ToUnifiedFormat(count, vdata);           // handles any vector type
    result.SetVectorType(VectorType::FLAT_VECTOR); // ← EXPLICIT FLATTENING
    auto result_data = FlatVector::GetData<RESULT_TYPE>(result);
    ExecuteLoop(ldata, result_data, count, vdata.sel, ...);   // write flat
    break;
}
```

The dictionary optimization within `UnaryExecutor` fires only when `dict_size * 2 ≤ count` (chunk size, typically 2048). For a PHJ with `build_range = 1,000,000`, we have `1,000,000 * 2 >> 2048`, so the condition fails and falls through to `default`, which **explicitly calls `result.SetVectorType(VectorType::FLAT_VECTOR)`**.

### Result: GROUP BY Receives a Flat Vector

The compress projection's output chunk contains `UTINYINT` values in a **flat vector** — not a dictionary vector. The GROUP BY pipeline proceeds:

```cpp
// src/execution/operator/aggregate/radix_partitioned_hashtable.cpp
void RadixPartitionedHashTable::Sink(ExecutionContext &context, DataChunk &chunk, ...) {
    auto &group_chunk = lstate.group_chunk;
    PopulateGroupChunk(group_chunk, chunk);  // Reference() from chunk → preserves flat type
    ht.AddChunk(group_chunk, payload_input, filter);
}

idx_t GroupedAggregateHashTable::AddChunk(DataChunk &groups, ...) {
    auto result = TryAddCompressedGroups(groups, payload, filter);
    // → groups.data[0].GetVectorType() == FLAT_VECTOR
    // → NOT DICTIONARY_VECTOR → TryAddDictionaryGroups NOT called
    if (result.IsValid()) return result.GetIndex();

    groups.Hash(state.hashes);       // regular hashing
    return AddChunk(groups, state.hashes, payload, filter);
}
```

`TryAddCompressedGroups` at line 512 checks:
```cpp
if (groups.ColumnCount() == 1 &&
    groups.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR && ...)
    return TryAddDictionaryGroups(...);
return optional_idx();   // ← RETURNS INVALID: not a dictionary vector
```

Since the group column is now a **flat UTINYINT vector**, this check fails and `TryAddDictionaryGroups` is never invoked.

### Summary of the Flattening Path

```
1. PHJ::ProbePerfectHashTable()
   result_vector.Dictionary(perfect_hash_table[i], build_sel_vec)
   → DICTIONARY_VECTOR { dict=flat_array[build_range], sel=probe_sel, dict_size=build_range, dict_id="UUID" }

2. CM CompressProjection::Execute() → ExpressionExecutor::Execute(BoundFunctionExpression)
   → TryExecuteDictionaryExpression: dict_size=build_range ≥ 20000 → RETURNS FALSE
   → IntegralCompressFunction callback → UnaryExecutor::ExecuteStandard
   → DICTIONARY_VECTOR case: dict_size*2 >> 2048 → FALLTHROUGH to default
   → result.SetVectorType(FLAT_VECTOR)  ← FLATTENING POINT
   → FLAT_VECTOR { data=compressed_UTINYINT_array }

3. RadixPartitionedHashTable::Sink() → PopulateGroupChunk() → group_chunk.Reference(flat_vec)
   → AddChunk() → TryAddCompressedGroups()
   → groups.data[0].GetVectorType() == FLAT_VECTOR ≠ DICTIONARY_VECTOR
   → TryAddDictionaryGroups NOT invoked
   → Falls back to: groups.Hash(hashes) + regular FindOrCreateGroups
```

### The Two Thresholds at Play

| Threshold | Value | Location | Controls |
|-----------|-------|----------|---------|
| `MAX_DICTIONARY_SIZE_THRESHOLD` | 20,000 | `execute_function.cpp:50` | Whether `TryExecuteDictionaryExpression` preserves dictionary encoding through a function call |
| `DICTIONARY_THRESHOLD` | 2 | `unary_executor.hpp:176` | Whether `UnaryExecutor` dictionary optimization fires (`dict_size * 2 ≤ count`) |
| `MAX_DICTIONARY_SIZE_THRESHOLD` | 20,000 | `aggregate_hashtable.cpp:361` | Whether `TryAddDictionaryGroups` fires for dictionaries with known IDs |
| `JOIN_BUILD_CARDINALITY_THRESHOLD` | 1,048,576 | `compressed_materialization.hpp:80` | Whether CM is applied to joins in release mode |
| PHJ `MAX_BUILD_SIZE` | 1,048,576 | `perfect_hash_join_executor.cpp:114` | Maximum build range for PHJ to activate |

The PHJ can have a build range of up to 1,048,576, while the threshold that allows dictionary propagation through a function call is only 20,000. For any PHJ with `build_range > 20,000`, the compress projection will produce flat vectors, preventing `TryAddDictionaryGroups` from exploiting the dictionary structure.

### Concrete Example

```sql
CREATE TABLE products(key INTEGER, category INTEGER);
-- products.key: 1,000,000 unique values in range [0, 999,999]
-- products.category: 5 distinct values in range [0, 4]

CREATE TABLE orders(order_key INTEGER, amount INTEGER);
-- orders.order_key: foreign key → products.key

SELECT p.category, COUNT(*)
FROM orders o
JOIN products p ON o.order_key = p.key
GROUP BY p.category;
```

**PHJ triggers** (build_range = 999,999 ≤ 1,048,576):
- `perfect_hash_table[0]` = `DictionaryVector::CreateReusableDictionary(INTEGER, 1,000,000)`
- PHJ emits per probe chunk: `p.category` as `DICTIONARY_VECTOR { dict_size=1,000,000, dict_id="UUID_A" }`

**CM triggers** on GROUP BY (statistics: `category ∈ [0, 4]`, range = 4 ≤ 255):
- Inserts compress projection: `__internal_compress_integral_utinyint(p.category, 0)`
- CM inserts decompress projection above GROUP BY: `__internal_decompress_integral_integer(result_c, 0)`

**At runtime (chunk of 2048 probe rows):**

| Step | Vector | Type | dict_size | dict_id | Notes |
|------|--------|------|-----------|---------|-------|
| PHJ output | `p.category` | `DICTIONARY_VECTOR` | 1,000,000 | "UUID_A" | References perfect_hash_table |
| `TryExecuteDictionaryExpression` | — | — | — | — | **FAILS**: 1,000,000 ≥ 20,000 |
| `UnaryExecutor` dict case | — | — | — | — | **FAILS**: 1,000,000 × 2 >> 2048 → fallthrough |
| Compress projection output | `p.category_c` | **FLAT_VECTOR** | — | — | `SetVectorType(FLAT_VECTOR)` |
| GROUP BY input | `p.category_c` | **FLAT_VECTOR** | — | — | `Reference()` from input chunk |
| `TryAddCompressedGroups` | — | — | — | — | **FAILS**: not DICTIONARY_VECTOR |
| Regular hashing | `p.category_c` | FLAT_VECTOR | — | — | Hash 2048 UTINYINT values |

**Net effect**: CM still helps — the GROUP BY hashes 1-byte values instead of 4-byte integers, reducing hash computation cost and hash table memory. But `TryAddDictionaryGroups`'s deduplication (hashing only 5 unique entries instead of 2048) is lost.

**If build_range were < 20,000** (e.g., keys in [0, 9,999]):
- `TryExecuteDictionaryExpression` would **succeed**
- Compress function runs on just 10,000 dict entries → output is a new `DICTIONARY_VECTOR { dict_size=10,000, dict_id="UUID_B" }`
- `TryAddDictionaryGroups`: dict_size=10,000 < 20,000, dict_id≠empty → **SUCCEEDS**
- Only the 5 unique compressed category values are hashed, not all 2048 rows per chunk

---

## 7. Files Reference

| File | Role |
|------|------|
| `src/include/duckdb/optimizer/compressed_materialization.hpp` | Class declaration, thresholds, data structures |
| `src/optimizer/compressed_materialization.cpp` | Core logic: `Compress`, `CreateProjections`, `GetCompressExpression`, `GetDecompressExpression` |
| `src/optimizer/compressed_materialization/compress_aggregate.cpp` | GROUP BY-specific compress logic |
| `src/optimizer/compressed_materialization/compress_comparison_join.cpp` | Join-specific compress logic with cardinality thresholds |
| `src/optimizer/compressed_materialization/compress_distinct.cpp` | DISTINCT compress logic |
| `src/optimizer/compressed_materialization/compress_order.cpp` | ORDER BY compress logic |
| `src/function/scalar/compressed_materialization/compress_integral.cpp` | `__internal_compress_integral_*` / `__internal_decompress_integral_*` functions |
| `src/function/scalar/compressed_materialization/compress_string.cpp` | `__internal_compress_string_*` / `__internal_decompress_string` functions |
| `src/function/scalar/compressed_materialization_utils.cpp` | Type enumerations, bind guard (internal-only) |
| `src/optimizer/statistics_propagator.cpp` | CM invocation point (bottom-up, per-node) |
| `src/execution/expression_executor/execute_function.cpp` | `TryExecuteDictionaryExpression` — dictionary-aware function evaluation |
| `src/include/duckdb/common/vector_operations/unary_executor.hpp` | `UnaryExecutor::ExecuteStandard` — dictionary path and flat fallthrough |
| `src/execution/operator/join/perfect_hash_join_executor.cpp` | PHJ probe: dictionary vector emission via `result_vector.Dictionary(...)` |
| `src/execution/aggregate_hashtable.cpp` | `TryAddDictionaryGroups`, `TryAddCompressedGroups`, `AddChunk` |
| `src/common/types/vector.cpp` | `DictionaryVector::CreateReusableDictionary`, `Vector::Dictionary` overloads |
