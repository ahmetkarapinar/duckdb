#include "duckdb/function/scalar/hash_join_probe_function.hpp"

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

HashJoinProbeFunctionData::HashJoinProbeFunctionData(optional_ptr<JoinHashTable> hash_table_p, LogicalType key_type_p)
    : hash_table(hash_table_p), key_type(std::move(key_type_p)) {
}

unique_ptr<FunctionData> HashJoinProbeFunctionData::Copy() const {
	return make_uniq<HashJoinProbeFunctionData>(hash_table, key_type);
}

bool HashJoinProbeFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<HashJoinProbeFunctionData>();
	return hash_table.get() == other.hash_table.get() && key_type == other.key_type;
}

namespace {

//! Per-thread scratch for the ht_probe scalar-function callback.
struct HashJoinProbeLocalState : public FunctionLocalState {
	HashJoinProbeLocalState(Allocator &allocator, const LogicalType &key_type)
	    : match_sel(STANDARD_VECTOR_SIZE), hashes(LogicalType::HASH) {
		single_key_chunk.Initialize(allocator, vector<LogicalType> {key_type});
		TupleDataCollection::InitializeChunkState(key_state, vector<LogicalType> {key_type});
	}

	JoinHashTable::ProbeState probe_state;
	TupleDataChunkState key_state;
	SelectionVector match_sel;
	Vector hashes;
	DataChunk single_key_chunk;
};

unique_ptr<FunctionLocalState> HashJoinProbeInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                           FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<HashJoinProbeFunctionData>();
	return make_uniq<HashJoinProbeLocalState>(state.GetAllocator(), bind_data.key_type);
}

void HashJoinProbeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &local = ExecuteFunctionState::GetFunctionState(state)->Cast<HashJoinProbeLocalState>();
	auto &expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = expr.bind_info->Cast<HashJoinProbeFunctionData>();
	auto &ht = *bind_data.hash_table;

	const idx_t count = args.size();

	// reference the input key into a persistent single-column chunk so RowMatcher::Match (called from
	// GetRowPointers) can re-check key equality against the build-side keys
	local.single_key_chunk.Reset();
	local.single_key_chunk.data[0].Reference(args.data[0]);
	local.single_key_chunk.SetCardinality(count);
	TupleDataCollection::ToUnifiedFormat(local.key_state, local.single_key_chunk);

	// single-key project scope: one VectorOperations::Hash call mirrors JoinHashTable::Hash
	VectorOperations::Hash(local.single_key_chunk.data[0], local.hashes, count);

	// resolve head-of-chain pointers: GetRowPointers writes hits at row positions in match_sel, leaves misses untouched
	idx_t out_count = count;
	// a previous chunk's dict fast path may have left result wrapped as a DictionaryBuffer
	// (TryExecuteDictionaryExpression ends with result.Dictionary(...)); DictionaryBuffer does not support
	// SetVectorType, so re-initialise to a fresh flat StandardVectorBuffer when result is not already flat
	if (result.GetVectorType() != VectorType::FLAT_VECTOR) {
		result.Initialize();
	}
	ht.ProbeKeysToHeadPointers(local.single_key_chunk, local.key_state, local.probe_state, local.hashes,
	                           /*sel=*/nullptr, out_count, result, local.match_sel, /*has_sel=*/false);

	// GetRowPointers leaves pointer slots for non-matching rows in an undefined state (the last salt-collision
	// pointer it tried). Encode hits via the validity mask: misses are signalled by an invalid validity bit.
	auto &validity = FlatVector::ValidityMutable(result);
	validity.SetAllInvalid(count);
	for (idx_t i = 0; i < out_count; i++) {
		validity.SetValid(local.match_sel.get_index(i));
	}
}

} // namespace

ScalarFunction HashJoinProbeScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction fun(HashJoinProbeScalarFun::NAME, {input_type}, LogicalType::POINTER, HashJoinProbeFunction);
	fun.SetInitStateCallback(HashJoinProbeInitLocalState);
	// the callback always runs (even on constant-NULL input it produces an all-null pointer vector)
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
