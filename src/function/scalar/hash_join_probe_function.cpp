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

//! Per-thread scratch for the ht_probe callback
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
	D_ASSERT(bind_data.hash_table);
	auto &ht = *bind_data.hash_table;

	const idx_t count = args.size();

	// GetRowPointers' RowMatcher re-checks key equality against the build side, which reads through unified format
	local.single_key_chunk.data[0].Reference(args.data[0]);
	local.single_key_chunk.SetCardinality(count);
	TupleDataCollection::ToUnifiedFormat(local.key_state, local.single_key_chunk);

	VectorOperations::Hash(local.single_key_chunk.data[0], local.hashes, count);

	// a prior dict-fast-path call may have left result holding a DictionaryBuffer; reset to a flat buffer
	// so FlatVector::ValidityMutable below is valid
	if (result.GetVectorType() != VectorType::FLAT_VECTOR) {
		result.Initialize();
	}
	const idx_t hit_count = ht.LookupHeadPointers(local.single_key_chunk, local.key_state, local.probe_state,
	                                              local.hashes, result, local.match_sel);

	// encode hit/miss in validity: miss slots may carry stale salt-collision pointers
	auto &validity = FlatVector::ValidityMutable(result);
	validity.SetAllInvalid(count);
	for (idx_t i = 0; i < hit_count; i++) {
		validity.SetValid(local.match_sel.get_index(i));
	}
}

} // namespace

ScalarFunction HashJoinProbeScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction fun(HashJoinProbeScalarFun::NAME, {input_type}, LogicalType::POINTER, HashJoinProbeFunction);
	fun.SetInitStateCallback(HashJoinProbeInitLocalState);
	// bypass the constant-NULL short-circuit so the callback always writes per-row hit/miss validity bits
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
