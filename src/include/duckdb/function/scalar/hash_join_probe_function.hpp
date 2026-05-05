//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/hash_join_probe_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

class JoinHashTable;

//! Carries a non-owning pointer to a finalised JoinHashTable into the ht_probe scalar-function callback.
struct HashJoinProbeFunctionData : public FunctionData {
	HashJoinProbeFunctionData(optional_ptr<JoinHashTable> hash_table, LogicalType key_type);

	optional_ptr<JoinHashTable> hash_table;
	LogicalType key_type;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

struct HashJoinProbeScalarFun {
	static constexpr const char *NAME = "ht_probe";

	//! Build a ScalarFunction that resolves head-of-chain pointers in the bound JoinHashTable.
	//! Constructed locally in PhysicalHashJoin::GetOperatorState; not registered in the catalog.
	static ScalarFunction GetFunction(const LogicalType &input_type);
};

} // namespace duckdb
