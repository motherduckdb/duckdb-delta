//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/delta_macros.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/database.hpp"

namespace duckdb {

class DeltaMacros {
public:
	static void RegisterMacros(DatabaseInstance &instance);

protected:
	static void RegisterTableMacro(DatabaseInstance &db, const string &name, const string &query,
	                               const vector<string> &params, const child_list_t<Value> &named_params);
};
} // namespace duckdb
