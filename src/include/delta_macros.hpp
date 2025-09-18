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
class ExtensionLoader;

class DeltaMacros {
public:
	static void RegisterMacros(ExtensionLoader &loader);

protected:
	static void RegisterTableMacro(ExtensionLoader &loader, const string &name, const string &query,
	                               const vector<string> &params, const child_list_t<Value> &named_params);
};
} // namespace duckdb
