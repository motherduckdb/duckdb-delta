//===----------------------------------------------------------------------===//
//                         DuckDB
//
// delta_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {
class ExtensionLoader;

class DeltaFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions(ExtensionLoader &loader);
	static vector<ScalarFunctionSet> GetScalarFunctions(ExtensionLoader &loader);

private:
	//! Table Functions
	static TableFunctionSet GetDeltaScanFunction(ExtensionLoader &loader);

	//! Scalar Functions
	static ScalarFunctionSet GetExpressionFunction(ExtensionLoader &loader);
};
} // namespace duckdb
