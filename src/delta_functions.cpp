#include "delta_functions.hpp"

#include "duckdb.hpp"

namespace duckdb {

vector<TableFunctionSet> DeltaFunctions::GetTableFunctions(ExtensionLoader &loader) {
	vector<TableFunctionSet> functions;

	functions.push_back(GetDeltaScanFunction(loader));

	return functions;
}

vector<ScalarFunctionSet> DeltaFunctions::GetScalarFunctions(ExtensionLoader &loader) {
	vector<ScalarFunctionSet> functions;

	functions.push_back(GetExpressionFunction(loader));
	functions.push_back(GetWriteFileFunction(loader));

	return functions;
}

}; // namespace duckdb
