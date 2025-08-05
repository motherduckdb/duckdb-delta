#include "delta_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

vector<TableFunctionSet> DeltaFunctions::GetTableFunctions(DatabaseInstance &instance) {
	vector<TableFunctionSet> functions;

	functions.push_back(GetDeltaScanFunction(instance));

	return functions;
}

vector<ScalarFunctionSet> DeltaFunctions::GetScalarFunctions(DatabaseInstance &instance) {
	vector<ScalarFunctionSet> functions;

	functions.push_back(GetExpressionFunction(instance));
	functions.push_back(GetWriteFileFunction(instance));

	return functions;
}

}; // namespace duckdb
