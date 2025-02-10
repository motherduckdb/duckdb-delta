#include "delta_functions.hpp"
#include "functions/delta_scan/delta_scan.hpp"
#include "functions/delta_scan/delta_multi_file_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

static InsertionOrderPreservingMap<string> DeltaFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;

	if (input.table_function.function_info) {
		auto &table_info = input.table_function.function_info->Cast<DeltaFunctionInfo>();
		result["Table"] = table_info.table_name;
	}

	return result;
}

TableFunctionSet DeltaFunctions::GetDeltaScanFunction(DatabaseInstance &instance) {
	// Parquet extension needs to be loaded for this to make sense
	ExtensionHelper::AutoLoadExtension(instance, "parquet");

	// The delta_scan function is constructed by grabbing the parquet scan from the Catalog, then injecting the
	// DeltaMultiFileReader into it to create a Delta-based multi file read
	auto &parquet_scan = ExtensionUtil::GetTableFunction(instance, "parquet_scan");
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Register the MultiFileReader as the driver for reads
		function.get_multi_file_reader = DeltaMultiFileReader::CreateInstance;

		// Unset all of these: they are either broken, very inefficient.
		// TODO: implement/fix these
		function.serialize = nullptr;
		function.deserialize = nullptr;
		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = nullptr;

		function.to_string = DeltaFunctionToString;

		// Schema param is just confusing here
		function.named_parameters.erase("schema");

		// Demonstration of a generated column based on information from DeltaMultiFileList
		function.named_parameters["delta_file_number"] = LogicalType::BOOLEAN;

		function.name = "delta_scan";
	}

	parquet_scan_copy.name = "delta_scan";
	return parquet_scan_copy;
}

} // namespace duckdb
