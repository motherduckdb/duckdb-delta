//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/delta_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_utils.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
class DeltaMultiFileList;

struct DeltaFunctionInfo : public TableFunctionInfo {
	shared_ptr<DeltaMultiFileList> snapshot;
	string expected_path;
	string table_name;
};

} // namespace duckdb
