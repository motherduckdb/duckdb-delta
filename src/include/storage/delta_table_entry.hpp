//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
class DeltaMultiFileList;

class DeltaTableEntry : public TableCatalogEntry {
public:
	DeltaTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);
	~DeltaTableEntry();

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data, const EntryLookupInfo &lookup_info) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

public:
	shared_ptr<DeltaMultiFileList> snapshot;

protected:
    TableFunction GetScanFunctionInternal(ClientContext &context, unique_ptr<FunctionData> &bind_data, optional_ptr<const EntryLookupInfo > lookup_info);
};

} // namespace duckdb
