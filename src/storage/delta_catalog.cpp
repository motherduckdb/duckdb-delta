#include "storage/delta_catalog.hpp"
#include "storage/delta_schema_entry.hpp"
#include "storage/delta_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

#include "functions/delta_scan/delta_multi_file_list.hpp"

namespace duckdb {

DeltaCatalog::DeltaCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode), use_cache(false), pushdown_partition_info(true),
      filter_pushdown_mode(DEFAULT_PUSHDOWN_MODE) {
}

DeltaCatalog::~DeltaCatalog() = default;

void DeltaCatalog::Initialize(bool load_builtin) {
	CreateSchemaInfo info;
	main_schema = make_uniq<DeltaSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> DeltaCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw BinderException("Delta tables do not support creating new schemas");
}

void DeltaCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw BinderException("Delta tables do not support dropping schemas");
}

void DeltaCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> DeltaCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA || schema_lookup.GetEntryName() == INVALID_SCHEMA) {
		return main_schema.get();
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	return nullptr;
}

bool DeltaCatalog::InMemory() {
	return false;
}

string DeltaCatalog::GetDBPath() {
	return path;
}

bool DeltaCatalog::UseCachedSnapshot() {
	return use_cache;
}

optional_idx DeltaCatalog::GetCatalogVersion(ClientContext &context) {
	auto &delta_transaction = DeltaTransaction::Get(context, *this);
	idx_t version = DConstants::INVALID_INDEX;

	// Option 1: snapshot is cached table-wide
	auto cached_snapshot = main_schema->GetCachedTable();
	if (cached_snapshot) {
		version = cached_snapshot->snapshot->GetVersion();
	}

	// Option 2: snapshot is cached in transaction
	auto transaction_table_entry = delta_transaction.GetTableEntry();
	if (transaction_table_entry) {
		version = transaction_table_entry->snapshot->GetVersion();
	}

	if (version != DConstants::INVALID_INDEX) {
		return version;
	}

	return optional_idx::Invalid();
}

DatabaseSize DeltaCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

PhysicalOperator &DeltaCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                  LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("DeltaCatalog does not support creating new tables");
}
PhysicalOperator &DeltaCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support inserts");
}
PhysicalOperator &DeltaCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("DeltaCatalog does not support deletes");
}
PhysicalOperator &DeltaCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("DeltaCatalog does not support updates");
}
unique_ptr<LogicalOperator> DeltaCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                          TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("DeltaCatalog does not support creating indices");
}

} // namespace duckdb
