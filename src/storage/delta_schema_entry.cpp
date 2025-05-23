#include "storage/delta_schema_entry.hpp"

#include "functions/delta_scan/delta_multi_file_list.hpp"
#include "storage/delta_catalog.hpp"

#include "delta_extension.hpp"

#include "storage/delta_table_entry.hpp"
#include "storage/delta_transaction.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

DeltaSchemaEntry::DeltaSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

DeltaSchemaEntry::~DeltaSchemaEntry() {
}

DeltaTransaction &GetDeltaTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<DeltaTransaction>();
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw BinderException("Delta tables do not support creating tables");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("Delta tables do not support creating functions");
}

void DeltaUnqualifyColumnRef(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto name = std::move(colref.column_names.back());
		colref.column_names = {std::move(name)};
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(expr, DeltaUnqualifyColumnRef);
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	throw NotImplementedException("CreateIndex");
}

string GetDeltaCreateView(CreateViewInfo &info) {
	throw NotImplementedException("GetCreateView");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw BinderException("Delta tables do not support creating views");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("Delta databases do not support creating types");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("Delta databases do not support creating sequences");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                 CreateTableFunctionInfo &info) {
	throw BinderException("Delta databases do not support creating table functions");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                CreateCopyFunctionInfo &info) {
	throw BinderException("Delta databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                  CreatePragmaFunctionInfo &info) {
	throw BinderException("Delta databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                             CreateCollationInfo &info) {
	throw BinderException("Delta databases do not support creating collations");
}

void DeltaSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("Delta tables do not support altering");
}

static bool CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return true;
	default:
		return false;
	}
}

unique_ptr<DeltaTableEntry> DeltaSchemaEntry::CreateTableEntry(ClientContext &context) {
	auto &delta_catalog = catalog.Cast<DeltaCatalog>();
	auto snapshot = make_shared_ptr<DeltaMultiFileList>(context, delta_catalog.GetDBPath());

	// Get the names and types from the delta snapshot
	vector<LogicalType> return_types;
	vector<string> names;
	snapshot->Bind(return_types, names);

	CreateTableInfo table_info;
	for (idx_t i = 0; i < return_types.size(); i++) {
		table_info.columns.AddColumn(ColumnDefinition(names[i], return_types[i]));
	}
	table_info.table = delta_catalog.GetName();
	auto table_entry = make_uniq<DeltaTableEntry>(delta_catalog, *this, table_info);
	table_entry->snapshot = std::move(snapshot);

	return table_entry;
}

void DeltaSchemaEntry::Scan(ClientContext &context, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	if (CatalogTypeIsSupported(type)) {
		auto transaction = catalog.GetCatalogTransaction(context);
		auto lookup_info = EntryLookupInfo(type, catalog.GetName());
		auto default_table = LookupEntry(transaction, lookup_info);
		if (default_table) {
			callback(*default_table);
		}
	}
}

void DeltaSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void DeltaSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("Delta tables do not support dropping");
}

optional_ptr<CatalogEntry> DeltaSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	if (!transaction.HasContext()) {
		throw NotImplementedException("Can not DeltaSchemaEntry::GetEntry without context");
	}
	auto &context = transaction.GetContext();

	auto type = lookup_info.GetCatalogType();
	auto &name = lookup_info.GetEntryName();
	if (type == CatalogType::TABLE_ENTRY && name == catalog.GetName()) {
		auto &delta_transaction = GetDeltaTransaction(transaction);
		auto &delta_catalog = catalog.Cast<DeltaCatalog>();

		auto transaction_table_entry = delta_transaction.GetTableEntry();
		if (transaction_table_entry) {
			return *transaction_table_entry;
		}

		if (delta_catalog.UseCachedSnapshot()) {
			unique_lock<mutex> l(lock);
			if (!cached_table) {
				cached_table = CreateTableEntry(context);
			}
			return *cached_table;
		}

		return delta_transaction.InitializeTableEntry(context, *this);
	}
	return nullptr;
}

optional_ptr<DeltaTableEntry> DeltaSchemaEntry::GetCachedTable() {
	lock_guard<mutex> lck(lock);
	if (cached_table) {
		return *cached_table;
	}
	return nullptr;
}

} // namespace duckdb
