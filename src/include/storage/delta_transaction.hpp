//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/delta_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "delta_utils.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class DeltaCatalog;
class DeltaSchemaEntry;
class DeltaTableEntry;
class DeltaMultiFileList;
struct DeltaDataFile;
struct DeltaMultiFileColumnDefinition;

enum class DeltaTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class DeltaTransaction : public Transaction {
public:
	DeltaTransaction(DeltaCatalog &delta_catalog, TransactionManager &manager, ClientContext &context);
	~DeltaTransaction() override;

	void Start();
	void Commit(ClientContext &context);
	void Rollback();

    void Append(ClientContext &context, const vector<DeltaDataFile> &append_files);

	void SetTransactionVersion(const string &app_id, idx_t new_version, Value expected_value);

	static DeltaTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const;

    bool HasOutstandingAppends() const;

	optional_ptr<DeltaTableEntry> GetTableEntry(idx_t version);

	DeltaTableEntry &InitializeTableEntry(ClientContext &context, DeltaSchemaEntry &schema_entry, idx_t version);
    vector<DeltaMultiFileColumnDefinition> GetWriteSchema(ClientContext &context);

    //! Removes all outstanding appends and removes the files if possible
    void CleanUpFiles();

protected:
    void InitializeTransaction(ClientContext &context);

private:
	mutable mutex lock;

    //! Cached table entry (without a specified version)
    unique_ptr<DeltaTableEntry> table_entry;
    //! Cached table entries at specific versions
    unordered_map<idx_t, unique_ptr<DeltaTableEntry>> versioned_table_entries;

	//	DeltaConnection connection;
	DeltaTransactionState transaction_state;

    const AccessMode access_mode;

    vector<DeltaDataFile> outstanding_appends;

    KernelExclusiveTransaction kernel_transaction;

    //! stores a ptr to the table entry that this transaction is writing to
    optional_ptr<DeltaTableEntry> write_entry;

	// Versions registered to this transaction
	struct TransactionVersion {
		idx_t new_version;
		Value expected_version;
	};
	unordered_map<string, TransactionVersion> app_versions;
};

} // namespace duckdb
