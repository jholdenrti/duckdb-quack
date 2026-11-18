//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "storage/quack_schema.hpp"

namespace duckdb {

class QuackCatalog;
class QuackClientConnection;

enum class QuackTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class QuackTransaction : public Transaction {
public:
	QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p, ClientContext &context_p);
	~QuackTransaction() override;

	//! Lazily start a transaction - this won't actually do anything until the first query is fired
	void Start();
	//! Forcibly start a transaction - ensure we actually start a transaction server-side
	void ForceStart();
	void Commit();
	void Rollback();

	static QuackTransaction &Get(ClientContext &context, Catalog &catalog);
	static QuackTransaction &Get(CatalogTransaction transaction);

	unique_ptr<ColumnDataCollection> Query(const string &query);

	//! The connection this transaction is pinned to: checked out of the catalog's pool on first use and held
	//! until Commit/Rollback, so all of the transaction's statements share one server-side session
	shared_ptr<QuackClientConnection> GetConnection(ClientContext &context);

	idx_t StartVersion() const {
		return start_version;
	}
	void Retain(shared_ptr<QuackSchemaSet> snapshot);
	optional_ptr<CatalogEntry> Retain(unique_ptr<CatalogEntry> entry);

private:
	QuackCatalog &quack_catalog;
	QuackTransactionState transaction_state;
	idx_t start_version;
	mutex retained_lock;
	vector<shared_ptr<QuackSchemaSet>> retained_snapshots;
	vector<unique_ptr<CatalogEntry>> retained_entries;
	shared_ptr<QuackClientConnection> connection;
	//! The pinned connection may carry a half-run statement or an open server transaction; never reuse it
	bool dirty = false;

	void ReleaseConnection(bool force_dirty);
};

} // namespace duckdb
