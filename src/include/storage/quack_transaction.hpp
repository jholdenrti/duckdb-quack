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

	//! Returns the connection this transaction is pinned to, checking one out
	//! from the catalog's pool on first call and holding it for the
	//! transaction's lifetime (per-transaction affinity, design.md Approach #3).
	QuackClientConnection &GetConnection(ClientContext &context);

private:
	//! Release the pinned connection back to the pool exactly once. Pass
	//! forced_dirty=true to discard it even if no query has flagged dirty.
	void ReleaseConnectionIfHeld(bool forced_dirty);

private:
	QuackCatalog &quack_catalog;
	QuackTransactionState transaction_state;
	//! The pooled connection this transaction is pinned to. Null until the
	//! first GetConnection() call. Released back to the pool on Commit/Rollback
	//! (clean) or in the destructor / on cancellation (dirty).
	shared_ptr<QuackClientConnection> connection;
	//! Set true when this transaction may have left the server connection in an
	//! unknown/aborted state (a metadata query threw, or the txn is destroyed
	//! without a clean Commit/Rollback). A dirty connection is discarded by the
	//! pool, never returned to the idle set (design.md Approach #3, spec §5.4).
	bool dirty = false;
};

} // namespace duckdb
