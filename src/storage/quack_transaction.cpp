#include "storage/quack_transaction.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_client.hpp"

namespace duckdb {

QuackTransaction::QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p,
                                   ClientContext &context_p)
    : Transaction(manager_p, context_p), quack_catalog(quack_catalog_p),
      transaction_state(QuackTransactionState::TRANSACTION_NOT_YET_STARTED),
      start_version(quack_catalog_p.InvalidatedVersion()) {
}

QuackTransaction::~QuackTransaction() {
	// not released by Commit/Rollback (e.g. COMMIT threw): the server-side state is unknown
	ReleaseConnection(true);
}

void QuackTransaction::Start() {
	transaction_state = QuackTransactionState::TRANSACTION_NOT_YET_STARTED;
}

void QuackTransaction::ForceStart() {
	if (transaction_state == QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_STARTED;
		Query("BEGIN TRANSACTION");
	}
}

void QuackTransaction::Commit() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		Query("COMMIT");
	}
	ReleaseConnection(false);
}

void QuackTransaction::Rollback() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		Query("ROLLBACK");
	}
	ReleaseConnection(false);
}

shared_ptr<QuackClientConnection> QuackTransaction::GetConnection(ClientContext &context) {
	if (!connection) {
		connection = quack_catalog.CheckoutConnection(context);
	}
	return connection;
}

void QuackTransaction::ReleaseConnection(bool force_dirty) {
	if (!connection) {
		return;
	}
	auto released = std::move(connection);
	connection = nullptr;
	quack_catalog.ReleaseConnection(released, dirty || force_dirty);
}

void QuackTransaction::Retain(shared_ptr<QuackSchemaSet> snapshot) {
	lock_guard<mutex> guard(retained_lock);
	if (retained_snapshots.empty() || retained_snapshots.back() != snapshot) {
		retained_snapshots.push_back(std::move(snapshot));
	}
}

optional_ptr<CatalogEntry> QuackTransaction::Retain(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(retained_lock);
	retained_entries.push_back(std::move(entry));
	return retained_entries.back().get();
}

QuackTransaction &QuackTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<QuackTransaction>();
}

QuackTransaction &QuackTransaction::Get(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return transaction.transaction->Cast<QuackTransaction>();
}

unique_ptr<ColumnDataCollection> QuackTransaction::Query(const string &query) {
	ForceStart();
	auto context_ref = context.lock();
	if (!context_ref) {
		// context has been destroyed - silently ignore the query (a server transaction may stay open on it)
		dirty = true;
		return nullptr;
	}
	try {
		return quack_catalog.ExecuteCommandInternal(*context_ref, query, *GetConnection(*context_ref));
	} catch (...) {
		dirty = true;
		throw;
	}
}

} // namespace duckdb
