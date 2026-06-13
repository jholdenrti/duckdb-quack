#include "storage/quack_transaction.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/quack_catalog.hpp"
#include "quack_client.hpp"

namespace duckdb {

QuackTransaction::QuackTransaction(QuackCatalog &quack_catalog_p, TransactionManager &manager_p,
                                   ClientContext &context_p)
    : Transaction(manager_p, context_p), quack_catalog(quack_catalog_p),
      transaction_state(QuackTransactionState::TRANSACTION_NOT_YET_STARTED) {
}

QuackTransaction::~QuackTransaction() {
	// Safety net: if the connection was never released by Commit/Rollback,
	// the server-side state is unknown - discard it (dirty) so it is not
	// returned clean to the idle pool. No-op if already released (connection
	// is nulled after release).
	ReleaseConnectionIfHeld(true);
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
	ReleaseConnectionIfHeld(false);
}

void QuackTransaction::Rollback() {
	if (transaction_state == QuackTransactionState::TRANSACTION_STARTED) {
		transaction_state = QuackTransactionState::TRANSACTION_FINISHED;
		Query("ROLLBACK");
	}
	ReleaseConnectionIfHeld(false);
}

QuackClientConnection &QuackTransaction::GetConnection(ClientContext &context) {
	if (!connection) {
		// First metadata access: check one connection out of the catalog's pool and
		// hold it for this transaction's entire lifetime (affinity). At pool_size==1
		// this returns the primary connection, preserving single-lane behavior.
		connection = quack_catalog.CheckoutConnection(context);
	}
	return *connection;
}

void QuackTransaction::ReleaseConnectionIfHeld(bool forced_dirty) {
	if (!connection) {
		return;
	}
	bool release_dirty = dirty || forced_dirty;
	quack_catalog.ReleaseConnection(connection, release_dirty);
	connection = nullptr; // prevent double-release (dtor safety-net)
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
		// context has been destroyed - silently ignore the query
		return nullptr;
	}
	try {
		return quack_catalog.ExecuteCommandInternal(*context_ref, query, GetConnection(*context_ref));
	} catch (...) {
		// The connection may now carry a half-executed statement or an aborted
		// server-side transaction. Mark it dirty so it is discarded rather than
		// returned clean to the idle pool (design.md Approach #3, spec §5.4).
		dirty = true;
		throw;
	}
}

} // namespace duckdb
