#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/database_size.hpp"

#include "storage/quack_catalog.hpp"
#include "storage/quack_table.hpp"
#include "quack_scan.hpp"
#include "storage/quack_insert.hpp"
#include "quack_message.hpp"
#include "quack_client.hpp"
#include "storage/quack_transaction.hpp"

#include <chrono>

// FIXME bunch of stuff copied from postgres scanner, can probably be simplified!

namespace duckdb {

QuackCatalog::QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri, ClientContext &context,
                           const string &token, idx_t pool_size)
    : Catalog(db_p), pool_size(pool_size), token(token) {
	// connect to the server
	client_connection = QuackClient::ConnectToServer(context, server_uri, token);

	// load the entire catalog up-front
	auto load_info = LoadCatalog(context);
	schemas = make_uniq<QuackSchemaSet>(context, *this, load_info);
}

QuackLoadCatalogData QuackCatalog::LoadCatalog(ClientContext &context) {
	QuackLoadCatalogData result;
	result.schemas = ExecuteCommandInternal(context, QuackSchemaSet::GetLoadQuery());
	result.tables = ExecuteCommandInternal(context, QuackTableSet::GetLoadQuery());
	return result;
}

QuackCatalog::~QuackCatalog() {
	// Idle pooled connections in `idle_connections` are released here when
	// the deque is destroyed: each QuackClientConnection's dtor sends a
	// DISCONNECT to the server. Checked-out connections (held by the
	// caller) DISCONNECT when the caller drops them. The primary
	// `client_connection` DISCONNECTs the same way. No explicit cleanup
	// is required.
}

shared_ptr<QuackClientConnection> QuackCatalog::CheckoutConnection(ClientContext &context) {
	// Fast path: single-connection catalogs (default) always use the primary.
	// This is load-bearing for bare-quack correctness (see design §4.1-4.2):
	// primary stays the only connection and no new server ids are minted.
	if (pool_size <= 1) {
		return client_connection;
	}

	unique_lock<mutex> guard(pool_lock);
	while (true) {
		// 1) Reuse an idle pooled connection if one is available.
		if (!idle_connections.empty()) {
			auto conn = idle_connections.front();
			idle_connections.pop_front();
			in_use_count++;
			return conn;
		}
		// 2) Lazily grow the pool up to pool_size. live_count includes the
		//    primary, so the pool can mint (pool_size - 1) extra connections.
		if (live_count < pool_size) {
			live_count++;
			in_use_count++;
			// Mint outside the lock would be cleaner, but ConnectToServer is
			// safe to call here; keep it simple. If ConnectToServer throws,
			// roll back the counts so the pool stays consistent.
			guard.unlock();
			try {
				auto conn = QuackClient::ConnectToServer(context, GetServerUri(), token);
				return conn;
			} catch (...) {
				guard.lock();
				live_count--;
				in_use_count--;
				pool_cv.notify_one();
				throw;
			}
		}
		// 3) Saturated: block until a connection is released or we time out /
		//    the query is interrupted. Bound the wait so we never hang.
		//    Accumulate the blocked time into checkout_wait_us for observability
		//    (quack_pool_status / spec §7.4); measure across the wait_for call
		//    while still holding pool_lock so the counter update is guarded.
		if (context.IsInterrupted()) {
			throw InterruptException();
		}
		auto wait_start = std::chrono::steady_clock::now();
		auto status = pool_cv.wait_for(guard, std::chrono::seconds(5));
		checkout_wait_us += (idx_t)std::chrono::duration_cast<std::chrono::microseconds>(
		                        std::chrono::steady_clock::now() - wait_start)
		                        .count();
		if (status == std::cv_status::timeout) {
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
			// Record the give-up before throwing (still under pool_lock).
			checkout_timeout_count++;
			throw IOException(
			    "quack connection pool exhausted: all %llu connections are in use and none "
			    "were released within the timeout",
			    (unsigned long long)pool_size);
		}
		// Spurious or genuine wakeup: loop and re-evaluate.
	}
}

void QuackCatalog::ReleaseConnection(const shared_ptr<QuackClientConnection> &conn, bool dirty) {
	// Nothing to return for the single-connection fast path or the primary.
	if (pool_size <= 1 || conn.get() == client_connection.get()) {
		return;
	}
	lock_guard<mutex> guard(pool_lock);
	if (in_use_count > 0) {
		in_use_count--;
	}
	if (dirty) {
		// Discard: do not return a dirty connection to the idle set. We do
		// not retain it here, so the caller's shared_ptr is the last owner;
		// when it drops, QuackClientConnection's dtor DISCONNECTs. Free the
		// slot so a future checkout can mint a replacement.
		if (live_count > 1) {
			live_count--;
		}
	} else {
		// Clean connection: return it to the warm idle set for reuse.
		idle_connections.push_back(conn);
	}
	pool_cv.notify_one();
}

void QuackCatalog::Initialize(bool load_builtin) {
}

optional_ptr<SchemaCatalogEntry> QuackCatalog::LookupSchema(CatalogTransaction transaction,
                                                            const EntryLookupInfo &schema_lookup,
                                                            OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	auto schema_entry = schemas->GetEntry(schema_name);
	if (schema_entry) {
		return schema_entry->Cast<SchemaCatalogEntry>();
	}
	switch (if_not_found) {
	case OnEntryNotFound::THROW_EXCEPTION:
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	case OnEntryNotFound::RETURN_NULL:
	default:
		return nullptr;
	}
}

const QuackUri &QuackCatalog::GetServerUri() {
	return client_connection->ServerURI();
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommandInternal(ClientContext &context, const string &query) {
	return ExecuteCommandInternal(context, query, *client_connection);
}

unique_ptr<ColumnDataCollection> QuackCatalog::ExecuteCommandInternal(ClientContext &context, const string &query,
                                                                      QuackClientConnection &conn) {
	auto chunk_collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
	// get a client to query
	auto client_wrapper = conn.GetClient(context);
	auto &client = client_wrapper->GetClient();
	auto response =
	    client.Request<PrepareResponseMessage>(context, make_uniq<PrepareRequestMessage>(conn.ConnectionId(), query));
	chunk_collection->Initialize(response->Types());
	for (auto &chunk : response->MutableResults()) {
		chunk_collection->Append(chunk->Chunk());
	}
	// The first PREPARE batch only carries up to quack_fetch_batch_chunks chunks. If the result is
	// larger, keep issuing FETCH on the same connection/result until the server returns an empty
	// batch (mirrors the data-path loop in QuackScan). Without this, catalog loads silently
	// truncate above ~one batch of rows (~24k by default), dropping tables/schemas.
	if (response->NeedsMoreFetch()) {
		auto result_uuid = response->ResultUUID();
		while (true) {
			auto fetch_response = client.Request<FetchResponseMessage>(
			    context, make_uniq<FetchRequestMessage>(conn.ConnectionId(), result_uuid));
			if (fetch_response->MutableResults().empty()) {
				// server is done
				break;
			}
			for (auto &chunk : fetch_response->MutableResults()) {
				chunk_collection->Append(chunk->Chunk());
			}
		}
	}
	return chunk_collection;
}

shared_ptr<QuackClientConnection> QuackCatalog::GetClientConnection() {
	return client_connection;
}

void QuackCatalog::Refresh(ClientContext &context) {
	auto load_info = LoadCatalog(context);
	schemas->Reload(context, *this, load_info);
}

const string &QuackCatalog::GetConnectionId() {
	return client_connection->ConnectionId();
}

optional_ptr<CatalogEntry> QuackCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &quack_transaction = QuackTransaction::Get(transaction);
	// create schema remotely
	quack_transaction.Query(info.ToString());
	// register schema locally
	auto schema_entry = make_uniq<QuackSchemaCatalogEntry>(*this, info);
	return schemas->CreateEntry(std::move(schema_entry), info.on_conflict);
}

void QuackCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	for (auto &schema : schemas->GetAllCatalogEntries()) {
		callback(schema.get().Cast<SchemaCatalogEntry>());
	}
}

PhysicalOperator &QuackCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanDelete not implemented yet");
}
PhysicalOperator &QuackCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                           PhysicalOperator &plan) {
	throw NotImplementedException("PlanUpdate not implemented yet");
}

unique_ptr<LogicalOperator> QuackCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                          TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("BindCreateIndex not implemented yet");
}

DatabaseSize QuackCatalog::GetDatabaseSize(ClientContext &context) {
	throw NotImplementedException("GetDatabaseSize not implemented yet");
}

bool QuackCatalog::InMemory() {
	return false;
}
string QuackCatalog::GetDBPath() {
	return client_connection->ServerURI().Uri();
}

void QuackCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// TODO should we just send over the drop info in a dropmessage???
	throw NotImplementedException("DropSchema not implemented yet");
}

} // namespace duckdb
