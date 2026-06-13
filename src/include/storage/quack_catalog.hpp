//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/quack_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "storage/quack_schema.hpp"
#include "quack_uri.hpp"

#include <mutex>
#include <condition_variable>
#include <deque>

namespace duckdb {

class QuackCatalog;
class QuackClient;
class QuackClientConnection;

//! Snapshot of one catalog's connection-pool state, returned by
//! QuackCatalog::GetPoolStats() and surfaced by the quack_pool_status()
//! table function (client-side observability, design §7.4).
struct QuackPoolStats {
	idx_t pool_size;
	idx_t live;
	idx_t in_use;
	idx_t idle;
	idx_t checkout_wait_us;
	idx_t checkout_timeouts;
};

class QuackCatalog : public Catalog {
public:
	explicit QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context,
	                      const string &token, idx_t pool_size = 1);
	~QuackCatalog() override;

public:
	string GetCatalogType() override {
		return "quack";
	}
	static bool IsQuackScan(const string &name);
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query);
	//! Execute a metadata command on a specific connection (used by QuackTransaction
	//! to keep all of a transaction's SQL on its pinned, checked-out connection).
	//! The primary-connection overload above remains for ATTACH-time LoadCatalog /
	//! Refresh, which always run on the catalog's primary connection.
	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query,
	                                                        QuackClientConnection &conn);
	const QuackUri &GetServerUri();
	const string &GetConnectionId();

	shared_ptr<QuackClientConnection> GetClientConnection();

	//! Check out a connection for the duration of a unit of work. At
	//! pool_size == 1 this returns the primary connection directly (no new
	//! server-side connection ids are minted). Otherwise it returns an idle
	//! pooled connection, lazily mints a new one if live_count < pool_size,
	//! or blocks (bounded by the context's interrupt/cancellation and a
	//! finite default timeout) until one is released — throwing on timeout.
	shared_ptr<QuackClientConnection> CheckoutConnection(ClientContext &context);

	//! Return a connection previously obtained from CheckoutConnection. At
	//! pool_size == 1, or when conn is the primary, this is a no-op. If
	//! `dirty` is true the connection is discarded (dropped so its dtor
	//! DISCONNECTs) instead of being returned to the idle set.
	void ReleaseConnection(const shared_ptr<QuackClientConnection> &conn, bool dirty);

	idx_t PoolSize() const {
		return pool_size;
	}

	//! Read a consistent snapshot of the connection-pool counters under
	//! pool_lock. Used by the quack_pool_status() table function.
	QuackPoolStats GetPoolStats();

	void Refresh(ClientContext &context);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	QuackLoadCatalogData LoadCatalog(ClientContext &context);

private:
	shared_ptr<QuackClientConnection> client_connection;
	unique_ptr<QuackSchemaSet> schemas;
	idx_t pool_size;

	//! Authentication token captured at ATTACH time, needed to mint new
	//! pooled connections.
	string token;

	//! Guards the pool bookkeeping below.
	mutex pool_lock;
	//! Signalled when a connection is released, to wake a blocked checkout.
	std::condition_variable pool_cv;
	//! Warm, idle pooled connections available for checkout (does NOT include
	//! the primary `client_connection`). On catalog destruction these drop and
	//! their QuackClientConnection dtors DISCONNECT from the server.
	std::deque<shared_ptr<QuackClientConnection>> idle_connections;
	//! Total live connections counted against pool_size: the primary plus any
	//! lazily-minted pooled connections (idle or checked out). Starts at 1.
	idx_t live_count = 1;
	//! Connections currently checked out (excludes the primary fast path).
	idx_t in_use_count = 0;
	//! Cumulative time (microseconds) callers spent BLOCKED in CheckoutConnection
	//! waiting for a connection to free up under saturation. Surfaced by
	//! quack_pool_status() (design success criteria / spec §7.4 "checkout-wait").
	idx_t checkout_wait_us = 0;
	//! Cumulative count of checkouts that gave up because the pool stayed
	//! saturated past the wait timeout. Surfaced by quack_pool_status().
	idx_t checkout_timeout_count = 0;
};

} // namespace duckdb
