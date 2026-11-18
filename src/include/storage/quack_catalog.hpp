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

#include <condition_variable>
#include <deque>

namespace duckdb {

class QuackCatalog;
class QuackClient;
class QuackClientConnection;

//! Connection-pool counters of one catalog, surfaced by quack_pool_status()
struct QuackPoolStats {
	idx_t pool_size;
	idx_t live;
	idx_t in_use;
	idx_t idle;
	//! Connections closed on release because pool_size were already live: raise pool_size if this keeps growing
	idx_t overflow;
};

class QuackCatalog : public Catalog {
public:
	explicit QuackCatalog(AttachedDatabase &db_p, const QuackUri &server_uri_p, ClientContext &context,
	                      const string &token, string client_id, idx_t heartbeat_timeout_seconds, idx_t pool_size = 1);
	~QuackCatalog() override;

public:
	string GetCatalogType() override {
		return "quack";
	}
	bool SupportsNestedSchemas() const override {
		return true;
	}
	static QuackCatalog &GetQuackCatalog(ClientContext &context, Value &catalog_name);
	static bool IsQuackScan(const string &name);
	bool SupportsPushdown(const TableRef &ref) override;
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

	bool Supports(RemoteCapability capability) const override {
		switch (capability) {
		case RemoteCapability::IS_REMOTE:
		case RemoteCapability::EXECUTE_QUERY_NODE:
		case RemoteCapability::EXECUTE_STATEMENT:
		case RemoteCapability::CONNECT:
			return true;
		default:
			return false;
		}
	}
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<QueryNode> node) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, unique_ptr<SQLStatement> statement) override;
	unique_ptr<TableRef> RemoteExecute(ClientContext &context, const string &sql) override;
	string GetConnectDisplay() override {
		return GetDBPath();
	}

	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query);
	unique_ptr<ColumnDataCollection> ExecuteCommandInternal(ClientContext &context, const string &query,
	                                                        QuackClientConnection &connection);
	const QuackUri &GetServerUri();
	//! The id of the connection opened by ATTACH
	const string &GetConnectionId();

	//! The connection opened by ATTACH
	shared_ptr<QuackClientConnection> GetClientConnection();
	//! The connection "context" talks over: with pool_size > 1 and an active transaction, the pooled connection
	//! that transaction is pinned to; otherwise the connection opened by ATTACH
	shared_ptr<QuackClientConnection> GetClientConnection(ClientContext &context);

	//! Check out a connection for one transaction. pool_size 1 always hands out the ATTACH connection. Otherwise
	//! an idle pooled connection is reused, or a new one is opened
	shared_ptr<QuackClientConnection> CheckoutConnection(ClientContext &context);
	//! Return a connection from CheckoutConnection. It goes back to the idle set unless it is dirty (its
	//! server-side state is unknown) or pool_size connections are live already; then it is dropped, which
	//! DISCONNECTs it
	void ReleaseConnection(const shared_ptr<QuackClientConnection> &connection, bool dirty);
	idx_t PoolSize() const {
		return pool_size;
	}
	QuackPoolStats GetPoolStats();

	void Refresh(ClientContext &context);
	void Invalidate();
	void InvalidateIfChangedSince(idx_t version);
	idx_t InvalidatedVersion();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	//! Build the "quack_query_by_name" table function call that runs "sql" on the server. Statements pushed
	//! down by the RemotePushdownOptimizer change the remote catalog, so they ask for a catalog refresh
	unique_ptr<TableRef> CreateRemoteQueryRef(const string &sql, bool refresh_catalog, bool use_transaction = true);

	QuackLoadCatalogData LoadCatalog(ClientContext &context);
	QuackSchemaSet &GetSchemas(ClientContext &context);
	shared_ptr<QuackSchemaSet> AcquireSnapshot(ClientContext &context);
	bool ServesOwnConnection(ClientContext &context);

private:
	shared_ptr<QuackClientConnection> client_connection;
	//! What a pooled connection is minted with, resolved at ATTACH: a mint runs without a context, so it cannot
	//! consult the secret manager
	string token;
	string client_id;
	idx_t heartbeat_timeout_seconds;
	idx_t pool_size;
	mutex pool_lock;
	//! Warm pooled connections (never the ATTACH connection); each keeps its own heartbeat running
	std::deque<shared_ptr<QuackClientConnection>> idle_connections;
	//! Live connections, the ATTACH connection included
	idx_t live_count = 1;
	idx_t in_use_count = 0;
	idx_t overflow_count = 0;
	mutex snapshot_lock;
	std::condition_variable snapshot_loaded;
	shared_ptr<QuackSchemaSet> snapshot;
	idx_t invalidated_version = 0;
	idx_t loaded_version = 0;
	optional_ptr<ClientContext> loading_context;
};

} // namespace duckdb
