#pragma once

#include <thread>
#include <condition_variable>

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include "quack_uri.hpp"

#include "httplib.hpp" // TODO forward declare

namespace duckdb {

class ClientContext;
class SecretManager;
struct SecretMatch;
struct CatalogTransaction;
class QuackMessage;
class Connection;
class MemoryStream;
class QueryResult;
class DatabaseInstance;
class PreparedStatement;
class EncryptionState;

enum class QuackQueryState : uint8_t { IDLE, ACTIVE, FINISHED, CANCELLED };

struct QuackConnection {
	explicit QuackConnection(string session_id_p);
	~QuackConnection();

	//! True iff the underlying DuckDB connection currently holds an open transaction.
	//! Null-guarded; safe to call when the connection has not issued any query yet.
	bool InTransaction() const;

	mutex lock;
	unique_ptr<Connection> duckdb_connection;
	unique_ptr<QueryResult> duckdb_query_result;
	//! Monotonic counter assigned per FETCH batch — enables order-preserving parallel scans on
	idx_t next_batch_index = 1;
	//! Current result UUID
	hugeint_t result_uuid;
	string session_id;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
	//! Wall-clock time the last message was handled for this session. Drives the idle reaper.
	timestamp_t last_activity {0};
};

struct QuackConnectionSnapshot {
	string server_id;
	string session_id;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
	timestamp_t last_activity {0};
	bool in_transaction = false;
};

class QuackServer {
public:
	static constexpr const idx_t QUACK_VERSION = 1;

public:
	explicit QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p,
	                     int64_t idle_in_transaction_timeout_p = 120, int64_t reaper_sweep_interval_p = 30);
	virtual ~QuackServer();

	//! Stop accepting new connections (close the listener socket) without
	//! joining listener threads. Safe to call from a request-handler thread —
	//! does not wait on httplib's task-queue, which would deadlock when the
	//! caller is itself a worker.
	virtual void StopAccepting() {};

	//! Synchronously stop accepting connections and join the listener threads.
	//! Must NOT be called from a worker / request-handler thread; httplib's
	//! listen-loop teardown joins all workers, which would deadlock.
	virtual void Close() {};

	shared_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &session_id);
	bool DisconnectConnection(const string &session_id);

	//! Sweep active_connections and roll back orphaned idle-in-transaction sessions.
	//! Reaps an entry only when it is the sole holder of its shared_ptr (use_count()==1),
	//! is not running a query, holds an open transaction, and has been idle past the TTL.
	//! `now` is injectable so the predicate is unit-testable against a synthetic clock.
	void ReapIdleConnections(timestamp_t now);

	string GenerateSessionId();

	//! Generate a fresh CSPRNG-backed 128-bit token, hex-encoded (32 chars).
	static string GenerateRandomToken(DatabaseInstance &db);

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	//! The scope a quack secret gets when the user doesn't specify one. It prefix-matches
	//! every quack URI, so a secret carrying it says nothing about which endpoint it is for.
	static constexpr const char *DEFAULT_SECRET_SCOPE = "quack:";

	//! Look up the quack secret matching `uri`. Takes a transaction and secret manager
	//! rather than a ClientContext so it also serves the context-free request path.
	//! Scopes are matched as plain string prefixes, so the canonical form is tried first
	//! and the raw spelling second - see the definition for why both are needed.
	static SecretMatch LookupSecret(CatalogTransaction transaction, SecretManager &secret_manager,
	                                const QuackUri &uri);

	//! Look up a quack secret matching `uri` and return its token, or an empty
	//! string when no secret matches. Lets a token be sourced from the secret
	//! manager when the caller didn't supply one explicitly.
	static string TokenFromSecret(ClientContext &context, const QuackUri &uri);

	//! As TokenFromSecret, but for the token a *server* listens with. Ignores a secret
	//! carrying only the catch-all DEFAULT_SECRET_SCOPE: such a secret was written to
	//! reach some other endpoint, and silently reusing it as this listener's token would
	//! grant its holders access to the new server.
	static string ListenTokenFromSecret(ClientContext &context, const QuackUri &uri);

	vector<QuackConnectionSnapshot> GetActiveConnectionSnap();

	const string &Token() {
		return token;
	}

	const QuackUri &ListenUri() const {
		return uri;
	}

	idx_t ActiveConnectionCount() {
		std::lock_guard<std::mutex> lock(active_connections_mutex);
		return active_connections.size();
	}

protected:
	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	unique_ptr<QuackMessage> HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

protected:
	std::vector<std::thread> listen_threads;

	weak_ptr<DatabaseInstance> db_ptr;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;

	mutex session_id_rng_mutex;
	shared_ptr<EncryptionState> session_id_rng;

	//! Idle-in-transaction reaper config (seconds). idle_in_transaction_timeout == 0 disables the reaper.
	int64_t idle_in_transaction_timeout;
	int64_t reaper_sweep_interval;

private:
	QuackUri uri;
	string token;
};

class HttpQuackServer : public QuackServer {
public:
	HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p,
	                int64_t idle_in_transaction_timeout_p = 120, int64_t reaper_sweep_interval_p = 30);

	void StopAccepting() override;
	void Close() override;

	~HttpQuackServer() override;

private:
	static void ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port);

	//! Background reaper: interruptible sleep of reaper_sweep_interval, then ReapIdleConnections(now).
	static void ReaperThread(HttpQuackServer *server);

	unique_ptr<QuackMessage> ReadMessage(MemoryStream &read_stream);

	unique_ptr<duckdb_httplib::Server> server;
	bool is_running = false;

	std::thread reaper_thread;
	std::mutex reaper_mutex;
	std::condition_variable reaper_cv;
	bool reaper_stop = false;
};

} // namespace duckdb
