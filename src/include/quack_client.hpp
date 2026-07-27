#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_uri.hpp"

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

class QuackClient {
public:
	explicit QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p);
	virtual ~QuackClient();

	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		auto response_message = RequestInternal(context, std::move(request_message));
		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				// if we get an error throw it immediately
				response_message->Cast<ErrorResponse>().Error().Throw();
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr_cast<QuackMessage, TARGET>(std::move(response_message));
	}

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token);
	//! Context-free connect: performs the CONNECT handshake without a ClientContext,
	//! so it can run from inside an in-flight query/transaction without re-entering
	//! the busy context's lock. Requires a non-empty token (no secret-manager
	//! fallback, since that needs a context) — resolve it once at ATTACH via
	//! ResolveToken and store it on the catalog.
	static shared_ptr<QuackClientConnection> ConnectToServer(DatabaseInstance &db, const QuackUri &uri, string token);
	//! Resolve an auth token: if `token` is non-empty return it unchanged; otherwise
	//! look it up from the secret manager (TYPE QUACK secret matching `uri`). Shared
	//! by the context ConnectToServer and the catalog ctor so the token stored for
	//! later context-free mints is identical to the one the primary connection uses.
	static string ResolveToken(ClientContext &context, const QuackUri &uri, string token);

protected:
	mutex request_mutex;
	MemoryStream read_stream, write_stream;
	DatabaseInstance &db;
	QuackUri uri;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                                 unique_ptr<QuackMessage> request_message) = 0;
};

class QuackClientConnection : public enable_shared_from_this<QuackClientConnection> {
public:
	// max_connections_cached caps how many idle keep-alive clients this
	// connection_id retains for reuse. A single query can check out several
	// clients concurrently (parallel metadata/data-file RPCs); anything beyond
	// this cap is destroyed on return, closing its connection and churning a fresh
	// TCP connection (and a TIME_WAIT socket) on the next checkout. Caching the
	// working set instead keeps those connections alive and reused. Default sized
	// to cover typical per-query RPC concurrency.
	explicit QuackClientConnection(unique_ptr<QuackClient> client_p, QuackUri uri_p, string connection_id_p,
	                               idx_t max_connections_cached = 8);
	~QuackClientConnection();

	const string &ConnectionId() const {
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}

	//! Get a client (either a cached one, or open a new one if required)
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

private:
	QuackUri uri;
	string connection_id;
	mutable mutex lock;
	mutable vector<unique_ptr<QuackClient>> cached_clients;
	idx_t max_connections_cached;
};

struct QuackClientWrapper {
	QuackClientWrapper(unique_ptr<QuackClient> client, shared_ptr<const QuackClientConnection> client_connection);
	~QuackClientWrapper();

	QuackClient &GetClient();

private:
	unique_ptr<QuackClient> client;
	shared_ptr<const QuackClientConnection> client_connection;
};

class HttpsQuackClient : public QuackClient {
public:
	static constexpr uint64_t HTTP_TIMEOUT_SECONDS = 86400;
	//! Quack pins its own retry count rather than inheriting httpfs's `http_retries`, for the
	//! same reason it pins the timeout. It must stay non-zero: `http_client` below is a
	//! persistent keep-alive client, and DuckDB rebuilds it only from the retry callback
	//! (HTTPUtil::Request's `on_retry`), which never fires at retries=0. Without a retry the
	//! first RPC after quackd or a proxy drops an idle connection surfaces as a hard
	//! IOException instead of transparently reconnecting.
	static constexpr uint64_t HTTP_RETRIES = 3;

	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message) override;

private:
	unique_ptr<HTTPParams> http_params;
	//! Extra HTTP headers resolved once from the `quack` secret (EXTRA_HTTP_HEADERS),
	//! injected into every request. Loaded lazily alongside http_params.
	HTTPHeaders extra_headers;
	// Persistent keep-alive client to quackd, reused across every RPC this client
	// issues. Passing it to the two-arg HTTPUtil::Request makes SendRequest lazily
	// initialize it once and reuse the underlying connection; the one-arg overload
	// instead builds and tears down a client per call, which opened a fresh TCP
	// connection per RPC and churned TIME_WAIT sockets to ephemeral-port
	// exhaustion under concurrency. request_mutex serializes all use of this
	// client, so a single reused connection is safe.
	unique_ptr<HTTPClient> http_client;
};

} // namespace duckdb
