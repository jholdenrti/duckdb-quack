#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

#include "quack_server.hpp"
#include "quack_message.hpp"
#include "quack_uri.hpp"

#include "httplib.hpp"

#include <condition_variable>
#include <deque>
#include <unordered_set>

namespace duckdb {

//! httplib hands each accepted socket to the task queue as one task spanning the connection's whole
//! keep-alive lifetime, so a fixed pool deadlocks once idle connections pin every worker. This pool
//! grows a thread per connection up to `max_threads` and sheds at the cap (clients retry).
class ElasticThreadPool final : public duckdb_httplib::TaskQueue {
public:
	explicit ElasticThreadPool(idx_t max_threads_p) : max_threads(max_threads_p) {
	}

	~ElasticThreadPool() override {
		shutdown();
	}

	bool enqueue(std::function<void()> fn) override {
		std::unique_lock<std::mutex> guard(lock);
		JoinFinishedWorkers(guard);
		// Re-checked after JoinFinishedWorkers: it drops the lock, so shutdown() may have
		// started meanwhile and a worker spawned now would never be joined.
		if (shutting_down) {
			return false;
		}
		// A notified worker may not have decremented idle_workers yet, so queued jobs each claim one
		// idle worker; without this surplus check a burst of accepts can queue a job nobody picks up.
		if (idle_workers > jobs.size()) {
			jobs.push_back(std::move(fn));
			cv.notify_one();
			return true;
		}
		if (live_workers >= max_threads) {
			return false;
		}
		jobs.push_back(std::move(fn));
		try {
			workers.emplace_back(&ElasticThreadPool::WorkerLoop, this);
		} catch (...) {
			// Thread creation can fail under resource pressure; shed this connection.
			jobs.pop_back();
			return false;
		}
		live_workers++;
		return true;
	}

	void shutdown() override {
		std::unique_lock<std::mutex> guard(lock);
		if (!shutting_down) {
			shutting_down = true;
			cv.notify_all();
		}
		done_cv.wait(guard, [&] { return live_workers == 0; });
		JoinFinishedWorkers(guard);
	}

private:
	void WorkerLoop() {
		std::unique_lock<std::mutex> guard(lock);
		for (;;) {
			idle_workers++;
			bool has_work = cv.wait_for(guard, std::chrono::milliseconds(IDLE_WORKER_TIMEOUT_MS),
			                            [&] { return !jobs.empty() || shutting_down; });
			idle_workers--;
			if (!jobs.empty()) {
				auto fn = std::move(jobs.front());
				jobs.pop_front();
				guard.unlock();
				fn();
				guard.lock();
				continue;
			}
			if (shutting_down || !has_work) {
				// Idle for the full timeout (or shutting down): exit to shrink the pool.
				break;
			}
		}
		live_workers--;
		finished_workers.insert(std::this_thread::get_id());
		if (shutting_down && live_workers == 0) {
			done_cv.notify_all();
		}
	}

	//! Join threads that have exited (self-reaped or shut down). Caller holds `lock`.
	void JoinFinishedWorkers(std::unique_lock<std::mutex> &guard) {
		if (finished_workers.empty()) {
			return;
		}
		std::vector<std::thread> to_join;
		for (auto it = workers.begin(); it != workers.end();) {
			if (finished_workers.count(it->get_id())) {
				finished_workers.erase(it->get_id());
				to_join.push_back(std::move(*it));
				it = workers.erase(it);
			} else {
				++it;
			}
		}
		// The exited threads may still be between "finished_workers.insert" and returning;
		// join outside the lock so they can finish their epilogue.
		guard.unlock();
		for (auto &thread : to_join) {
			thread.join();
		}
		guard.lock();
	}

private:
	static constexpr uint64_t IDLE_WORKER_TIMEOUT_MS = 30000;

	const idx_t max_threads;
	std::mutex lock;
	std::condition_variable cv;
	std::condition_variable done_cv;
	std::deque<std::function<void()>> jobs;
	std::vector<std::thread> workers;
	std::unordered_set<std::thread::id> finished_workers;
	idx_t live_workers = 0;
	idx_t idle_workers = 0;
	bool shutting_down = false;
};

void HttpQuackServer::StopAccepting() {
	// Closes the listening socket only. Idempotent. Safe to call from a
	// request-handler thread.
	if (is_running) {
		server->stop();
		is_running = false;
	}
}

void HttpQuackServer::Close() {
	// Stops accepting new connections AND joins the listener threads (NOT the
	// httplib worker pool). Must not be called from a worker thread — the
	// listener's exit path inside httplib joins all workers, so a worker
	// joining the listener would deadlock through that chain.
	StopAccepting();
	// Stop the reaper here (not in StopAccepting): joining a thread is unsafe from a
	// worker, and Close() already carries the "not from a worker thread" contract.
	{
		std::lock_guard<std::mutex> lock(reaper_mutex);
		reaper_stop = true;
	}
	reaper_cv.notify_all();
	if (reaper_thread.joinable()) {
		reaper_thread.join();
	}
	for (auto &thread : listen_threads) {
		if (thread.joinable()) {
			thread.join();
		}
	}
}

HttpQuackServer::~HttpQuackServer() {
	try {
		HttpQuackServer::Close();
	} catch (std::exception &) {
	}
}

void HttpQuackServer::ListenThread(HttpQuackServer *server, const string &listen_host, int listen_port) {
	D_ASSERT(server->server);
	// Ports 1 and 65535 are both valid — the privileged-port rule is a bind()
	// policy, not a range limit (and does not apply on every platform).
	D_ASSERT(listen_port >= 1 && listen_port <= 65535);
	// Socket is already bound (synchronously, in the constructor).
	// Catch everything so the listener thread never lets an exception escape — that
	// would call std::terminate and abort the host process.
	try {
		server->server->listen_after_bind();
	} catch (...) {
		server->is_running = false;
	}
}

void HttpQuackServer::ReaperThread(HttpQuackServer *server) {
	std::unique_lock<std::mutex> lock(server->reaper_mutex);
	while (!server->reaper_stop) {
		// Interruptible sleep: woken early by Close()'s notify, otherwise every sweep interval.
		server->reaper_cv.wait_for(lock, std::chrono::seconds(server->reaper_sweep_interval),
		                           [server] { return server->reaper_stop; });
		if (server->reaper_stop) {
			break;
		}
		lock.unlock();
		try {
			server->ReapIdleConnections(Timestamp::GetCurrentTimestamp());
		} catch (...) {
			// Never let a sweep failure escape and terminate the process.
		}
		lock.lock();
	}
}

HttpQuackServer::HttpQuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p,
                                 int64_t idle_in_transaction_timeout_p, int64_t reaper_sweep_interval_p)
    : QuackServer(context_p, uri_p, token_p, idle_in_transaction_timeout_p, reaper_sweep_interval_p) {
	server = make_uniq<duckdb_httplib::Server>();

	// The elastic pool makes idle cached client connections cheap (one sleeping thread each,
	// reaped on close) and turns pool exhaustion into load shedding instead of deadlock.
	auto &db_config = DBConfig::GetConfig(*context_p.db);
	Value max_connections_val = Value::UBIGINT(1024);
	db_config.TryGetCurrentSetting("quack_server_max_connections", max_connections_val);
	auto max_connections = MaxValue<idx_t>(1, max_connections_val.GetValue<idx_t>());
	Value keep_alive_timeout_val = Value::UBIGINT(300);
	db_config.TryGetCurrentSetting("quack_server_keep_alive_timeout", keep_alive_timeout_val);
	auto keep_alive_timeout = MaxValue<idx_t>(1, keep_alive_timeout_val.GetValue<idx_t>());

	server->new_task_queue = [max_connections] {
		return new ElasticThreadPool(max_connections);
	};
	// Requests per connection before a forced close; effectively unlimited now that a cached
	// client connection is expected to live for a whole session.
	server->set_keep_alive_max_count(1 << 20);
	server->set_keep_alive_timeout(NumericCast<time_t>(keep_alive_timeout));
	server->set_tcp_nodelay(true);

	server->Get("/", [=](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_content("This is a DuckDB Quack RPC endpoint. Use ATTACH 'quack:...' to connect here.\n", "text/plain");
	});

	// TODO: this is very liberal, and there might be reasonable cases to restrict to trusted domains (note, this is
	// only relevant from within a Web browser, since other actors can just ignore the CORS convention
	server->Options("/quack", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_header("Access-Control-Allow-Origin", "*");
		res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
		res.set_header("Access-Control-Allow-Headers", "*");
		res.status = 204;
	});

	server->Post("/quack", [&](const duckdb_httplib::Request &, duckdb_httplib::Response &res,
	                           const duckdb_httplib::ContentReader &content_reader) {
		res.set_header("Access-Control-Allow-Origin", "*");
		MemoryStream stream;
		content_reader([&](const char *data, size_t data_length) {
			stream.WriteData((data_ptr_t)data, data_length);
			return true;
		});
		auto response = HandleMessage(stream);
		response->ToMemoryStream(stream);
		res.set_content((const char *)stream.GetData(), stream.GetPosition(), "application/vnd.duckdb");
	});

	if (!server->is_valid()) {
		throw IOException("Failed to instantiate DuckDB server at %s / %s", uri_p.Uri(), uri_p.Http());
	}
	is_running = true;

	// Bind synchronously here so that bind() failures (e.g. EADDRINUSE)
	// propagate to the caller of quack_serve()
	if (!server->bind_to_port(uri_p.Host(), uri_p.Port())) {
		throw IOException("Failed to bind DuckDB Quack RPC server to %s (address in use, permission denied, "
		                  "or invalid host/port)",
		                  uri_p.Http());
	}

	listen_threads.push_back(std::thread(ListenThread, this, uri_p.Host(), uri_p.Port()));

	// Start the idle-in-transaction reaper. idle_in_transaction_timeout == 0 disables it entirely.
	if (idle_in_transaction_timeout > 0) {
		reaper_thread = std::thread(ReaperThread, this);
	}
}

} // namespace duckdb
