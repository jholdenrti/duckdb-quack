# Spec: Idle / orphaned-connection reaper for `quack_serve`

**Status:** Implemented (in-transaction only)
**Target release:** the DuckDB **1.5.4** bump (carrier release — the extension rebuild + coordinated quackd/API roll is already being paid for)
**Owner:** TBD
**Branch:** `feat/idle-connection-reaper`

---

## 1. Problem

A `quack_serve` server leaks server-side connections — and the transactions they hold open — whenever a client goes away without sending an explicit disconnect. In production this manifested as:

- `duckdb_memory()` on a backing instance showing a `TRANSACTION` tag that **grows without bound**.
- `CHECKPOINT` failing with *"there are other write transactions active"*.
- The stuck transaction **surviving a full client (API) restart** — only restarting the quack server process (the quackd-managed `duckdb` child) cleared it.

On a continuously-busy catalog (e.g. the Interface `__admin__` project, which the API hits constantly via obs flushes and health probes) the instance never goes idle, so quackd's instance-level idle reaper never stops it, and the orphan persists indefinitely.

### Why it matters

An orphaned write transaction pins the MVCC version horizon on the catalog. Every commit by the *live* connections then accumulates version history that can't be reclaimed, so memory climbs steadily until the process is killed. Because one quack server can host the metadata catalog that many clients depend on, an unbounded climb is a shared-blast-radius availability risk (OOM of the backing instance), not just wasted RAM.

### Why `CHECKPOINT` fails (verified mechanism)

DuckDB's manual `CHECKPOINT` throws *"there are other write transactions active"* only when another transaction holds the **shared checkpoint lock**. That lock is taken by `DuckTransaction::SetModifications` (`duckdb/src/transaction/duck_transaction.cpp:300`) for **catalog/DDL operations** — `Create`/`Drop`/`Alter` catalog entry, `Sequence`, `CreateIndex` — and for `UpdateData`. A plain row **append (`INSERT`) takes only the *vacuum* lock**, so an INSERT-only orphan never blocks `CHECKPOINT`. The production trigger was therefore an orphaned transaction holding a **catalog/DDL** lock — e.g. **a schema migration (`BEGIN; CREATE/ALTER …`) whose client died before `COMMIT`**. Under **ducklake every write is a catalog/metadata mutation**, so an orphaned ducklake *write* transaction holds the same lock — which is why the symptom showed up on ordinary writes, not only explicit DDL. End-to-end proof (orphan blocks `CHECKPOINT` → reaper rolls it back → `CHECKPOINT` recovers), plus the INSERT-doesn't-block contrast, is in `test/integration/reaper_unblocks_checkpoint.sh`.

## 2. Root cause (confirmed in source)

The server keys a **persistent DuckDB connection** per logical client session:

```cpp
// src/include/quack_server.hpp:107
unordered_map<string, shared_ptr<QuackConnection>> active_connections;   // keyed by session_id

// QuackConnection — src/include/quack_server.hpp:30
unique_ptr<Connection> duckdb_connection;   // a real, long-lived DuckDB connection
```

A client `BEGIN`/write runs on that persistent connection and holds the transaction open across requests. The **only** path that removes an entry is an explicit client message:

```
DISCONNECT_MESSAGE  ->  DisconnectConnection(session_id)  ->  active_connections.erase(...)
                        (src/quack_server.cpp:79-87, dispatched at :289-292)
```

The erase is correct — dropping the map's `shared_ptr` runs `~QuackConnection`, which destroys `duckdb_connection`, closing the DuckDB connection and **rolling back any open transaction**. The defect is that this only ever fires on a client-initiated disconnect. The header says so explicitly:

```cpp
// src/include/quack_server.hpp:72
// TODO need something to destroy connections
```

Confirmed gaps:

- **No reaper.** The only `std::thread` in the server is the HTTP listener (`src/quack_http_server.cpp:110`). There is no idle sweep and no per-connection `last_activity` tracking.
- **HTTP keep-alive does not help.** The server sets `set_keep_alive_timeout(10)` (`src/quack_http_server.cpp:68`), but the logical connection is keyed by `session_id` carried **in the message body**, fully decoupled from the TCP socket. The socket dying does nothing to the `QuackConnection`.
- **Existing per-connection fields track the current *query*, not liveness.** `query_state` (IDLE/ACTIVE/FINISHED/CANCELLED) and `query_started_at` are set per statement (`src/quack_server.cpp:324-325`); neither reflects "last touched" or "holds an open transaction."

### How this matches every observed symptom

| Symptom | Explanation |
|---|---|
| Survives client restart | A SIGKILL/OOMKill client never sends `DISCONNECT_MESSAGE`; the entry stays in `active_connections` under a dead `session_id` forever. A restarted client gets new session ids and cannot clean up the old one. |
| Keep-alive timeout doesn't clear it | Logical connection is body-keyed, not socket-bound. |
| Memory climbs steadily | Orphaned transaction pins the version horizon; live commits pile up unreclaimable history. |
| Only a server restart clears it | Destroying `QuackServer` is the only thing that tears down `active_connections`. |
| Visible in `quack_active_connections()` | `GetActiveConnectionSnap()` (`src/quack_server.cpp:38`) lists every map entry; the orphan appears as a session no live client owns. |

## 3. Goals / non-goals

**Goals**
- Server-side recovery from connections abandoned by a dead/crashed/partitioned client, with **no client protocol change required**.
- Bound `active_connections` so a churn of dead clients cannot grow it without limit.
- Roll back orphaned transactions promptly so they stop pinning the version horizon.
- Make orphans observable (extend `quack_active_connections`).

**Non-goals**
- Detecting TCP disconnects (unreliable through pooled HTTP + an L7 proxy; we go time-based instead).
- Changing quackd's instance-level idle reaper (orthogonal — it stops whole idle instances; this reaps connections *within* a live instance).
- Any client-side (`ducklake:quack:` / API) change. Graceful client shutdown is a separate, additive improvement and does not fix the SIGKILL/OOM case.

## 4. Design

This is the server-side analogue of Postgres `idle_in_transaction_session_timeout`.

### 4.1 Per-connection activity + transaction tracking

Add to `QuackConnection` (`src/include/quack_server.hpp`):

- `timestamp_t last_activity {0};` — set to "now" each time a message is handled for the session (in `HandleMessageInternal`, for CONNECTION/PREPARE/FETCH/APPEND).
- A helper `bool InTransaction() const;` that asks the underlying connection — likely `duckdb_connection->context->transaction.HasActiveTransaction()`. **Verify the exact accessor against the bundled DuckDB 1.5.4 headers** (the project vendors DuckDB under `duckdb/`); this API is version-sensitive.

### 4.2 Reaper thread

Owned by `HttpQuackServer` (alongside the listener), started in the constructor, stopped + joined in `StopAccepting()` / `Close()`:

- Wakes every `sweep_interval` (default 30s).
- Under `active_connections_mutex`, for each entry, mark it **reapable** when:
  - `query_state != ACTIVE` (never reap an in-flight query), **and**
  - `now - last_activity > idle_ttl`, **and**
  - (primary target) `InTransaction()` is true. A second, larger TTL may also reap plain-idle no-transaction connections to bound the map — they are cheap to recreate.
- **Safe-erase invariant:** only reap an entry whose `shared_ptr` `use_count() == 1` (i.e. only the map holds it). Because `GetConnection` also takes `active_connections_mutex`, no in-flight handler can be mid-acquire while we hold it; `use_count() > 1` means a handler already holds the connection, so we skip it this sweep. This avoids destroying a `QuackConnection` while a worker holds its embedded `lock` (which would be UB).
- To erase: `std::move` the `shared_ptr` out of the map, `erase` the key, **release `active_connections_mutex`**, then let the moved pointer drop — so `~QuackConnection` (DuckDB connection close + `ROLLBACK`) runs **outside** both the map lock and the connection's own `lock`, keeping lock hold-time bounded.
- Log every reap: `session_id`, idle duration, `in_transaction`, last `sql_query`.

### 4.3 Configuration

| Setting | Default | Notes |
|---|---|---|
| `idle_ttl` (idle-in-transaction) | 120s | Must exceed the longest legitimate gap *between statements within a transaction*. Interface commits request-scoped transactions at request end and flushes obs sub-second, so legitimate idle-in-transaction is ~0; 120s is very safe. |
| `plain_idle_ttl` (no transaction) | 600s (or off) | Bounds the map; lower urgency. |
| `sweep_interval` | 30s | |
| reaper enable | on; `idle_ttl = 0` disables | Escape hatch. |

Surface as `quack_serve` named params and/or DuckDB settings (mirror existing `quack_*` settings), and thread through quackd's child-spawn init script as env-driven knobs (e.g. `QUACK_CONN_IDLE_TTL`).

### 4.4 Observability

Extend `QuackConnectionSnapshot` / `GetActiveConnectionSnap()` (`src/quack_server.cpp:38`) and `quack_active_connections()` with:

- `last_activity`
- `in_transaction` (bool)
- `idle_seconds` (derived)

so operators can see an idle-in-transaction connection directly instead of inferring it from `duckdb_memory()` deltas. This is additive (no protocol break).

## 5. Risks & edge cases

- **False reap of a slow-but-legitimate transaction.** A client that runs `BEGIN`, pauses longer than `idle_ttl`, then `COMMIT` would be reaped and lose its uncommitted work (rolled back — never committed, so no durable data lost, but the operation fails and must retry). Mitigation: a generous `idle_ttl`, and `query_state == ACTIVE` already protects in-flight statements. Document the contract: **do not hold a quack transaction open across long client-side idle gaps.**
- **Concurrency/UB.** Destroying a `QuackConnection` while a worker holds its `lock` is UB; the `use_count() == 1` invariant (§4.2) prevents it. Audit `GetConnection` callers to confirm they only obtain the `shared_ptr` under `active_connections_mutex`.
- **Thread lifecycle / deadlock.** The reaper must be joined without going through httplib's task queue — follow the existing `StopAccepting()` vs `Close()` split (`src/include/quack_server.hpp:58-67`) and never join from a worker thread.
- **DuckDB 1.5.4 API drift.** The transaction-state accessor and any `ClientContext`/`MetaTransaction` shape must be re-verified against the 1.5.4 headers as part of the bump. This is the cheapest moment to do it (we're rebuilding anyway).
- **Durability.** Reaping only ever rolls back *uncommitted* work; committed catalog state (WAL-fsynced, RPO≈0 on kill -9 per `docs/usage.md`) is untouched.

## 6. Test plan

`test/sql` + a C++/unit harness with an injectable clock:

1. **Reaps idle-in-transaction.** Open a connection, `BEGIN` + a write, advance the clock past `idle_ttl`, run a sweep → entry gone, transaction rolled back (`CHECKPOINT` succeeds, `duckdb_memory()` `TRANSACTION` drops).
2. **Never reaps ACTIVE.** A connection mid-query (`query_state == ACTIVE`) is not reaped regardless of `last_activity`.
3. **Activity bumps reset the clock.** Touching a session within the TTL prevents reaping.
4. **In-use connection skipped.** A connection with `use_count() > 1` (held by a handler) is skipped, not destroyed.
5. **Orphan simulation (integration).** Open a transaction, drop the client without `DISCONNECT_MESSAGE`, confirm the reaper clears it within `idle_ttl + sweep_interval`.
6. **`quack_active_connections` surfaces** `in_transaction` / `idle_seconds`.

## 7. Rollout

- Ships **with the DuckDB 1.5.4 bump** as a single coordinated release: the quackd StatefulSet rolls and the API's pinned `quack` client version moves in lockstep (`quackd/Dockerfile` version pin). No standalone extension bump needed.
- Server-only change; clients are unaffected and need no upgrade for correctness (older clients simply benefit from the server reaping their orphans).
- Conservative defaults; `idle_ttl = 0` disables if a problem surfaces in staging.

## 8. Open questions

- **Resolved — idle-in-transaction only.** The shipped reaper reaps **only** connections holding an open transaction; plain-idle (no-transaction) connections are intentionally **never** reaped. Reaping a healthy plain-idle session would break that client: its next request returns `ErrorResponse("Invalid connection id")` (`src/quack_server.cpp:222`), the client's `Request<>` throws immediately on any error (`src/include/quack_client.hpp:25-26`), and there is no reconnect/retry path (`src/storage/quack_transaction.cpp:88-96` only marks the connection dirty and rethrows). Only an orphaned *transaction* actually leaks (it pins the MVCC version horizon), so that is the only thing worth the risk of reaping. Config: `quack_serve(idle_in_transaction_timeout := 120, reaper_sweep_interval := 30)`; `idle_in_transaction_timeout = 0` disables the reaper.
- Expose a counter metric (`quack_reaped_connections_total`) for alerting on a high reap rate (a signal that clients are dying mid-transaction — i.e. the *upstream* bug, e.g. API OOMKills, is still happening)?
- Should the reaper also fire on `StopAccepting()` (drain) to roll back orphans before a planned instance stop, or is destructor teardown sufficient?
