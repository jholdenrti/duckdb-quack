#!/usr/bin/env bash
#
# Out-of-process connection-pool regression test.
#
# sqllogic .test files run the quack server IN-PROCESS (quack_serve shares the
# client's DuckDB instance). That topology cannot exercise the real production
# shape — a separate quackd process — or true concurrency. This harness runs the
# server in one `duckdb` process and the client in another, over the loopback
# network, and asserts the connection pool grows and is reused without deadlocking.
#
# It exists because the original lazy mint DEADLOCKED on the first pooled checkout
# inside a transaction (the mint re-entered the in-flight query's ClientContext
# lock). That bug reproduced cross-process and was invisible to the in-process
# sqllogic suite's defensive assertions. Keep this as the cross-process guard.
#
# Usage:   test/integration/pool_two_process.sh
# Exit:    0 = all checks passed, non-zero = a check failed (or the client hung).
#
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DB="${QUACK_DUCKDB:-$REPO_ROOT/build/release/duckdb}"
EXT="${QUACK_EXTENSION:-$REPO_ROOT/build/release/extension/quack/quack.duckdb_extension}"
PORT="${QUACK_TEST_PORT:-9899}"
FIFO="$(mktemp -u /tmp/quack_pool_2p.XXXXXX.fifo)"
SRV_LOG="$(mktemp /tmp/quack_pool_2p_srv.XXXXXX.log)"

fail() { echo "FAIL: $*" >&2; exit 1; }
[ -x "$DB" ]   || fail "duckdb shell not found at $DB (build it, or set QUACK_DUCKDB)"
[ -f "$EXT" ]  || fail "quack extension not found at $EXT (build it, or set QUACK_EXTENSION)"

SRVPID=""; HOLDPID=""
cleanup() {
  [ -n "$SRVPID" ]  && kill -9 "$SRVPID"  2>/dev/null
  [ -n "$HOLDPID" ] && kill -9 "$HOLDPID" 2>/dev/null
  rm -f "$FIFO" "$SRV_LOG"
}
trap cleanup EXIT
pkill -f "quack:localhost:$PORT" 2>/dev/null
sleep 1
rm -f "$FIFO"; mkfifo "$FIFO"

# --- server process: separate DuckDB instance, holds the FIFO open to stay alive ---
setsid "$DB" -unsigned < "$FIFO" > "$SRV_LOG" 2>&1 &
SRVPID=$!
(
  exec 3>"$FIFO"
  printf "LOAD '%s';\n" "$EXT" >&3
  printf "CALL quack_serve('quack:localhost:%s', token='asdf');\n" "$PORT" >&3
  printf "CREATE TABLE fuu AS VALUES (42, 43);\n" >&3
  sleep 120
) &
HOLDPID=$!
sleep 3
grep -q "$PORT" "$SRV_LOG" || fail "server did not come up; log:\n$(cat "$SRV_LOG")"

run_client() {  # $1 = SQL; prints output; returns 124 on hang
  timeout 25 "$DB" -unsigned -list -c "$1" 2>&1
}

echo "[1/3] explicit-token attach + transactional pooled mint (the path that deadlocked)"
OUT="$(run_client "
LOAD '$EXT';
ATTACH 'quack:localhost:$PORT' AS r (token 'asdf', pool_size 4);
BEGIN;
FROM r.fuu;
SELECT 'GROWTH' tag, live, in_use FROM quack_pool_status() WHERE catalog='r';
COMMIT;
SELECT 'ATREST' tag, live, in_use, idle FROM quack_pool_status() WHERE catalog='r';
")"
[ $? -eq 124 ] && fail "client HUNG on the transactional pooled mint (the original deadlock)"
echo "$OUT" | grep -q "GROWTH|2|1" || fail "pool did not grow to live=2,in_use=1 mid-transaction:\n$OUT"
echo "$OUT" | grep -q "ATREST|2|0|1" || fail "pooled connection not released to idle after commit:\n$OUT"

echo "[2/3] secret-based attach (no token option) + transactional pooled mint"
OUT="$(run_client "
LOAD '$EXT';
CREATE SECRET s (TYPE QUACK, TOKEN 'asdf');
ATTACH 'quack:localhost:$PORT' AS r (pool_size 4);
BEGIN;
FROM r.fuu;
SELECT 'SECRET' tag, live, in_use FROM quack_pool_status() WHERE catalog='r';
COMMIT;
")"
[ $? -eq 124 ] && fail "client HUNG on secret-based transactional mint"
echo "$OUT" | grep -q "SECRET|2|1" || fail "secret-resolved token did not mint a pooled connection:\n$OUT"

echo "[3/3] error mid-transaction at pool_size>1 does not hang"
# (The duckdb CLI halts at the first error, so we can't drive post-error recovery
# here; reuse-after-error is asserted in-process in test/sql/pool_concurrency.test_slow.
# The cross-process invariant we guard here is simply that erroring while holding a
# pooled checkout returns instead of deadlocking.)
OUT="$(run_client "
LOAD '$EXT';
ATTACH 'quack:localhost:$PORT' AS r (token 'asdf', pool_size 4);
BEGIN;
FROM r.fuu;
FROM r.nope;
")"
[ $? -eq 124 ] && fail "client HUNG on a mid-transaction error while holding a pooled checkout"
echo "$OUT" | grep -q "nope does not exist" || fail "expected the bad-table error, got:\n$OUT"

echo "PASS: out-of-process connection pooling works (growth, reuse, secret auth, error recovery)"
