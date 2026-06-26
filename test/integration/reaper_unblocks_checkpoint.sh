#!/usr/bin/env bash
#
# Proves the production symptom AND its fix, end to end.
#
# SYMPTOM: a DDL migration that dies mid-transaction — `BEGIN; CREATE/ALTER ...;` with
# the client SIGKILLed before COMMIT — leaves an orphaned server-side transaction that
# holds DuckDB's shared checkpoint lock. A concurrent `CHECKPOINT` then fails with
# "there are other write transactions active", and the orphan survives the client's
# death (no DISCONNECT is ever sent). Only a server restart cleared it. Exactly the bug.
#
# FIX: the idle-in-transaction reaper rolls the orphan back after idle_in_transaction_timeout,
# releasing the checkpoint lock, after which CHECKPOINT succeeds again — with no client change.
#
# WHY DDL (and not a plain INSERT): the shared checkpoint lock is taken by
# DuckTransaction::SetModifications (duckdb/src/transaction/duck_transaction.cpp:300) for
# catalog/DDL ops (Create/Drop/Alter catalog entry, Sequence, CreateIndex) and UpdateData.
# A plain row append (INSERT) takes only the *vacuum* lock, so an INSERT-only orphan never
# blocks CHECKPOINT. This test asserts BOTH: the DDL orphan blocks (and is fixed by the
# reaper), and the INSERT orphan does not block — so the proof cannot silently decay back
# into a vacuous "checkpoint succeeds" check.
#
# NOTE (ducklake): under ducklake every write is a catalog/metadata mutation, so an
# orphaned ducklake *write* transaction holds the checkpoint lock just like the DDL case
# here. That is why production observed this on ordinary writes, not only explicit DDL.
#
# Usage:   test/integration/reaper_unblocks_checkpoint.sh
# Exit:    0 = proven, non-zero = a check failed.
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DB="${QUACK_DUCKDB:-$REPO_ROOT/build/release/duckdb}"
EXT="${QUACK_EXTENSION:-$REPO_ROOT/build/release/extension/quack/quack.duckdb_extension}"
PORT="${QUACK_TEST_PORT:-9931}"
TTL=6   # idle_in_transaction_timeout (s): long enough to probe before the reaper fires

T="${CLAUDE_JOB_DIR:-/tmp}/tmp"; mkdir -p "$T" 2>/dev/null || T=/tmp
CAT="$(mktemp -u "$T/reapck_cat.XXXXXX.db")"
SRV_FIFO="$(mktemp -u "$T/reapck_srv.XXXXXX.fifo")"
ORPH_FIFO="$(mktemp -u "$T/reapck_orph.XXXXXX.fifo")"
SRV_LOG="$(mktemp "$T/reapck_srv.XXXXXX.log")"
ORPH_LOG="$(mktemp "$T/reapck_orph.XXXXXX.log")"

SRVPID=""; ORPHPID=""
fail() { echo "FAIL: $*" >&2; exit 1; }
cleanup() {
  for p in "$ORPHPID" "$SRVPID"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
  rm -f "$SRV_FIFO" "$ORPH_FIFO" "$SRV_LOG" "$ORPH_LOG" "$CAT" "$CAT.wal" 2>/dev/null
}
trap cleanup EXIT
[ -x "$DB" ]  || fail "duckdb shell not found at $DB (build it, or set QUACK_DUCKDB)"
[ -f "$EXT" ] || fail "quack extension not found at $EXT (build it, or set QUACK_EXTENSION)"
pkill -f "quack:localhost:$PORT" 2>/dev/null; sleep 1
mkfifo "$SRV_FIFO" "$ORPH_FIFO"

# rpc SQL — run SQL on the server from a fresh, short-lived client over RPC. The client
# exits (EOF) after one statement, so its output flushes reliably (unlike a long-lived
# shell whose stdout to a file is block-buffered). This is how we introspect the server.
rpc() {
  "$DB" -unsigned -batch -noheader -list <<SQL 2>&1
LOAD '$EXT';
FROM quack_query('quack:localhost:$PORT', '$1', token='asdf');
SQL
}
intxn() { rpc "SELECT count(*) FROM quack_active_connections() WHERE in_transaction" | tail -1; }
# ckpt — classify a CHECKPOINT issued over RPC (on its own fresh server session).
ckpt() {
  local out; out="$(rpc 'CHECKPOINT')"
  if echo "$out" | grep -qi "other write transactions active"; then echo "BLOCKED"
  elif echo "$out" | grep -qi "error\|exception";            then echo "ERROR:$(echo "$out" | tr -d '\n')"
  else echo "SUCCEEDED"; fi
}

# --- server: separate process, on-disk catalog, real background reaper ---
setsid "$DB" -unsigned "$CAT" < "$SRV_FIFO" > "$SRV_LOG" 2>&1 & SRVPID=$!
exec 3>"$SRV_FIFO"
printf "LOAD '%s';\n" "$EXT" >&3
printf "CALL quack_serve('quack:localhost:%s', token='asdf', idle_in_transaction_timeout=%s, reaper_sweep_interval=1);\n" "$PORT" "$TTL" >&3
printf "CREATE TABLE t(i INTEGER); INSERT INTO t VALUES (10);\n" >&3
ready=""
for _ in $(seq 1 60); do rpc "SELECT 42" 2>/dev/null | grep -q 42 && { ready=1; break; }; sleep 0.25; done
[ -n "$ready" ] || fail "server never became reachable over RPC; log:\n$(cat "$SRV_LOG")"

# start_orphan SQL — a client that ATTACHes, opens a transaction, runs SQL, and is kept
# alive (fd 4 held) WITHOUT committing. Simulates a migration/writer that hung or crashed.
start_orphan() {
  setsid "$DB" -unsigned < "$ORPH_FIFO" > "$ORPH_LOG" 2>&1 & ORPHPID=$!
  exec 4>"$ORPH_FIFO"
  printf "LOAD '%s';\n" "$EXT" >&4
  printf "ATTACH 'quack:localhost:%s' AS r (token 'asdf');\n" "$PORT" >&4
  printf "BEGIN;\n" >&4
  printf "%s\n" "$1" >&4
  sleep 2   # let the server execute the statement and hold the transaction open
}
kill_orphan() { kill -9 "$ORPHPID" 2>/dev/null; ORPHPID=""; exec 4>&- 2>/dev/null; }

# ============================================================================
echo "[1/3] A dead DDL migration blocks CHECKPOINT; the reaper unblocks it"
# A migration: create a new table inside a transaction, then the client dies pre-COMMIT.
start_orphan "CREATE TABLE r.migration_v2(id INTEGER);"
[ "$(intxn)" -ge 1 ] || fail "orphan did not establish an in-transaction session (saw $(intxn))"
before="$(ckpt)"
[ "$before" = "BLOCKED" ] || fail "expected CHECKPOINT to be BLOCKED while the orphaned DDL txn is held, got: $before"
echo "      before reap: in_transaction=$(intxn), CHECKPOINT=BLOCKED (symptom reproduced)"

kill_orphan
sleep $((TTL + 3))   # past idle_in_transaction_timeout + a sweep; only the reaper can clear it
[ "$(intxn)" = "0" ] || fail "reaper did NOT roll back the orphaned DDL txn (in_transaction=$(intxn))"
after="$(ckpt)"
[ "$after" = "SUCCEEDED" ] || fail "CHECKPOINT still not working after reap, got: $after"
echo "      after  reap: in_transaction=0, CHECKPOINT=SUCCEEDED (fix proven)"

# ============================================================================
echo "[2/3] Contrast: an INSERT-only orphan does NOT block CHECKPOINT (append != checkpoint lock)"
start_orphan "INSERT INTO r.t VALUES (1);"
[ "$(intxn)" -ge 1 ] || fail "insert orphan did not establish an in-transaction session"
ins="$(ckpt)"
[ "$ins" = "SUCCEEDED" ] || fail "expected CHECKPOINT to SUCCEED with only an INSERT orphan held, got: $ins"
echo "      INSERT orphan held: in_transaction=$(intxn), CHECKPOINT=SUCCEEDED (no checkpoint-lock conflict)"
kill_orphan
sleep $((TTL + 3))
[ "$(intxn)" = "0" ] || fail "reaper did not clear the insert orphan (in_transaction=$(intxn))"

# ============================================================================
echo "[3/3] The reaper also rolled the migration back: the orphan's table never persisted"
# migration_v2 was created inside the rolled-back orphan txn and never committed, so it
# must not exist server-side. (duckdb_tables over RPC; empty => rolled back as expected.)
tbls="$(rpc "SELECT count(*) FROM duckdb_tables WHERE table_name = ''migration_v2''" | tail -1)"
[ "$tbls" = "0" ] || fail "orphaned migration table leaked (count=$tbls) — rollback did not happen"
echo "      migration_v2 absent: the orphaned transaction was rolled back, not committed"

echo "PASS: orphaned DDL/migration txn blocks CHECKPOINT; the idle-in-transaction reaper rolls it back and CHECKPOINT recovers"
