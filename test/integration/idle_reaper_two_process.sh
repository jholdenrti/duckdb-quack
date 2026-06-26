#!/usr/bin/env bash
#
# Out-of-process idle-in-transaction reaper regression test.
#
# Exercises the REAL background reaper thread (not a clock-injected unit predicate)
# with small intervals, in the production-shaped topology: the quack server runs in
# one `duckdb` process holding an on-disk catalog, clients run in separate processes
# over loopback.
#
#   (a) ORPHAN / in-transaction: a client BEGINs a transaction and writes, then is
#       SIGKILLed WITHOUT sending a disconnect. After ttl+interval the server must
#       roll back the orphaned transaction: the in-transaction session count drops to
#       0 and a CHECKPOINT on the backing catalog succeeds (no "other write
#       transactions active").
#
#   (b) NON-REAP: a plain-idle client (attached, no open transaction) left quiet past
#       the ttl is NOT reaped — its next query still succeeds. Guards the deliberate
#       in-transaction-only scope (the client has no reconnect-on-"Invalid connection
#       id" path, so reaping a healthy plain-idle session would break it).
#
#   (c) SURVIVAL / live in-transaction: a client that BEGINs + writes and then keeps
#       touching its session (a remote read every ~ttl/2) across several sweep intervals
#       must NOT be reaped. This exercises the last_activity bump together with the
#       use_count()==1 / query_state safety guards: a regression that dropped any of them
#       would reap this live transaction and break its next query with "Invalid
#       connection id" (spec §6 #2/#3/#4). Branch (b) cannot catch that — an
#       InTransaction()==false session can never satisfy the reap predicate, so it would
#       survive even with every safety guard deleted.
#
#   NOTE: spec §6 #2 (mid-query ACTIVE) and #4 (use_count()>1 in-flight handler) are not
#   independently forced from shell — they hold for sub-second statements only by code
#   review of the reap predicate (quack_server.cpp). Branch (c) covers the last_activity
#   wiring (#3) end to end.
#
# Usage:   test/integration/idle_reaper_two_process.sh
# Exit:    0 = all checks passed, non-zero = a check failed.
#
set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DB="${QUACK_DUCKDB:-$REPO_ROOT/build/release/duckdb}"
EXT="${QUACK_EXTENSION:-$REPO_ROOT/build/release/extension/quack/quack.duckdb_extension}"
PORT="${QUACK_TEST_PORT:-9897}"

SRV_FIFO="$(mktemp -u /tmp/quack_reap_srv.XXXXXX.fifo)"
IDLE_FIFO="$(mktemp -u /tmp/quack_reap_idle.XXXXXX.fifo)"
ORPH_FIFO="$(mktemp -u /tmp/quack_reap_orph.XXXXXX.fifo)"
SURV_FIFO="$(mktemp -u /tmp/quack_reap_surv.XXXXXX.fifo)"
SRV_LOG="$(mktemp /tmp/quack_reap_srv.XXXXXX.log)"
IDLE_LOG="$(mktemp /tmp/quack_reap_idle.XXXXXX.log)"
ORPH_LOG="$(mktemp /tmp/quack_reap_orph.XXXXXX.log)"
SURV_LOG="$(mktemp /tmp/quack_reap_surv.XXXXXX.log)"
CAT_DB="$(mktemp -u /tmp/quack_reap_cat.XXXXXX.db)"

fail() { echo "FAIL: $*" >&2; exit 1; }
[ -x "$DB" ]  || fail "duckdb shell not found at $DB (build it, or set QUACK_DUCKDB)"
[ -f "$EXT" ] || fail "quack extension not found at $EXT (build it, or set QUACK_EXTENSION)"

SRVPID=""; IDLEPID=""; ORPHPID=""; SURVPID=""
cleanup() {
  for pid in "$SURVPID" "$ORPHPID" "$IDLEPID" "$SRVPID"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
  done
  rm -f "$SRV_FIFO" "$IDLE_FIFO" "$ORPH_FIFO" "$SURV_FIFO" \
        "$SRV_LOG" "$IDLE_LOG" "$ORPH_LOG" "$SURV_LOG" \
        "$CAT_DB" "$CAT_DB.wal" 2>/dev/null
}
trap cleanup EXIT
pkill -f "quack:localhost:$PORT" 2>/dev/null
sleep 1

# wait_for PATTERN FILE TIMEOUT_SECS — poll until FILE contains PATTERN.
wait_for() {
  local i=0 max=$(( ${3:-10} * 5 ))
  while ! grep -q "$1" "$2" 2>/dev/null; do
    sleep 0.2; i=$((i+1)); [ "$i" -gt "$max" ] && return 1
  done
  return 0
}

mkfifo "$SRV_FIFO" "$IDLE_FIFO" "$ORPH_FIFO" "$SURV_FIFO"

# --- server: separate DuckDB instance, on-disk catalog, ttl=1s sweep=1s ---
setsid "$DB" -unsigned "$CAT_DB" < "$SRV_FIFO" > "$SRV_LOG" 2>&1 &
SRVPID=$!
exec 3>"$SRV_FIFO"   # main script keeps the server's stdin open
printf "LOAD '%s';\n" "$EXT" >&3
printf "CALL quack_serve('quack:localhost:%s', token='asdf', idle_in_transaction_timeout=1, reaper_sweep_interval=1);\n" "$PORT" >&3
printf "CREATE TABLE t(i INTEGER);\n" >&3
printf "SELECT 'SRVUP=' || '%s';\n" "$PORT" >&3
wait_for "SRVUP=$PORT" "$SRV_LOG" 15 || fail "server did not come up; log:\n$(cat "$SRV_LOG")"

# --- (b) non-reap: a plain-idle client, attached, no transaction, kept alive ---
setsid "$DB" -unsigned < "$IDLE_FIFO" > "$IDLE_LOG" 2>&1 &
IDLEPID=$!
exec 4>"$IDLE_FIFO"
printf "LOAD '%s';\n" "$EXT" >&4
printf "ATTACH 'quack:localhost:%s' AS r (token 'asdf');\n" "$PORT" >&4
printf "SELECT 'IDLE_FIRST=' || count(*) FROM r.t;\n" >&4
wait_for "IDLE_FIRST=" "$IDLE_LOG" 15 || fail "plain-idle client could not attach / read; log:\n$(cat "$IDLE_LOG")"

# --- (a) orphan: a client that BEGINs + writes, then is killed mid-transaction ---
setsid "$DB" -unsigned < "$ORPH_FIFO" > "$ORPH_LOG" 2>&1 &
ORPHPID=$!
exec 5>"$ORPH_FIFO"
printf "LOAD '%s';\n" "$EXT" >&5
printf "ATTACH 'quack:localhost:%s' AS r (token 'asdf');\n" "$PORT" >&5
printf "BEGIN;\n" >&5
printf "INSERT INTO r.t VALUES (1);\n" >&5
printf "SELECT 'ORPH_WROTE=' || count(*) FROM r.t;\n" >&5
wait_for "ORPH_WROTE=1" "$ORPH_LOG" 15 || fail "orphan client could not BEGIN + write; log:\n$(cat "$ORPH_LOG")"

echo "[1/4] pre-reap: an open write transaction is visible server-side"
printf "SELECT 'INTXN_PRE=' || count(*) FROM quack_active_connections() WHERE in_transaction;\n" >&3
wait_for "INTXN_PRE=" "$SRV_LOG" 15 || fail "server did not report connection snapshot"
grep -q "INTXN_PRE=0" "$SRV_LOG" && fail "expected >=1 in-transaction session before reap, saw 0:\n$(cat "$SRV_LOG")"

echo "[2/4] kill the orphan client (no disconnect) and let the background reaper run"
kill -9 "$ORPHPID" 2>/dev/null
ORPHPID=""
# ttl=1s + sweep=1s; sleep generously to cover several sweeps. Killing the client sends
# no disconnect, so ONLY the reaper thread can clear the orphaned in-transaction session.
sleep 6

printf "SELECT 'INTXN_POST=' || count(*) FROM quack_active_connections() WHERE in_transaction;\n" >&3
wait_for "INTXN_POST=" "$SRV_LOG" 15 || fail "server did not report post-reap snapshot"
grep -q "INTXN_POST=0" "$SRV_LOG" \
  || fail "reaper did NOT clear the orphaned in-transaction session:\n$(grep INTXN_POST "$SRV_LOG")"
echo "      orphaned in-transaction session reaped (1 -> 0)"

# With the orphan transaction rolled back, a non-force CHECKPOINT on the backing catalog
# must succeed (no "other write transactions active").
printf "CHECKPOINT;\n" >&3
printf "SELECT 'CKPT_POST_DONE=1';\n" >&3
wait_for "CKPT_POST_DONE=1" "$SRV_LOG" 15 || fail "server did not finish post-reap checkpoint"
if tail -n 8 "$SRV_LOG" | grep -qi "other write transactions active\|Cannot CHECKPOINT"; then
  fail "CHECKPOINT blocked after reap — orphan txn was not rolled back:\n$(tail -n 8 "$SRV_LOG")"
fi
echo "      CHECKPOINT on the backing catalog succeeds"

echo "[3/4] non-reap: the plain-idle client (idle past ttl) is still alive"
printf "SELECT 'IDLE_SECOND=' || count(*) FROM r.t;\n" >&4
wait_for "IDLE_SECOND=\|Invalid connection id" "$IDLE_LOG" 15 || fail "plain-idle client never answered second query"
if grep -q "Invalid connection id" "$IDLE_LOG"; then
  fail "plain-idle (no-transaction) client was wrongly reaped — next query failed:\n$(cat "$IDLE_LOG")"
fi
grep -q "IDLE_SECOND=" "$IDLE_LOG" || fail "plain-idle client second query did not succeed:\n$(cat "$IDLE_LOG")"
echo "      plain-idle client survived (not reaped)"

# --- (c) survival: a LIVE in-transaction client kept warm across several sweeps ---
# Started only now, AFTER the orphan reap was verified (so it can't perturb the
# INTXN_POST=0 assertion above). It opens its own transaction and keeps touching it.
setsid "$DB" -unsigned < "$SURV_FIFO" > "$SURV_LOG" 2>&1 &
SURVPID=$!
exec 6>"$SURV_FIFO"
printf "LOAD '%s';\n" "$EXT" >&6
printf "ATTACH 'quack:localhost:%s' AS r (token 'asdf');\n" "$PORT" >&6
printf "BEGIN;\n" >&6
printf "INSERT INTO r.t VALUES (2);\n" >&6
printf "SELECT 'SURV_WROTE=' || count(*) FROM r.t;\n" >&6
wait_for "SURV_WROTE=" "$SURV_LOG" 15 || fail "survival client could not BEGIN + write; log:\n$(cat "$SURV_LOG")"

echo "[4/4] survival: a live in-transaction client kept active across several sweeps is spared"
# Keep the session warm: a remote read every ~0.3s (well under ttl=1s) for 12 rounds
# (~3.6s + round-trips, covering several sweep intervals). Each remote read is a server
# message that bumps last_activity, so idle never crosses the ttl while we hold it.
for n in $(seq 1 12); do
  printf "SELECT 'SURV_KEEP_%s=' || count(*) FROM r.t;\n" "$n" >&6
  sleep 0.3
done
wait_for "SURV_KEEP_12=" "$SURV_LOG" 15 || fail "survival client keepalive loop did not complete:\n$(cat "$SURV_LOG")"
if grep -q "Invalid connection id" "$SURV_LOG"; then
  fail "LIVE in-transaction client was wrongly reaped during keepalive:\n$(cat "$SURV_LOG")"
fi
# It must still be present AND still in-transaction server-side.
printf "SELECT 'INTXN_SURV=' || count(*) FROM quack_active_connections() WHERE in_transaction;\n" >&3
wait_for "INTXN_SURV=" "$SRV_LOG" 15 || fail "server did not report survival snapshot"
grep -q "INTXN_SURV=0" "$SRV_LOG" \
  && fail "live in-transaction session missing after keepalive — it was wrongly reaped:\n$(grep INTXN_SURV "$SRV_LOG")"
# And its next query still succeeds.
printf "SELECT 'SURV_FINAL=' || count(*) FROM r.t;\n" >&6
wait_for "SURV_FINAL=\|Invalid connection id" "$SURV_LOG" 15 || fail "survival client never answered final query"
grep -q "Invalid connection id" "$SURV_LOG" \
  && fail "live in-transaction client's next query failed — it was wrongly reaped:\n$(cat "$SURV_LOG")"
grep -q "SURV_FINAL=" "$SURV_LOG" || fail "survival client final query did not succeed:\n$(cat "$SURV_LOG")"
echo "      live in-transaction client survived keepalive across sweeps"

echo "PASS: idle-in-transaction reaper rolls back orphaned write txns, spares plain-idle and live in-transaction sessions"
