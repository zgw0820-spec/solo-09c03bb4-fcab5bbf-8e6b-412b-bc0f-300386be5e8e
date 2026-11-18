#!/usr/bin/env bash
# End-to-end smoke test with two real uvicorn processes.
#
# Covers:
#   1. lost receipt (armed on submit) -> verified by command id, exactly one
#      execution at the gateway;
#   2. per-device FIFO: follower not delivered while the head is unresolved,
#      other devices unaffected;
#   3. duplicate submit -> original record, no new execution;
#   4. live "drop receipt" of an in-flight command;
#   5. gateway outage -> command stays pending, never lost;
#   6. restart of both services -> unfinished work resumes, nothing re-executed;
#   7. gateway killed mid-execution -> id frozen as unknown, web keeps it
#      unresolved and blocks that device; the id is never executed;
#   8. post-maintenance reconciliation: clean run is consistent; a gateway
#      record lost after a disk swap is reported as missing and persistently
#      flags the device so new commands are never claimed while other devices
#      run; an unreachable gateway yields "unverified" (never consistent or
#      missing); flags and findings survive a restart of both services.
#
# Expiry rules (unexecuted vs. still-verifying) are covered by pytest at
# second granularity; the web form only accepts whole minutes.
#
# Requirements: curl, python3 with the service dependencies installed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="${PYTHON:-python3}"
TMP="$(mktemp -d)"
GW_PORT="${GW_PORT:-18001}"
WEB_PORT="${WEB_PORT:-18000}"
DELAY="${DELAY:-1.2}"
GW_PID=""
WEB_PID=""

cleanup() {
  [ -n "$WEB_PID" ] && kill "$WEB_PID" 2>/dev/null || true
  [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_gateway() {
  ( cd "$ROOT" && GATEWAY_DB="$TMP/gateway.db" GATEWAY_EXECUTION_DELAY="$DELAY" \
      "$PY" -m uvicorn gateway.app.main:app --host 127.0.0.1 --port "$GW_PORT" \
      --log-level warning ) &
  GW_PID=$!
}
start_web() {
  ( cd "$ROOT" && OPS_DB="$TMP/ops.db" GATEWAY_URL="http://127.0.0.1:$GW_PORT" \
      "$PY" -m uvicorn web.app.main:app --host 127.0.0.1 --port "$WEB_PORT" \
      --log-level warning ) &
  WEB_PID=$!
}
stop_web() { kill "$WEB_PID" 2>/dev/null || true; wait "$WEB_PID" 2>/dev/null || true; WEB_PID=""; }
stop_gateway() { kill "$GW_PID" 2>/dev/null || true; wait "$GW_PID" 2>/dev/null || true; GW_PID=""; }
hard_kill_gateway() { kill -9 "$GW_PID" 2>/dev/null || true; wait "$GW_PID" 2>/dev/null || true; GW_PID=""; }

wait_http() {
  for _ in $(seq 1 150); do
    if curl -fsS "$1" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "service at $1 did not start" >&2
  return 1
}

field() { "$PY" -c "import json,sys; print(json.load(sys.stdin).get('$1'))"; }
web_state() { curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/$1" | field state; }
web_detail() { curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/$1" | field detail; }
wait_state() { # command expected [tries]
  for _ in $(seq 1 "${3:-200}"); do
    [ "$(web_state "$1")" = "$2" ] && return 0
    sleep 0.1
  done
  echo "timed out waiting for $1=$2 (got $(web_state "$1"))" >&2
  return 1
}
wait_detail() { # command substring [tries]
  for _ in $(seq 1 "${3:-200}"); do
    case "$(web_detail "$1")" in *"$2"*) return 0;; esac
    sleep 0.1
  done
  echo "timed out waiting for '$2' in $1 detail (got $(web_detail "$1"))" >&2
  return 1
}
submit() { # device command minutes [drop]
  curl -sS -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$WEB_PORT/commands" \
    --data-urlencode "device_id=$1" --data-urlencode "command_id=$2" \
    --data-urlencode "deadline_minutes=$3" ${4:+--data-urlencode "drop_receipt=$4"}
}
gw_get() { curl -sS "http://127.0.0.1:$GW_PORT/api/commands/$1"; }
gw_code() { curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$GW_PORT/api/commands/$1"; }

start_gateway
start_web
wait_http "http://127.0.0.1:$WEB_PORT/healthz"
wait_http "http://127.0.0.1:$GW_PORT/healthz"
echo "== services up =="

echo "-- [1/2] lost receipt + FIFO"
submit device-A ok-1 5 true >/dev/null
submit device-A ok-2 5 >/dev/null
submit device-B ok-3 5 >/dev/null

sleep 0.3
echo -n "ok-2 at gateway while ok-1 unresolved: http="
gw_code ok-2
wait_state ok-1 executed
echo "ok-1: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/ok-1")"
echo "ok-1 at gateway: $(gw_get ok-1)"

echo "-- [3] duplicate submit of ok-1"
submit device-A ok-1 5 >/dev/null
sleep 0.3
echo "ok-1 after duplicate: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/ok-1")"

echo "-- [4] live drop-receipt of an in-flight command"
submit device-F live-1 5 >/dev/null
wait_state live-1 inflight 30
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST \
  "http://127.0.0.1:$WEB_PORT/commands/live-1/drop-receipt")
echo "drop-receipt endpoint http=$code"
wait_state live-1 executed
echo "live-1: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/live-1")"

wait_state ok-2 executed
wait_state ok-3 executed

echo "-- [7] gateway killed mid-execution"
submit device-K kill-1 5 >/dev/null
submit device-K kill-2 5 >/dev/null
for _ in $(seq 1 100); do
  [ "$(gw_code kill-1)" = "200" ] && break
  sleep 0.05
done
echo "kill-1 accepted by gateway: $(gw_get kill-1)"
hard_kill_gateway   # SIGKILL mid-execution: row stays 'running' in SQLite
wait_detail kill-1 "不明" 100
echo "kill-1 after gateway death: state=$(web_state kill-1) detail=$(web_detail kill-1)"
echo "kill-2 while kill-1 unresolved: state=$(web_state kill-2)"

start_gateway
wait_http "http://127.0.0.1:$GW_PORT/healthz"
wait_detail kill-1 "重启" 150
echo "kill-1 after gateway restart: state=$(web_state kill-1) detail=$(web_detail kill-1)"
echo "kill-1 at gateway after restart: $(gw_get kill-1)"
submit device-B kill-3 5 >/dev/null
wait_state kill-3 executed
echo "kill-3 (other device) ran while kill-1 is unresolved: $(gw_get kill-3)"
echo "kill-2 still blocked: state=$(web_state kill-2) gateway_http=$(gw_code kill-2)"

echo "-- [5] gateway down: submit stays pending, not lost"
stop_gateway
submit device-D resume-1 5 >/dev/null
sleep 1.5
echo "resume-1 while gateway down: $(web_state resume-1)"

echo "-- [6] restart both services"
stop_web
start_gateway
wait_http "http://127.0.0.1:$GW_PORT/healthz"
start_web
wait_http "http://127.0.0.1:$WEB_PORT/healthz"
wait_state resume-1 executed
echo "resume-1 after restart: $(curl -sS "http://127.0.0.1:$WEB_PORT/api/commands/resume-1")"

sleep 0.5

# ============================================================================
# [8] Post-maintenance reconciliation (网关换盘后的历史执行记录对账)
# ============================================================================
echo "-- [8a] reconciliation: healthy baseline is consistent, no flag"
submit device-R rec-keep 5 >/dev/null
submit device-R rec-lose 5 >/dev/null
wait_state rec-keep executed
wait_state rec-lose executed
rec_run=$(curl -sS -X POST "http://127.0.0.1:$WEB_PORT/api/reconcile" \
  -H 'content-type: application/json' -d '{"device_id":"device-R"}' | field id)
echo "reconcile run #$rec_run created for device-R"
for _ in $(seq 1 100); do
  st=$(curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$rec_run" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['status'])")
  case "$st" in done_*) break;; esac
  sleep 0.1
done
echo "baseline run status: $st"
[ "$st" = "done_ok" ] || { echo "expected done_ok, got $st" >&2; exit 1; }
curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/flags" | grep -q device-R \
  && { echo "device-R must not be flagged on a clean run" >&2; exit 1; } || true

echo "-- [8b] gateway disk swap: rec-lose record disappears -> missing + flag"
"$PY" - "$TMP/gateway.db" <<'PYTAMPER'
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute("DELETE FROM executions WHERE command_id='rec-lose'")
conn.commit()
PYTAMPER
diff_run=$(curl -sS -X POST "http://127.0.0.1:$WEB_PORT/api/reconcile" \
  -H 'content-type: application/json' -d '{"device_id":"device-R"}' | field id)
for _ in $(seq 1 100); do
  st=$(curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$diff_run" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['status'])")
  case "$st" in done_*) break;; esac
  sleep 0.1
done
echo "diff run status: $st"
[ "$st" = "done_diff" ] || { echo "expected done_diff, got $st" >&2; exit 1; }
curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$diff_run" \
  | "$PY" -c "import json,sys; d=json.load(sys.stdin); i={x['command_id']:x['result'] for x in d['items']}; print(i); assert i=={'rec-keep':'consistent','rec-lose':'missing'}"
curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/flags" | grep -q device-R \
  || { echo "device-R must be flagged after discrepancy" >&2; exit 1; }

echo "-- [8c] flagged device: new commands are never claimed, other devices run"
submit device-R rec-new 5 >/dev/null
submit device-Q rec-other 5 >/dev/null
sleep 2
echo "rec-new on flagged device: $(web_state rec-new) (expect pending)"
[ "$(web_state rec-new)" = "pending" ] || { echo "flagged device was dispatched!" >&2; exit 1; }
[ "$(gw_code rec-new)" = "404" ] || { echo "rec-new must never reach gateway" >&2; exit 1; }
wait_state rec-other executed
echo "rec-other on another device: $(web_state rec-other) (expect executed)"

echo "-- [8d] gateway unreachable during reconcile -> unverified, not consistent/missing"
stop_gateway
down_run=$(curl -sS -X POST "http://127.0.0.1:$WEB_PORT/api/reconcile" \
  -H 'content-type: application/json' -d '{"device_id":"device-Q"}' | field id)
for _ in $(seq 1 150); do
  st=$(curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$down_run" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['status'])")
  case "$st" in done_*) break;; esac
  sleep 0.1
done
echo "unreachable run status: $st"
[ "$st" = "done_unverified" ] || { echo "expected done_unverified, got $st" >&2; exit 1; }
curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$down_run" \
  | "$PY" -c "import json,sys; d=json.load(sys.stdin); assert d['device_flagged'] is False; assert all(i['result']=='unverified' for i in d['items']); print('unverified items:', len(d['items']))"

echo "-- [8e] restart both services: flag and findings remain visible"
stop_web
start_gateway
wait_http "http://127.0.0.1:$GW_PORT/healthz"
start_web
wait_http "http://127.0.0.1:$WEB_PORT/healthz"
sleep 0.5
curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/flags" | grep -q device-R \
  || { echo "device-R flag did not survive restart" >&2; exit 1; }
curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$diff_run" \
  | "$PY" -c "import json,sys; d=json.load(sys.stdin); assert d['status']=='done_diff' and d['diff_count']==1; print('diff run persisted after restart')"
# Still blocked after restart.
sleep 1.5
[ "$(web_state rec-new)" = "pending" ] || { echo "flag enforcement lost after restart" >&2; exit 1; }
echo "rec-new still pending after restart: $(web_state rec-new)"

echo "-- [8f] re-reconcile after gateway recovery resolves the unverified run"
re_run=$(curl -sS -X POST "http://127.0.0.1:$WEB_PORT/api/reconcile" \
  -H 'content-type: application/json' -d '{"device_id":"device-Q"}' | field id)
for _ in $(seq 1 100); do
  st=$(curl -sS "http://127.0.0.1:$WEB_PORT/api/reconcile/runs/$re_run" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['status'])")
  case "$st" in done_*) break;; esac
  sleep 0.1
done
echo "recovered run status: $st"
[ "$st" = "done_ok" ] || { echo "expected done_ok after recovery, got $st" >&2; exit 1; }

echo "-- final states"
for c in ok-1 ok-2 ok-3 live-1 resume-1 kill-1 kill-2 kill-3; do
  printf '%s=%s ' "$c" "$(web_state "$c")"
done
echo
echo "-- gateway execution records"
curl -sS "http://127.0.0.1:$GW_PORT/api/executions"; echo

"$PY" - "$GW_PORT" <<'PYCHECK'
import json, sys, urllib.request
port = sys.argv[1]
rows = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/executions"))
by_id = {r["command_id"]: r for r in rows}

must_execute_once = ["ok-1", "ok-2", "ok-3", "live-1", "resume-1", "kill-3"]
for cid in must_execute_once:
    assert cid in by_id, f"{cid} missing at gateway"
    assert by_id[cid]["state"] == "executed", by_id[cid]
    assert by_id[cid]["executions"] == 1, by_id[cid]

assert by_id.get("kill-1", {}).get("state") == "unknown", by_id.get("kill-1")
assert by_id.get("kill-1", {}).get("executions", 0) == 0, by_id.get("kill-1")
assert "kill-2" not in by_id, "kill-2 must never reach the gateway while blocked"
print("executed exactly once:", ", ".join(must_execute_once))
print("kill-1 frozen as unknown, kill-2 never delivered")
PYCHECK

echo "== PASS =="
