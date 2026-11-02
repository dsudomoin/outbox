# Harness of the production check; sourced by run.sh. The instance and Docker helpers are the chaos scenarios' own
# (bench/scenarios/chaos-lib.sh and lib.sh, stand-lock.sh among them): the same PID discipline and the same stand lock.
# Instance I listens on 8079+I and 8084+I, so 1001..1005 are 9080..9084 and 9085..9089; the target is 1011.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../bench/scenarios/chaos-lib.sh"
# Ends with a scenario's own containers removed, after its instances stopped and before the stand lock goes.
pc_exit() {
  local rc=$? i
  if [ -n "${AFTER:-}" ]; then
    for i in "${!APP_PIDS[@]}"; do inst_stop "$i" || true; done
    eval "$AFTER" > /dev/null 2>&1 || true
  fi
  cleanup
  exit "$rc"
}
trap pc_exit EXIT
PC=$ROOT/prodcheck
PG=kora-outbox-prodcheck-source # the helpers' psql_q and cleanup act on this container
KAFKA=kora-outbox-prodcheck-no-kafka # no Kafka yet: cleanup must not start the stand's
TGT=kora-outbox-prodcheck-target
SOURCE_B=kora-outbox-prodcheck-source-b
RESULT_DIR=${RESULT_DIR:-$PC/build/results}
mkdir -p "$RESULT_DIR"
ADMIN_KEY=prodcheck-admin
READER_KEY=prodcheck-reader
SHOP=$PC/shop/build/install/shop-n/bin/shop
SHOP_N1=$PC/shop/build/install/shop-n1/bin/shop
SHOP_N2=$PC/shop/build/install/shop-n2/bin/shop
COURIER=$PC/courier/build/install/courier/bin/courier
TARGET=$PC/target/build/install/target/bin/target
ACCOUNTS=$PC/accounts/build/install/accounts/bin/accounts
ACCOUNTS_FULL=$PC/accounts/build/install/accounts-full/bin/accounts
COMPOSE=(docker compose -f "$PC/docker-compose.yml")
export PG_VERSION=${PG_VERSION:-16} # also the compose file's
PG_IMAGE=kora-outbox-prodcheck-pg:$PG_VERSION
FAKETIME_LIB=/usr/lib/aarch64-linux-gnu/faketime
T=1011 # the target's instance number
PC_PG_URL=jdbc:postgresql://localhost:55432/outbox
PC_TARGET_URL=http://localhost:9090
PC_TARGET_DB_URL=jdbc:postgresql://localhost:55433/outbox
BASES=(outbox)
REPAIRED="сама"
export JAVA_HOME=${JAVA_HOME:-$(/usr/libexec/java_home -v 21)}

target_q() { PG=$TGT psql_q "$1"; }
pub() { echo "http://localhost:$((8079 + $1))"; }

# pc_begin NAME: both databases empty, ledger tables in place.
pc_begin() {
  SCN=$1 NAME=$1 T0=$(now) CHECKS="" CHECK_FAILED="" FAULTS="" LOADS=()
  log "=== $SCN"
  docker rm -f -v "$SOURCE_B" > /dev/null 2>&1 || true
  docker volume rm kora-outbox-prodcheck-backup > /dev/null 2>&1 || true
  # a replica's or a pooler's leftovers (synchronous_standby_names, say) or another server version: the stand from scratch
  local image
  image=$(docker inspect -f '{{.Config.Image}}' "$PG" 2> /dev/null || echo "$PG_IMAGE")
  if [ "$image" != "$PG_IMAGE" ] || docker inspect kora-outbox-prodcheck-replica > /dev/null 2>&1 || docker inspect kora-outbox-prodcheck-pooler > /dev/null 2>&1; then
    "${COMPOSE[@]}" --profile failover --profile pooler down -v > /dev/null 2>&1 || true
  fi
  "${COMPOSE[@]}" up -d --wait > /dev/null 2>&1
  local c
  for c in "$PG" "$TGT"; do
    docker exec "$c" psql -U outbox -d postgres -Atq -c "drop database if exists outbox with (force)" -c "create database outbox"
    docker exec -i "$c" psql -U outbox -d outbox -q < "$PC/scenarios/ledger.sql"
  done
  rm -f "${LOGS:?}/pc-${SCN:?}-"*.log
}

# source_replace DOCKER_ARGS [PORT [IMAGE]]: another Postgres takes over as the source: the same image and settings, new
# data unless DOCKER_ARGS bring a volume; on PORT (default the compose source's 55432, already stopped then). pc_begin
# removes it.
source_replace() {
  PG=$SOURCE_B
  # shellcheck disable=SC2086
  docker run -d --name "$PG" -e POSTGRES_DB=outbox -e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox --cpus 2 --memory 2g \
    --shm-size 512m -p "127.0.0.1:${2:-55432}:5432" $1 "${3:-$PG_IMAGE}" \
    postgres -c max_connections=300 -c shared_buffers=512MB -c max_locks_per_transaction=256 -c synchronous_commit=on > /dev/null
  ready() { docker exec "$PG" pg_isready -U outbox -d outbox -q; }
  wait_for 60 ready
  sleep 2
}

pc_target() {
  APP=$TARGET inst_start "$T" "pc-$SCN-target" POSTGRES_URL="$PC_TARGET_DB_URL" INSTANCE_NAME=target "JAVA_OPTS=-Xmx768m ${TARGET_OPTS:-}"
}

# pc_app BIN I [-Dkey=value...]: (re)starts instance I of BIN; its load comes back with it.
pc_app() {
  local bin=$1 i=$2
  shift 2
  APP=$bin inst_start "$i" "pc-$SCN-$i" POSTGRES_URL="$PC_PG_URL" TARGET_DB_URL="$PC_TARGET_DB_URL" TARGET_URL="$PC_TARGET_URL" \
    DB_POOL_SIZE="${DB_POOL:-24}" INSTANCE_NAME="app-$i" "JAVA_OPTS=-Xmx768m ${PC_OPTS:-} $*"
  if [ -n "${LOADS[$i]:-}" ]; then pc_load "$i" "${LOADS[$i]}"; fi
}
pc_shop() { pc_app "$SHOP" "$@"; }
pc_courier() { pc_app "$COURIER" "$@"; }

# thread_dump I LABEL: the instance's stacks into build/results, and the outbox threads' top frames to the log.
thread_dump() {
  local f=$RESULT_DIR/$SCN-$1-$2.threads
  "$JAVA_HOME/bin/jstack" "${APP_PIDS[$1]}" > "$f" 2>&1 || true
  log "threads of $1 ($2): $f"
  grep -A6 -E '^"outbox-(maintenance|heartbeat|dead-letters|dead-letter-hook|inbox-maintenance)' "$f" | grep -E '^"|^\s+at ' | cut -c1-110 >&2 || true
}

drain_call() { # drain_call I MAX_SECONDS: the JSON /drain answers, empty when the call did not return in time
  curl -s --max-time "$2" -XPOST "$(pub "$1")/drain" || true
}

pc_load() { LOADS[$1]=$2; curl -sf --max-time 10 -XPOST "$(pub "$1")/load?$2" > /dev/null; }
pc_unload() { LOADS[$1]=""; curl -sf --max-time 60 -XDELETE "$(pub "$1")/load" > /dev/null || true; }
tfault() { curl -sf --max-time 10 -XPUT "$(pub "$T")/fault?$1" > /dev/null; }
hctl() { curl -sf --max-time 10 -XPUT "$(pub "$1")/handler?$2" > /dev/null; }
tx_send() { curl -s --max-time 20 -XPOST "$(pub "$1")/tx/send?$2"; }
stats() { curl -sf --max-time 10 "$(pub "$1")/stats"; }

# admin I METHOD PATH [BODY]: the status code on the first line, the body after it; never fails the shell.
admin() {
  curl -s --max-time 90 -w '\n%{http_code}\n' -X "$2" -H "X-Api-Key: $ADMIN_KEY" -H 'Content-Type: application/json' \
    ${4:+-d "$4"} "$(pub "$1")/kora-outbox/admin/v1/$3" | python3 -c 'import sys; l = sys.stdin.read().rstrip("\n").rsplit("\n", 1); print(l[-1]); print(l[0] if len(l) > 1 else "")' || true
}
admin_body() { admin "$@" | tail -n +2; }
admin_code() { admin "$@" | sed -n 1p; }
# j EXPR: EXPR over the JSON on stdin, as d.
j() { python3 -c "import json, sys; d = json.load(sys.stdin); print($1)"; }
st() { admin_body "$1" GET status | j "$2"; } # st I EXPR: EXPR over instance I's /status

# wait_for SECONDS CMD...: until CMD succeeds, 1 on timeout.
wait_for() {
  local deadline=$(($(now) + $1))
  shift
  until "$@" > /dev/null 2>&1; do
    [ "$(now)" -lt "$deadline" ] || return 1
    sleep 1
  done
}
# passes CMD...: 0 or 1 for check.
passes() { if "$@" > /dev/null 2>&1; then echo 0; else echo 1; fi; }
logs_have() { grep -qE "$1" "$LOGS/pc-$SCN-"*.log; }
logs_count() { cat "$LOGS/pc-$SCN-"*.log | grep -cE "$1" || true; }
is() { [ "$@" ]; }
gt() { [ "$1" -gt "$2" ]; }
effects() { # effects [SINK]: effects done so far, both databases
  local w=${1:+"where sink = '$1'"}
  echo $(($(psql_q "select count(*) from ${EFFECTS_TABLE:-pc_effect} $w") + $(target_q "select count(*) from pc_effect $w")))
}
# bounded SECONDS CMD...: a process that should end by itself: its exit code; still running after SECONDS: killed, 0.
bounded() {
  local limit=$1 pid n=0 rc=0
  shift
  "$@" &
  pid=$!
  while ours "$pid" && [ "$n" -lt "$limit" ]; do sleep 1; n=$((n + 1)); done
  if ours "$pid"; then
    kill -9 "$pid"
    wait "$pid" 2> /dev/null || true
    return 0
  fi
  wait "$pid" || rc=$?
  return "$rc"
}
effects_grow() { local a b; a=$(effects "${2:-}"); sleep "$1"; b=$(effects "${2:-}"); [ "$b" -gt "$a" ]; }

pc_kill() { inst_kill9 "$1"; fault "kill -9 экземпляра $1"; }
# pc_term I: SIGTERM, prints how long the stop took.
pc_term() { local t=$(now); inst_stop "$1"; echo $(($(now) - t)); }

# proxy_up LISTEN TARGET: tcpfault in front of TARGET; proxy MODE [PORT] switches it.
proxy_up() {
  PROXY_CTL=$LOGS/pc-proxy-$1.ctl
  echo pass > "$PROXY_CTL"
  python3 "$PC/scenarios/tcpfault.py" "$1" "$2" "$PROXY_CTL" >> "$LOGS/pc-$SCN-proxy.log" 2>&1 &
  APP_PIDS[$((1020 + ${PROXY_N:-1}))]=$!
  PROXY_N=$((${PROXY_N:-1} + 1))
  sleep 1
}
proxy() { echo "$*" > "$PROXY_CTL"; fault "сеть: $*"; }

# pc_finish [ledger args]: stops the loads, waits for the drain, runs the ledger and /status checks, writes the result.
pc_finish() {
  local i rc=0 led alive="" b args=(${LEDGER_ARGS[@]+"${LEDGER_ARGS[@]}"})
  for i in "${!APP_PIDS[@]}"; do
    if [ "$i" -lt "$T" ] && up "$i"; then pc_unload "$i"; alive=${alive:-$i}; fi
  done
  for b in "${BASES[@]}"; do args+=(--base "$b"); done
  led=$(python3 "$PC/scenarios/ledger.py" --source "$PG" --target "$TGT" "${args[@]}" "$@") || rc=$?
  local status='{}'
  if [ -n "${NO_ADMIN:-}" ]; then
    : # this app has no admin API: the ledger's retry, parked and dead counts and the scenario's SQL stand for /status
  elif [ -n "$alive" ]; then
    status=$(admin_body "$alive" GET status)
    local bad
    bad=$(echo "$status" | j '[k for k, v in {"halted": d["halted"], "orphans": d["orphans"], "tailOverflow": False}.items() if v]') || bad="status unreadable"
    [ "$bad" = "[]" ] || CHECKS="$CHECKS
- **НЕ ПРОЙДЕНА**: /status после сверки: $bad"
    [ "$bad" = "[]" ] || CHECK_FAILED=1
  else
    CHECKS="$CHECKS
- **НЕ ПРОЙДЕНА**: к концу не осталось живых инстансов: /status не прочитан"
    CHECK_FAILED=1
  fi
  [ -z "${CHECK_FAILED:-}" ] || [ "$rc" != 0 ] || rc=1
  LEDGER=$led STATUS=$status CHECKS=$CHECKS FAULTS=$FAULTS REPAIRED=$REPAIRED RC=$rc SCN=$SCN SECONDS_TOTAL=$(($(now) - T0)) python3 - > "$RESULT_DIR/$SCN.json" << 'PY'
import json, os
try:
    ledger = json.loads(os.environ["LEDGER"])
except ValueError:
    ledger = {"violations": ["ledger output unreadable"]}
try:
    status = json.loads(os.environ["STATUS"])
except ValueError:
    status = {}
print(json.dumps({"scenario": os.environ["SCN"], "verdict": "PASS" if os.environ["RC"] == "0" else "FAIL",
                  "repaired": os.environ["REPAIRED"], "seconds": int(os.environ["SECONDS_TOTAL"]), "ledger": ledger,
                  "checks": [c[2:] for c in os.environ["CHECKS"].splitlines() if c.startswith("- ")],
                  "faults": [c[2:] for c in os.environ["FAULTS"].splitlines() if c.startswith("- ")],
                  "status": {k: status.get(k) for k in ("halted", "retrySize", "parkedSize", "waitingSize", "deadLetters", "orphans", "tailOverflow", "premade", "lagSeconds")}},
                 ensure_ascii=False, indent=1))
PY
  log "$SCN: $([ "$rc" = 0 ] && echo PASS || echo "FAIL (rc $rc)"): $(echo "$led" | j '{k: d[k] for k in ("sent", "effects", "dead", "lost", "duplicates", "latency_p99", "violations")}' 2> /dev/null || echo "$led")"
  [ -z "$CHECKS" ] || echo "$CHECKS" >&2
  for i in "${!APP_PIDS[@]}"; do inst_stop "$i"; done
  return "$rc"
}
