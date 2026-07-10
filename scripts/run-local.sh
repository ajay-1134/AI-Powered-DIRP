#!/usr/bin/env bash
# Run the whole system locally with no Docker — builds the six binaries and
# starts them wired together, with the platform's OTLP receiver collecting
# traces and the user service driving baseline load. Ctrl-C stops everything.
#
#   ./scripts/run-local.sh
#   # then, in another terminal:
#   ./scripts/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BIN="$(mktemp -d)/bin"
mkdir -p "$BIN"
echo "▶ Building binaries into $BIN"
go build -o "$BIN/platform" ./cmd/platform
for s in user order payment ledger notification; do
  go build -o "$BIN/$s" ./cmd/services/$s
done

pids=()
cleanup() { echo; echo "stopping…"; for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM

echo "▶ Starting platform (dashboard :8080, OTLP :4317)"
"$BIN/platform" & pids+=($!)
sleep 1

export OTLP_ENDPOINT=localhost:4317
echo "▶ Starting microservices with tracing + baseline load"
PAYMENT_DB_MAX_CONNS=10 "$BIN/payment"       & pids+=($!)
"$BIN/notification"                          & pids+=($!)
"$BIN/ledger"                                & pids+=($!)
"$BIN/order"                                 & pids+=($!)
LOAD_RPS=12 "$BIN/user"                      & pids+=($!)

echo
echo "✅ Up. Open http://localhost:8080 — then run ./scripts/demo.sh to inject the fault."
echo "   (Ctrl-C here stops the whole stack.)"
wait
