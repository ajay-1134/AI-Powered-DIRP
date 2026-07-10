#!/usr/bin/env bash
# Inject the signature failure and print the incident the platform produces.
# Works against either the docker-compose stack or a local run — both expose the
# Payment chaos endpoint on :8083 and the platform on :8080.
set -euo pipefail

PLATFORM="${PLATFORM_URL:-http://localhost:8080}"
PAYMENT="${PAYMENT_URL:-http://localhost:8083}"
HOLDERS="${HOLDERS:-10}"
DURATION="${DURATION:-45s}"

echo "▶ Injecting DB connection-pool exhaustion on payment-service"
echo "  (${HOLDERS} held connections for ${DURATION})"
curl -s -X POST "${PAYMENT}/chaos/db-pool-exhaust?holders=${HOLDERS}&duration=${DURATION}" >/dev/null
echo "  injected. Watch the dashboard at ${PLATFORM}"
echo

echo "⏳ Waiting for the detection + analysis pipeline…"
for i in $(seq 1 20); do
  sleep 2
  body="$(curl -s "${PLATFORM}/api/incidents" || true)"
  if echo "$body" | grep -q '"root_cause"' && echo "$body" | grep -q '"service"'; then
    echo
    echo "🚨 INCIDENT DIAGNOSED:"
    echo "$body" | python3 -c '
import sys, json
d = json.load(sys.stdin)
if not d:
    sys.exit()
i = d[0]
rc = i.get("root_cause") or {}
print("  {}  severity={}  status={}".format(i["id"], i["severity"], i["status"]))
print("  affected: {}".format(i["affected_service"]))
print("  evidence:")
for e in i.get("evidence", []):
    print("    - [{}] {}".format(e["service"], e["detail"]))
print("  root cause: {}  (source={}, confidence={})".format(
    rc.get("service"), rc.get("source"), rc.get("confidence")))
print("    {}".format(rc.get("summary")))
print("  remediation:")
for r in (rc.get("remediation") or []):
    print("    - {}".format(r))
'
    exit 0
  fi
done

echo "No incident detected yet — is the stack warmed up (needs ~15s of baseline traffic)?"
exit 1
