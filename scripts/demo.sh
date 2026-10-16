#!/usr/bin/env bash
# Inject Payment pool exhaustion, wait for diagnosis, then verify recovery.
set -Eeuo pipefail

PLATFORM="${PLATFORM_URL:-http://localhost:8080}"
PAYMENT="${PAYMENT_URL:-http://localhost:8083}"
HOLDERS="${HOLDERS:-10}"
DURATION="${DURATION:-45s}"
DIAGNOSIS_TIMEOUT="${DIAGNOSIS_TIMEOUT:-60}"
RECOVERY_TIMEOUT="${RECOVERY_TIMEOUT:-120}"

# Verify the endpoints before changing application state.
curl -fsS "$PLATFORM/api/incidents" >/dev/null
curl -fsS "$PAYMENT/chaos/status" >/dev/null

# Record existing IDs so old incidents cannot satisfy this demo.
BASELINE_IDS="$(
  curl -fsS "$PLATFORM/api/incidents" |
    python3 -c '
import json, sys
print(",".join(i["id"] for i in json.load(sys.stdin)))
'
)"
export BASELINE_IDS

echo "== AI-powered-DIRP cloud demo =="
echo "Platform: $PLATFORM"
echo "Payment:  $PAYMENT"
echo "Fault:    $HOLDERS connections for $DURATION"
echo

echo "Injecting connection-pool exhaustion..."
curl -fsS -X POST \
  "$PAYMENT/chaos/db-pool-exhaust?holders=${HOLDERS}&duration=${DURATION}"
echo
echo "Fault injected. Waiting for a NEW incident diagnosis..."

INCIDENT=""
for ((elapsed=0; elapsed<DIAGNOSIS_TIMEOUT; elapsed+=2)); do
  BODY="$(curl -fsS "$PLATFORM/api/incidents")"

  INCIDENT="$(
    printf '%s' "$BODY" |
      python3 -c '
import json, os, sys
before = set(filter(None, os.environ["BASELINE_IDS"].split(",")))
incidents = json.load(sys.stdin)
for i in incidents:
    rc = i.get("root_cause")
    if i.get("id") not in before and isinstance(rc, dict) and rc.get("service"):
        print(json.dumps(i))
        break
'
  )"

  if [[ -n "$INCIDENT" ]]; then
    break
  fi

  sleep 2
done

if [[ -z "$INCIDENT" ]]; then
  echo "ERROR: No new incident with a completed root-cause diagnosis."
  echo "Inspect: docker compose -f docker-compose.cloud.yml logs --tail=100 platform"
  exit 1
fi

INCIDENT_ID="$(
  printf '%s' "$INCIDENT" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
)"
ROOT_SERVICE="$(
  printf '%s' "$INCIDENT" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["root_cause"]["service"])'
)"

echo
echo "== INCIDENT DIAGNOSED =="
printf '%s' "$INCIDENT" | python3 -c '
import json, sys
i = json.load(sys.stdin)
rc = i["root_cause"]
print("ID:          ", i["id"])
print("Severity:    ", i.get("severity"))
print("Status:      ", i.get("status"))
print("Affected:    ", i.get("affected_service"))
print("Evidence:")
for e in i.get("evidence") or []:
    print("  - [{}] {}".format(e.get("service"), e.get("detail")))
print("Root cause:  ", rc["service"])
print("Confidence:  ", rc.get("confidence"))
print("Source:      ", rc.get("source"))
print("Summary:     ", rc.get("summary"))
print("Remediation:")
for r in rc.get("remediation") or []:
    print("  -", r)
'

if [[ "$ROOT_SERVICE" != "payment-service" ]]; then
  echo
  echo "WARNING: Expected payment-service; got $ROOT_SERVICE."
  ROOT_CAUSE_CORRECT=0
else
  echo
  echo "PASS: Correct root cause identified: payment-service."
  ROOT_CAUSE_CORRECT=1
fi

echo
echo "Waiting for incident $INCIDENT_ID to resolve..."
RESOLVED=0

for ((elapsed=0; elapsed<RECOVERY_TIMEOUT; elapsed+=2)); do
  STATUS="$(
    curl -fsS "$PLATFORM/api/incidents/$INCIDENT_ID" |
      python3 -c '
import json, sys
print(json.load(sys.stdin).get("status", ""))
' 2>/dev/null || true
  )"

  if [[ "$STATUS" == "resolved" ]]; then
    RESOLVED=1
    break
  fi

  sleep 2
done

if [[ "$RESOLVED" -ne 1 ]]; then
  echo "ERROR: Incident did not resolve within ${RECOVERY_TIMEOUT}s."
  echo "Current status: ${STATUS:-unknown}"
  exit 1
fi

echo "PASS: Incident $INCIDENT_ID resolved."

if [[ "$ROOT_CAUSE_CORRECT" -ne 1 ]]; then
  echo "FAIL: Recovery succeeded, but the root cause was incorrect."
  exit 2
fi

echo
echo "SUCCESS: Fault detected, correct root cause identified, and incident resolved."
EOF
