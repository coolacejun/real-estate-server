#!/bin/zsh
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1}"
ENV_FILE="${ENV_FILE:-/Users/jun/server/.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

ADMIN_TOKEN_VALUE="${ADMIN_TOKEN:-}"
typeset -a ADMIN_HEADERS
ADMIN_HEADERS=()
if [[ -n "$ADMIN_TOKEN_VALUE" ]]; then
  ADMIN_HEADERS=(-H "x-admin-token: $ADMIN_TOKEN_VALUE")
fi

pass_count=0
fail_count=0

results_file="$(/usr/bin/mktemp)"
cleanup() {
  rm -f "$results_file"
}
trap cleanup EXIT

has_expected_code() {
  local code="$1"
  local expected_csv="$2"
  local item
  for item in ${(s:,:)expected_csv}; do
    if [[ "$code" == "$item" ]]; then
      return 0
    fi
  done
  return 1
}

record_result() {
  local result_state="$1"
  local method="$2"
  local endpoint_path="$3"
  local code="$4"
  local expected="$5"
  local name="$6"
  printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$result_state" "$method" "$endpoint_path" "$code" "$expected" "$name" >> "$results_file"
}

run_case() {
  local name="$1"
  local method="$2"
  local endpoint_path="$3"
  local expected_csv="$4"
  local body="${5:-}"
  local use_admin="${6:-false}"

  local url="${BASE_URL}${endpoint_path}"
  local body_file
  body_file="$(/usr/bin/mktemp)"
  local -a cmd
  cmd=(/usr/bin/curl -sS -o "$body_file" -w "%{http_code}" -X "$method" "$url")

  if [[ "$use_admin" == "true" && -n "$ADMIN_TOKEN_VALUE" ]]; then
    cmd+=("${ADMIN_HEADERS[@]}")
  fi

  if [[ -n "$body" ]]; then
    cmd+=(-H "Content-Type: application/json" --data "$body")
  fi

  local code
  if ! code="$("${cmd[@]}" 2>/dev/null)"; then
    code="000"
  fi

  local result_state="FAIL"
  if has_expected_code "$code" "$expected_csv"; then
    result_state="PASS"
    pass_count=$((pass_count + 1))
  else
    fail_count=$((fail_count + 1))
  fi

  record_result "$result_state" "$method" "$endpoint_path" "$code" "$expected_csv" "$name"
  if [[ "$result_state" == "FAIL" ]]; then
    local detail
    detail="$(/usr/bin/head -c 220 "$body_file" | /usr/bin/tr '\n' ' ')"
    printf "  detail: %s\n" "$detail" >> "$results_file"
  fi

  rm -f "$body_file"
}

run_sse_case() {
  local name="$1"
  local endpoint_path="$2"
  local expected_csv="$3"
  local use_admin="${4:-false}"

  local url="${BASE_URL}${endpoint_path}"
  local header_file body_file
  header_file="$(/usr/bin/mktemp)"
  body_file="$(/usr/bin/mktemp)"

  local -a cmd
  cmd=(/usr/bin/curl -sS -N -D "$header_file" --max-time 4 "$url")
  if [[ "$use_admin" == "true" && -n "$ADMIN_TOKEN_VALUE" ]]; then
    cmd+=("${ADMIN_HEADERS[@]}")
  fi

  "${cmd[@]}" >"$body_file" 2>/dev/null || true

  local code
  code="$(/usr/bin/awk 'tolower($1) ~ /^http/ {status=$2} END {print status+0}' "$header_file")"
  if [[ "$code" == "0" ]]; then
    code="000"
  fi

  local result_state="FAIL"
  if has_expected_code "$code" "$expected_csv"; then
    result_state="PASS"
    pass_count=$((pass_count + 1))
  else
    fail_count=$((fail_count + 1))
  fi

  local stream_seen="no"
  if /usr/bin/grep -Eq 'event:|data:' "$body_file"; then
    stream_seen="yes"
  fi
  record_result "$result_state" "GET" "$endpoint_path" "$code" "$expected_csv" "$name (stream_event=$stream_seen)"

  if [[ "$result_state" == "FAIL" ]]; then
    local detail
    detail="$(/usr/bin/head -c 220 "$body_file" | /usr/bin/tr '\n' ' ')"
    printf "  detail: %s\n" "$detail" >> "$results_file"
  fi

  rm -f "$header_file" "$body_file"
}

# Public APIs
run_case "health" "GET" "/health" "200"
run_case "tile png" "GET" "/v1/tiles/cadastral/1/0/0.png" "200"
run_case "app config" "GET" "/v1/app-config?platform=android" "200"
run_case "tile config" "GET" "/v1/tile-config" "200"
run_case "admin page" "GET" "/admin" "200,404"
run_case "admin page slash" "GET" "/admin/" "200,404"
run_case "admin logs page" "GET" "/admin/logs" "200,404"
run_case "admin logs page slash" "GET" "/admin/logs/" "200,404"
run_case "simple data" "GET" "/v1/simple-data/sample-doc" "200"
run_case "single data fetch" "GET" "/v1/data/building_info/1111010100100010000?format=compressed" "200"
run_case "batch data fetch" "POST" "/v1/data/batch" "200" '{"items":[{"collection":"building_info","pnu":"1111010100100010000"}],"format":"compressed"}'
run_case "tile fetch" "GET" "/v1/tile/root-a/parent-a/id-a" "200"
run_case "tile batch" "POST" "/v1/tile/batch" "200" '{"tiles":[{"root":"root-a","parent":"parent-a","id":"id-a"}]}'
run_case "pnu polygon" "GET" "/v1/pnu/1111010100100010000/polygon?format=raw" "200"
run_case "geo building" "GET" "/v1/geo/building?pnu=1111010100100010000&limit=5" "200"
run_case "geo building violations" "GET" "/v1/geo/building/violations?pnu=1111010100100010000&limit=5" "200"
run_case "geo land" "GET" "/v1/geo/land/1111010100100010000?limit=5" "200"
run_case "geo land polygons" "POST" "/v1/geo/land/polygons" "200" '{"prefixes":["11110"]}'
run_case "geo land features" "POST" "/v1/geo/land/features" "200" '{"prefixes":["11110"]}'
run_case "addr search" "GET" "/v1/addr/search?query=seoul&page=1&page_size=5" "200"
run_case "coord2address" "GET" "/v1/addr/coord2address?x=127.0276&y=37.4979" "200"
run_case "coord2region" "GET" "/v1/addr/coord2region?x=127.0276&y=37.4979" "200"
run_case "position" "GET" "/v1/addr/position?lng=127.0276&lat=37.4979" "200"
run_case "geocode" "GET" "/v1/addr/geocode?address=seoul&epsg=EPSG:4326" "200"

# Admin APIs (safe smoke; mutation endpoints are exercised with non-destructive invalid payloads)
run_case "admin releases list" "GET" "/v1/admin/cadastral/releases?limit=5" "200,403" "" "true"
run_case "admin import jobs list" "GET" "/v1/admin/cadastral/import-jobs?limit=5" "200,403" "" "true"
run_case "admin import job workers list" "GET" "/v1/admin/cadastral/import-job-workers?limit=5" "200,403" "" "true"
run_case "admin import path options" "GET" "/v1/admin/cadastral/import-path-options?data_type=cadastral&operation_mode=full&limit=10" "200,403" "" "true"
run_sse_case "admin cadastral events stream" "/v1/admin/cadastral/events?interval_ms=1000&release_limit=5&job_limit=5&worker_limit=5" "200,403" "true"
run_sse_case "admin server logs stream" "/v1/admin/server-logs/events?interval_ms=1000&tail_lines=5" "200,403" "true"
run_case "admin clear data type (confirm false)" "POST" "/v1/admin/cadastral/data-types/cadastral/clear" "400,403" '{"confirm":false}' "true"
run_case "admin create release (missing version)" "POST" "/v1/admin/cadastral/releases" "400,403" '{}' "true"
run_case "admin activate release (missing id)" "POST" "/v1/admin/cadastral/releases/999999999/activate" "404,403" "" "true"
run_case "admin patch release (empty body)" "PATCH" "/v1/admin/cadastral/releases/999999999" "400,403" '{}' "true"
run_case "admin create import job (invalid)" "POST" "/v1/admin/cadastral/import-jobs" "400,403" '{}' "true"
run_case "admin upload and import (disabled)" "POST" "/v1/admin/cadastral/upload-and-import" "410,403" "" "true"
run_case "admin import from path (invalid)" "POST" "/v1/admin/cadastral/import-from-path" "400,403" '{}' "true"
run_case "admin patch import job (empty body)" "PATCH" "/v1/admin/cadastral/import-jobs/999999999" "400,403" '{}' "true"
run_case "admin run import job (missing id)" "POST" "/v1/admin/cadastral/import-jobs/999999999/run" "404,403" '{}' "true"

echo "=== API Smoke Test Summary (${BASE_URL}) ==="
echo "pass=${pass_count} fail=${fail_count} total=$((pass_count + fail_count))"
echo
printf "%-5s %-6s %-55s %-5s %-9s %s\n" "STAT" "METHOD" "PATH" "CODE" "EXPECT" "NAME"
printf "%-5s %-6s %-55s %-5s %-9s %s\n" "-----" "------" "-------------------------------------------------------" "-----" "---------" "-------------------------"
while IFS=$'\t' read -r stat method endpoint_path code expect name; do
  printf "%-5s %-6s %-55s %-5s %-9s %s\n" "$stat" "$method" "$endpoint_path" "$code" "$expect" "$name"
done < "$results_file"

if [[ "$fail_count" -gt 0 ]]; then
  echo
  echo "FAILED DETAIL:"
  /usr/bin/awk '/^  detail: / {print}' "$results_file" || true
  exit 1
fi
