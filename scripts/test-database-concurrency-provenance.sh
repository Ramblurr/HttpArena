#!/usr/bin/env bash
# shellcheck disable=SC1091,SC2034
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT_DIR/scripts/lib/database-concurrency.sh"
work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT

write_fixture() {
    rm -rf "$work_dir"
    mkdir -p "$work_dir/data" "$work_dir/frameworks/http-kit"
    cp "$ROOT_DIR/data/database-concurrency-contract.json" "$work_dir/data/"
    cat > "$work_dir/frameworks/http-kit/meta.json" <<'JSON'
{"type":"emerging","mode":"tuned","tests":["database-concurrency"]}
JSON
    python3 - "$work_dir/data/database-concurrency-contract.json" "$work_dir/frameworks/http-kit/database-concurrency.json" <<'PY'
import json
import sys
contract = json.load(open(sys.argv[1]))
provenance = {key: contract[key] for key in ("profile", "io_model", "route", "dependencies", "pool")}
json.dump(provenance, open(sys.argv[2], "w"))
PY
}

reject() {
    if validate_database_concurrency_contract "$work_dir" http-kit >/dev/null 2>&1; then
        echo "FAIL: accepted invalid $1" >&2
        exit 1
    fi
}

write_fixture
validate_database_concurrency_contract "$work_dir" http-kit

python3 - "$work_dir/frameworks/http-kit/meta.json" <<'PY'
import json, sys
path = sys.argv[1]; data = json.load(open(path)); data["mode"] = "standard"; json.dump(data, open(path, "w"))
PY
reject "metadata mode"
write_fixture
python3 - "$work_dir/frameworks/http-kit/database-concurrency.json" <<'PY'
import json, sys
path = sys.argv[1]; data = json.load(open(path)); data["route"]["path"] = "/async-db"; json.dump(data, open(path, "w"))
PY
reject "route contract"
write_fixture
python3 - "$work_dir/frameworks/http-kit/database-concurrency.json" <<'PY'
import json, sys
path = sys.argv[1]; data = json.load(open(path)); data["dependencies"]["next.jdbc"] = "0"; json.dump(data, open(path, "w"))
PY
reject "dependency contract"
write_fixture
python3 - "$work_dir/frameworks/http-kit/database-concurrency.json" <<'PY'
import json, sys
path = sys.argv[1]; data = json.load(open(path)); data["pool"]["maximum_pool_size"] = "CPU"; json.dump(data, open(path, "w"))
PY
reject "pool contract"
write_fixture
python3 - "$work_dir/data/database-concurrency-contract.json" <<'PY'
import json, sys
path = sys.argv[1]; data = json.load(open(path)); data["cohort"] = ["http-kit"]; json.dump(data, open(path, "w"))
PY
reject "cohort"

source "$ROOT_DIR/scripts/lib/profiles.sh"
source "$ROOT_DIR/scripts/lib/tools/gcannon.sh"
PORT=8080
REQUESTS_DIR="$ROOT_DIR/requests"
THREADS=64
validate_profiles
mapfile -t args < <(gcannon_build_args database-concurrency 1024 1 10s 0)
[ "${PROFILES[database-concurrency]}" = "1|0|0-31,64-95|1024|database-concurrency" ]
[ "${args[1]}" = "--raw" ]
[[ "${args[2]}" == *"database-concurrency-5.raw"* ]]
[[ "${args[2]}" == *"database-concurrency-50.raw"* ]]
echo "PASS: database-concurrency static contract and profile dispatch"
