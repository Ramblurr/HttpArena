# scripts/lib/database-concurrency.sh — fail-closed static contract checks.
# shellcheck shell=bash

validate_database_concurrency_contract() {
    local root_dir="$1" framework="$2"

    python3 - "$root_dir" "$framework" <<'PY'
import json
import sys
from pathlib import Path

EXPECTED_COHORT = ("http-kit", "pedestal", "ring", "ring-jetty9-adapter")
root = Path(sys.argv[1])
framework = sys.argv[2]
contract_path = root / "data" / "database-concurrency-contract.json"
meta_path = root / "frameworks" / framework / "meta.json"
provenance_path = root / "frameworks" / framework / "database-concurrency.json"

try:
    contract = json.loads(contract_path.read_text())
    meta = json.loads(meta_path.read_text())
    provenance = json.loads(provenance_path.read_text())
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"FAIL: database-concurrency contract: {error}")

expected_provenance = {
    key: contract[key]
    for key in ("profile", "io_model", "route", "dependencies", "pool")
}
if tuple(contract.get("cohort", ())) != EXPECTED_COHORT:
    raise SystemExit(
        "FAIL: database-concurrency cohort must be exactly "
        "http-kit, pedestal, ring, ring-jetty9-adapter"
    )
if framework not in EXPECTED_COHORT:
    raise SystemExit(f"FAIL: {framework} is not in the database-concurrency cohort")
if (
    meta.get("type") != contract["metadata"]["type"]
    or meta.get("mode") != contract["metadata"]["mode"]
    or contract["profile"] not in meta.get("tests", [])
    or provenance != expected_provenance
    ):
    raise SystemExit(
        "FAIL: database-concurrency metadata or declaration does not match the shared contract"
    )

print(f"PASS: database-concurrency static contract ({framework})")
PY
}
