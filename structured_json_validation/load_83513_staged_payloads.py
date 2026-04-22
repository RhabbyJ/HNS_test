#!/usr/bin/env python3
"""Load a generated MIL-DTL-83513 rebuild into the DB staging payload table."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pdf_storage.sync_83513_to_supabase import get_server_key, load_env_file, require_env
from postgresql.backfill_torque_profile_model import RestClient


DEFAULT_ENV_FILE = REPO_ROOT / ".env.local"
DEFAULT_STAGING_ROOT = REPO_ROOT / "structured_json_validation" / "staging"
STAGING_TABLE = "m83513_staged_payload_rows"
PAYLOAD_TABLES = [
    "base_configurations",
    "hns_wire_options",
    "torque_values",
    "text_chunks",
    "extraction_runs",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load local rebuild payloads into the DB staging payload table.")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--table", default=STAGING_TABLE)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--apply", action="store_true", help="Write rows. Without this flag, only print a summary.")
    return parser.parse_args()


def client_from_env(env_file: Path) -> RestClient:
    env = load_env_file(env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    return RestClient(supabase_url, server_key)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def staged_dir_for_run(staging_root: Path, run_id: str) -> Path:
    run_dir = staging_root / run_id
    staged_dir = run_dir / "staged"
    if not staged_dir.exists():
        raise RuntimeError(f"Staged directory does not exist: {staged_dir}")
    return staged_dir


def slash_for_row(table_name: str, row: dict[str, Any], base_id_to_slash: dict[str, str]) -> str | None:
    if row.get("slash_sheet"):
        return row["slash_sheet"]
    if table_name == "hns_wire_options":
        return base_id_to_slash.get(row.get("base_config_id"))
    return None


def build_stage_rows(staging_root: Path, run_id: str) -> list[dict[str, Any]]:
    staged_dir = staged_dir_for_run(staging_root, run_id)
    run_dir = staged_dir.parent
    base_rows = read_json(staged_dir / "base_configurations.json")
    base_id_to_slash = {
        row["id"]: row["slash_sheet"]
        for row in base_rows
        if row.get("id") and row.get("slash_sheet")
    }

    stage_rows: list[dict[str, Any]] = []
    for table_name in PAYLOAD_TABLES:
        rows = read_json(staged_dir / f"{table_name}.json")
        for row in rows:
            stage_rows.append(
                {
                    "run_id": run_id,
                    "table_name": table_name,
                    "slash_sheet": slash_for_row(table_name, row, base_id_to_slash),
                    "row_data": row,
                }
            )

    torque_path = staged_dir / "torque_resolution.json"
    if torque_path.exists():
        torque_resolution = read_json(torque_path)
        effective = torque_resolution.get("effective_facts_by_slash", {})
        for slash_sheet, row in effective.items():
            stage_rows.append(
                {
                    "run_id": run_id,
                    "table_name": "torque_resolution_effective_facts",
                    "slash_sheet": slash_sheet,
                    "row_data": row,
                }
            )

    report_path = run_dir / "rebuild_diff_report.json"
    if report_path.exists():
        report = read_json(report_path)
        for row in report.get("edge_checks", []):
            stage_rows.append(
                {
                    "run_id": run_id,
                    "table_name": "rebuild_edge_checks",
                    "slash_sheet": None,
                    "row_data": row,
                }
            )

    return stage_rows


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_table = Counter(row["table_name"] for row in rows)
    by_table_slash = Counter(
        (row["table_name"], row.get("slash_sheet") or "")
        for row in rows
    )
    return {
        "total_rows": len(rows),
        "by_table": dict(sorted(by_table.items())),
        "by_table_slash": {
            f"{table_name}:{slash_sheet or 'none'}": count
            for (table_name, slash_sheet), count in sorted(by_table_slash.items())
        },
    }


def delete_existing_run(client: RestClient, table_name: str, run_id: str) -> None:
    client.request("DELETE", table_name, query=[("run_id", f"eq.{run_id}")], prefer="return=minimal")


def insert_batches(client: RestClient, table_name: str, rows: list[dict[str, Any]], batch_size: int) -> None:
    for offset in range(0, len(rows), batch_size):
        client.insert(table_name, rows[offset : offset + batch_size])


def main() -> int:
    args = parse_args()
    rows = build_stage_rows(args.staging_root, args.run_id)
    summary = summarize_rows(rows)
    run_dir = args.staging_root / args.run_id
    report_path = run_dir / "staged_payload_db_load_report.json"

    if not args.apply:
        print(json.dumps(summary, indent=2, sort_keys=True))
        print("Dry run only. Re-run with --apply to write staged payload rows.")
        return 0

    client = client_from_env(args.env_file)
    delete_existing_run(client, args.table, args.run_id)
    insert_batches(client, args.table, rows, args.batch_size)
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Loaded {summary['total_rows']} staged rows into {args.table} for run {args.run_id}.")
    print(f"Wrote staged load report to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
