#!/usr/bin/env python3
"""Load MIL-DTL-83513 torque_values from extraction JSON without reloading connector rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pdf_storage.sync_83513_to_supabase import (
    create_supabase_client,
    get_server_key,
    load_env_file,
    require_env,
)
from postgresql.m83513_load_extraction import load_json, slash_sheet_value, torque_rows


DEFAULT_OUTPUTS_DIR = REPO_ROOT / "structured_json_validation" / "outputs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load torque_values from MIL-DTL-83513 extraction JSON outputs.")
    parser.add_argument("--input-json", type=Path, help="Single extraction JSON file to load.")
    parser.add_argument("--outputs-dir", type=Path, default=DEFAULT_OUTPUTS_DIR, help="Directory of extraction JSON files.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env.local")
    parser.add_argument("--apply", action="store_true", help="Write torque rows to Supabase.")
    return parser.parse_args()


def input_paths(args: argparse.Namespace) -> list[Path]:
    if args.input_json:
        return [args.input_json]
    return sorted(args.outputs_dir.glob("m83513_*_extraction_output.json"))


def load_torque_rows(args: argparse.Namespace) -> int:
    env = load_env_file(args.env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    supabase = create_supabase_client(supabase_url, server_key)

    total_rows = 0
    for path in input_paths(args):
        extraction = load_json(path)
        source = extraction["source"]
        slash_sheet = slash_sheet_value(extraction)
        rows = torque_rows(extraction)
        total_rows += len(rows)
        print(f"{path.name}: {len(rows)} torque rows")
        if not args.apply:
            continue
        supabase.table("torque_values").delete().eq("spec_sheet", source["spec_sheet"]).eq("slash_sheet", slash_sheet).execute()
        if rows:
            supabase.table("torque_values").upsert(rows, on_conflict="torque_key").execute()
    return total_rows


def main() -> int:
    args = parse_args()
    try:
        total_rows = load_torque_rows(args)
    except Exception as exc:  # pragma: no cover
        print(f"Torque loader failed: {exc}", file=sys.stderr)
        return 1

    print(f"Total torque rows: {total_rows}")
    if not args.apply:
        print("Dry run only. Re-run with --apply to write to Supabase.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
