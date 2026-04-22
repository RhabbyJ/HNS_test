#!/usr/bin/env python3
"""Promote a green MIL-DTL-83513 rebuild output set into the live derived tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pdf_storage.sync_83513_to_supabase import get_server_key, load_env_file, require_env
from postgresql.backfill_torque_profile_model import (
    RestClient,
    apply_backfill,
    build_evidence,
    build_mappings,
    build_profile_values,
    build_profiles,
    build_status_rows,
    fetch_legacy_rows,
    load_documents_from_database,
    profile_ids_by_code,
)
from postgresql.m83513_load_extraction import (
    apply_rows,
    base_rows_for_extraction,
    load_json,
    slash_sheet_value,
    wire_rows_for_base,
)


DEFAULT_ENV_FILE = REPO_ROOT / ".env.local"
DEFAULT_STAGING_ROOT = REPO_ROOT / "structured_json_validation" / "staging"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote a staged MIL-DTL-83513 rebuild to live tables.")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--apply", action="store_true", help="Write live tables. Without this flag, dry-run only.")
    return parser.parse_args()


def slash_sort_key(slash_sheet: str) -> tuple[int, int]:
    if slash_sheet == "base":
        return (0, 0)
    return (1, int(slash_sheet))


def run_dir(staging_root: Path, run_id: str) -> Path:
    path = staging_root / run_id
    if not path.exists():
        raise RuntimeError(f"Rebuild run directory does not exist: {path}")
    return path


def outputs_dir(staging_root: Path, run_id: str) -> Path:
    path = run_dir(staging_root, run_id) / "staged" / "outputs"
    if not path.exists():
        raise RuntimeError(f"Staged outputs directory does not exist: {path}")
    return path


def load_rebuild_report(staging_root: Path, run_id: str) -> dict[str, Any]:
    report_path = run_dir(staging_root, run_id) / "rebuild_diff_report.json"
    if not report_path.exists():
        raise RuntimeError(f"Rebuild report does not exist: {report_path}")
    return json.loads(report_path.read_text(encoding="utf-8"))


def assert_green_rebuild(report: dict[str, Any]) -> None:
    failed = [check for check in report.get("edge_checks", []) if check.get("status") != "pass"]
    if failed:
        names = ", ".join(check.get("name", "unnamed") for check in failed)
        raise RuntimeError(f"Refusing to promote rebuild with failed edge checks: {names}")
    if report.get("mode") != "fresh_extraction":
        raise RuntimeError(f"Refusing to promote non-fresh rebuild mode: {report.get('mode')}")


def extraction_paths(staging_root: Path, run_id: str) -> list[Path]:
    paths = sorted(outputs_dir(staging_root, run_id).glob("m83513_*_extraction_output.json"))
    if not paths:
        raise RuntimeError("No staged extraction outputs found.")

    def path_key(path: Path) -> tuple[int, int]:
        extraction = load_json(path)
        return slash_sort_key(slash_sheet_value(extraction))

    return sorted(paths, key=path_key)


def summarize_extraction(path: Path) -> dict[str, Any]:
    extraction = load_json(path)
    base_rows = base_rows_for_extraction(extraction)
    wire_rows = sum(
        len(wire_rows_for_base(extraction, f"dry-run-base-{index}"))
        for index, _ in enumerate(base_rows)
    )
    return {
        "path": str(path),
        "spec_sheet": extraction["source"]["spec_sheet"],
        "slash_sheet": slash_sheet_value(extraction),
        "source_sha256": extraction["source"].get("source_sha256"),
        "base_rows": len(base_rows),
        "wire_rows": wire_rows,
        "torque_rows": len(extraction.get("torque_values", [])),
        "text_chunks": len(extraction.get("chunks", [])),
        "fallback_flags": extraction.get("fallback_flags", []),
    }


def client_from_env(env_file: Path) -> RestClient:
    env = load_env_file(env_file)
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    return RestClient(require_env(env, "SUPABASE_URL"), server_key)


def rebuild_torque_profiles(env_file: Path) -> dict[str, int]:
    client = client_from_env(env_file)
    documents = load_documents_from_database(client)
    legacy_rows = fetch_legacy_rows(client)
    profiles = build_profiles(legacy_rows)
    client.upsert("torque_profiles", profiles, "profile_code")
    profile_ids = profile_ids_by_code(client)
    profile_values = build_profile_values(legacy_rows, profile_ids)
    profile_value_counts = {
        profile_code: sum(1 for row in profile_values if row["profile_id"] == profile_id)
        for profile_code, profile_id in profile_ids.items()
    }
    statuses = build_status_rows(documents, legacy_rows, profile_value_counts)
    mappings = build_mappings(documents, profile_ids)
    evidence = build_evidence(legacy_rows, profile_ids)
    apply_backfill(client, documents, profiles, profile_values, statuses, mappings, evidence)
    return {
        "documents": len(documents),
        "legacy_torque_rows": len(legacy_rows),
        "profiles": len(profiles),
        "profile_values": len(profile_values),
        "document_profile_mappings": len(mappings),
        "source_evidence_rows": len(evidence),
    }


def main() -> int:
    args = parse_args()
    report = load_rebuild_report(args.staging_root, args.run_id)
    assert_green_rebuild(report)
    paths = extraction_paths(args.staging_root, args.run_id)
    summaries = [summarize_extraction(path) for path in paths]
    output_report = {
        "run_id": args.run_id,
        "apply": args.apply,
        "documents": len(paths),
        "extractions": summaries,
        "torque_backfill": None,
    }

    if not args.apply:
        print(json.dumps(output_report, indent=2, sort_keys=True))
        print("Dry run only. Re-run with --apply to promote staged outputs.")
        return 0

    for path in paths:
        apply_rows(load_json(path), args.env_file)
    output_report["torque_backfill"] = rebuild_torque_profiles(args.env_file)
    report_path = run_dir(args.staging_root, args.run_id) / "live_promote_report.json"
    report_path.write_text(json.dumps(output_report, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Promoted {len(paths)} staged extraction outputs for run {args.run_id}.")
    print(f"Wrote live promote report to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
