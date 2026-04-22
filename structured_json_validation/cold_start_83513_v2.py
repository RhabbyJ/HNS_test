#!/usr/bin/env python3
"""Cold-start platform v2 rebuild for MIL-DTL-83513 from current Storage PDFs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from assist.assist_83513_common import (
    build_output_name,
    storage_document_label,
    sort_document_key,
    utc_timestamp,
)
from assist.discover_83513 import discover_documents
from pdf_storage.sync_83513_to_supabase import (
    create_supabase_client,
    get_server_key,
    load_env_file,
    optional_env,
    require_env,
)
from structured_json_validation.build_83513_v2_release import (
    build_release_payload,
    load_extractions,
    summarize_payload,
    write_payloads,
)
from structured_json_validation.load_platform_v2_release import (
    client_from_env as v2_client_from_env,
    load_batches,
    load_payloads,
)


DEFAULT_ENV_FILE = REPO_ROOT / ".env.local"
DEFAULT_STAGING_ROOT = REPO_ROOT / "structured_json_validation" / "staging"
EXPECTED_INSERTS = {
    "02": ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
    "04": ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
}
EXPECTED_CONFIG_COUNTS = {
    "01": 48,
    "02": 60,
    "03": 48,
    "04": 60,
    "06": 7,
    "07": 7,
    "08": 7,
    "09": 7,
}
EXPECTED_WIRE_COUNTS = {
    "03": 2784,
    "04": 3480,
    "08": 154,
    "09": 406,
}
CLASS_M_FINISHES = ["A", "C", "K", "N", "P", "T"]
CLASS_P_SLASHES = ["06", "07", "08", "09"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a V2 cold-start rebuild from latest MIL-DTL-83513 PDFs in Supabase Storage."
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--release-name", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--skip-extract", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Load and publish V2 rows after gates pass.")
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    return parser.parse_args()


def utc_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def normalize_slash(document_key: str) -> str:
    return "base" if document_key == "base" else f"{int(document_key):02d}"


def spec_sheet_for_document(document: dict[str, Any]) -> str:
    revision = document.get("current_doc_revision") or ""
    if document["document_key"] == "base":
        return f"MIL-DTL-83513{revision}"
    return f"MIL-DTL-83513/{int(document['document_key'])}{revision}"


def output_path_for_manifest(outputs_dir: Path, row: dict[str, Any]) -> Path:
    label = "base" if row["slash_sheet"] == "base" else row["slash_sheet"]
    return outputs_dir / f"m83513_{label}_extraction_output.json"


def source_manifest(env_file: Path, bucket_name: str | None) -> list[dict[str, Any]]:
    env = load_env_file(env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    bucket = bucket_name or require_env(env, "SUPABASE_STORAGE_BUCKET")
    storage_prefix = optional_env(env, "SUPABASE_STORAGE_PREFIX", "mil-dtl-83513")
    search_term = optional_env(env, "ASSIST_SEARCH_TERM", "MIL-DTL-83513")

    supabase = create_supabase_client(supabase_url, server_key)
    catalog = discover_documents(search_term)
    documents = sorted(catalog["documents"], key=lambda row: sort_document_key(row["document_key"]))
    manifest: list[dict[str, Any]] = []
    for document in documents:
        document_key = document["document_key"]
        revision = document.get("current_doc_revision")
        if not revision:
            raise RuntimeError(f"Discovered document without current revision: {document}")
        storage_label = storage_document_label(document_key)
        storage_path = f"{storage_prefix}/{storage_label}/{build_output_name(document_key, revision)}"
        payload = supabase.storage.from_(bucket).download(storage_path)
        checksum = hashlib.sha256(payload).hexdigest()
        manifest.append(
            {
                **document,
                "slash_sheet": normalize_slash(document_key),
                "spec_sheet": spec_sheet_for_document(document),
                "bucket_name": bucket,
                "storage_path": storage_path,
                "checksum": checksum,
                "file_size_bytes": len(payload),
                "selected_at_utc": utc_timestamp(),
            }
        )
    return manifest


def run_extractor(manifest: list[dict[str, Any]], outputs_dir: Path, env_file: Path, bucket: str) -> list[dict[str, Any]]:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for index, row in enumerate(manifest, start=1):
        output_json = output_path_for_manifest(outputs_dir, row)
        command = [
            sys.executable,
            str(REPO_ROOT / "m83513_extraction_engine.py"),
            "--env-file",
            str(env_file),
            "--bucket",
            bucket,
            "--storage-path",
            row["storage_path"],
            "--document-key",
            row["document_key"],
            "--spec-sheet",
            row["spec_sheet"],
            "--title",
            row["title"],
            "--source-url",
            row["details_url"],
            "--output-json",
            str(output_json),
        ]
        env = os.environ.copy()
        python_path_parts = [str(REPO_ROOT)]
        if env.get("PYTHONPATH"):
            python_path_parts.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(python_path_parts)
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=240,
        )
        results.append(
            {
                "index": index,
                "total": len(manifest),
                "slash_sheet": row["slash_sheet"],
                "storage_path": row["storage_path"],
                "output_json": str(output_json),
                "returncode": completed.returncode,
                "stdout": completed.stdout.strip(),
                "stderr": completed.stderr.strip(),
            }
        )
    return results


def extraction_by_slash(extractions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    from postgresql.m83513_load_extraction import slash_sheet_value

    return {slash_sheet_value(extraction): extraction for extraction in extractions}


def manifest_by_slash(manifest: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["slash_sheet"]: row for row in manifest}


def config_rows_by_slash(payload: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {}
    for row in payload["catalog.configurations"]:
        rows.setdefault(row["slash_sheet"], []).append(row)
    return rows


def wire_counts_by_slash(payload: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    config_slash_by_id = {
        row["id"]: row["slash_sheet"]
        for row in payload["catalog.configurations"]
    }
    counts = {slash: 0 for slash in EXPECTED_WIRE_COUNTS}
    for row in payload["catalog.wire_options"]:
        slash = config_slash_by_id[row["configuration_id"]]
        counts[slash] = counts.get(slash, 0) + 1
    return counts


def gate(name: str, passed: bool, details: Any = None) -> dict[str, Any]:
    return {"name": name, "status": "pass" if passed else "fail", "details": details}


def acceptance_gates(
    manifest: list[dict[str, Any]],
    extraction_results: list[dict[str, Any]],
    extractions: list[dict[str, Any]],
    payload: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    manifest_map = manifest_by_slash(manifest)
    extraction_map = extraction_by_slash(extractions)
    config_rows = config_rows_by_slash(payload)
    wire_counts = wire_counts_by_slash(payload)
    source_02 = manifest_map.get("02", {})

    gates.append(gate("all 34 docs selected", len(manifest) == 34, {"count": len(manifest)}))
    gates.append(gate("all source hashes logged", all(row.get("checksum") for row in manifest)))
    gates.append(
        gate(
            "latest /02 resolves to /2H Amendment 4 source",
            source_02.get("spec_sheet") == "MIL-DTL-83513/2H"
            and str(source_02.get("doc_id", "")).upper().startswith("MIL-DTL-83513/2H(4)"),
            source_02,
        )
    )
    failed_extractions = [row for row in extraction_results if row["returncode"] != 0]
    gates.append(gate("no failed extractions", not failed_extractions, failed_extractions))

    unexpected_flags = []
    for slash_sheet, extraction in extraction_map.items():
        flags = extraction.get("fallback_flags", [])
        for flag in flags:
            if flag in {"unexpected_insert_arrangements", "unexpected_shell_finish_codes"}:
                unexpected_flags.append({"slash_sheet": slash_sheet, "flag": flag})
    gates.append(gate("no unexpected insert/finish validation failures", not unexpected_flags, unexpected_flags))

    hash_mismatches = []
    for slash_sheet, extraction in extraction_map.items():
        expected_hash = manifest_map[slash_sheet]["checksum"]
        observed_hash = extraction["source"].get("source_sha256")
        if observed_hash != expected_hash:
            hash_mismatches.append({"slash_sheet": slash_sheet, "expected": expected_hash, "observed": observed_hash})
    gates.append(gate("no stale-source warnings", not hash_mismatches, hash_mismatches))

    for slash_sheet, expected_count in EXPECTED_CONFIG_COUNTS.items():
        gates.append(
            gate(
                f"/{slash_sheet} configuration count = {expected_count}",
                len(config_rows.get(slash_sheet, [])) == expected_count,
                {"actual": len(config_rows.get(slash_sheet, []))},
            )
        )

    for slash_sheet, expected_inserts in EXPECTED_INSERTS.items():
        actual = sorted({row["insert_arrangement_code"] for row in config_rows.get(slash_sheet, [])})
        gates.append(gate(f"/{slash_sheet} inserts match expected set", actual == expected_inserts, actual))

    for slash_sheet in CLASS_P_SLASHES:
        actual = sorted({row["shell_finish_code"] for row in config_rows.get(slash_sheet, [])})
        gates.append(gate(f"/{slash_sheet} finish codes are NULL only", actual == [None], actual))

    for slash_sheet in ["01", "02", "03", "04"]:
        actual = sorted({row["shell_finish_code"] for row in config_rows.get(slash_sheet, []) if row["shell_finish_code"]})
        gates.append(gate(f"/{slash_sheet} class-M finishes are A,C,K,N,P,T", actual == CLASS_M_FINISHES, actual))

    for slash_sheet, expected_count in EXPECTED_WIRE_COUNTS.items():
        gates.append(
            gate(
                f"/{slash_sheet} wire count = {expected_count}",
                wire_counts.get(slash_sheet, 0) == expected_count,
                {"actual": wire_counts.get(slash_sheet, 0)},
            )
        )

    profile_by_code = {row["profile_code"]: row for row in payload["catalog.torque_profiles"]}
    values_by_profile = {}
    for row in payload["catalog.torque_profile_values"]:
        values_by_profile.setdefault(row["profile_id"], []).append(row)
    profile_05 = profile_by_code.get("m83513_05_main")
    values_05 = values_by_profile.get(profile_05["id"], []) if profile_05 else []
    links_by_slash = {}
    doc_slash_by_id = {row["id"]: row["slash_sheet"] for row in payload["ingest.documents"]}
    for row in payload["catalog.document_profile_links"]:
        links_by_slash[doc_slash_by_id[row["document_id"]]] = row
    gates.append(
        gate(
            "/05 owns canonical numeric profile",
            bool(profile_05)
            and profile_05["profile_kind"] == "canonical"
            and len(values_05) == 6
            and links_by_slash.get("05", {}).get("mapping_type") == "uses_profile",
            {"profile": profile_05, "value_count": len(values_05), "link": links_by_slash.get("05")},
        )
    )
    for slash_sheet in ["02", "04"]:
        link = links_by_slash.get(slash_sheet, {})
        gates.append(
            gate(
                f"/{slash_sheet} inherits /05",
                link.get("profile_id") == (profile_05 or {}).get("id")
                and link.get("mapping_type") == "references_profile"
                and link.get("values_inherited")
                and link.get("values_verified"),
                link,
            )
        )
    torque_pairs = {
        (row.get("fastener_thread"), row.get("arrangement_scope"))
        for row in values_05
    }
    expected_pairs = {
        ("#2-56", "Metal shell"),
        ("#2-56", "Plastic shell"),
        ("#4-40", "Metal shell"),
    }
    gates.append(
        gate(
            "/05 torque has #2-56/#4-40 metal/plastic coverage",
            expected_pairs.issubset(torque_pairs),
            sorted(torque_pairs),
        )
    )
    return gates


def main() -> int:
    args = parse_args()
    run_id = args.run_id or utc_run_id()
    release_name = args.release_name or f"83513-v2-cold-{run_id}"
    run_dir = args.staging_root / run_id
    manifest_path = run_dir / "source_manifest.json"
    outputs_dir = run_dir / "fresh_extraction_outputs"
    payload_dir = run_dir / "v2_payloads"
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    manifest = source_manifest(args.env_file, args.bucket)
    write_json(manifest_path, {"generated_at_utc": utc_timestamp(), "documents": manifest})
    bucket = manifest[0]["bucket_name"] if manifest else args.bucket

    if args.skip_extract:
        extraction_results: list[dict[str, Any]] = []
    else:
        extraction_results = run_extractor(manifest, outputs_dir, args.env_file, bucket)
        write_json(logs_dir / "extraction_results.json", extraction_results)
        if args.delay_seconds > 0:
            time.sleep(args.delay_seconds)

    extractions = load_extractions(outputs_dir)
    payload = build_release_payload(
        extractions,
        release_name=release_name,
        created_from_run_id=run_id,
        release_status="published",
        metadata_by_slash=manifest_by_slash(manifest),
    )
    write_payloads(payload_dir, payload)
    gates = acceptance_gates(manifest, extraction_results, extractions, payload)
    failed = [row for row in gates if row["status"] != "pass"]
    report = {
        "run_id": run_id,
        "release_name": release_name,
        "created_at_utc": utc_timestamp(),
        "source_manifest": str(manifest_path),
        "outputs_dir": str(outputs_dir),
        "payload_dir": str(payload_dir),
        "payload_summary": summarize_payload(payload),
        "gates": gates,
        "failed_gate_count": len(failed),
        "loaded_to_v2": False,
    }
    if failed:
        write_json(run_dir / "cold_start_v2_report.json", report)
        print(f"Cold-start V2 gates failed: {len(failed)}")
        print(f"Report: {run_dir / 'cold_start_v2_report.json'}")
        return 2

    if args.apply:
        client = v2_client_from_env(args.env_file)
        payloads = load_payloads(payload_dir)
        load_batches(client, payloads, batch_size=500)
        report["loaded_to_v2"] = True

    write_json(run_dir / "cold_start_v2_report.json", report)
    print(f"Cold-start V2 gates passed: {len(gates)}")
    print(f"Release: {release_name}")
    print(f"Payload: {payload_dir}")
    print(f"Report: {run_dir / 'cold_start_v2_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
