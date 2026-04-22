#!/usr/bin/env python3
"""Non-destructive full rebuild and diff workflow for MIL-DTL-83513 extraction data."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from hashlib import sha1
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pdf_storage.sync_83513_to_supabase import get_server_key, load_env_file, require_env
from postgresql.backfill_torque_profile_model import (
    PROFILE_05,
    PROFILE_08,
    PROFILE_PCB,
    RestClient,
    build_mappings,
    build_profile_values,
    build_profiles,
    build_status_rows,
)
from postgresql.m83513_load_extraction import (
    base_rows_for_extraction,
    chunk_rows,
    extraction_run_row,
    slash_sheet_value,
    torque_rows,
    wire_rows_for_base,
)


DEFAULT_ENV_FILE = REPO_ROOT / ".env.local"
DEFAULT_OUTPUTS_DIR = REPO_ROOT / "structured_json_validation" / "outputs"
DEFAULT_STAGING_ROOT = REPO_ROOT / "structured_json_validation" / "staging"
SNAPSHOT_TABLES = {
    "pdf_objects": {
        "select": "*",
        "filters": [("spec_family", "eq.MIL-DTL-83513")],
        "order": "sort_order.asc,slash_sheet.asc",
    },
    "base_configurations": {
        "select": "*",
        "filters": [("spec_family", "eq.83513")],
        "order": "slash_sheet.asc,cavity_count.asc,shell_finish_code.asc",
    },
    "hns_wire_options": {
        "select": "*",
        "filters": [],
        "order": "base_config_id.asc,wire_type_code.asc,id.asc",
    },
    "torque_values": {
        "select": "*",
        "filters": [("spec_family", "eq.83513")],
        "order": "slash_sheet.asc,context.asc,fastener_thread.asc",
    },
    "document_torque_status": {
        "select": "*",
        "filters": [("spec_family", "eq.83513")],
        "order": "slash_sheet.asc",
    },
    "torque_profiles": {
        "select": "*",
        "filters": [],
        "order": "profile_code.asc",
    },
    "torque_profile_values": {
        "select": "*",
        "filters": [],
        "order": "context.asc,fastener_thread.asc,arrangement_scope.asc",
    },
    "document_torque_profile_map": {
        "select": "*",
        "filters": [],
        "order": "spec_sheet.asc",
    },
    "torque_source_evidence": {
        "select": "*",
        "filters": [],
        "order": "slash_sheet.asc,source_page.asc",
    },
    "text_chunks": {
        "select": "*",
        "filters": [("spec_family", "eq.83513")],
        "order": "slash_sheet.asc,page_number.asc,chunk_index.asc",
    },
    "extraction_runs": {
        "select": "*",
        "filters": [("spec_family", "eq.83513")],
        "order": "slash_sheet.asc,created_at.desc",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Snapshot live MIL-DTL-83513 extraction tables, regenerate or reuse extraction JSON, "
            "build staged payloads locally, and diff staged facts against live data."
        )
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--bucket", default="mil-spec-pdfs")
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument("--outputs-dir", type=Path, default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Reuse an existing outputs directory instead of regenerating JSON from PDFs.",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        help="Optional slash sheets to rebuild, e.g. base 01 02 04. Mainly for smoke tests.",
    )
    parser.add_argument("--limit", type=int, help="Optional document limit for smoke tests.")
    parser.add_argument(
        "--fail-on-checks",
        action="store_true",
        help="Exit nonzero when audit edge checks fail. Report generation still succeeds.",
    )
    return parser.parse_args()


def utc_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def normalize_slash_sheet(value: str) -> str:
    if value == "base":
        return "base"
    return value.zfill(2)


def document_key_for_slash(slash_sheet: str) -> str:
    return "base" if slash_sheet == "base" else str(int(slash_sheet))


def spec_sheet_for_document(document: dict[str, Any]) -> str:
    slash_sheet = normalize_slash_sheet(document["slash_sheet"])
    revision = document.get("revision_letter") or ""
    if slash_sheet == "base":
        return f"MIL-DTL-83513{revision}"
    return f"MIL-DTL-83513/{int(slash_sheet)}{revision}"


def output_path_for_document(outputs_dir: Path, document: dict[str, Any]) -> Path:
    slash_sheet = normalize_slash_sheet(document["slash_sheet"])
    label = "base" if slash_sheet == "base" else slash_sheet
    return outputs_dir / f"m83513_{label}_extraction_output.json"


def selected_documents(documents: list[dict[str, Any]], only: list[str] | None, limit: int | None) -> list[dict[str, Any]]:
    selected = documents
    if only:
        wanted = {normalize_slash_sheet(value) for value in only}
        selected = [document for document in selected if normalize_slash_sheet(document["slash_sheet"]) in wanted]
    if limit is not None:
        selected = selected[:limit]
    return selected


def client_from_env(env_file: Path) -> RestClient:
    env = load_env_file(env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    return RestClient(supabase_url, server_key)


def fetch_all(
    client: RestClient,
    table: str,
    *,
    select: str,
    filters: list[tuple[str, str]] | None = None,
    order: str | None = None,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    filters = filters or []
    while True:
        query = [("select", select), *filters, ("limit", str(page_size)), ("offset", str(offset))]
        if order:
            query.append(("order", order))
        page = client.fetch(table, query=query)
        rows.extend(page)
        if len(page) < page_size:
            return rows
        offset += page_size


def fetch_documents(client: RestClient) -> list[dict[str, Any]]:
    rows = fetch_all(
        client,
        "pdf_objects",
        select=(
            "slash_sheet,sort_order,revision_letter,title,storage_path,status,source_url,"
            "source_doc_id,document_date,checksum,file_size_bytes,bucket_name"
        ),
        filters=[("spec_family", "eq.MIL-DTL-83513"), ("status", "eq.active")],
        order="sort_order.asc,slash_sheet.asc",
    )
    return sorted(rows, key=lambda row: row.get("sort_order") or 0)


def snapshot_live_tables(client: RestClient, snapshot_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, config in SNAPSHOT_TABLES.items():
        rows = fetch_all(
            client,
            table,
            select=config["select"],
            filters=config["filters"],
            order=config["order"],
        )
        write_json(snapshot_dir / f"{table}.json", rows)
        counts[table] = len(rows)
    return counts


def copy_current_outputs(source_dir: Path, snapshot_dir: Path) -> int:
    target_dir = snapshot_dir / "current_extraction_outputs"
    target_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for path in sorted(source_dir.glob("m83513_*_extraction_output.json")):
        shutil.copy2(path, target_dir / path.name)
        count += 1
    return count


def run_extractor_for_document(
    document: dict[str, Any],
    output_path: Path,
    *,
    env_file: Path,
    bucket: str,
) -> dict[str, Any]:
    slash_sheet = normalize_slash_sheet(document["slash_sheet"])
    command = [
        sys.executable,
        str(REPO_ROOT / "m83513_extraction_engine.py"),
        "--env-file",
        str(env_file),
        "--bucket",
        bucket,
        "--storage-path",
        document["storage_path"],
        "--document-key",
        document_key_for_slash(slash_sheet),
        "--spec-sheet",
        spec_sheet_for_document(document),
        "--title",
        document["title"],
        "--source-url",
        document["source_url"],
        "--output-json",
        str(output_path),
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
    return {
        "slash_sheet": slash_sheet,
        "output_json": str(output_path),
        "storage_path": document.get("storage_path"),
        "storage_checksum": document.get("checksum"),
        "storage_file_size_bytes": document.get("file_size_bytes"),
        "source_doc_id": document.get("source_doc_id"),
        "document_date": document.get("document_date"),
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "command": command,
    }


def regenerate_outputs(
    documents: list[dict[str, Any]],
    outputs_dir: Path,
    *,
    env_file: Path,
    bucket: str,
    logs_dir: Path,
) -> list[dict[str, Any]]:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for index, document in enumerate(documents, start=1):
        output_path = output_path_for_document(outputs_dir, document)
        result = run_extractor_for_document(document, output_path, env_file=env_file, bucket=bucket)
        result["index"] = index
        result["total"] = len(documents)
        results.append(result)
        log_path = logs_dir / f"extract_{result['slash_sheet']}.log"
        log_path.write_text(
            "\n".join(
                [
                    f"returncode={result['returncode']}",
                    "stdout:",
                    result["stdout"],
                    "stderr:",
                    result["stderr"],
                ]
            ),
            encoding="utf-8",
        )
    return results


def extraction_paths(outputs_dir: Path) -> list[Path]:
    return sorted(outputs_dir.glob("m83513_*_extraction_output.json"))


def load_extractions(outputs_dir: Path, selected_slashes: set[str] | None = None) -> list[dict[str, Any]]:
    extractions: list[dict[str, Any]] = []
    for path in extraction_paths(outputs_dir):
        extraction = json.loads(path.read_text(encoding="utf-8"))
        if selected_slashes and slash_sheet_value(extraction) not in selected_slashes:
            continue
        extractions.append(extraction)
    return extractions


def staged_base_id(row: dict[str, Any]) -> str:
    key_parts = [
        row.get("spec_sheet"),
        row.get("slash_sheet"),
        str(row.get("cavity_count") or ""),
        str(row.get("shell_size_letter") or ""),
        str(row.get("shell_finish_code") or ""),
        str(row.get("insert_arrangement_ref") or ""),
        str(row.get("example_full_pin") or ""),
    ]
    digest = sha1("|".join(key_parts).encode("utf-8")).hexdigest()[:24]
    return f"staged-base-{digest}"


def build_staged_payloads(extractions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    payloads: dict[str, list[dict[str, Any]]] = {
        "base_configurations": [],
        "hns_wire_options": [],
        "torque_values": [],
        "text_chunks": [],
        "extraction_runs": [],
    }
    for extraction in extractions:
        base_rows = base_rows_for_extraction(extraction)
        for base_row in base_rows:
            staged_row = {**base_row, "id": staged_base_id(base_row)}
            payloads["base_configurations"].append(staged_row)
            payloads["hns_wire_options"].extend(wire_rows_for_base(extraction, staged_row["id"]))
        payloads["torque_values"].extend(torque_rows(extraction))
        payloads["text_chunks"].extend(chunk_rows(extraction))
        payloads["extraction_runs"].append(extraction_run_row(extraction))
    return payloads


def connector_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("connector_type") not in {"GENERAL_SPECIFICATION", "MOUNTING_HARDWARE"}
    ]


def has_hardware_options(row: dict[str, Any]) -> bool:
    extra_data = row.get("extra_data")
    if row.get("mounting_hardware_ref"):
        return True
    if isinstance(extra_data, dict):
        if extra_data.get("hardware_options"):
            return True
        details = extra_data.get("mounting_hardware_details")
        if isinstance(details, dict) and details.get("hardware_options"):
            return True
    return False


def missing_connector_fields(rows: list[dict[str, Any]], wire_counts_by_slash: Counter[str]) -> dict[str, int]:
    missing: Counter[str] = Counter()
    for row in connector_rows(rows):
        slash_sheet = row["slash_sheet"]
        checks = {
            "PN": bool(row.get("example_full_pin")),
            "Description": bool(row.get("description")),
            "Cavity/Pin Count": row.get("cavity_count") is not None,
            "Pin/Socket": bool(row.get("contact_type")),
            "Plug/Receptacle": bool(row.get("gender")),
            "Proper Hardware Options": has_hardware_options(row),
            "Mating Connector": bool(row.get("mates_with")),
            "Shell Material": bool(row.get("shell_material")),
        }
        for field_name, present in checks.items():
            if not present:
                missing[field_name] += 1

        connector_type = row.get("connector_type") or ""
        extra_data = row.get("extra_data") if isinstance(row.get("extra_data"), dict) else {}
        is_crimp = "CRIMP" in connector_type.upper()
        has_wire_limit = bool(extra_data.get("wire_constraints"))
        if is_crimp and wire_counts_by_slash[slash_sheet] == 0:
            missing["Wire Range"] += 1
        if not is_crimp and not has_wire_limit:
            missing["Wire Range"] += 1
    return dict(sorted(missing.items()))


def summarize_base_rows(rows: list[dict[str, Any]], wire_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    base_id_to_slash = {row["id"]: row["slash_sheet"] for row in rows if row.get("id")}
    wire_counts_by_slash: Counter[str] = Counter()
    for row in wire_rows:
        slash_sheet = base_id_to_slash.get(row.get("base_config_id"))
        if slash_sheet:
            wire_counts_by_slash[slash_sheet] += 1

    summary: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "base_rows": 0,
        "connector_rows": 0,
        "wire_rows": 0,
        "insert_arrangements": set(),
        "shell_finish_codes": set(),
        "example_pin_missing": 0,
    })
    for row in rows:
        slash_sheet = row["slash_sheet"]
        entry = summary[slash_sheet]
        entry["base_rows"] += 1
        if row.get("connector_type") not in {"GENERAL_SPECIFICATION", "MOUNTING_HARDWARE"}:
            entry["connector_rows"] += 1
        if row.get("insert_arrangement_ref"):
            entry["insert_arrangements"].add(row["insert_arrangement_ref"])
        if row.get("shell_finish_code") is not None:
            entry["shell_finish_codes"].add(row["shell_finish_code"])
        if row.get("connector_type") != "GENERAL_SPECIFICATION" and not row.get("example_full_pin"):
            entry["example_pin_missing"] += 1

    missing = missing_connector_fields(rows, wire_counts_by_slash)
    for slash_sheet, count in wire_counts_by_slash.items():
        summary[slash_sheet]["wire_rows"] = count

    normalized = {}
    for slash_sheet, entry in summary.items():
        normalized[slash_sheet] = {
            **entry,
            "insert_arrangements": sorted(entry["insert_arrangements"]),
            "shell_finish_codes": sorted(entry["shell_finish_codes"]),
        }
    return {
        "by_slash": dict(sorted(normalized.items())),
        "missing_connector_fields": missing,
    }


def summarize_torque_values(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts[row["slash_sheet"]] += 1
    return dict(sorted(counts.items()))


def documents_from_extractions(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    documents = []
    for extraction in extractions:
        source = extraction["source"]
        documents.append(
            {
                "spec_family": "83513",
                "spec_sheet": source["spec_sheet"],
                "slash_sheet": slash_sheet_value(extraction),
                "revision": source["revision"],
            }
        )
    return sorted(documents, key=lambda row: -1 if row["slash_sheet"] == "base" else int(row["slash_sheet"]))


def insert_arrangements_from_extraction(extraction: dict[str, Any] | None) -> list[str]:
    if not extraction:
        return []
    return [
        item["insert_arrangement"]
        for item in extraction.get("pin_components", {}).get("insert_arrangements", [])
        if item.get("insert_arrangement")
    ]


def source_version_checks(
    documents: list[dict[str, Any]],
    extractions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    documents_by_slash = {normalize_slash_sheet(document["slash_sheet"]): document for document in documents}
    extractions_by_slash = {slash_sheet_value(extraction): extraction for extraction in extractions}

    document_02 = documents_by_slash.get("02", {})
    extraction_02 = extractions_by_slash.get("02")
    source_02 = extraction_02.get("source", {}) if extraction_02 else {}
    inserts_02 = insert_arrangements_from_extraction(extraction_02)
    expected_inserts_02 = ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"]
    source_doc_id = str(document_02.get("source_doc_id") or "")
    storage_checksum = document_02.get("checksum")
    extraction_checksum = source_02.get("source_sha256")

    checks.append(
        {
            "name": "/02 Storage source is MIL-DTL-83513/2H Amendment 4",
            "status": "pass"
            if document_02.get("revision_letter") == "H"
            and source_doc_id.upper().startswith("MIL-DTL-83513/2H(4)")
            and document_02.get("document_date") == "2025-12-17"
            else "fail",
            "details": {
                "source_doc_id": source_doc_id,
                "document_date": document_02.get("document_date"),
                "revision_letter": document_02.get("revision_letter"),
                "storage_path": document_02.get("storage_path"),
                "storage_checksum": storage_checksum,
            },
        }
    )
    checks.append(
        {
            "name": "/02 extraction metadata matches latest source and includes J,K",
            "status": "pass"
            if source_02.get("spec_sheet") == "MIL-DTL-83513/2H"
            and source_02.get("revision") == "H"
            and inserts_02 == expected_inserts_02
            else "fail",
            "details": {
                "spec_sheet": source_02.get("spec_sheet"),
                "revision": source_02.get("revision"),
                "inserts": inserts_02,
                "source_sha256": extraction_checksum,
                "source_size_bytes": source_02.get("source_size_bytes"),
            },
        }
    )
    checks.append(
        {
            "name": "/02 extraction read the declared Storage object hash",
            "status": "pass"
            if storage_checksum and extraction_checksum == storage_checksum
            else "fail",
            "details": {
                "storage_checksum": storage_checksum,
                "extraction_source_sha256": extraction_checksum,
            },
        }
    )
    return checks


def build_staged_torque_resolution(
    documents: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    profile_ids = {
        PROFILE_05: "staged-profile-05",
        PROFILE_PCB: "staged-profile-pcb",
        PROFILE_08: "staged-profile-08",
    }
    try:
        profiles = build_profiles(legacy_rows)
        profile_values = build_profile_values(legacy_rows, profile_ids)
        profile_value_counts = {
            profile_code: sum(1 for row in profile_values if row["profile_id"] == profile_id)
            for profile_code, profile_id in profile_ids.items()
        }
        statuses = build_status_rows(documents, legacy_rows, profile_value_counts)
        mappings = build_mappings(documents, profile_ids)
    except Exception as exc:
        return {
            "error": str(exc),
            "effective_facts_by_slash": {},
            "statuses": [],
            "mappings": [],
            "profile_value_counts": {},
        }

    profiles_by_code = {row["profile_code"]: row for row in profiles}
    mapping_type_by_spec = {row["spec_sheet"]: row["mapping_type"] for row in mappings}
    effective: dict[str, dict[str, Any]] = {}
    for status in statuses:
        profile_code = status.get("torque_profile_code")
        profile = profiles_by_code.get(profile_code or "")
        mapping_type = mapping_type_by_spec.get(status["spec_sheet"])
        effective[status["slash_sheet"]] = {
            "spec_sheet": status["spec_sheet"],
            "torque_mode": status["torque_mode"],
            "profile_code": profile_code,
            "effective_fact_count": profile_value_counts.get(profile_code or "", 0),
            "values_verified": bool(profile and profile.get("approval_status") == "approved"),
            "values_inherited": mapping_type == "references_profile" or (profile or {}).get("profile_kind") == "shared_derived",
            "needs_review": status["audit_status"] == "needs_review"
            or bool(profile and profile.get("approval_status") in {"needs_review", "rejected", "pending"}),
        }
    return {
        "effective_facts_by_slash": dict(sorted(effective.items())),
        "statuses": statuses,
        "mappings": mappings,
        "profile_value_counts": profile_value_counts,
    }


def summarize_live_snapshot(snapshot_dir: Path) -> dict[str, Any]:
    base_rows = json.loads((snapshot_dir / "base_configurations.json").read_text(encoding="utf-8"))
    wire_rows = json.loads((snapshot_dir / "hns_wire_options.json").read_text(encoding="utf-8"))
    torque_rows_payload = json.loads((snapshot_dir / "torque_values.json").read_text(encoding="utf-8"))
    effective_path = snapshot_dir / "v_83513_torque_effective_facts.json"
    effective_rows = json.loads(effective_path.read_text(encoding="utf-8")) if effective_path.exists() else []
    base_summary = summarize_base_rows(base_rows, wire_rows)
    effective_counts: Counter[str] = Counter()
    effective_details: dict[str, dict[str, Any]] = {}
    for row in effective_rows:
        slash_sheet = row["slash_sheet"]
        effective_counts[slash_sheet] += 1
        effective_details.setdefault(
            slash_sheet,
            {
                "spec_sheet": row["spec_sheet"],
                "torque_mode": row["torque_mode"],
                "profile_code": row["resolved_profile_code"],
                "values_verified": row["values_verified"],
                "values_inherited": row["values_inherited"],
                "needs_review": row["needs_review"],
            },
        )
    for slash_sheet, count in effective_counts.items():
        effective_details[slash_sheet]["effective_fact_count"] = count
    return {
        "base": base_summary,
        "torque_evidence_by_slash": summarize_torque_values(torque_rows_payload),
        "effective_facts_by_slash": dict(sorted(effective_details.items())),
    }


def snapshot_effective_facts(client: RestClient, snapshot_dir: Path) -> int:
    rows = fetch_all(
        client,
        "v_83513_torque_effective_facts",
        select="*",
        order="slash_sheet.asc,context.asc,fastener_thread.asc,arrangement_scope.asc",
    )
    write_json(snapshot_dir / "v_83513_torque_effective_facts.json", rows)
    return len(rows)


def compare_count_map(
    live: dict[str, Any],
    staged: dict[str, Any],
    *,
    field: str,
) -> dict[str, dict[str, int]]:
    diff: dict[str, dict[str, int]] = {}
    for slash_sheet in sorted(set(live) | set(staged)):
        live_value = int((live.get(slash_sheet) or {}).get(field, 0))
        staged_value = int((staged.get(slash_sheet) or {}).get(field, 0))
        if live_value != staged_value:
            diff[slash_sheet] = {
                "live": live_value,
                "staged": staged_value,
                "delta": staged_value - live_value,
            }
    return diff


def compare_simple_counts(live: dict[str, int], staged: dict[str, int]) -> dict[str, dict[str, int]]:
    diff: dict[str, dict[str, int]] = {}
    for slash_sheet in sorted(set(live) | set(staged)):
        live_value = int(live.get(slash_sheet, 0))
        staged_value = int(staged.get(slash_sheet, 0))
        if live_value != staged_value:
            diff[slash_sheet] = {
                "live": live_value,
                "staged": staged_value,
                "delta": staged_value - live_value,
            }
    return diff


def build_diff_report(live_summary: dict[str, Any], staged_summary: dict[str, Any]) -> dict[str, Any]:
    live_base = live_summary["base"]["by_slash"]
    staged_base = staged_summary["base"]["by_slash"]
    live_effective = live_summary["effective_facts_by_slash"]
    staged_effective = staged_summary["effective_facts_by_slash"]
    return {
        "base_row_count_diff": compare_count_map(live_base, staged_base, field="base_rows"),
        "wire_row_count_diff": compare_count_map(live_base, staged_base, field="wire_rows"),
        "torque_evidence_count_diff": compare_simple_counts(
            live_summary["torque_evidence_by_slash"],
            staged_summary["torque_evidence_by_slash"],
        ),
        "effective_torque_fact_count_diff": compare_count_map(
            live_effective,
            staged_effective,
            field="effective_fact_count",
        ),
        "live_missing_connector_fields": live_summary["base"]["missing_connector_fields"],
        "staged_missing_connector_fields": staged_summary["base"]["missing_connector_fields"],
    }


def edge_checks(staged_summary: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    base_by_slash = staged_summary["base"]["by_slash"]
    effective = staged_summary["effective_facts_by_slash"]

    expected_base_rows = {
        "01": 48,
        "02": 60,
        "03": 48,
        "04": 60,
        "06": 7,
        "07": 7,
        "08": 7,
        "09": 7,
    }
    for slash_sheet, expected_count in expected_base_rows.items():
        row = base_by_slash.get(slash_sheet, {})
        checks.append(
            {
                "name": f"/{slash_sheet} staged base row count is {expected_count}",
                "status": "pass" if row.get("base_rows") == expected_count else "fail",
                "details": row,
            }
        )

    expected_insert_sets = {
        "02": ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
        "04": ["A", "B", "C", "D", "E", "F", "G", "H", "J", "K"],
    }
    for slash_sheet, expected_inserts in expected_insert_sets.items():
        row = base_by_slash.get(slash_sheet, {})
        checks.append(
            {
                "name": f"/{slash_sheet} staged insert arrangements include H,J,K",
                "status": "pass" if row.get("insert_arrangements") == expected_inserts else "fail",
                "details": {"insert_arrangements": row.get("insert_arrangements", [])},
            }
        )

    expected_wire_rows = {"03": 2784, "04": 3480, "08": 154, "09": 406}
    for slash_sheet, expected_count in expected_wire_rows.items():
        row = base_by_slash.get(slash_sheet, {})
        checks.append(
            {
                "name": f"/{slash_sheet} staged wire row count is {expected_count}",
                "status": "pass" if row.get("wire_rows") == expected_count else "fail",
                "details": {"wire_rows": row.get("wire_rows", 0)},
            }
        )

    for slash_sheet in ("02", "04"):
        row = effective.get(slash_sheet, {})
        checks.append(
            {
                "name": f"/{slash_sheet} inherits verified /05 torque",
                "status": "pass"
                if row.get("effective_fact_count") == 6
                and row.get("values_inherited")
                and row.get("values_verified")
                and not row.get("needs_review")
                else "fail",
                "details": row,
            }
        )

    row_05 = effective.get("05", {})
    checks.append(
        {
            "name": "/05 owns six verified torque facts",
            "status": "pass"
            if row_05.get("effective_fact_count") == 6 and row_05.get("torque_mode") == "owns_profile"
            else "fail",
            "details": row_05,
        }
    )

    for slash_sheet in ("06", "07", "08", "09"):
        row = base_by_slash.get(slash_sheet, {})
        checks.append(
            {
                "name": f"/{slash_sheet} class-P rows do not emit metal shell finish suffixes",
                "status": "pass" if not row.get("shell_finish_codes") else "fail",
                "details": row,
            }
        )

    return checks


def diff_edge_checks(diff_report: dict[str, Any]) -> list[dict[str, Any]]:
    live_wire_missing = diff_report["live_missing_connector_fields"].get("Wire Range", 0)
    staged_wire_missing = diff_report["staged_missing_connector_fields"].get("Wire Range", 0)
    return [
        {
            "name": "staged missing Wire Range does not regress",
            "status": "pass" if staged_wire_missing <= live_wire_missing else "fail",
            "details": {
                "live_missing_wire_range": live_wire_missing,
                "staged_missing_wire_range": staged_wire_missing,
            },
        }
    ]


def main() -> int:
    args = parse_args()
    run_id = args.run_id or utc_run_id()
    run_dir = args.staging_root / run_id
    snapshot_dir = run_dir / "snapshot"
    staged_dir = run_dir / "staged"
    logs_dir = run_dir / "logs"
    outputs_dir = args.outputs_dir or (DEFAULT_OUTPUTS_DIR if args.skip_extract else staged_dir / "outputs")

    client = client_from_env(args.env_file)
    documents = selected_documents(fetch_documents(client), args.only, args.limit)
    if not documents:
        raise RuntimeError("No active MIL-DTL-83513 documents matched the rebuild selection.")

    snapshot_counts = snapshot_live_tables(client, snapshot_dir)
    snapshot_counts["v_83513_torque_effective_facts"] = snapshot_effective_facts(client, snapshot_dir)
    snapshot_counts["current_extraction_outputs"] = copy_current_outputs(DEFAULT_OUTPUTS_DIR, snapshot_dir)
    write_json(snapshot_dir / "documents.json", documents)

    extraction_results: list[dict[str, Any]] = []
    if args.skip_extract:
        if not outputs_dir.exists():
            raise RuntimeError(f"Outputs directory does not exist: {outputs_dir}")
    else:
        extraction_results = regenerate_outputs(
            documents,
            outputs_dir,
            env_file=args.env_file,
            bucket=args.bucket,
            logs_dir=logs_dir,
        )
        write_json(logs_dir / "extraction_results.json", extraction_results)
        failures = [row for row in extraction_results if row["returncode"] != 0]
        if failures:
            write_json(run_dir / "rebuild_failed.json", {"failures": failures, "run_id": run_id})
            print(f"Extraction failed for {len(failures)} documents. See {logs_dir}.")
            return 1

    selected_slashes = {normalize_slash_sheet(document["slash_sheet"]) for document in documents}
    extractions = load_extractions(outputs_dir, selected_slashes)
    loaded_slashes = {slash_sheet_value(extraction) for extraction in extractions}
    missing_output_slashes = sorted(selected_slashes.difference(loaded_slashes))
    if missing_output_slashes and not args.skip_extract:
        raise RuntimeError(f"Fresh extraction did not produce outputs for: {', '.join(missing_output_slashes)}")
    staged_payloads = build_staged_payloads(extractions)
    for table_name, rows in staged_payloads.items():
        write_json(staged_dir / f"{table_name}.json", rows)

    documents_from_json = documents_from_extractions(extractions)
    staged_torque = build_staged_torque_resolution(documents_from_json, staged_payloads["torque_values"])
    write_json(staged_dir / "torque_resolution.json", staged_torque)

    live_summary = summarize_live_snapshot(snapshot_dir)
    staged_base_summary = summarize_base_rows(
        staged_payloads["base_configurations"],
        staged_payloads["hns_wire_options"],
    )
    staged_summary = {
        "base": staged_base_summary,
        "torque_evidence_by_slash": summarize_torque_values(staged_payloads["torque_values"]),
        "effective_facts_by_slash": staged_torque["effective_facts_by_slash"],
    }
    diff_report = build_diff_report(live_summary, staged_summary)
    checks = []
    checks.extend(source_version_checks(documents, extractions))
    checks.extend(edge_checks(staged_summary))
    checks.extend(diff_edge_checks(diff_report))

    report = {
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "mode": "reuse_existing_outputs" if args.skip_extract else "fresh_extraction",
        "documents_selected": len(documents),
        "extractions_loaded": len(extractions),
        "missing_output_slashes": missing_output_slashes,
        "outputs_dir": str(outputs_dir),
        "snapshot_counts": snapshot_counts,
        "live_summary": live_summary,
        "staged_summary": staged_summary,
        "diff_report": diff_report,
        "edge_checks": checks,
        "next_step": "Review this report before running any production loader or swap.",
    }
    write_json(run_dir / "rebuild_diff_report.json", report)

    failed_checks = [check for check in checks if check["status"] != "pass"]
    print(f"Run: {run_id}")
    print(f"Documents selected: {len(documents)}")
    print(f"Mode: {report['mode']}")
    print(f"Snapshot: {snapshot_dir}")
    print(f"Staged artifacts: {staged_dir}")
    print(f"Diff report: {run_dir / 'rebuild_diff_report.json'}")
    print(f"Edge checks: {len(checks) - len(failed_checks)} passed, {len(failed_checks)} failed")
    print("No live tables were modified.")
    return 2 if args.fail_on_checks and failed_checks else 0


if __name__ == "__main__":
    raise SystemExit(main())
