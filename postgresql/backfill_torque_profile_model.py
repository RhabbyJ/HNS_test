#!/usr/bin/env python3
"""Backfill normalized MIL-DTL-83513 torque profile tables from legacy torque_values."""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pdf_storage.sync_83513_to_supabase import get_server_key, load_env_file, require_env
from postgresql.m83513_load_extraction import load_json, slash_sheet_value


DEFAULT_OUTPUTS_DIR = REPO_ROOT / "structured_json_validation" / "outputs"
EXTRACTOR_VERSION = "m83513_torque_profile_backfill_v1"
PROFILE_05 = "m83513_05_main"
PROFILE_PCB = "m83513_10_33_pcb_standard"
PROFILE_08 = "m83513_08_provisional"
REFERENCE_TO_05_SLASHES = {"01", "03", "06", "07", "09"}
PCB_STANDARD_SLASHES = {f"{number:02d}" for number in range(10, 34)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill normalized torque profile tables from legacy torque_values.")
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env.local")
    parser.add_argument("--outputs-dir", type=Path, default=DEFAULT_OUTPUTS_DIR)
    parser.add_argument("--apply", action="store_true", help="Write normalized torque rows to Supabase.")
    return parser.parse_args()


class RestClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key

    def headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
        }

    def request(
        self,
        method: str,
        table: str,
        query: list[tuple[str, str]] | None = None,
        payload: Any | None = None,
        prefer: str | None = None,
    ) -> list[dict[str, Any]]:
        query_string = urllib.parse.urlencode(query or [])
        url = f"{self.supabase_url}/rest/v1/{table}"
        if query_string:
            url = f"{url}?{query_string}"

        headers = self.headers()
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["content-type"] = "application/json"
        if prefer:
            headers["Prefer"] = prefer

        request = urllib.request.Request(url, headers=headers, data=data, method=method)
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
        return json.loads(body) if body else []

    def fetch(self, table: str, query: list[tuple[str, str]]) -> list[dict[str, Any]]:
        return self.request("GET", table, query=query)

    def upsert(self, table: str, rows: list[dict[str, Any]], on_conflict: str) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.request(
            "POST",
            table,
            query=[("on_conflict", on_conflict)],
            payload=rows,
            prefer="resolution=merge-duplicates,return=representation",
        )

    def insert(self, table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []
        return self.request("POST", table, payload=rows, prefer="return=representation")

    def delete_eq(self, table: str, column: str, value: str) -> None:
        self.request("DELETE", table, query=[(column, f"eq.{value}")], prefer="return=minimal")


def output_paths(outputs_dir: Path) -> list[Path]:
    return sorted(outputs_dir.glob("m83513_*_extraction_output.json"))


def load_documents(outputs_dir: Path) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for path in output_paths(outputs_dir):
        extraction = load_json(path)
        source = extraction["source"]
        documents.append(
            {
                "spec_family": "83513",
                "spec_sheet": source["spec_sheet"],
                "slash_sheet": slash_sheet_value(extraction),
                "revision": source["revision"],
            }
        )
    return sorted(documents, key=lambda item: slash_sort_key(item["slash_sheet"]))


def slash_sort_key(slash_sheet: str) -> tuple[int, str]:
    if slash_sheet.isdigit():
        return (int(slash_sheet), slash_sheet)
    return (-1, slash_sheet)


def numeric_value(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def format_number(value: Any) -> str:
    number = numeric_value(value)
    if number is None:
        return ""
    return f"{number:g}"


def normalized_fact_key(row: dict[str, Any]) -> str:
    pieces = [
        row.get("context") or "",
        row.get("fastener_thread") or "",
        row.get("source_thread_label") or "",
        row.get("arrangement_scope") or "",
        format_number(row.get("torque_min_in_lbf")),
        format_number(row.get("torque_max_in_lbf")),
    ]
    raw_key = "|".join(pieces).lower()
    return re.sub(r"[^a-z0-9]+", "_", raw_key).strip("_")


def profile_code_for_slash(slash_sheet: str) -> str | None:
    if slash_sheet == "05" or slash_sheet in REFERENCE_TO_05_SLASHES:
        return PROFILE_05
    if slash_sheet in PCB_STANDARD_SLASHES:
        return PROFILE_PCB
    if slash_sheet == "08":
        return PROFILE_08
    return None


def build_profiles(legacy_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    row_05 = next(row for row in legacy_rows if row["slash_sheet"] == "05")
    row_08 = next(row for row in legacy_rows if row["slash_sheet"] == "08")
    row_10 = next(row for row in legacy_rows if row["slash_sheet"] == "10")
    now = utc_timestamp()
    return [
        {
            "profile_code": PROFILE_05,
            "profile_name": "MIL-DTL-83513/5 verified mounting and mating torque",
            "source_spec_sheet": row_05["spec_sheet"],
            "source_revision": row_05["revision"],
            "source_page": 7,
            "profile_status": "verified",
            "notes": "Audited /05 page 7 Table I and Table II torque values.",
            "updated_at": now,
        },
        {
            "profile_code": PROFILE_PCB,
            "profile_name": "MIL-DTL-83513/10 through /33 PCB hardware torque",
            "source_spec_sheet": row_10["spec_sheet"],
            "source_revision": row_10["revision"],
            "source_page": row_10["source_page"],
            "profile_status": "provisional",
            "notes": "Repeated extracted PCB hardware torque limits shared by /10 through /33; pending focused audit.",
            "updated_at": now,
        },
        {
            "profile_code": PROFILE_08,
            "profile_name": "MIL-DTL-83513/8 mounting hardware torque",
            "source_spec_sheet": row_08["spec_sheet"],
            "source_revision": row_08["revision"],
            "source_page": row_08["source_page"],
            "profile_status": "provisional",
            "notes": "Single extracted /08 mounting torque row; pending focused audit.",
            "updated_at": now,
        },
    ]


def value_payload(row: dict[str, Any], profile_id: str) -> dict[str, Any]:
    payload = {
        "profile_id": profile_id,
        "context": row["context"],
        "fastener_thread": row.get("fastener_thread"),
        "source_thread_label": row.get("source_thread_label"),
        "arrangement_scope": row.get("arrangement_scope"),
        "torque_min_in_lbf": row.get("torque_min_in_lbf"),
        "torque_max_in_lbf": row.get("torque_max_in_lbf"),
    }
    payload["normalized_fact_key"] = normalized_fact_key(payload)
    return payload


def dedupe_values(rows: list[dict[str, Any]], profile_id: str) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.get("torque_min_in_lbf") is None or row.get("torque_max_in_lbf") is None:
            continue
        payload = value_payload(row, profile_id)
        deduped[payload["normalized_fact_key"]] = payload
    return [deduped[key] for key in sorted(deduped)]


def build_profile_values(legacy_rows: list[dict[str, Any]], profile_ids: dict[str, str]) -> list[dict[str, Any]]:
    rows_by_profile: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in legacy_rows:
        profile_code = profile_code_for_slash(row["slash_sheet"])
        if profile_code:
            rows_by_profile[profile_code].append(row)

    values: list[dict[str, Any]] = []
    for profile_code, rows in rows_by_profile.items():
        values.extend(dedupe_values(rows, profile_ids[profile_code]))
    return values


def build_status_rows(
    documents: list[dict[str, Any]],
    legacy_rows: list[dict[str, Any]],
    profile_value_counts: dict[str, int],
) -> list[dict[str, Any]]:
    rows_by_spec: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in legacy_rows:
        rows_by_spec[row["spec_sheet"]].append(row)

    now = utc_timestamp()
    statuses: list[dict[str, Any]] = []
    for document in documents:
        slash_sheet = document["slash_sheet"]
        torque_rows = rows_by_spec.get(document["spec_sheet"], [])
        profile_code = profile_code_for_slash(slash_sheet)
        referenced_spec_sheet = None
        audit_status = "pending"
        torque_mode = "none"
        notes = None

        if slash_sheet == "05":
            torque_mode = "canonical"
            audit_status = "verified"
            notes = "Canonical audited /05 torque profile."
        elif slash_sheet in REFERENCE_TO_05_SLASHES:
            torque_mode = "references_other_doc"
            referenced_spec_sheet = "MIL-DTL-83513/5H"
            notes = "Source document references MIL-DTL-83513/5 for hardware torque."
        elif slash_sheet in PCB_STANDARD_SLASHES:
            torque_mode = "direct_numeric"
            notes = "Mapped to shared provisional PCB hardware torque profile."
        elif slash_sheet == "08":
            torque_mode = "direct_numeric"
            audit_status = "needs_review"
            notes = "Single extracted numeric torque row; needs focused audit."
        elif torque_rows:
            torque_mode = "needs_review"
            audit_status = "needs_review"
            notes = "Legacy torque rows exist but no normalized profile rule has been assigned."

        last_extracted_at = max((row["extracted_at"] for row in torque_rows if row.get("extracted_at")), default=None)
        statuses.append(
            {
                **document,
                "torque_profile_code": profile_code,
                "torque_mode": torque_mode,
                "referenced_spec_sheet": referenced_spec_sheet,
                "extracted_row_count": len(torque_rows),
                "canonical_row_count": profile_value_counts.get(profile_code or "", 0),
                "audit_status": audit_status,
                "extractor_version": EXTRACTOR_VERSION,
                "last_extracted_at": last_extracted_at,
                "notes": notes,
                "updated_at": now,
            }
        )
    return statuses


def mapping_type_for_slash(slash_sheet: str) -> str | None:
    if slash_sheet == "05":
        return "uses_profile"
    if slash_sheet in REFERENCE_TO_05_SLASHES:
        return "references_profile"
    if slash_sheet in PCB_STANDARD_SLASHES or slash_sheet == "08":
        return "provisional_profile"
    return None


def build_mappings(documents: list[dict[str, Any]], profile_ids: dict[str, str]) -> list[dict[str, Any]]:
    mappings: list[dict[str, Any]] = []
    for document in documents:
        profile_code = profile_code_for_slash(document["slash_sheet"])
        mapping_type = mapping_type_for_slash(document["slash_sheet"])
        if not profile_code or not mapping_type:
            continue
        mappings.append(
            {
                "spec_sheet": document["spec_sheet"],
                "profile_id": profile_ids[profile_code],
                "mapping_type": mapping_type,
            }
        )
    return mappings


def build_evidence(legacy_rows: list[dict[str, Any]], profile_ids: dict[str, str]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for row in legacy_rows:
        profile_code = profile_code_for_slash(row["slash_sheet"])
        evidence.append(
            {
                "spec_sheet": row["spec_sheet"],
                "slash_sheet": row["slash_sheet"],
                "revision": row["revision"],
                "profile_id": profile_ids.get(profile_code) if profile_code else None,
                "source_document": row["source_document"],
                "source_page": row["source_page"],
                "source_url": row["source_url"],
                "storage_path": row["storage_path"],
                "torque_text": row["torque_text"],
                "extracted_context": row["context"],
                "extracted_fastener_thread": row.get("fastener_thread"),
                "extracted_source_thread_label": row.get("source_thread_label"),
                "extracted_arrangement_scope": row.get("arrangement_scope"),
                "extracted_min_in_lbf": row.get("torque_min_in_lbf"),
                "extracted_max_in_lbf": row.get("torque_max_in_lbf"),
                "extractor_version": EXTRACTOR_VERSION,
                "extracted_at": row["extracted_at"],
            }
        )
    return evidence


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat()


def fetch_legacy_rows(client: RestClient) -> list[dict[str, Any]]:
    return client.fetch(
        "torque_values",
        query=[
            (
                "select",
                ",".join(
                    [
                        "spec_sheet",
                        "slash_sheet",
                        "revision",
                        "context",
                        "applies_to",
                        "fastener_thread",
                        "source_thread_label",
                        "arrangement_scope",
                        "torque_min_in_lbf",
                        "torque_max_in_lbf",
                        "torque_text",
                        "source_document",
                        "source_page",
                        "source_url",
                        "storage_path",
                        "extracted_at",
                    ]
                ),
            ),
            ("spec_family", "eq.83513"),
            ("order", "slash_sheet.asc,context.asc,fastener_thread.asc,arrangement_scope.asc"),
        ],
    )


def profile_ids_by_code(client: RestClient) -> dict[str, str]:
    rows = client.fetch(
        "torque_profiles",
        query=[
            ("select", "id,profile_code"),
            ("profile_code", f"in.({','.join([PROFILE_05, PROFILE_PCB, PROFILE_08])})"),
        ],
    )
    return {row["profile_code"]: row["id"] for row in rows}


def apply_backfill(
    client: RestClient,
    documents: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
    profile_values: list[dict[str, Any]],
    statuses: list[dict[str, Any]],
    mappings: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
) -> None:
    spec_sheets = [document["spec_sheet"] for document in documents]
    for spec_sheet in spec_sheets:
        client.delete_eq("document_torque_profile_map", "spec_sheet", spec_sheet)
        client.delete_eq("torque_source_evidence", "spec_sheet", spec_sheet)

    for profile_id in {row["profile_id"] for row in profile_values}:
        client.delete_eq("torque_profile_values", "profile_id", profile_id)

    client.upsert("document_torque_status", statuses, "spec_sheet")
    client.upsert("torque_profiles", profiles, "profile_code")
    client.insert("torque_profile_values", profile_values)
    client.insert("document_torque_profile_map", mappings)
    client.insert("torque_source_evidence", evidence)


def main() -> int:
    args = parse_args()
    env = load_env_file(args.env_file)
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    client = RestClient(require_env(env, "SUPABASE_URL"), server_key)

    documents = load_documents(args.outputs_dir)
    legacy_rows = fetch_legacy_rows(client)
    profiles = build_profiles(legacy_rows)

    if args.apply:
        client.upsert("torque_profiles", profiles, "profile_code")
    profile_ids = profile_ids_by_code(client) if args.apply else {
        PROFILE_05: "dry-run-05",
        PROFILE_PCB: "dry-run-pcb",
        PROFILE_08: "dry-run-08",
    }

    profile_values = build_profile_values(legacy_rows, profile_ids)
    profile_value_counts = {
        profile_code: sum(1 for row in profile_values if row["profile_id"] == profile_id)
        for profile_code, profile_id in profile_ids.items()
    }
    statuses = build_status_rows(documents, legacy_rows, profile_value_counts)
    mappings = build_mappings(documents, profile_ids)
    evidence = build_evidence(legacy_rows, profile_ids)

    print(f"Documents: {len(documents)}")
    print(f"Profiles: {len(profiles)}")
    print(f"Profile values: {len(profile_values)}")
    print(f"Document profile mappings: {len(mappings)}")
    print(f"Source evidence rows: {len(evidence)}")
    print(f"Legacy torque rows read: {len(legacy_rows)}")

    if not args.apply:
        print("Dry run only. Re-run with --apply to write normalized torque tables.")
        return 0

    apply_backfill(client, documents, profiles, profile_values, statuses, mappings, evidence)
    print("Normalized torque backfill completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
