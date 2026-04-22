#!/usr/bin/env python3
"""Build release-scoped v2 backend payloads from MIL-DTL-83513 extraction outputs."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from postgresql.backfill_torque_profile_model import (
    build_mappings,
    build_profile_values,
    build_profiles,
    build_status_rows,
    profile_code_for_slash,
)
from postgresql.m83513_load_extraction import (
    base_rows_for_extraction,
    slash_sheet_value,
    torque_rows,
    wire_rows_for_base,
)


DEFAULT_OUTPUTS_DIR = REPO_ROOT / "structured_json_validation" / "outputs"
DEFAULT_STAGING_ROOT = REPO_ROOT / "structured_json_validation" / "staging"
PARSER_VERSION = "m83513_pdf_first_v2"
REGISTRY_VERSION = "m83513_registry_exact_inserts_v2"
SPEC_FAMILY = "83513"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build v2 release payload JSON for MIL-DTL-83513.")
    parser.add_argument("--outputs-dir", type=Path, default=DEFAULT_OUTPUTS_DIR)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--release-name", default=None)
    parser.add_argument("--created-from-run-id", default=None)
    parser.add_argument(
        "--release-status",
        choices=["draft", "staged", "published", "archived"],
        default="staged",
    )
    parser.add_argument("--documents-json", type=Path, default=None)
    return parser.parse_args()


def utc_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def stable_uuid(*parts: Any) -> str:
    return str(uuid5(NAMESPACE_URL, "hns-platform-v2:" + "|".join(str(part) for part in parts)))


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def normalize_slash(value: str) -> str:
    if value == "base":
        return "base"
    return value.zfill(2)


def slash_sort_key(value: str) -> tuple[int, int]:
    if value == "base":
        return (0, 0)
    return (1, int(value))


def extraction_paths(outputs_dir: Path) -> list[Path]:
    paths = sorted(outputs_dir.glob("m83513_*_extraction_output.json"))
    if not paths:
        raise RuntimeError(f"No extraction outputs found in {outputs_dir}")
    return paths


def load_extractions(outputs_dir: Path) -> list[dict[str, Any]]:
    extractions = [read_json(path) for path in extraction_paths(outputs_dir)]
    return sorted(extractions, key=lambda extraction: slash_sort_key(slash_sheet_value(extraction)))


def load_document_metadata(path: Path | None) -> dict[str, dict[str, Any]]:
    if not path or not path.exists():
        return {}
    rows = read_json(path)
    if isinstance(rows, dict) and "documents" in rows:
        rows = rows["documents"]
    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("slash_sheet")
        if key is None:
            key = row.get("document_key")
        if key is None:
            continue
        metadata[normalize_slash(str(key))] = row
    return metadata


def field_presence(extraction: dict[str, Any]) -> dict[str, Any]:
    return {
        "configuration_rows": len(extraction.get("configuration_rows", [])),
        "insert_arrangements": len(extraction.get("pin_components", {}).get("insert_arrangements", [])),
        "shell_finish_options": len(extraction.get("pin_components", {}).get("shell_finish_options", [])),
        "wire_options": len(extraction.get("wire_options", [])),
        "torque_values": len(extraction.get("torque_values", [])),
        "chunks": len(extraction.get("chunks", [])),
    }


def amendment_from_doc_id(doc_id: str | None) -> str | None:
    if not doc_id:
        return None
    match = re.search(r"\((\d+)\)", doc_id)
    return match.group(1) if match else None


def build_documents(
    extractions: list[dict[str, Any]],
    metadata_by_slash: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    documents: list[dict[str, Any]] = []
    document_id_by_slash: dict[str, str] = {}
    for extraction in extractions:
        source = extraction["source"]
        slash_sheet = slash_sheet_value(extraction)
        metadata = metadata_by_slash.get(slash_sheet, {})
        document_id = stable_uuid("document", SPEC_FAMILY, slash_sheet, source.get("revision") or "")
        document_id_by_slash[slash_sheet] = document_id
        documents.append(
            {
                "id": document_id,
                "spec_family": SPEC_FAMILY,
                "slash_sheet": slash_sheet,
                "spec_sheet": source["spec_sheet"],
                "revision": source.get("revision"),
                "amendment": amendment_from_doc_id(metadata.get("source_doc_id") or metadata.get("doc_id")),
                "document_date": metadata.get("document_date") or metadata.get("doc_date"),
                "title": source["title"],
                "source_url": source["source_url"],
                "storage_path": source["storage_path"],
                "checksum": source.get("source_sha256") or metadata.get("checksum"),
                "source_size_bytes": source.get("source_size_bytes") or metadata.get("file_size_bytes"),
                "is_latest": True,
                "status": "active",
                "attributes": {
                    "document_type": source.get("document_type"),
                    "source_doc_id": metadata.get("source_doc_id") or metadata.get("doc_id"),
                    "source_ident_number": metadata.get("source_ident_number"),
                },
            }
        )
    return documents, document_id_by_slash


def build_document_chunks(
    extractions: list[dict[str, Any]],
    document_id_by_slash: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for extraction in extractions:
        document_id = document_id_by_slash[slash_sheet_value(extraction)]
        for chunk in extraction.get("chunks", []):
            chunk_index = int(str(chunk["chunk_id"]).split("-")[-1])
            rows.append(
                {
                    "id": stable_uuid("document_chunk", document_id, chunk["page_number"], chunk_index),
                    "document_id": document_id,
                    "page_number": chunk["page_number"],
                    "chunk_index": chunk_index,
                    "text_content": chunk["text"],
                    "layout": None,
                }
            )
    return rows


def build_extraction_layer(
    extractions: list[dict[str, Any]],
    document_id_by_slash: dict[str, str],
    release_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    runs: list[dict[str, Any]] = []
    outputs: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    run_id_by_slash: dict[str, str] = {}
    now = utc_now()
    for extraction in extractions:
        slash_sheet = slash_sheet_value(extraction)
        source = extraction["source"]
        document_id = document_id_by_slash[slash_sheet]
        run_id = stable_uuid("extraction_run", release_name, document_id, source.get("source_sha256") or "")
        run_id_by_slash[slash_sheet] = run_id
        runs.append(
            {
                "id": run_id,
                "document_id": document_id,
                "parser_version": PARSER_VERSION,
                "registry_version": REGISTRY_VERSION,
                "run_type": "release_build",
                "status": "completed",
                "confidence_score": extraction.get("confidence_score"),
                "validation_summary": {
                    "validation_checks": extraction.get("validation_checks", []),
                    "fallback_flags": extraction.get("fallback_flags", []),
                },
                "started_at": now,
                "completed_at": now,
            }
        )
        outputs.append(
            {
                "id": stable_uuid("extraction_output", run_id, document_id),
                "run_id": run_id,
                "document_id": document_id,
                "output_json": extraction,
                "issues_json": extraction.get("issues", []),
                "field_presence": field_presence(extraction),
                "fallback_required": bool(extraction.get("llm_fallback_required")),
                "source_hash": source.get("source_sha256"),
            }
        )
        for value in torque_rows(extraction):
            evidence.append(
                {
                    "id": stable_uuid("extraction_evidence", run_id, value["torque_key"]),
                    "run_id": run_id,
                    "document_id": document_id,
                    "fact_type": "torque",
                    "fact_key": value["torque_key"],
                    "page_number": value.get("source_page"),
                    "source_text": value.get("torque_text"),
                    "source_ref": source["spec_sheet"],
                    "confidence": extraction.get("confidence_score"),
                    "attributes": {
                        "context": value.get("context"),
                        "fastener_thread": value.get("fastener_thread"),
                        "arrangement_scope": value.get("arrangement_scope"),
                    },
                }
            )
    return runs, outputs, evidence, run_id_by_slash


def termination_style(row: dict[str, Any]) -> str | None:
    connector_type = (row.get("connector_type") or "").upper()
    if "CRIMP" in connector_type:
        return "Crimp"
    if "SOLDER" in connector_type:
        return "Solder"
    return None


def configuration_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("spec_sheet"),
        row.get("slash_sheet"),
        row.get("cavity_count"),
        row.get("shell_size_letter"),
        row.get("shell_finish_code"),
        row.get("insert_arrangement_ref"),
        row.get("example_full_pin"),
    )


def parse_related_slash(value: str) -> tuple[str | None, str | None]:
    match = re.search(r"MIL-DTL-(?P<family>\d+)/(?:0?)(?P<slash>\d+)", value or "", re.IGNORECASE)
    if not match:
        return None, None
    return match.group("family"), match.group("slash").zfill(2)


def hardware_option_rows(
    release_id: str,
    document_id: str,
    configuration_id: str,
    base_row: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    extra_data = base_row.get("extra_data") if isinstance(base_row.get("extra_data"), dict) else {}
    options = extra_data.get("hardware_options") if isinstance(extra_data, dict) else None
    if not isinstance(options, list):
        return rows
    for index, option in enumerate(options):
        if not isinstance(option, dict):
            continue
        code = option.get("code") or option.get("dash_number")
        description = option.get("description")
        if not description:
            continue
        rows.append(
            {
                "id": stable_uuid("hardware_option", release_id, configuration_id, code or index),
                "configuration_id": configuration_id,
                "document_id": document_id,
                "hardware_code": code,
                "hardware_type": option.get("hardware_type"),
                "thread": option.get("thread"),
                "profile": option.get("profile"),
                "drive": option.get("drive"),
                "description": description,
                "attributes": option,
            }
        )
    return rows


def build_catalog_configurations(
    extractions: list[dict[str, Any]],
    document_id_by_slash: dict[str, str],
    release_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[tuple[Any, ...], str]]:
    configurations: list[dict[str, Any]] = []
    wire_options: list[dict[str, Any]] = []
    hardware_options: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    config_id_by_key: dict[tuple[Any, ...], str] = {}

    for extraction in extractions:
        slash_sheet = slash_sheet_value(extraction)
        document_id = document_id_by_slash[slash_sheet]
        base_rows = base_rows_for_extraction(extraction)
        for index, base_row in enumerate(base_rows):
            key = configuration_key(base_row)
            configuration_id = stable_uuid("configuration", release_id, *key)
            config_id_by_key[key] = configuration_id
            attributes = {
                "dimensions": base_row.get("dimensions"),
                "shell_size_description": base_row.get("shell_size_description"),
                "shell_finish_description": base_row.get("shell_finish_description"),
                "shell_finish_notes": base_row.get("shell_finish_notes"),
                "current_rating_per_contact": base_row.get("current_rating_per_contact"),
                "polarization": base_row.get("polarization"),
                "mates_with": base_row.get("mates_with") or [],
                "mounting_hardware_ref": base_row.get("mounting_hardware_ref"),
                "source_document": base_row.get("source_document"),
                "source_page": base_row.get("source_page"),
                "source_url": base_row.get("source_url"),
                "figure_references": base_row.get("figure_references"),
                "confidence_score": base_row.get("confidence_score"),
                "legacy_extra_data": base_row.get("extra_data"),
            }
            configurations.append(
                {
                    "id": configuration_id,
                    "release_id": release_id,
                    "document_id": document_id,
                    "spec_family": base_row["spec_family"],
                    "slash_sheet": base_row["slash_sheet"],
                    "spec_sheet": base_row["spec_sheet"],
                    "revision": base_row.get("revision"),
                    "part_number_example": base_row.get("example_full_pin"),
                    "connector_type": base_row["connector_type"],
                    "class_code": base_row.get("class"),
                    "shell_material": base_row.get("shell_material"),
                    "contact_type": base_row.get("contact_type"),
                    "gender": base_row.get("gender"),
                    "termination_style": termination_style(base_row),
                    "cavity_count": base_row.get("cavity_count"),
                    "insert_arrangement_code": base_row.get("insert_arrangement_ref"),
                    "shell_finish_code": base_row.get("shell_finish_code"),
                    "shell_size_letter": base_row.get("shell_size_letter"),
                    "name": base_row["name"],
                    "description": base_row.get("description"),
                    "attributes": attributes,
                }
            )
            hardware_options.extend(hardware_option_rows(release_id, document_id, configuration_id, base_row))
            for mate in base_row.get("mates_with") or []:
                related_family, related_slash = parse_related_slash(mate)
                relationships.append(
                    {
                        "id": stable_uuid("mating_relationship", release_id, configuration_id, mate),
                        "configuration_id": configuration_id,
                        "related_spec_family": related_family,
                        "related_slash_sheet": related_slash,
                        "relationship_type": "mates_with",
                        "attributes": {"source_value": mate},
                    }
                )
            if base_row.get("mounting_hardware_ref"):
                related_family, related_slash = parse_related_slash(base_row["mounting_hardware_ref"])
                relationships.append(
                    {
                        "id": stable_uuid("mounting_hardware_ref", release_id, configuration_id, base_row["mounting_hardware_ref"]),
                        "configuration_id": configuration_id,
                        "related_spec_family": related_family,
                        "related_slash_sheet": related_slash,
                        "relationship_type": "mounting_hardware_ref",
                        "attributes": {"source_value": base_row["mounting_hardware_ref"]},
                    }
                )
            for wire_row in wire_rows_for_base(extraction, configuration_id):
                wire_options.append(
                    {
                        "id": stable_uuid("wire_option", release_id, configuration_id, wire_row["wire_type_code"]),
                        "configuration_id": configuration_id,
                        "wire_type_code": wire_row["wire_type_code"],
                        "wire_specification": wire_row.get("wire_specification"),
                        "wire_length_inches": wire_row.get("wire_length_inches"),
                        "wire_notes": wire_row.get("wire_notes"),
                        "is_space_approved": wire_row.get("is_space_approved", False),
                        "attributes": {},
                    }
                )
    return configurations, wire_options, hardware_options, relationships, config_id_by_key


def build_legacy_torque_rows(extractions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now = utc_now()
    for extraction in extractions:
        for row in torque_rows(extraction):
            row["extracted_at"] = now
            rows.append(row)
    return rows


def build_torque_catalog(
    extractions: list[dict[str, Any]],
    document_id_by_slash: dict[str, str],
    release_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    legacy_rows = build_legacy_torque_rows(extractions)
    if not legacy_rows:
        return [], [], [], []
    profile_ids = {
        profile_code: stable_uuid("torque_profile", release_id, profile_code)
        for profile_code in sorted({profile_code_for_slash(row["slash_sheet"]) for row in legacy_rows if profile_code_for_slash(row["slash_sheet"])})
    }
    legacy_profiles = build_profiles(legacy_rows)
    profiles: list[dict[str, Any]] = []
    for profile in legacy_profiles:
        profile_code = profile["profile_code"]
        source_document_id = document_id_by_slash.get(profile["source_spec_sheet"].split("/")[-1][:-1].zfill(2)) if "/" in profile["source_spec_sheet"] else None
        governing_document_id = document_id_by_slash.get(profile["governing_spec_sheet"].split("/")[-1][:-1].zfill(2)) if profile.get("governing_spec_sheet") and "/" in profile["governing_spec_sheet"] else None
        profiles.append(
            {
                "id": profile_ids[profile_code],
                "release_id": release_id,
                "profile_code": profile_code,
                "source_document_id": source_document_id,
                "governing_document_id": governing_document_id,
                "profile_status": profile["profile_status"],
                "profile_kind": profile["profile_kind"],
                "attributes": profile,
            }
        )

    legacy_values = build_profile_values(legacy_rows, profile_ids)
    values = [
        {
            "id": stable_uuid("torque_profile_value", value["profile_id"], value["normalized_fact_key"]),
            "profile_id": value["profile_id"],
            "context": value["context"],
            "fastener_thread": value.get("fastener_thread"),
            "source_thread_label": value.get("source_thread_label"),
            "arrangement_scope": value.get("arrangement_scope"),
            "torque_min_in_lbf": value.get("torque_min_in_lbf"),
            "torque_max_in_lbf": value.get("torque_max_in_lbf"),
            "attributes": {"normalized_fact_key": value["normalized_fact_key"]},
        }
        for value in legacy_values
    ]

    documents = [
        {
            "spec_family": SPEC_FAMILY,
            "spec_sheet": extraction["source"]["spec_sheet"],
            "slash_sheet": slash_sheet_value(extraction),
            "revision": extraction["source"].get("revision"),
        }
        for extraction in extractions
    ]
    profile_value_counts = Counter(value["profile_id"] for value in values)
    counts_by_profile_code = {
        profile_code: profile_value_counts[profile_id]
        for profile_code, profile_id in profile_ids.items()
    }
    statuses = build_status_rows(documents, legacy_rows, counts_by_profile_code)
    status_by_spec = {status["spec_sheet"]: status for status in statuses}
    profile_by_id = {profile["id"]: profile for profile in profiles}
    mappings = build_mappings(documents, profile_ids)
    links: list[dict[str, Any]] = []
    for mapping in mappings:
        status = status_by_spec[mapping["spec_sheet"]]
        document_id = document_id_by_slash[status["slash_sheet"]]
        profile = profile_by_id[mapping["profile_id"]]
        profile_attributes = profile.get("attributes", {})
        links.append(
            {
                "id": stable_uuid("document_profile_link", release_id, document_id, mapping["profile_id"]),
                "release_id": release_id,
                "document_id": document_id,
                "profile_id": mapping["profile_id"],
                "mapping_type": mapping["mapping_type"],
                "values_inherited": mapping["mapping_type"] == "references_profile",
                "values_verified": profile_attributes.get("approval_status") == "approved"
                or profile.get("profile_status") == "verified",
                "attributes": status,
            }
        )

    evidence: list[dict[str, Any]] = []
    for row in legacy_rows:
        document_id = document_id_by_slash[row["slash_sheet"]]
        evidence.append(
            {
                "id": stable_uuid("torque_fact_evidence", release_id, row["torque_key"]),
                "entity_type": "document_torque_fact",
                "entity_id": document_id,
                "fact_name": row["context"],
                "document_id": document_id,
                "page_number": row.get("source_page"),
                "source_text": row.get("torque_text"),
                "confidence": None,
                "attributes": row,
            }
        )
    return profiles, values, links, evidence


def build_release_payload(
    extractions: list[dict[str, Any]],
    *,
    release_name: str,
    created_from_run_id: str | None = None,
    release_status: str = "staged",
    metadata_by_slash: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    metadata_by_slash = metadata_by_slash or {}
    release_id = stable_uuid("release", SPEC_FAMILY, release_name)
    documents, document_id_by_slash = build_documents(extractions, metadata_by_slash)
    runs, outputs, extraction_evidence, _ = build_extraction_layer(extractions, document_id_by_slash, release_name)
    configurations, wires, hardware, relationships, _ = build_catalog_configurations(
        extractions,
        document_id_by_slash,
        release_id,
    )
    profiles, profile_values, profile_links, torque_evidence = build_torque_catalog(
        extractions,
        document_id_by_slash,
        release_id,
    )

    return {
        "publish.releases": [
            {
                "id": release_id,
                "spec_family": SPEC_FAMILY,
                "release_name": release_name,
                "created_from_run_id": created_from_run_id,
                "status": release_status,
                "notes": "Generated from fresh extraction outputs for platform v2 canonical model.",
                "published_at": utc_now() if release_status == "published" else None,
            }
        ],
        "publish.active_releases": [
            {
                "spec_family": SPEC_FAMILY,
                "release_id": release_id,
            }
        ],
        "ingest.documents": documents,
        "ingest.document_chunks": build_document_chunks(extractions, document_id_by_slash),
        "extract.extraction_runs": runs,
        "extract.extraction_outputs": outputs,
        "extract.extraction_evidence": extraction_evidence,
        "catalog.configurations": configurations,
        "catalog.wire_options": wires,
        "catalog.hardware_options": hardware,
        "catalog.torque_profiles": profiles,
        "catalog.torque_profile_values": profile_values,
        "catalog.document_profile_links": profile_links,
        "catalog.mating_relationships": relationships,
        "catalog.fact_evidence": torque_evidence,
    }


def summarize_payload(payload: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    config_counts = Counter(row["slash_sheet"] for row in payload["catalog.configurations"])
    wire_counts: Counter[str] = Counter()
    config_slash_by_id = {row["id"]: row["slash_sheet"] for row in payload["catalog.configurations"]}
    for row in payload["catalog.wire_options"]:
        wire_counts[config_slash_by_id[row["configuration_id"]]] += 1
    return {
        "table_counts": {table_name: len(rows) for table_name, rows in payload.items()},
        "configuration_counts_by_slash": dict(sorted(config_counts.items(), key=lambda item: slash_sort_key(item[0]))),
        "wire_counts_by_slash": dict(sorted(wire_counts.items(), key=lambda item: slash_sort_key(item[0]))),
    }


def write_payloads(output_dir: Path, payload: dict[str, list[dict[str, Any]]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for table_name, rows in payload.items():
        write_json(output_dir / f"{table_name.replace('.', '__')}.json", rows)
    write_json(output_dir / "summary.json", summarize_payload(payload))


def main() -> int:
    args = parse_args()
    run_id = args.run_id or utc_run_id()
    release_name = args.release_name or f"83513-v2-{run_id}"
    extractions = load_extractions(args.outputs_dir)
    metadata_by_slash = load_document_metadata(args.documents_json)
    payload = build_release_payload(
        extractions,
        release_name=release_name,
        created_from_run_id=args.created_from_run_id or run_id,
        release_status=args.release_status,
        metadata_by_slash=metadata_by_slash,
    )
    output_dir = args.staging_root / run_id / "v2_payloads"
    write_payloads(output_dir, payload)
    summary = summarize_payload(payload)
    print(f"Built v2 release payload: {release_name}")
    print(f"Output: {output_dir}")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
