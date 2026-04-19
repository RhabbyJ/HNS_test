#!/usr/bin/env python3
"""Load structured MIL-DTL-83513 extraction JSON into normalized extracted-spec tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from sync_83513_to_supabase import create_supabase_client, get_server_key, load_env_file, require_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load MIL-DTL-83513 extraction JSON into normalized tables.")
    parser.add_argument("--input-json", type=Path, required=True, help="Path to the extraction JSON.")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parent / ".env.local")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write to Supabase. Without this flag the loader prints a dry-run summary only.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def slash_sheet_value(extraction: dict[str, Any]) -> str:
    key = extraction["source"]["document_key"]
    return "base" if key == "base" else key.zfill(2)


def finish_map(extraction: dict[str, Any]) -> dict[str, str]:
    return {
        item["code"]: item["description"]
        for item in extraction.get("pin_components", {}).get("shell_finish_options", [])
    }


def insert_arrangement_map(extraction: dict[str, Any]) -> dict[int, str]:
    return {
        item["cavity_count"]: item["insert_arrangement"]
        for item in extraction.get("pin_components", {}).get("insert_arrangements", [])
    }


def current_rating(extraction: dict[str, Any]) -> float | None:
    value = extraction.get("attributes", {}).get("current_rating_per_contact")
    return float(value) if value is not None else None


def connector_type_code(extraction: dict[str, Any]) -> str:
    attributes = extraction.get("attributes", {})
    gender = (attributes.get("gender") or "").upper()
    contact_type = (attributes.get("contact_type") or "").upper()
    if gender == "PLUG" and contact_type == "PIN":
        return "PLUG_PIN_CRIMP"
    if gender == "PLUG" and contact_type == "SOCKET":
        return "PLUG_SOCKET_CRIMP"
    if gender == "RECEPTACLE" and contact_type == "PIN":
        return "RECEPTACLE_PIN_CRIMP"
    if gender == "RECEPTACLE" and contact_type == "SOCKET":
        return "RECEPTACLE_SOCKET_CRIMP"
    return "SIGNAL_CONNECTOR"


def connector_name(extraction: dict[str, Any], cavity_count: int) -> str:
    attributes = extraction.get("attributes", {})
    gender = attributes.get("gender") or "Connector"
    contact_type = attributes.get("contact_type") or "Contact"
    class_code = attributes.get("class") or ""
    class_label = f"Class {class_code} " if class_code else ""
    return f"Micro-D {gender} {cavity_count}-Pin {class_label}Crimp ({contact_type})".strip()


def connector_description(extraction: dict[str, Any], cavity_count: int, finish_description: str) -> str:
    attributes = extraction.get("attributes", {})
    gender = (attributes.get("gender") or "Connector").upper()
    contact_type = (attributes.get("contact_type") or "Contact").upper()
    class_code = (attributes.get("class") or "").upper()
    shell_material = (attributes.get("shell_material") or "").upper()
    pieces = [
        "MICRO-D",
        gender,
        f"{cavity_count} PIN",
        f"CLASS {class_code}" if class_code else None,
        "CRIMP",
        shell_material if shell_material else None,
        finish_description.upper(),
        contact_type,
    ]
    return ", ".join(piece for piece in pieces if piece)


def base_rows_for_general_spec(extraction: dict[str, Any]) -> list[dict[str, Any]]:
    source = extraction["source"]
    return [
        {
            "spec_family": "83513",
            "spec_sheet": source["spec_sheet"],
            "slash_sheet": "base",
            "connector_type": "GENERAL_SPECIFICATION",
            "name": "Micro-D General Specification",
            "description": source["title"].upper(),
            "cavity_count": None,
            "shell_size_letter": None,
            "shell_size_description": None,
            "dimensions": None,
            "shell_material": None,
            "shell_finish_code": None,
            "shell_finish_description": None,
            "shell_finish_notes": None,
            "current_rating_per_contact": current_rating(extraction),
            "contact_type": None,
            "gender": None,
            "class": None,
            "polarization": extraction.get("attributes", {}).get("polarization"),
            "mates_with": extraction.get("mates_with", []),
            "mounting_hardware_ref": extraction.get("attributes", {}).get("mounting_hardware_ref"),
            "insert_arrangement_ref": None,
            "source_document": source["spec_sheet"],
            "source_page": 1,
            "source_url": source["source_url"],
            "revision": source["revision"],
            "confidence_score": extraction["confidence_score"],
            "example_full_pin": None,
            "figure_references": extraction.get("figure_references", []),
            "extra_data": {
                "document_type": source["document_type"],
                "finish_codes": extraction.get("finish_codes", []),
            },
        }
    ]


def base_rows_for_plug_receptacle(extraction: dict[str, Any]) -> list[dict[str, Any]]:
    source = extraction["source"]
    finish_descriptions = finish_map(extraction)
    insert_map = insert_arrangement_map(extraction)
    attributes = extraction.get("attributes", {})
    figures = extraction.get("figure_references", [])
    mates_with = extraction.get("mates_with", [])
    prefix = extraction.get("pin_components", {}).get("prefix")
    rows: list[dict[str, Any]] = []

    for config in extraction.get("configuration_rows", []):
        cavity_count = int(config["cavity_count"])
        shell_size = config["shell_size_letter"]
        insert_arrangement = insert_map.get(cavity_count)
        source_page = int(config["page_number"])

        for finish_code, finish_description in finish_descriptions.items():
            example_full_pin = None
            if prefix and insert_arrangement:
                example_full_pin = f"{prefix}-{insert_arrangement}01{finish_code}"

            rows.append(
                {
                    "spec_family": "83513",
                    "spec_sheet": source["spec_sheet"],
                    "slash_sheet": slash_sheet_value(extraction),
                    "connector_type": connector_type_code(extraction),
                    "name": connector_name(extraction, cavity_count),
                    "description": connector_description(extraction, cavity_count, finish_description),
                    "cavity_count": cavity_count,
                    "shell_size_letter": shell_size,
                    "shell_size_description": f"{cavity_count} position",
                    "dimensions": config["dimensions"],
                    "shell_material": attributes.get("shell_material"),
                    "shell_finish_code": finish_code,
                    "shell_finish_description": finish_description,
                    "shell_finish_notes": "Interface critical shell finish from slash-sheet PIN.",
                    "current_rating_per_contact": current_rating(extraction),
                    "contact_type": attributes.get("contact_type"),
                    "gender": attributes.get("gender"),
                    "class": attributes.get("class"),
                    "polarization": attributes.get("polarization"),
                    "mates_with": mates_with,
                    "mounting_hardware_ref": attributes.get("mounting_hardware_ref"),
                    "insert_arrangement_ref": insert_arrangement,
                    "source_document": source["spec_sheet"],
                    "source_page": source_page,
                    "source_url": source["source_url"],
                    "revision": source["revision"],
                    "confidence_score": extraction["confidence_score"],
                    "example_full_pin": example_full_pin,
                    "figure_references": figures,
                    "extra_data": {
                        "pin_components": extraction.get("pin_components"),
                        "document_type": source["document_type"],
                    },
                }
            )

    return rows


def base_rows_for_extraction(extraction: dict[str, Any]) -> list[dict[str, Any]]:
    document_type = extraction["source"]["document_type"]
    if document_type == "base_spec":
        return base_rows_for_general_spec(extraction)
    if document_type == "plug_receptacle":
        return base_rows_for_plug_receptacle(extraction)
    raise RuntimeError(f"Unsupported document type for loader: {document_type}")


def wire_rows_for_base(extraction: dict[str, Any], base_config_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for option in extraction.get("wire_options", []):
        rows.append(
            {
                "base_config_id": base_config_id,
                "wire_type_code": option["wire_type_code"],
                "wire_specification": option["wire_specification"],
                "wire_length_inches": option["wire_length_inches"],
                "wire_notes": " | ".join(option.get("note_texts", [])) or None,
                "is_space_approved": option.get("is_space_approved", False),
            }
        )
    return rows


def chunk_rows(extraction: dict[str, Any]) -> list[dict[str, Any]]:
    source = extraction["source"]
    rows: list[dict[str, Any]] = []
    for chunk in extraction.get("chunks", []):
        chunk_index = int(chunk["chunk_id"].split("-")[-1])
        rows.append(
            {
                "spec_family": "83513",
                "spec_sheet": source["spec_sheet"],
                "slash_sheet": slash_sheet_value(extraction),
                "revision": source["revision"],
                "page_number": chunk["page_number"],
                "chunk_index": chunk_index,
                "text_content": chunk["text"],
                "source_url": source["source_url"],
                "storage_path": source["storage_path"],
            }
        )
    return rows


def extraction_run_row(extraction: dict[str, Any]) -> dict[str, Any]:
    source = extraction["source"]
    return {
        "spec_family": "83513",
        "spec_sheet": source["spec_sheet"],
        "slash_sheet": slash_sheet_value(extraction),
        "revision": source["revision"],
        "extraction_method": extraction["extraction_method"],
        "confidence_score": extraction["confidence_score"],
        "llm_fallback_required": extraction["llm_fallback_required"],
        "issues": extraction.get("issues", []),
    }


def apply_rows(extraction: dict[str, Any], env_file: Path) -> None:
    env = load_env_file(env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    supabase = create_supabase_client(supabase_url, server_key)

    source = extraction["source"]
    slash_sheet = slash_sheet_value(extraction)
    base_rows = base_rows_for_extraction(extraction)
    chunk_payload = chunk_rows(extraction)
    run_payload = extraction_run_row(extraction)

    supabase.table("base_configurations").delete().eq("spec_sheet", source["spec_sheet"]).eq("slash_sheet", slash_sheet).execute()

    inserted_rows: list[dict[str, Any]] = []
    for base_row in base_rows:
        inserted = supabase.table("base_configurations").insert(base_row).execute()
        inserted_rows.append(inserted.data[0])

    for inserted_row in inserted_rows:
        wire_payload = wire_rows_for_base(extraction, inserted_row["id"])
        if wire_payload:
            supabase.table("hns_wire_options").insert(wire_payload).execute()

    if chunk_payload:
        supabase.table("text_chunks").upsert(
            chunk_payload,
            on_conflict="spec_family,spec_sheet,page_number,chunk_index",
        ).execute()
    supabase.table("extraction_runs").insert(run_payload).execute()


def main() -> int:
    args = parse_args()
    try:
        extraction = load_json(args.input_json)
        base_rows = base_rows_for_extraction(extraction)
        wire_count = len(extraction.get("wire_options", []))
        print(f"Document type: {extraction['source']['document_type']}")
        print(f"Base configuration rows: {len(base_rows)}")
        print(f"Wire options per base row: {wire_count}")
        print(f"Chunk rows: {len(extraction.get('chunks', []))}")

        if not args.apply:
            print("Dry run only. Re-run with --apply to write to Supabase.")
            if base_rows:
                print(json.dumps(base_rows[0], indent=2))
            return 0

        apply_rows(extraction, args.env_file)
    except Exception as exc:  # pragma: no cover
        print(f"Loader failed: {exc}", file=sys.stderr)
        return 1

    print("Loader completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
