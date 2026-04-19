#!/usr/bin/env python3
"""Load structured /03 extraction JSON into normalized M83513 tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sync_83513_to_supabase import create_supabase_client, get_server_key, load_env_file, require_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load /03 extraction JSON into M83513 normalized tables.")
    parser.add_argument("--input-json", type=Path, required=True, help="Path to the /03 extraction JSON.")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parent / ".env.local")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write to Supabase. Without this flag the loader prints a dry-run summary only.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def finish_map(extraction: dict) -> dict[str, str]:
    return {
        item["code"]: item["description"]
        for item in extraction.get("pin_components", {}).get("shell_finish_options", [])
    }


def insert_arrangement_map(extraction: dict) -> dict[int, str]:
    return {
        item["cavity_count"]: item["insert_arrangement"]
        for item in extraction.get("pin_components", {}).get("insert_arrangements", [])
    }


def current_rating(extraction: dict) -> float | None:
    value = extraction.get("attributes", {}).get("current_rating_per_contact")
    return float(value) if value is not None else None


def base_rows_for_03(extraction: dict) -> list[dict]:
    source = extraction["source"]
    title = source["title"]
    finish_descriptions = finish_map(extraction)
    insert_map = insert_arrangement_map(extraction)
    attributes = extraction.get("attributes", {})
    figures = extraction.get("figure_references", [])
    mates_with = extraction.get("mates_with", [])
    rows: list[dict] = []

    for config in extraction.get("configuration_rows", []):
        cavity_count = int(config["cavity_count"])
        shell_size = config["shell_size_letter"]
        insert_arrangement = insert_map.get(cavity_count)
        source_page = int(config["page_number"])

        for finish_code, finish_description in finish_descriptions.items():
            example_full_pin = None
            if insert_arrangement:
                example_full_pin = f"M83513/03-{insert_arrangement}01{finish_code}"

            rows.append(
                {
                    "spec_family": "83513",
                    "spec_sheet": source["spec_sheet"],
                    "slash_sheet": source["document_key"].zfill(2),
                    "connector_type": "PLUG_PIN_CRIMP",
                    "name": f"Micro-D Plug {cavity_count}-Pin Class M Crimp",
                    "description": f"MICRO-D, PLUG, {cavity_count} PIN, CLASS M, CRIMP, METAL SHELL, {finish_description.upper()}",
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


def wire_rows_for_03(extraction: dict, base_config_id: str) -> list[dict]:
    rows: list[dict] = []
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


def chunk_rows(extraction: dict) -> list[dict]:
    source = extraction["source"]
    rows: list[dict] = []
    for chunk in extraction.get("chunks", []):
        chunk_index = int(chunk["chunk_id"].split("-")[-1])
        rows.append(
            {
                "spec_sheet": source["spec_sheet"],
                "slash_sheet": source["document_key"].zfill(2),
                "revision": source["revision"],
                "page_number": chunk["page_number"],
                "chunk_index": chunk_index,
                "text_content": chunk["text"],
                "source_url": source["source_url"],
                "storage_path": source["storage_path"],
            }
        )
    return rows


def extraction_run_row(extraction: dict) -> dict:
    source = extraction["source"]
    return {
        "spec_sheet": source["spec_sheet"],
        "slash_sheet": source["document_key"].zfill(2),
        "revision": source["revision"],
        "extraction_method": extraction["extraction_method"],
        "confidence_score": extraction["confidence_score"],
        "llm_fallback_required": extraction["llm_fallback_required"],
        "issues": extraction.get("issues", []),
    }


def ensure_03_document(extraction: dict) -> None:
    if extraction["source"]["document_key"] != "3":
        raise RuntimeError("This loader currently supports slash sheet /03 only.")


def apply_rows(extraction: dict, env_file: Path) -> None:
    env = load_env_file(env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    supabase = create_supabase_client(supabase_url, server_key)

    base_rows = base_rows_for_03(extraction)
    chunk_payload = chunk_rows(extraction)
    run_payload = extraction_run_row(extraction)

    for base_row in base_rows:
        inserted = (
            supabase.table("m83513_base_configurations")
            .upsert(base_row, on_conflict="spec_sheet,cavity_count,shell_size_letter,shell_finish_code")
            .execute()
        )
        base_config_id = inserted.data[0]["id"]
        wire_payload = wire_rows_for_03(extraction, base_config_id)
        if wire_payload:
            supabase.table("m83513_hns_wire_options").delete().eq("base_config_id", base_config_id).execute()
            supabase.table("m83513_hns_wire_options").insert(wire_payload).execute()

    if chunk_payload:
        supabase.table("m83513_text_chunks").upsert(
            chunk_payload,
            on_conflict="spec_sheet,page_number,chunk_index",
        ).execute()
    supabase.table("m83513_extraction_runs").insert(run_payload).execute()


def main() -> int:
    args = parse_args()
    try:
        extraction = load_json(args.input_json)
        ensure_03_document(extraction)
        base_rows = base_rows_for_03(extraction)
        wire_count = len(extraction.get("wire_options", []))
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
