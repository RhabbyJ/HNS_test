#!/usr/bin/env python3
"""Backfill torque values into existing MIL-DTL-83513 extraction JSON outputs."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from hybrid_extraction.m83513_extraction_engine import extract_pages, parse_torque_values
from pdf_storage.sync_83513_to_supabase import (
    create_supabase_client,
    get_server_key,
    load_env_file,
    require_env,
)


DEFAULT_OUTPUTS_DIR = Path(__file__).resolve().parent / "outputs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Add torque_values to existing extraction JSON outputs.")
    parser.add_argument("--outputs-dir", type=Path, default=DEFAULT_OUTPUTS_DIR)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env.local")
    parser.add_argument(
        "--local-pdf-dir",
        type=Path,
        help="Optional directory of local PDFs to use before Supabase Storage, keyed as MIL-DTL-83513H.pdf or MIL-DTL-83513_<n>.pdf.",
    )
    parser.add_argument(
        "--from-pdfs",
        action="store_true",
        help="Download source PDFs from Supabase Storage and rebuild torque values from current PDF text.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def pages_from_chunks(extraction: dict[str, Any]) -> list[str]:
    pages: dict[int, list[str]] = {}
    for chunk in extraction.get("chunks", []):
        page_number = int(chunk["page_number"])
        pages.setdefault(page_number, [])
        pages[page_number].append(chunk["text"])
    if not pages:
        return []
    return [" ".join(pages[page_number]) for page_number in sorted(pages)]


def slash_sheet_key(extraction: dict[str, Any]) -> str:
    document_key = extraction["source"]["document_key"]
    if document_key == "base":
        return "base"
    return str(int(document_key))


def current_storage_paths(env: dict[str, str]) -> dict[str, str]:
    supabase_url = require_env(env, "SUPABASE_URL").rstrip("/")
    server_key = get_server_key(env)
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    params = [
        ("select", "slash_sheet,storage_path"),
        ("spec_family", "eq.MIL-DTL-83513"),
        ("status", "eq.active"),
        ("is_latest", "eq.true"),
    ]
    request = urllib.request.Request(
        f"{supabase_url}/rest/v1/pdf_objects?{urllib.parse.urlencode(params)}",
        headers={"apikey": server_key, "Authorization": f"Bearer {server_key}"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        rows = json.loads(response.read())
    return {str(row["slash_sheet"]): row["storage_path"] for row in rows}


def local_pdf_path(document_key: str, local_pdf_dir: Path | None) -> Path | None:
    if not local_pdf_dir:
        return None
    candidates: list[Path]
    if document_key == "base":
        candidates = [local_pdf_dir / "MIL-DTL-83513H.pdf"]
    else:
        number = int(document_key)
        candidates = [
            local_pdf_dir / f"MIL-DTL-83513_{number}.pdf",
            local_pdf_dir / f"new_MIL-DTL-83513_{number}.pdf",
        ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def storage_pdf_pages(
    extraction: dict[str, Any],
    supabase,
    storage_paths: dict[str, str],
    local_pdf_dir: Path | None,
) -> tuple[list[str], str]:
    source = extraction["source"]
    local_path = local_pdf_path(source["document_key"], local_pdf_dir)
    storage_path = storage_paths.get(slash_sheet_key(extraction)) or source.get("storage_path")
    if local_path:
        return extract_pages(local_path.read_bytes()), storage_path
    if not storage_path:
        raise RuntimeError(f"Missing storage_path for {source.get('spec_sheet')}")
    pdf_bytes = supabase.storage.from_("mil-spec-pdfs").download(storage_path)
    return extract_pages(pdf_bytes), storage_path


def with_torque_values_after_wire_options(extraction: dict[str, Any], torque_values: list[dict[str, Any]]) -> dict[str, Any]:
    updated: dict[str, Any] = {}
    inserted = False
    for key, value in extraction.items():
        if key == "torque_values":
            continue
        updated[key] = value
        if key == "wire_options":
            updated["torque_values"] = torque_values
            inserted = True
    if not inserted:
        updated["torque_values"] = torque_values
    return updated


def main() -> int:
    args = parse_args()
    paths = sorted(args.outputs_dir.glob("m83513_*_extraction_output.json"))
    updated_count = 0
    total_torque_values = 0
    supabase = None
    storage_paths: dict[str, str] = {}

    if args.from_pdfs:
        env = load_env_file(args.env_file)
        supabase_url = require_env(env, "SUPABASE_URL")
        server_key = get_server_key(env)
        if not server_key:
            raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
        supabase = create_supabase_client(supabase_url, server_key)
        storage_paths = current_storage_paths(env)

    for path in paths:
        extraction = load_json(path)
        storage_path_changed = False
        if args.from_pdfs:
            pages, current_storage_path = storage_pdf_pages(extraction, supabase, storage_paths, args.local_pdf_dir)
            storage_path_changed = extraction["source"].get("storage_path") != current_storage_path
            extraction["source"]["storage_path"] = current_storage_path
        else:
            pages = pages_from_chunks(extraction)
        torque_values = [asdict(value) for value in parse_torque_values(pages)]
        total_torque_values += len(torque_values)

        if extraction.get("torque_values") == torque_values and not storage_path_changed:
            print(f"{path.name}: unchanged ({len(torque_values)} torque rows)")
            continue

        updated_count += 1
        storage_note = " storage_path updated" if storage_path_changed else ""
        print(f"{path.name}: {len(torque_values)} torque rows{storage_note}")
        if not args.dry_run:
            updated = with_torque_values_after_wire_options(extraction, torque_values)
            path.write_text(json.dumps(updated, indent=2), encoding="utf-8")

    print(f"Updated files: {updated_count}")
    print(f"Total torque rows: {total_torque_values}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
