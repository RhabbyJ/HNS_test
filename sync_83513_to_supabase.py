#!/usr/bin/env python3
"""Discover MIL-DTL-83513 documents and sync the latest base revision PDFs to Supabase."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

from assist_83513_common import (
    AssistSession,
    build_output_name,
    download_latest_revision_bytes,
    storage_document_label,
    sort_document_key,
    sort_order_for_document_key,
    utc_timestamp,
)
from discover_83513 import discover_documents


FAMILY_NAME = "MIL-DTL-83513"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover MIL-DTL-83513 documents and upload the latest base revision PDFs to Supabase Storage."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).resolve().parent / ".env.local",
        help="Path to the local environment file.",
    )
    parser.add_argument(
        "--catalog-out",
        type=Path,
        default=Path(__file__).resolve().parent / "83513_documents.json",
        help="Where to save the refreshed discovery catalog.",
    )
    parser.add_argument(
        "--sync-report-out",
        type=Path,
        default=Path(__file__).resolve().parent / "83513_supabase_sync.json",
        help="Where to write the sync report JSON.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap for testing only the first N documents.",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        raise RuntimeError(f"Environment file not found: {path}")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")

    return env


def require_env(env: dict[str, str], key: str) -> str:
    value = env.get(key) or os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required configuration value: {key}")
    return value


def optional_env(env: dict[str, str], key: str, default: str = "") -> str:
    return env.get(key) or os.environ.get(key) or default


def get_server_key(env: dict[str, str]) -> str:
    return (
        env.get("SUPABASE_SECRET_KEY")
        or os.environ.get("SUPABASE_SECRET_KEY")
        or env.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    )


def create_supabase_client(url: str, service_role_key: str):
    try:
        from supabase import create_client
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Supabase client is not installed. Run 'python -m pip install supabase'."
        ) from exc

    return create_client(url, service_role_key)


def upload_pdf(storage_api, bucket_name: str, storage_path: str, pdf_bytes: bytes) -> None:
    storage_api.from_(bucket_name).upload(
        path=storage_path,
        file=pdf_bytes,
        file_options={
            "content-type": "application/pdf",
            "cache-control": "3600",
            "upsert": "true",
        },
    )


def metadata_payload(document: dict, bucket_name: str, storage_path: str, resolved) -> dict:
    return {
        "spec_family": FAMILY_NAME,
        "slash_sheet": document["slash_sheet"] or "base",
        "sort_order": sort_order_for_document_key(document["document_key"]),
        "revision_letter": resolved.revision_letter,
        "document_date": resolved.revision_date.date().isoformat(),
        "title": document["title"],
        "source_doc_id": document["doc_id"],
        "source_ident_number": document["ident_number"],
        "source_url": resolved.details_url,
        "bucket_name": bucket_name,
        "storage_path": storage_path,
        "status": "active",
        "is_latest": True,
        "last_checked_at": utc_timestamp(),
    }


def upsert_metadata(database_api, table_name: str, payload: dict) -> None:
    database_api.table(table_name).upsert(
        payload,
        on_conflict="spec_family,slash_sheet",
    ).execute()


def main() -> int:
    args = parse_args()

    try:
        env = load_env_file(args.env_file)
        supabase_url = require_env(env, "SUPABASE_URL")
        supabase_server_key = get_server_key(env)
        if not supabase_server_key:
            raise RuntimeError(
                "Missing required configuration value: SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY"
            )
        bucket_name = require_env(env, "SUPABASE_STORAGE_BUCKET")
        storage_prefix = optional_env(env, "SUPABASE_STORAGE_PREFIX", "mil-dtl-83513")
        metadata_table = optional_env(env, "SUPABASE_METADATA_TABLE", "")
        search_term = optional_env(env, "ASSIST_SEARCH_TERM", "MIL-DTL-83513")
        delay_seconds = float(optional_env(env, "SYNC_DELAY_SECONDS", "0.75"))

        supabase = create_supabase_client(supabase_url, supabase_server_key)
        catalog = discover_documents(search_term)
        documents = sorted(
            catalog["documents"],
            key=lambda item: sort_document_key(item["document_key"]),
        )
        if args.limit is not None:
            documents = documents[: args.limit]

        args.catalog_out.write_text(json.dumps(catalog, indent=2), encoding="utf-8")

        session = AssistSession()
        report = {
            "generated_at_utc": utc_timestamp(),
            "catalog_out": str(args.catalog_out),
            "document_count": len(documents),
            "storage_bucket": bucket_name,
            "storage_prefix": storage_prefix,
            "results": [],
        }

        for index, document in enumerate(documents, start=1):
            document_key = document["document_key"]
            print(f"[{index}/{len(documents)}] Syncing {document['doc_id']} ({document['ident_number']})")
            try:
                downloaded = download_latest_revision_bytes(
                    ident_number=document["ident_number"],
                    document_key=document_key,
                    session=session,
                )
                file_name = build_output_name(document_key, downloaded.resolved.revision_letter)
                storage_label = storage_document_label(document_key)
                storage_path = f"{storage_prefix}/{storage_label}/{file_name}"
                upload_pdf(supabase.storage, bucket_name, storage_path, downloaded.pdf_bytes)

                metadata_row = metadata_payload(
                    document=document,
                    bucket_name=bucket_name,
                    storage_path=storage_path,
                    resolved=downloaded.resolved,
                )
                metadata_row["file_size_bytes"] = len(downloaded.pdf_bytes)
                metadata_row["checksum"] = hashlib.sha256(downloaded.pdf_bytes).hexdigest()
                if metadata_table:
                    upsert_metadata(supabase, metadata_table, metadata_row)

                report["results"].append(
                    {
                        "document_key": document_key,
                        "doc_id": document["doc_id"],
                        "ident_number": document["ident_number"],
                        "status": "uploaded",
                        "storage_path": storage_path,
                        "downloaded_revision": downloaded.resolved.revision_letter,
                        "downloaded_revision_date": downloaded.resolved.revision_date.date().isoformat(),
                    }
                )
            except Exception as exc:
                report["results"].append(
                    {
                        "document_key": document_key,
                        "doc_id": document["doc_id"],
                        "ident_number": document["ident_number"],
                        "status": "failed",
                        "error": str(exc),
                    }
                )
                print(f"Failed: {document['doc_id']} ({document['ident_number']}): {exc}", file=sys.stderr)

            if delay_seconds > 0 and index < len(documents):
                time.sleep(delay_seconds)

        report["success_count"] = sum(1 for item in report["results"] if item["status"] == "uploaded")
        report["failure_count"] = sum(1 for item in report["results"] if item["status"] == "failed")
        args.sync_report_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        print(f"Sync failed: {exc}", file=sys.stderr)
        return 1

    print(
        f"Uploaded {report['success_count']} of {report['document_count']} documents to Supabase bucket {bucket_name}."
    )
    print(f"Wrote sync report to {args.sync_report_out}")
    return 0 if report["failure_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
