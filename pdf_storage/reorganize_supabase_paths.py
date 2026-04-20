#!/usr/bin/env python3
"""Move existing Supabase objects to ordered base/01/02... paths and update pdf_objects."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from assist.assist_83513_common import (
    build_output_name,
    sort_order_for_document_key,
    storage_document_label,
)
from pdf_storage.sync_83513_to_supabase import (
    create_supabase_client,
    get_server_key,
    load_env_file,
    optional_env,
    require_env,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reorganize existing Supabase storage paths for MIL-DTL-83513 objects."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".env.local",
        help="Path to the local environment file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the planned moves without modifying Supabase.",
    )
    return parser.parse_args()


def download_object(storage_bucket, path: str) -> bytes:
    return storage_bucket.download(path)


def upload_object(storage_bucket, path: str, payload: bytes) -> None:
    storage_bucket.upload(
        path=path,
        file=payload,
        file_options={
            "content-type": "application/pdf",
            "cache-control": "3600",
            "upsert": "true",
        },
    )


def delete_object(storage_bucket, path: str) -> None:
    storage_bucket.remove([path])


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
        table_name = require_env(env, "SUPABASE_METADATA_TABLE")

        supabase = create_supabase_client(supabase_url, supabase_server_key)
        rows = (
            supabase.table(table_name)
            .select("id,spec_family,slash_sheet,revision_letter,storage_path")
            .eq("spec_family", "MIL-DTL-83513")
            .execute()
        )
        storage_bucket = supabase.storage.from_(bucket_name)

        moves = []
        for row in rows.data:
            document_key = row["slash_sheet"] or "base"
            storage_label = storage_document_label(document_key)
            file_name = build_output_name(document_key, row["revision_letter"])
            desired_path = f"{storage_prefix}/{storage_label}/{file_name}"
            if row["storage_path"] == desired_path:
                continue
            moves.append((row, desired_path))

        if not moves:
            print("No path updates needed.")
            return 0

        for index, (row, desired_path) in enumerate(moves, start=1):
            print(f"[{index}/{len(moves)}] {row['storage_path']} -> {desired_path}")
            if args.dry_run:
                continue

            payload = download_object(storage_bucket, row["storage_path"])
            upload_object(storage_bucket, desired_path, payload)
            delete_object(storage_bucket, row["storage_path"])
            supabase.table(table_name).update(
                {
                    "storage_path": desired_path,
                    "sort_order": sort_order_for_document_key(row["slash_sheet"] or "base"),
                }
            ).eq("id", row["id"]).execute()

        if args.dry_run:
            print("Dry run only; no Supabase changes applied.")
            return 0
    except Exception as exc:  # pragma: no cover
        print(f"Reorganization failed: {exc}", file=sys.stderr)
        return 1

    print(f"Moved {len(moves)} objects into ordered storage paths.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
