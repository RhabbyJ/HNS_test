#!/usr/bin/env python3
"""Discover MIL-DTL-83513 documents and sync the latest base revision PDFs to Supabase."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

from assist.assist_83513_common import (
    AssistSession,
    build_output_name,
    download_latest_revision_bytes,
    storage_document_label,
    sort_document_key,
    sort_order_for_document_key,
    utc_timestamp,
)
from assist.discover_83513 import discover_documents


FAMILY_NAME = "MIL-DTL-83513"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover MIL-DTL-83513 documents and upload the latest base revision PDFs to Supabase Storage."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / ".env.local",
        help="Path to the local environment file.",
    )
    parser.add_argument(
        "--catalog-out",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "assist" / "artifacts" / "83513_documents.json",
        help="Where to save the refreshed discovery catalog.",
    )
    parser.add_argument(
        "--sync-report-out",
        type=Path,
        default=Path(__file__).resolve().parent / "artifacts" / "83513_supabase_sync.json",
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


class SimpleResponse:
    def __init__(self, data):
        self.data = data


class SimpleStorageBucket:
    def __init__(self, client: "SimpleSupabaseClient", bucket_name: str):
        self._client = client
        self._bucket_name = bucket_name

    def _object_url(self, path: str) -> str:
        encoded_path = "/".join(urllib.parse.quote(part) for part in path.split("/"))
        return f"{self._client.url}/storage/v1/object/{self._bucket_name}/{encoded_path}"

    def download(self, path: str) -> bytes:
        request = urllib.request.Request(
            self._object_url(path),
            headers=self._client.auth_headers(),
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()

    def upload(self, path: str, file: bytes, file_options: dict | None = None) -> SimpleResponse:
        file_options = file_options or {}
        headers = self._client.auth_headers()
        headers["content-type"] = file_options.get("content-type", "application/octet-stream")
        if str(file_options.get("upsert", "")).lower() == "true":
            headers["x-upsert"] = "true"
        request = urllib.request.Request(
            self._object_url(path),
            headers=headers,
            data=file,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
        return SimpleResponse(json.loads(body) if body else None)

    def remove(self, paths: list[str]) -> SimpleResponse:
        encoded = json.dumps({"prefixes": paths}).encode("utf-8")
        request = urllib.request.Request(
            f"{self._client.url}/storage/v1/object/{self._bucket_name}",
            headers={**self._client.auth_headers(), "content-type": "application/json"},
            data=encoded,
            method="DELETE",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
        return SimpleResponse(json.loads(body) if body else None)


class SimpleStorageApi:
    def __init__(self, client: "SimpleSupabaseClient"):
        self._client = client

    def from_(self, bucket_name: str) -> SimpleStorageBucket:
        return SimpleStorageBucket(self._client, bucket_name)


class SimpleTableQuery:
    def __init__(self, client: "SimpleSupabaseClient", table_name: str):
        self._client = client
        self._table_name = table_name
        self._method = "GET"
        self._payload = None
        self._filters: list[tuple[str, str]] = []
        self._headers: dict[str, str] = {}
        self._query: dict[str, str] = {}

    def insert(self, payload):
        self._method = "POST"
        self._payload = payload
        self._headers["Prefer"] = "return=representation"
        return self

    def upsert(self, payload, on_conflict: str | None = None):
        self._method = "POST"
        self._payload = payload
        prefer = "resolution=merge-duplicates,return=representation"
        self._headers["Prefer"] = prefer
        if on_conflict:
            self._query["on_conflict"] = on_conflict
        return self

    def delete(self):
        self._method = "DELETE"
        self._headers["Prefer"] = "return=representation"
        return self

    def eq(self, column: str, value):
        quoted = urllib.parse.quote(str(value), safe="")
        self._filters.append((column, f"eq.{quoted}"))
        return self

    def execute(self) -> SimpleResponse:
        query_items = list(self._query.items()) + self._filters
        query_string = urllib.parse.urlencode(query_items, doseq=True)
        url = f"{self._client.url}/rest/v1/{self._table_name}"
        if query_string:
            url = f"{url}?{query_string}"

        payload = None
        headers = self._client.auth_headers()
        headers.update(self._headers)
        if self._payload is not None:
            payload = json.dumps(self._payload).encode("utf-8")
            headers["content-type"] = "application/json"

        request = urllib.request.Request(url, headers=headers, data=payload, method=self._method)
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read()
        return SimpleResponse(json.loads(body) if body else [])


class SimpleSupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.service_role_key = service_role_key
        self.storage = SimpleStorageApi(self)

    def auth_headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
        }

    def table(self, table_name: str) -> SimpleTableQuery:
        return SimpleTableQuery(self, table_name)


def create_supabase_client(url: str, service_role_key: str):
    try:
        from supabase import create_client
    except ImportError:  # pragma: no cover
        return SimpleSupabaseClient(url, service_role_key)

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
                    expected_revision_letter=document.get("current_doc_revision"),
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
