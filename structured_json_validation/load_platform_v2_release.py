#!/usr/bin/env python3
"""Load platform v2 release payload JSON into Supabase schemas."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pdf_storage.sync_83513_to_supabase import get_server_key, load_env_file, require_env


DEFAULT_ENV_FILE = REPO_ROOT / ".env.local"
LOAD_ORDER = [
    "publish.releases",
    "ingest.documents",
    "ingest.document_chunks",
    "extract.extraction_runs",
    "extract.extraction_outputs",
    "extract.extraction_evidence",
    "catalog.configurations",
    "catalog.wire_options",
    "catalog.hardware_options",
    "catalog.torque_profiles",
    "catalog.torque_profile_values",
    "catalog.document_profile_links",
    "catalog.mating_relationships",
    "catalog.fact_evidence",
    "publish.active_releases",
]
CONFLICT_COLUMNS = {
    "publish.active_releases": "spec_family",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load platform v2 release payloads into Supabase.")
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--payload-dir", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--apply", action="store_true", help="Write to Supabase. Without this flag, dry-run only.")
    return parser.parse_args()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def payload_path(payload_dir: Path, table_name: str) -> Path:
    return payload_dir / f"{table_name.replace('.', '__')}.json"


def load_payloads(payload_dir: Path) -> dict[str, list[dict[str, Any]]]:
    payloads: dict[str, list[dict[str, Any]]] = {}
    for table_name in LOAD_ORDER:
        path = payload_path(payload_dir, table_name)
        if not path.exists():
            raise RuntimeError(f"Missing payload file: {path}")
        payload = read_json(path)
        if not isinstance(payload, list):
            raise RuntimeError(f"Expected JSON list in {path}")
        payloads[table_name] = payload
    return payloads


class SchemaRestClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key

    def headers(self, schema: str) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Profile": schema,
            "Accept-Profile": schema,
            "content-type": "application/json",
        }

    def request(
        self,
        method: str,
        table_name: str,
        *,
        payload: Any | None = None,
        query: list[tuple[str, str]] | None = None,
        prefer: str | None = None,
    ) -> list[dict[str, Any]]:
        schema, table = table_name.split(".", 1)
        url = f"{self.supabase_url}/rest/v1/{table}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = self.headers(schema)
        if prefer:
            headers["Prefer"] = prefer
        request = urllib.request.Request(url, headers=headers, data=data, method=method)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {table_name} failed with HTTP {exc.code}: {body}") from exc
        return json.loads(body) if body else []

    def upsert(self, table_name: str, rows: list[dict[str, Any]], on_conflict: str) -> None:
        if not rows:
            return
        self.request(
            "POST",
            table_name,
            payload=rows,
            query=[("on_conflict", on_conflict)],
            prefer="resolution=merge-duplicates,return=minimal",
        )


def client_from_env(env_file: Path) -> SchemaRestClient:
    env = load_env_file(env_file)
    supabase_url = require_env(env, "SUPABASE_URL")
    server_key = get_server_key(env) or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
    if not server_key:
        raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    return SchemaRestClient(supabase_url, server_key)


def table_summary(payloads: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    return {table_name: len(rows) for table_name, rows in payloads.items()}


def load_batches(client: SchemaRestClient, payloads: dict[str, list[dict[str, Any]]], batch_size: int) -> None:
    for table_name in LOAD_ORDER:
        rows = payloads[table_name]
        on_conflict = CONFLICT_COLUMNS.get(table_name, "id")
        for offset in range(0, len(rows), batch_size):
            client.upsert(table_name, rows[offset : offset + batch_size], on_conflict)


def main() -> int:
    args = parse_args()
    payloads = load_payloads(args.payload_dir)
    summary = table_summary(payloads)
    if not args.apply:
        print(json.dumps(summary, indent=2, sort_keys=True))
        print("Dry run only. Re-run with --apply to write V2 payloads.")
        return 0

    client = client_from_env(args.env_file)
    load_batches(client, payloads, args.batch_size)
    print(f"Loaded platform v2 payloads from {args.payload_dir}")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
