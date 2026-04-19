#!/usr/bin/env python3
"""Discover all active base/slash-sheet MIL-DTL-83513 documents via Playwright."""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path

from assist_83513_common import SEARCH_URL, parse_assist_date, parse_search_doc_id, sort_document_key, utc_timestamp

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover
    sync_playwright = None


IDENT_NUMBER_PATTERN = re.compile(r"ident_number=(\d+)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover current active MIL-DTL-83513 base/slash-sheet documents."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parent / "83513_documents.json",
        help="Path to the discovery JSON file.",
    )
    parser.add_argument(
        "--search-term",
        default="MIL-DTL-83513",
        help="Document ID search term to submit to ASSIST Quick Search.",
    )
    return parser.parse_args()


def require_playwright() -> None:
    if sync_playwright is not None:
        return

    raise RuntimeError(
        "Playwright is not installed for this Python interpreter. "
        "Install it with 'python -m pip install playwright' and "
        "'python -m playwright install chromium'."
    )


def extract_ident_number(href: str | None) -> str | None:
    if not href:
        return None

    match = IDENT_NUMBER_PATTERN.search(href)
    if not match:
        return None
    return match.group(1)


def discover_documents(search_term: str) -> dict:
    require_playwright()
    records_by_key: dict[str, dict] = {}

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        page.fill("#DocumentIDTextBox", search_term)
        page.locator("#DocumentIDTextBox").press("Tab")
        page.click("#GetFilteredButton")
        page.wait_for_function(
            "() => document.querySelectorAll('tr.grid_item, tr.grid_alternate').length > 0",
            timeout=60000,
        )
        page.wait_for_timeout(1500)

        rows = page.locator("tr.grid_item, tr.grid_alternate")
        for index in range(rows.count()):
            row = rows.nth(index)
            cells = row.locator("td")
            if cells.count() < 6:
                continue

            doc_id = cells.nth(1).inner_text().strip()
            parsed_doc = parse_search_doc_id(doc_id)
            if not parsed_doc:
                continue

            status = cells.nth(2).inner_text().strip()
            if status.upper() != "A":
                continue

            document_key, revision_letter = parsed_doc
            doc_date = parse_assist_date(cells.nth(4).inner_text())
            title = cells.nth(5).inner_text().strip()
            href = row.locator("a[href*='qsDocDetails.aspx?ident_number=']").first.get_attribute("href")
            ident_number = extract_ident_number(href)
            if not ident_number:
                continue

            record = {
                "document_key": document_key,
                "slash_sheet": None if document_key == "base" else document_key,
                "doc_id": doc_id,
                "ident_number": ident_number,
                "status": status,
                "doc_date": doc_date.date().isoformat(),
                "current_doc_revision": revision_letter,
                "title": title,
                "details_url": urllib.parse.urljoin(SEARCH_URL, href),
            }

            existing = records_by_key.get(document_key)
            existing_doc_date = (
                datetime.fromisoformat(existing["doc_date"]) if existing else None
            )
            if not existing or doc_date > existing_doc_date:
                records_by_key[document_key] = record

        browser.close()

    documents = [
        records_by_key[key]
        for key in sorted(records_by_key, key=sort_document_key)
    ]

    return {
        "generated_at_utc": utc_timestamp(),
        "search_term": search_term,
        "search_url": SEARCH_URL,
        "document_count": len(documents),
        "documents": documents,
    }


def main() -> int:
    args = parse_args()

    try:
        catalog = discover_documents(args.search_term)
    except Exception as exc:  # pragma: no cover
        print(f"Discovery failed: {exc}", file=sys.stderr)
        return 1

    args.output.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    print(f"Saved {catalog['document_count']} documents to {args.output}")
    for document in catalog["documents"]:
        key = document["slash_sheet"] or "base"
        print(f"{key:>4}  {document['ident_number']:>6}  {document['doc_id']}  {document['doc_date']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
