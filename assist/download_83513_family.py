#!/usr/bin/env python3
"""Download the latest base-letter PDF for every discovered MIL-DTL-83513 document."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from assist.assist_83513_common import (
    AssistSession,
    build_output_name,
    download_latest_revision_pdf,
    load_catalog,
    utc_timestamp,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the latest base-letter PDF for every document in 83513_documents.json."
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        default=Path(__file__).resolve().parent / "83513_documents.json",
        help="Path to the discovery JSON file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory where PDFs should be saved.",
    )
    parser.add_argument(
        "--metadata-out",
        type=Path,
        default=Path(__file__).resolve().parent / "83513_downloads.json",
        help="Where to save the download metadata summary.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap for testing a smaller number of documents.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.75,
        help="Pause between document downloads to reduce transient ASSIST blocks.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.catalog.exists():
        print(f"Catalog not found: {args.catalog}", file=sys.stderr)
        return 1

    try:
        catalog = load_catalog(args.catalog)
        documents = catalog["documents"]
        if args.limit is not None:
            documents = documents[: args.limit]

        session = AssistSession()
        results = []
        for index, document in enumerate(documents, start=1):
            document_key = document["document_key"]
            ident_number = document["ident_number"]
            print(f"[{index}/{len(documents)}] {document['doc_id']} ({ident_number})")
            resolved = download_latest_revision_pdf(
                ident_number=ident_number,
                document_key=document_key,
                output_dir=args.output_dir,
                session=session,
            )
            file_name = build_output_name(document_key, resolved.revision_letter)
            output_path = args.output_dir / file_name
            results.append(
                {
                    "document_key": document_key,
                    "slash_sheet": document["slash_sheet"],
                    "doc_id": document["doc_id"],
                    "ident_number": ident_number,
                    "catalog_doc_date": document["doc_date"],
                    "downloaded_revision": resolved.revision_letter,
                    "downloaded_revision_date": resolved.revision_date.date().isoformat(),
                    "file_name": file_name,
                    "saved_to": str(output_path),
                    "pdf_url": resolved.pdf_url,
                    "details_url": resolved.details_url,
                }
            )
            if args.delay_seconds > 0 and index < len(documents):
                time.sleep(args.delay_seconds)

        metadata = {
            "generated_at_utc": utc_timestamp(),
            "catalog": str(args.catalog),
            "output_dir": str(args.output_dir),
            "document_count": len(results),
            "downloads": results,
        }
        args.metadata_out.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        print(f"Download failed: {exc}", file=sys.stderr)
        return 1

    print(f"Saved {len(results)} PDFs to {args.output_dir}")
    print(f"Wrote metadata to {args.metadata_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
