#!/usr/bin/env python3
"""Run golden mate-finder checks against the local FastAPI app contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from web_app.api.main import app


DEFAULT_CASES_PATH = Path(__file__).resolve().parent / "golden_mate_cases_83513.json"
DEFAULT_REPORT_PATH = Path(__file__).resolve().parent / "golden_mate_report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run golden mate-finder checks for MIL-DTL-83513.")
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES_PATH, help="Golden-case JSON file.")
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT_PATH, help="Where to write the JSON report.")
    return parser.parse_args()


def load_cases(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def representative_search_item(item: dict[str, Any]) -> dict[str, Any]:
    representative = item.get("representative_variant")
    if isinstance(representative, dict):
        return representative
    return item


def resolve_part_id(client: TestClient, selector: dict[str, Any]) -> tuple[str | None, list[dict[str, Any]]]:
    params = {"q": selector["example_full_pin"], "limit": 10}
    if selector.get("slash_sheet"):
        params["slash_sheet"] = selector["slash_sheet"]
    response = client.get("/search", params=params)
    response.raise_for_status()
    items = response.json()["items"]
    exact_matches = [
        representative_search_item(item)
        for item in items
        if representative_search_item(item).get("example_full_pin") == selector["example_full_pin"]
    ]
    if not exact_matches:
        return None, items
    return exact_matches[0]["id"], items


def summarize_duplicates(mates: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(mate["slash_sheet"] for mate in mates)
    return {slash_sheet: count for slash_sheet, count in counts.items() if count > 1}


def run_case(client: TestClient, case: dict[str, Any]) -> dict[str, Any]:
    part_id, search_items = resolve_part_id(client, case["selector"])
    if not part_id:
        return {
            "case_id": case["case_id"],
            "status": "unresolved_part",
            "selector": case["selector"],
            "search_items": search_items,
            "expected_valid_slash_sheets": case["expected_valid_slash_sheets"],
            "expected_invalid_slash_sheets": case["expected_invalid_slash_sheets"],
            "false_positives": [],
            "false_negatives": case["expected_valid_slash_sheets"],
            "duplicate_variant_noise": {},
            "source_evidence": case["source_evidence"],
        }

    part_response = client.get(f"/parts/{part_id}")
    part_response.raise_for_status()
    part_json = part_response.json()

    raw_mate_response = client.get(f"/parts/{part_id}/mates", params={"grouped": "false"})
    raw_mate_response.raise_for_status()
    raw_mate_json = raw_mate_response.json()
    mates = raw_mate_json["raw_variants"]

    grouped_mate_response = client.get(f"/parts/{part_id}/mates")
    grouped_mate_response.raise_for_status()
    grouped_mate_json = grouped_mate_response.json()
    grouped_mates = grouped_mate_json["mates"]

    returned_unique = sorted({mate["slash_sheet"] for mate in mates})
    expected_valid = sorted(case["expected_valid_slash_sheets"])
    expected_invalid = sorted(case["expected_invalid_slash_sheets"])

    false_positives = sorted(
        slash_sheet
        for slash_sheet in returned_unique
        if slash_sheet not in expected_valid
    )
    false_negatives = sorted(
        slash_sheet
        for slash_sheet in expected_valid
        if slash_sheet not in returned_unique
    )
    explicit_invalid_hits = sorted(
        slash_sheet
        for slash_sheet in returned_unique
        if slash_sheet in expected_invalid
    )
    duplicate_variant_noise = summarize_duplicates(mates)

    status = "pass"
    if false_positives or false_negatives:
        status = "fail"
    elif duplicate_variant_noise:
        status = "pass_with_duplicate_noise"

    return {
        "case_id": case["case_id"],
        "status": status,
        "selector": case["selector"],
        "part_id": part_id,
        "part_spec_sheet": part_json["spec_sheet"],
        "part_name": part_json["name"],
        "expected_valid_slash_sheets": expected_valid,
        "expected_invalid_slash_sheets": expected_invalid,
        "returned_unique_slash_sheets": returned_unique,
        "returned_mate_count": len(mates),
        "grouped_unique_slash_sheets": sorted({mate["mate_slash_sheet"] for mate in grouped_mates}),
        "grouped_mate_count": len(grouped_mates),
        "false_positives": false_positives,
        "false_negatives": false_negatives,
        "explicit_invalid_hits": explicit_invalid_hits,
        "duplicate_variant_noise": duplicate_variant_noise,
        "why": case["why"],
        "source_evidence": case["source_evidence"],
        "raw_variants": mates,
        "grouped_mates": grouped_mates,
    }


def summarize_report(results: list[dict[str, Any]]) -> dict[str, Any]:
    failing = [result for result in results if result["status"] == "fail"]
    duplicate_noise = [result for result in results if result["duplicate_variant_noise"]]
    unresolved = [result for result in results if result["status"] == "unresolved_part"]
    grouped_product_pass = [
        result
        for result in results
        if result["grouped_unique_slash_sheets"] == result["expected_valid_slash_sheets"]
    ]
    return {
        "total_cases": len(results),
        "pass_count": sum(1 for result in results if result["status"] == "pass"),
        "pass_with_duplicate_noise_count": sum(1 for result in results if result["status"] == "pass_with_duplicate_noise"),
        "fail_count": len(failing),
        "unresolved_count": len(unresolved),
        "duplicate_noise_case_count": len(duplicate_noise),
        "grouped_product_pass_count": len(grouped_product_pass),
        "failing_case_ids": [result["case_id"] for result in failing],
        "duplicate_noise_case_ids": [result["case_id"] for result in duplicate_noise],
    }


def main() -> int:
    args = parse_args()
    cases = load_cases(args.cases)
    client = TestClient(app)

    results = [run_case(client, case) for case in cases]
    summary = summarize_report(results)

    report = {
        "summary": summary,
        "results": results,
    }
    args.report_out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    return 1 if summary["fail_count"] or summary["unresolved_count"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
