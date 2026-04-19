#!/usr/bin/env python3
"""Phase-1 PDF-first extraction engine for MIL-DTL-83513."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from m83513_extraction_registry import (
    ALLOWED_CAVITY_COUNTS,
    ALLOWED_FINISH_CODES,
    DocumentTypeSpec,
    document_type_for_key,
)
from sync_83513_to_supabase import create_supabase_client, get_server_key, load_env_file, require_env


PART_NUMBER_PATTERN = re.compile(r"M83513/\d{1,2}(?:\s*-\s*[A-Z0-9]+)+")
REVISION_PATTERN = re.compile(
    r"MIL-DTL-83513(?:/\d{1,2})?(?P<revision>[A-Z])(?:\(\d+\))?(?:\s+NOT\s+\d+)?$",
    re.IGNORECASE,
)
MATE_PATTERN = re.compile(r"MIL-DTL-83513/\d{1,2}", re.IGNORECASE)
WIRE_PATTERN = re.compile(r"M22759/\d{1,2}-\d{1,2}-\d{1,2}", re.IGNORECASE)
PAGE_HEADER_PATTERN = re.compile(r"^\s*(.+?)\s*$", re.MULTILINE)
PIN_HEADER_PATTERN = re.compile(r"M83513/(?P<slash>\d{1,2})\s*-\s*(?P<insert>[A-Z])\s*(?P<wire>\d{2})\s*(?P<finish>[A-Z])")
FIGURE_PATTERN = re.compile(r"FIGURE\s+(?P<figure_no>\d+)\.?\s*(?P<title>[^.\n]+)?", re.IGNORECASE)
INSERT_MAP_PATTERN = re.compile(r"(?P<insert>[A-H])\s*=\s*(?P<cavity>9|15|21|25|31|37|51|100)\b")
FINISH_MAP_PATTERN = re.compile(r"(?P<code>[ACKNPT])\s*=\s*(?P<description>[A-Za-z][A-Za-z ,()/-]+)")
WIRE_ROW_START_PATTERN = re.compile(r"(?P<code>\d{2})\s*=\s*")
WIRE_NOTE_SPLIT_PATTERN = re.compile(r"(?=(?:^|\s)(\d)/\s)")
CURRENT_RATING_PATTERN = re.compile(r"Current rating, maximum:\s*(?P<amps>\d+(?:\.\d+)?)\s*amperes per contact", re.IGNORECASE)
CANONICAL_FINISH_DESCRIPTIONS = {
    "A": "Pure electrodeposited aluminum",
    "C": "Cadmium",
    "K": "Zinc nickel",
    "N": "Electroless nickel",
    "P": "Passivated stainless steel",
    "T": "Nickel fluorocarbon polymer",
}


@dataclass(frozen=True)
class ExtractionSource:
    spec_sheet: str
    document_key: str
    document_type: str
    title: str
    source_url: str
    storage_path: str
    revision: str | None = None


@dataclass(frozen=True)
class PageExtraction:
    page_number: int
    text_length: int
    detected_headers: list[str]
    dimension_hits: dict[str, float]
    cavity_counts: list[int]
    example_parts: list[str]
    mates_with: list[str]
    wire_specs: list[str]
    figure_references: list[dict[str, Any]]


@dataclass(frozen=True)
class ExtractionIssue:
    severity: str
    code: str
    message: str
    page_number: int | None = None


@dataclass(frozen=True)
class ChunkRecord:
    chunk_id: str
    page_number: int
    text: str


@dataclass
class ExtractionResult:
    source: ExtractionSource
    connector_type: str
    cavity_counts: list[int] = field(default_factory=list)
    dimensions: dict[str, float] = field(default_factory=dict)
    mates_with: list[str] = field(default_factory=list)
    example_parts: list[str] = field(default_factory=list)
    finish_codes: list[str] = field(default_factory=list)
    wire_specs: list[str] = field(default_factory=list)
    pin_components: dict[str, Any] = field(default_factory=dict)
    configuration_rows: list[dict[str, Any]] = field(default_factory=list)
    wire_options: list[dict[str, Any]] = field(default_factory=list)
    figure_references: list[dict[str, Any]] = field(default_factory=list)
    attributes: dict[str, Any] = field(default_factory=dict)
    page_summaries: list[PageExtraction] = field(default_factory=list)
    chunks: list[ChunkRecord] = field(default_factory=list)
    issues: list[ExtractionIssue] = field(default_factory=list)
    field_presence: dict[str, bool] = field(default_factory=dict)
    confidence_score: float = 0.0
    llm_fallback_required: bool = False
    llm_fallback_reason: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run phase-1 MIL-DTL-83513 extraction from a local PDF or Supabase Storage object."
    )
    parser.add_argument("--pdf", type=Path, help="Path to a local PDF file.")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parent / ".env.local")
    parser.add_argument("--bucket", default="mil-spec-pdfs")
    parser.add_argument("--storage-path", help="Supabase Storage path if pulling directly from the bucket.")
    parser.add_argument("--document-key", required=True, help="Document key such as base, 3, 15, or 33.")
    parser.add_argument("--spec-sheet", required=True, help="Full spec sheet, e.g. MIL-DTL-83513/3K.")
    parser.add_argument("--title", required=True, help="Document title as shown in ASSIST.")
    parser.add_argument("--source-url", required=True, help="Original ASSIST detail URL.")
    parser.add_argument("--output-json", type=Path, default=Path("m83513_extraction_output.json"))
    return parser.parse_args()


def load_pdf_bytes(args: argparse.Namespace) -> bytes:
    if args.pdf:
        return args.pdf.read_bytes()
    if args.storage_path:
        env = load_env_file(args.env_file)
        supabase_url = require_env(env, "SUPABASE_URL")
        server_key = get_server_key(env)
        if not server_key:
            raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY for storage download.")
        supabase = create_supabase_client(supabase_url, server_key)
        return supabase.storage.from_(args.bucket).download(args.storage_path)
    raise RuntimeError("Provide either --pdf or --storage-path.")


def extract_pages(pdf_bytes: bytes) -> list[str]:
    try:
        import pdfplumber
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "pdfplumber is required for extraction. Install it with 'python -m pip install -r requirements.txt'."
        ) from exc

    pages: list[str] = []
    with pdfplumber.open(_bytes_to_io(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return pages


def _bytes_to_io(pdf_bytes: bytes):
    from io import BytesIO

    return BytesIO(pdf_bytes)


def normalize_headers(text: str) -> list[str]:
    headers: list[str] = []
    for raw_line in PAGE_HEADER_PATTERN.findall(text):
        line = " ".join(raw_line.split())
        if len(line) < 3 or len(line) > 100:
            continue
        upper_line = line.upper()
        if upper_line == line and any(char.isalpha() for char in upper_line):
            headers.append(upper_line)
            continue
        if any(token in upper_line for token in ("HOW TO ORDER", "DIMENSIONS", "FIGURE", "REQUIREMENTS", "NOTES")):
            headers.append(upper_line)
    return list(dict.fromkeys(headers[:20]))


def normalize_example_part(text: str) -> str:
    normalized = re.sub(r"\s+", "", text.upper())
    normalized = normalized.replace("–", "-")
    return normalized


def extract_example_parts(text: str) -> list[str]:
    candidates = {normalize_example_part(match.group(0)) for match in PART_NUMBER_PATTERN.finditer(text)}
    return sorted(
        {
            candidate
            for candidate in candidates
            if re.fullmatch(r"M83513/\d{2}-[A-Z]\d{2}[A-Z]", candidate)
        }
    )


def extract_dimension_hits(text: str) -> dict[str, float]:
    hits: dict[str, float] = {}
    for match in re.finditer(r"\b([A-D])\s*[=:]?\s*(\d+\.\d+)\b", text):
        hits.setdefault(match.group(1), float(match.group(2)))
    return hits


def extract_cavity_counts(text: str) -> list[int]:
    found = {value for value in ALLOWED_CAVITY_COUNTS if re.search(rf"\b{value}\b", text)}
    return sorted(found)


def extract_finish_codes(text: str) -> list[str]:
    found = {code for code in ALLOWED_FINISH_CODES if re.search(rf"\b{code}\b", text)}
    return sorted(found)


def extract_figure_references(text: str, page_number: int) -> list[dict[str, Any]]:
    figures: list[dict[str, Any]] = []
    for match in FIGURE_PATTERN.finditer(text):
        title = " ".join((match.group("title") or "").split()).strip(" -")
        figures.append(
            {
                "figure_number": int(match.group("figure_no")),
                "label": f"Figure {match.group('figure_no')}",
                "title": title or None,
                "page_number": page_number,
            }
        )
    return figures


def build_page_summary(page_number: int, text: str) -> PageExtraction:
    return PageExtraction(
        page_number=page_number,
        text_length=len(text),
        detected_headers=normalize_headers(text),
        dimension_hits=extract_dimension_hits(text),
        cavity_counts=extract_cavity_counts(text),
        example_parts=extract_example_parts(text),
        mates_with=sorted({match.upper() for match in MATE_PATTERN.findall(text)}),
        wire_specs=sorted({match.upper() for match in WIRE_PATTERN.findall(text)}),
        figure_references=extract_figure_references(text, page_number),
    )


def build_chunks(pages: list[str]) -> list[ChunkRecord]:
    chunks: list[ChunkRecord] = []
    for page_number, text in enumerate(pages, start=1):
        normalized = " ".join(text.split())
        if not normalized:
            continue
        chunk_size = 1800
        for offset in range(0, len(normalized), chunk_size):
            chunk_text = normalized[offset : offset + chunk_size]
            chunk_id = f"page-{page_number}-chunk-{offset // chunk_size + 1}"
            chunks.append(ChunkRecord(chunk_id=chunk_id, page_number=page_number, text=chunk_text))
    return chunks


def current_mate_reference(document_key: str) -> str:
    if document_key == "base":
        return "MIL-DTL-83513"
    return f"MIL-DTL-83513/{int(document_key)}"


def aggregate_figure_references(page_summaries: list[PageExtraction]) -> list[dict[str, Any]]:
    deduped: dict[int, dict[str, Any]] = {}
    for page in page_summaries:
        for figure in page.figure_references:
            existing = deduped.get(figure["figure_number"])
            if existing is None or (not existing.get("title") and figure.get("title")):
                deduped[figure["figure_number"]] = figure
    return [deduped[key] for key in sorted(deduped)]


def parse_configuration_rows(pages: list[str]) -> list[dict[str, Any]]:
    normalized = " ".join(pages)
    tokens = normalized.split()
    rows: list[dict[str, Any]] = []
    for index, token in enumerate(tokens):
        if token not in {"9", "15", "21", "25", "31", "37", "51", "100"}:
            continue
        next_tokens = tokens[index + 1 : index + 6]
        if len(next_tokens) < 5:
            continue
        if not all(re.fullmatch(r"\d*\.\d+", item) for item in next_tokens[:4]):
            continue
        if not re.fullmatch(r"[A-Z]", next_tokens[4]):
            continue
        cavity_count = int(token)
        rows.append(
            {
                "page_number": 4,
                "cavity_count": cavity_count,
                "shell_size_letter": next_tokens[4].upper(),
                "dimensions": {
                    "A": float(next_tokens[0]),
                    "B": float(next_tokens[1]),
                    "C": float(next_tokens[2]),
                    "D": float(next_tokens[3]),
                    "unit": "inch",
                },
            }
        )
    deduped = {(row["cavity_count"], row["shell_size_letter"]): row for row in rows}
    return [deduped[key] for key in sorted(deduped, key=lambda item: item[0])]


def infer_dimensions(configuration_rows: list[dict[str, Any]]) -> dict[str, float]:
    if not configuration_rows:
        return {}
    return configuration_rows[0]["dimensions"]


def parse_pin_components(pages: list[str], document_key: str) -> dict[str, Any]:
    text = "\n".join(pages)
    header_match = PIN_HEADER_PATTERN.search(text)
    insert_map = [
        {"insert_arrangement": match.group("insert"), "cavity_count": int(match.group("cavity"))}
        for match in INSERT_MAP_PATTERN.finditer(text)
    ]

    finish_map: list[dict[str, str]] = []
    seen_finish_codes: set[str] = set()
    for match in FINISH_MAP_PATTERN.finditer(text):
        code = match.group("code").upper()
        if code not in ALLOWED_FINISH_CODES or code in seen_finish_codes:
            continue
        seen_finish_codes.add(code)
        description = " ".join(match.group("description").split())
        canonical = CANONICAL_FINISH_DESCRIPTIONS.get(code)
        finish_map.append({"code": code, "description": canonical or description})

    if not finish_map:
        finish_map = [
            {"code": code, "description": description}
            for code, description in CANONICAL_FINISH_DESCRIPTIONS.items()
        ]

    return {
        "prefix": f"M83513/{int(document_key):02d}",
        "format_example": normalize_example_part(header_match.group(0)) if header_match else None,
        "components": ["insert_arrangement", "wire_type_code", "shell_finish_code"],
        "insert_arrangements": insert_map,
        "shell_finish_options": finish_map,
    }


def parse_wire_note_map(pages: list[str]) -> dict[str, str]:
    text = " ".join(pages[8:])
    note_text = text[text.find("1/ ") :] if "1/ " in text else text
    notes: dict[str, str] = {}
    positions = list(WIRE_NOTE_SPLIT_PATTERN.finditer(note_text))
    for index, match in enumerate(positions):
        note_no = match.group(1)
        start = match.start()
        end = positions[index + 1].start() if index + 1 < len(positions) else len(note_text)
        segment = " ".join(note_text[start:end].split())
        if not segment.startswith(f"{note_no}/"):
            continue
        notes[note_no] = segment[len(f"{note_no}/") :].strip()
    return notes


def parse_wire_options(pages: list[str]) -> list[dict[str, Any]]:
    text = " ".join(pages[6:8])
    wire_note_map = parse_wire_note_map(pages)
    options: list[dict[str, Any]] = []
    row_starts = list(WIRE_ROW_START_PATTERN.finditer(text))
    for index, match in enumerate(row_starts):
        code = match.group("code")
        start = match.end()
        end = row_starts[index + 1].start() if index + 1 < len(row_starts) else len(text)
        segment = " ".join(text[start:end].split())
        if "See notes at end of wire type" in segment:
            segment = segment.split("See notes at end of wire type", 1)[0].strip()

        length_match = re.search(r"\s(?P<length>0\.5|1\.0|\d{2})\s(?P<notes>(?:\d+/[, ]*)+)", segment)
        if not length_match:
            continue

        spec = segment[: length_match.start()].strip(" ,")
        note_refs = re.findall(r"(\d+)/", length_match.group("notes"))
        option = {
            "wire_type_code": code,
            "wire_specification": spec,
            "wire_length_inches": float(length_match.group("length")),
            "note_refs": note_refs,
            "note_texts": [wire_note_map[ref] for ref in note_refs if ref in wire_note_map],
            "is_space_approved": any("space applications" in wire_note_map.get(ref, "").lower() for ref in note_refs),
        }
        options.append(option)
    deduped = {option["wire_type_code"]: option for option in options}
    return [deduped[key] for key in sorted(deduped)]


def infer_attributes(source: ExtractionSource, pages: list[str], configuration_rows: list[dict[str, Any]]) -> dict[str, Any]:
    title_upper = source.title.upper()
    joined = "\n".join(pages)
    current_rating_match = CURRENT_RATING_PATTERN.search(joined)
    insert_map = {row["shell_size_letter"]: row["cavity_count"] for row in configuration_rows}
    shell_material = "Metal" if "CLASS M" in title_upper else "Plastic" if "CLASS P" in title_upper else None
    gender = "Plug" if "PLUG" in title_upper else "Receptacle" if "RECEPTACLE" in title_upper else None
    contact_type = "Pin" if "PIN CONTACTS" in title_upper else "Socket" if "SOCKET CONTACTS" in title_upper else None
    return {
        "shell_material": shell_material,
        "gender": gender,
        "class": "M" if "CLASS M" in title_upper else "P" if "CLASS P" in title_upper else None,
        "contact_type": contact_type,
        "current_rating_per_contact": float(current_rating_match.group("amps")) if current_rating_match else None,
        "polarization": "Standard polarized shell",
        "insert_arrangement_map": insert_map,
        "mounting_hardware_ref": "MIL-DTL-83513/5" if "MIL-DTL-83513/5" in joined else None,
    }


def score_result(spec: DocumentTypeSpec, result: ExtractionResult) -> tuple[float, list[ExtractionIssue]]:
    issues: list[ExtractionIssue] = []
    field_presence = {
        "spec_sheet": bool(result.source.spec_sheet),
        "revision": bool(result.source.revision),
        "title": bool(result.source.title),
        "connector_type": bool(result.connector_type),
        "cavity_counts": bool(result.cavity_counts),
        "dimensions": bool(result.dimensions),
        "mates_with": bool(result.mates_with),
        "example_parts": bool(result.example_parts),
        "configuration_rows": bool(result.configuration_rows),
        "pin_components": bool(result.pin_components.get("insert_arrangements")),
        "wire_options": bool(result.wire_options),
        "figure_references": bool(result.figure_references),
    }
    result.field_presence = field_presence

    score = 1.0
    for field_name in spec.required_fields:
        if not field_presence.get(field_name, False):
            score -= 0.12
            issues.append(ExtractionIssue("error", "missing_required_field", f"Missing required field: {field_name}"))

    if spec.document_type == "plug_receptacle":
        for field_name in ("configuration_rows", "pin_components", "wire_options", "figure_references"):
            if not field_presence[field_name]:
                score -= 0.08
                issues.append(ExtractionIssue("warning", "missing_structured_field", f"Missing structured field: {field_name}"))

    found_headers = {header for page in result.page_summaries for header in page.detected_headers}
    missing_headers = [header for header in spec.expected_headers if not any(header in found for found in found_headers)]
    for header in missing_headers:
        score -= 0.03
        issues.append(ExtractionIssue("warning", "missing_header", f"Expected header not found: {header}"))

    if result.cavity_counts and any(value not in ALLOWED_CAVITY_COUNTS for value in result.cavity_counts):
        score -= 0.20
        issues.append(ExtractionIssue("error", "invalid_cavity_count", "Found unsupported cavity count value."))

    if not result.page_summaries:
        score = 0.0
        issues.append(ExtractionIssue("error", "empty_document", "No page text was extracted."))

    return max(score, 0.0), issues


def extract_phase_one(args: argparse.Namespace) -> ExtractionResult:
    pdf_bytes = load_pdf_bytes(args)
    pages = extract_pages(pdf_bytes)
    document_spec = document_type_for_key(args.document_key)
    revision_match = REVISION_PATTERN.search(args.spec_sheet.upper())
    source = ExtractionSource(
        spec_sheet=args.spec_sheet,
        document_key=args.document_key,
        document_type=document_spec.document_type,
        title=args.title,
        source_url=args.source_url,
        storage_path=args.storage_path or str(args.pdf),
        revision=revision_match.group("revision") if revision_match else None,
    )

    page_summaries = [build_page_summary(page_number, text) for page_number, text in enumerate(pages, start=1)]
    configuration_rows = parse_configuration_rows(pages)
    pin_components = parse_pin_components(pages, args.document_key)
    example_parts = sorted({value for page in page_summaries for value in page.example_parts})
    if pin_components.get("format_example"):
        example_parts.append(pin_components["format_example"])
        example_parts = sorted(dict.fromkeys(example_parts))

    result = ExtractionResult(
        source=source,
        connector_type=document_spec.connector_type,
        cavity_counts=sorted({count for page in page_summaries for count in page.cavity_counts}),
        dimensions=infer_dimensions(configuration_rows),
        mates_with=sorted(
            {
                value
                for page in page_summaries
                for value in page.mates_with
                if value != current_mate_reference(args.document_key)
            }
        ),
        example_parts=example_parts,
        finish_codes=extract_finish_codes("\n".join(pages)),
        wire_specs=sorted({value for page in page_summaries for value in page.wire_specs}),
        configuration_rows=configuration_rows,
        pin_components=pin_components,
        wire_options=parse_wire_options(pages),
        figure_references=aggregate_figure_references(page_summaries),
        attributes=infer_attributes(source, pages, configuration_rows),
        page_summaries=page_summaries,
        chunks=build_chunks(pages),
    )

    confidence, issues = score_result(document_spec, result)
    result.confidence_score = round(confidence, 2)
    result.issues.extend(issues)
    if confidence < 0.85:
        result.llm_fallback_required = True
        result.llm_fallback_reason = "Low deterministic confidence or missing required structured fields."
    return result


def result_to_jsonable(result: ExtractionResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["extraction_method"] = "pdf_first"
    payload["accepted_without_llm"] = not result.llm_fallback_required
    return payload


def main() -> int:
    args = parse_args()
    try:
        result = extract_phase_one(args)
        args.output_json.write_text(json.dumps(result_to_jsonable(result), indent=2), encoding="utf-8")
    except Exception as exc:  # pragma: no cover
        print(f"Extraction failed: {exc}", file=sys.stderr)
        return 1

    print(f"Wrote extraction output to {args.output_json}")
    print(f"Confidence score: {result.confidence_score:.2f}")
    print(f"LLM fallback required: {result.llm_fallback_required}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
