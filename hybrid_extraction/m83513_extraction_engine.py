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

from hybrid_extraction.m83513_extraction_registry import (
    ALLOWED_CAVITY_COUNTS,
    ALLOWED_FINISH_CODES,
    DocumentTypeSpec,
    document_type_for_key,
)
from pdf_storage.sync_83513_to_supabase import (
    create_supabase_client,
    get_server_key,
    load_env_file,
    require_env,
)


PART_NUMBER_PATTERN = re.compile(
    r"M83513/\d{1,2}\s*-\s*[A-Z](?:\s*\d{2})?(?:\s*\d+/)?\s*[A-Z](?:\s*[A-Z])?",
    re.IGNORECASE,
)
MOUNTING_HARDWARE_PATTERN = re.compile(r"M83513/05-\d{2}(?:RP)?", re.IGNORECASE)
REVISION_PATTERN = re.compile(
    r"MIL-DTL-83513(?:/\d{1,2})?(?P<revision>[A-Z])(?:\(\d+\))?(?:\s+NOT\s+\d+)?$",
    re.IGNORECASE,
)
SPEC_SHEET_HEADER_PATTERN = re.compile(
    r"\b(MIL-DTL-83513(?:/\d{1,2})?[A-Z])\b",
    re.IGNORECASE,
)
MATE_PATTERN = re.compile(r"MIL-DTL-83513/\d{1,2}", re.IGNORECASE)
WIRE_PATTERN = re.compile(r"M22759/\d{1,2}-\d{1,2}-\d{1,2}", re.IGNORECASE)
PAGE_HEADER_PATTERN = re.compile(r"^\s*(.+?)\s*$", re.MULTILINE)
PIN_HEADER_PATTERN = re.compile(
    r"M83513/(?P<slash>\d{1,2})\s*-\s*(?P<insert>[A-Z])(?:\s*(?P<wire>\d{2})(?:\s*\d+/)?)?\s*(?P<finish>[A-Z])(?:\s*(?P<hardware>[A-Z]))?",
    re.IGNORECASE,
)
FIGURE_PATTERN = re.compile(r"FIGURE\s+(?P<figure_no>\d+)\.?\s*(?P<title>[^.\n]+)?", re.IGNORECASE)
INSERT_MAP_PATTERN = re.compile(r"(?P<insert>[A-Z])\s*=\s*(?P<cavity>0?9|15|21|25|31|37|51|100)\b")
FINISH_MAP_PATTERN = re.compile(r"(?P<code>[ACKNPT])\s*=\s*(?P<description>[A-Za-z][A-Za-z ,()/-]+)")
TERMINATION_LENGTH_PATTERN = re.compile(r"(?P<code>\d{2})\s*=\s*(?P<length>\.\d{3})")
HARDWARE_OPTION_PATTERN = re.compile(
    r"(?P<code>[NPTW])\s*=\s*(?P<description>No hardware or threaded insert|jackpost attach|threaded insert|jackpost and threaded insert)",
    re.IGNORECASE,
)
VALID_CAVITY_TOKEN_VALUES = {"9", "15", "21", "25", "31", "37", "51", "100"}
INLINE_INSERT_ASSIGNMENT_PATTERN = re.compile(r"\b[A-Z]\s*=\s*(?:9|15|21|25|31|37|51|100)\b")
WIRE_ROW_PATTERN = re.compile(
    r"(?P<code>\d{2})\s*=\s*(?P<body>.*?)(?=(?:\s\d{2}\s*=)|(?:\s1/\s+These connectors)|$)",
    re.IGNORECASE | re.DOTALL,
)
WIRE_NOTE_SPLIT_PATTERN = re.compile(r"(?=(?:^|\s)(\d{1,2})/\s)")
CURRENT_RATING_PATTERN = re.compile(r"Current rating, maximum:\s*(?P<amps>\d+(?:\.\d+)?)\s*amperes per contact", re.IGNORECASE)
CANONICAL_FINISH_DESCRIPTIONS = {
    "A": "Pure electrodeposited aluminum",
    "C": "Cadmium",
    "K": "Zinc nickel",
    "N": "Electroless nickel",
    "P": "Passivated stainless steel",
    "T": "Nickel fluorocarbon polymer",
}
NOTE_BLEED_PATTERNS = (
    re.compile(r"\b\d+\s+MIL-DTL-83513/\d{1,2}[A-Z](?:\(\d+\))?(?:\s+NOT\s+\d+)?\b", re.IGNORECASE),
    re.compile(r"\bSUPERSEDES PAGE\b.*$", re.IGNORECASE),
    re.compile(r"\bAMSC N/A\b.*$", re.IGNORECASE),
)


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
class ValidationCheck:
    status: str
    code: str
    message: str
    details: dict[str, Any] | None = None


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
    validation_checks: list[ValidationCheck] = field(default_factory=list)
    fallback_flags: list[str] = field(default_factory=list)
    field_presence: dict[str, bool] = field(default_factory=dict)
    confidence_score: float = 0.0
    llm_fallback_required: bool = False
    llm_fallback_reason: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run phase-1 MIL-DTL-83513 extraction from a local PDF or Supabase Storage object."
    )
    parser.add_argument("--pdf", type=Path, help="Path to a local PDF file.")
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parents[1] / ".env.local")
    parser.add_argument("--bucket", default="mil-spec-pdfs")
    parser.add_argument("--storage-path", help="Supabase Storage path if pulling directly from the bucket.")
    parser.add_argument("--document-key", required=True, help="Document key such as base, 3, 15, or 33.")
    parser.add_argument("--spec-sheet", required=True, help="Full spec sheet, e.g. MIL-DTL-83513/3K.")
    parser.add_argument("--title", required=True, help="Document title as shown in ASSIST.")
    parser.add_argument("--source-url", required=True, help="Original ASSIST detail URL.")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "structured_json_validation" / "outputs" / "m83513_extraction_output.json",
    )
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
    normalized = re.sub(r"(?<=-[A-Z]\d{2})\d+/", "", normalized)
    return normalized


def extract_example_parts(text: str) -> list[str]:
    candidates = {normalize_example_part(match.group(0)) for match in PART_NUMBER_PATTERN.finditer(text)}
    candidates.update(match.group(0).upper() for match in MOUNTING_HARDWARE_PATTERN.finditer(text))
    return sorted(
        {
            candidate
            for candidate in candidates
            if re.fullmatch(r"M83513/\d{2}-[A-Z](?:\d{2})?[A-Z](?:[A-Z])?", candidate)
            or re.fullmatch(r"M83513/05-\d{2}(?:RP)?", candidate)
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


def detect_spec_sheet_from_pages(pages: list[str]) -> str | None:
    if not pages:
        return None

    first_page = pages[0].upper()
    match = SPEC_SHEET_HEADER_PATTERN.search(first_page)
    if not match:
        return None
    return match.group(1)


def aggregate_figure_references(page_summaries: list[PageExtraction]) -> list[dict[str, Any]]:
    deduped: dict[int, dict[str, Any]] = {}
    for page in page_summaries:
        for figure in page.figure_references:
            existing = deduped.get(figure["figure_number"])
            if existing is None or (not existing.get("title") and figure.get("title")):
                deduped[figure["figure_number"]] = figure
    return [deduped[key] for key in sorted(deduped)]


def trim_note_bleed(text: str) -> str:
    cleaned = " ".join(text.split())
    for pattern in NOTE_BLEED_PATTERNS:
        match = pattern.search(cleaned)
        if match:
            cleaned = cleaned[: match.start()].strip(" ,;:-")
    return cleaned


def has_note_bleed(text: str) -> bool:
    return any(pattern.search(text) for pattern in NOTE_BLEED_PATTERNS)


def parse_configuration_rows(pages: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    def normalized_decimal(token: str) -> str | None:
        if re.fullmatch(r"\d*\.\d+", token):
            return token
        if re.fullmatch(r"\d{4}", token):
            return f".{token}"
        return None

    for page_number, page in enumerate(pages, start=1):
        for raw_line in page.splitlines():
            line = " ".join(raw_line.split())
            if not line:
                continue

            tokens = line.split()
            cavity_token = tokens[0].rstrip(".")
            if cavity_token not in VALID_CAVITY_TOKEN_VALUES:
                continue

            decimals: list[str] = []
            for token in tokens[1:]:
                value = normalized_decimal(token)
                if value:
                    decimals.append(value)
            if len(decimals) < 4:
                continue

            cavity_count = int(cavity_token)
            a_value = float(decimals[0])
            trailing_values = sorted(float(token) for token in decimals[1:4])
            shell_size_letter = next(
                (token.upper() for token in tokens[1:] if re.fullmatch(r"[A-Z]", token)),
                "B" if cavity_count == 51 else "C" if cavity_count == 100 else "A",
            )
            rows.append(
                {
                    "page_number": page_number,
                    "cavity_count": cavity_count,
                    "shell_size_letter": shell_size_letter,
                    "dimensions": {
                        "A": a_value,
                        "B": trailing_values[0],
                        "C": trailing_values[1],
                        "D": trailing_values[2],
                        "unit": "inch",
                    },
                }
            )

        page_tokens = page.split()
        for index, token in enumerate(page_tokens):
            cavity_token = token.rstrip(".")
            if cavity_token not in VALID_CAVITY_TOKEN_VALUES:
                continue

            decimals: list[str] = []
            for next_token in page_tokens[index + 1 : index + 8]:
                value = normalized_decimal(next_token)
                if value:
                    decimals.append(value)
                if len(decimals) == 4:
                    break
            if len(decimals) < 4:
                continue

            cavity_count = int(cavity_token)
            a_value = float(decimals[0])
            trailing_values = sorted(float(token) for token in decimals[1:4])
            rows.append(
                {
                    "page_number": page_number,
                    "cavity_count": cavity_count,
                    "shell_size_letter": "B" if cavity_count == 51 else "C" if cavity_count == 100 else "A",
                    "dimensions": {
                        "A": a_value,
                        "B": trailing_values[0],
                        "C": trailing_values[1],
                        "D": trailing_values[2],
                        "unit": "inch",
                    },
                }
            )
    deduped = {(row["cavity_count"], row["shell_size_letter"]): row for row in rows}
    return [deduped[key] for key in sorted(deduped, key=lambda item: item[0])]


def parse_pcb_configuration_rows(pages: list[str], valid_cavity_counts: set[int] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dimension_labels = ("A", "B", "D", "E", "F", "G", "H")

    for page_number, page in enumerate(pages, start=1):
        for raw_line in page.splitlines():
            line = " ".join(raw_line.split())
            tokens = line.split()
            if not tokens:
                continue

            cavity_token = tokens[0].rstrip(".")
            if cavity_token not in VALID_CAVITY_TOKEN_VALUES:
                continue

            cavity_count = int(cavity_token)
            if valid_cavity_counts and cavity_count not in valid_cavity_counts:
                continue

            decimals = [token for token in tokens[1:] if re.fullmatch(r"\d*\.\d+", token)]
            if len(decimals) < 6:
                continue

            dimensions = {"unit": "inch"}
            for label, value in zip(dimension_labels, decimals[: len(dimension_labels)], strict=False):
                dimensions[label] = float(value)

            rows.append(
                {
                    "page_number": page_number,
                    "cavity_count": cavity_count,
                    "shell_size_letter": None,
                    "dimensions": dimensions,
                }
            )

    deduped = {row["cavity_count"]: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def infer_dimensions(configuration_rows: list[dict[str, Any]]) -> dict[str, float]:
    if not configuration_rows:
        return {}
    return configuration_rows[0]["dimensions"]


def parse_pin_components(pages: list[str], document_key: str, document_type: str) -> dict[str, Any]:
    text = "\n".join(pages)
    if document_key == "base":
        return {
            "prefix": "M83513",
            "format_example": None,
            "components": [],
            "insert_arrangements": [],
            "shell_finish_options": [
                {"code": code, "description": description}
                for code, description in CANONICAL_FINISH_DESCRIPTIONS.items()
            ],
        }

    header_match = PIN_HEADER_PATTERN.search(text)
    insert_map = [
        {"insert_arrangement": match.group("insert"), "cavity_count": int(match.group("cavity").rstrip("."))}
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

    payload = {
        "prefix": f"M83513/{int(document_key):02d}",
        "format_example": normalize_example_part(header_match.group(0)) if header_match else None,
        "components": (
            ["insert_arrangement", "wire_type_code", "shell_finish_code"]
            if header_match and header_match.group("wire")
            else ["insert_arrangement", "shell_finish_code"]
        ),
        "insert_arrangements": insert_map,
        "shell_finish_options": finish_map,
    }
    if document_type == "pcb_tail":
        payload["components"] = [
            "insert_arrangement",
            "termination_length_code",
            "shell_finish_code",
            "hardware_code",
        ]
        payload["termination_length_options"] = [
            {
                "code": match.group("code"),
                "length_inches": float(match.group("length")),
            }
            for match in TERMINATION_LENGTH_PATTERN.finditer(text)
        ]
        payload["hardware_options"] = [
            {
                "code": match.group("code").upper(),
                "description": " ".join(match.group("description").split()),
            }
            for match in HARDWARE_OPTION_PATTERN.finditer(text)
        ]
        if header_match:
            payload["format_example"] = normalize_example_part(header_match.group(0))

    return payload


def parse_wire_note_map(pages: list[str]) -> dict[str, str]:
    text = " ".join(pages[5:])
    marker_match = re.search(r"1/\s+These connectors", text, re.IGNORECASE)
    start = marker_match.start() if marker_match else text.rfind("1/ ")
    note_text = text[start:] if start >= 0 else text
    notes: dict[str, str] = {}
    positions = list(WIRE_NOTE_SPLIT_PATTERN.finditer(note_text))
    for index, match in enumerate(positions):
        note_no = match.group(1)
        start = match.start()
        end = positions[index + 1].start() if index + 1 < len(positions) else len(note_text)
        segment = " ".join(note_text[start:end].split())
        if not segment.startswith(f"{note_no}/"):
            continue
        notes[note_no] = trim_note_bleed(segment[len(f"{note_no}/") :].strip())
    return notes


def parse_wire_options(pages: list[str]) -> list[dict[str, Any]]:
    text = " ".join(pages[5:])
    wire_note_map = parse_wire_note_map(pages)
    options: list[dict[str, Any]] = []
    for match in WIRE_ROW_PATTERN.finditer(text):
        code = match.group("code")
        segment = " ".join(match.group("body").split())
        if "See notes at end of wire type" in segment:
            segment = segment.split("See notes at end of wire type", 1)[0].strip()
        if "These connectors have leads attached" in segment:
            segment = segment.split("These connectors have leads attached", 1)[0].strip()

        length_match = re.search(
            r"\s(?P<length>0\.5|1\.0|\d{2})(?:\s+inch(?:es)?\s+long)?\s+(?P<notes>(?:\d+/[\s,]*)+)(?:\s+(?P<tail>.*))?$",
            segment,
        )
        if not length_match:
            continue

        spec_prefix = segment[: length_match.start()].strip(" ,")
        spec_tail = (length_match.group("tail") or "").strip(" ,")
        spec = " ".join(part for part in (spec_prefix, spec_tail) if part)
        spec = INLINE_INSERT_ASSIGNMENT_PATTERN.sub("", spec)
        spec = " ".join(spec.split()).strip(" ,;")
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
    insert_map = {
        match.group("insert"): int(match.group("cavity"))
        for match in INSERT_MAP_PATTERN.finditer(joined)
    }
    shell_material = "Metal" if "CLASS M" in title_upper else "Plastic" if "CLASS P" in title_upper else None
    gender = "Plug" if "PLUG" in title_upper else "Receptacle" if "RECEPTACLE" in title_upper else None
    contact_type = "Pin" if "PIN CONTACTS" in title_upper else "Socket" if "SOCKET CONTACTS" in title_upper else None
    termination_style = "Solder" if "SOLDER TYPE" in title_upper else "Crimp" if "CRIMP TYPE" in title_upper else None
    board_mount_style = "Right Angle" if "RIGHT ANGLE" in title_upper else "Straight" if "STRAIGHT" in title_upper else None
    profile = "Narrow" if "NARROW PROFILE" in title_upper else "Standard" if "STANDARD PROFILE" in title_upper else None
    row_count_match = re.search(r"\b([234])\s+ROW\b", title_upper)
    return {
        "shell_material": shell_material,
        "gender": gender,
        "class": "M" if "CLASS M" in title_upper else "P" if "CLASS P" in title_upper else None,
        "contact_type": contact_type,
        "termination_style": termination_style,
        "board_mount_style": board_mount_style,
        "profile": profile,
        "row_count": int(row_count_match.group(1)) if row_count_match else None,
        "current_rating_per_contact": float(current_rating_match.group("amps")) if current_rating_match else None,
        "polarization": "Standard polarized shell",
        "insert_arrangement_map": insert_map,
        "mounting_hardware_ref": "MIL-DTL-83513/5" if "MIL-DTL-83513/5" in joined else None,
    }


def synthesize_pcb_configuration_rows(pin_components: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in pin_components.get("insert_arrangements", []):
        cavity_count = int(item["cavity_count"])
        rows.append(
            {
                "page_number": 1,
                "cavity_count": cavity_count,
                "shell_size_letter": "B" if cavity_count == 51 else "C" if cavity_count == 100 else "A",
                "dimensions": {},
            }
        )
    deduped = {row["cavity_count"]: row for row in rows}
    return [deduped[key] for key in sorted(deduped)]


def missing_expected_cavity_counts(spec: DocumentTypeSpec, result: ExtractionResult) -> list[int]:
    if not spec.expected_cavity_counts:
        return []
    actual = set(result.cavity_counts)
    return [value for value in spec.expected_cavity_counts if value not in actual]


def missing_wire_codes(result: ExtractionResult) -> list[str]:
    codes = sorted(option["wire_type_code"] for option in result.wire_options)
    if not codes:
        return []
    expected = {f"{value:02d}" for value in range(int(codes[0]), int(codes[-1]) + 1)}
    return sorted(expected.difference(codes))


def build_validation_checks(spec: DocumentTypeSpec, result: ExtractionResult) -> tuple[list[ValidationCheck], list[str]]:
    checks: list[ValidationCheck] = []
    fallback_flags: list[str] = []

    missing_cavity_counts = missing_expected_cavity_counts(spec, result)
    if missing_cavity_counts:
        fallback_flags.append("missing_expected_cavity_counts")
        checks.append(
            ValidationCheck(
                status="fail",
                code="missing_expected_cavity_counts",
                message="The extracted cavity-count set is incomplete for this document class.",
                details={"missing": missing_cavity_counts},
            )
        )
    elif spec.expected_cavity_counts:
        checks.append(
            ValidationCheck(
                status="pass",
                code="expected_cavity_counts_present",
                message="Expected cavity counts were present.",
                details={"count": len(result.cavity_counts)},
            )
        )

    configuration_count = len(result.configuration_rows)
    if spec.expected_min_configuration_rows and configuration_count < spec.expected_min_configuration_rows:
        fallback_flags.append("too_few_configuration_rows")
        checks.append(
            ValidationCheck(
                status="fail",
                code="too_few_configuration_rows",
                message="Too few configuration rows were extracted.",
                details={"actual": configuration_count, "expected_min": spec.expected_min_configuration_rows},
            )
        )

    insert_arrangement_count = len(result.pin_components.get("insert_arrangements", []))
    if spec.expected_min_insert_arrangements and insert_arrangement_count < spec.expected_min_insert_arrangements:
        fallback_flags.append("too_few_insert_arrangements")
        checks.append(
            ValidationCheck(
                status="fail",
                code="too_few_insert_arrangements",
                message="Too few insert arrangements were extracted.",
                details={"actual": insert_arrangement_count, "expected_min": spec.expected_min_insert_arrangements},
            )
        )

    wire_option_count = len(result.wire_options)
    if spec.expected_min_wire_options and wire_option_count < spec.expected_min_wire_options:
        fallback_flags.append("too_few_wire_options")
        checks.append(
            ValidationCheck(
                status="fail",
                code="too_few_wire_options",
                message="Too few wire options were extracted.",
                details={"actual": wire_option_count, "expected_min": spec.expected_min_wire_options},
            )
        )

    missing_codes = missing_wire_codes(result)
    if missing_codes:
        fallback_flags.append("wire_code_gaps")
        checks.append(
            ValidationCheck(
                status="warn",
                code="wire_code_gaps",
                message="Wire type codes have gaps across the extracted range.",
                details={"missing": missing_codes},
            )
        )

    note_bleed_refs = sorted(
        {
            option["wire_type_code"]
            for option in result.wire_options
            if any(has_note_bleed(note) for note in option.get("note_texts", []))
        }
    )
    if note_bleed_refs:
        fallback_flags.append("note_footer_bleed")
        checks.append(
            ValidationCheck(
                status="warn",
                code="note_footer_bleed",
                message="Wire-note text still contains footer or reference bleed.",
                details={"wire_type_codes": note_bleed_refs},
            )
        )

    if not checks:
        checks.append(
            ValidationCheck(
                status="pass",
                code="deterministic_validation_clear",
                message="No validation failures or warnings were raised.",
            )
        )
    return checks, sorted(dict.fromkeys(fallback_flags))


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
        structured_fields = ["configuration_rows", "pin_components", "figure_references"]
        if spec.expected_min_wire_options:
            structured_fields.append("wire_options")
        for field_name in structured_fields:
            if not field_presence[field_name]:
                score -= 0.08
                issues.append(ExtractionIssue("warning", "missing_structured_field", f"Missing structured field: {field_name}"))

    missing_cavity_counts = missing_expected_cavity_counts(spec, result)
    if missing_cavity_counts:
        score -= 0.20
        issues.append(
            ExtractionIssue(
                "error",
                "missing_expected_cavity_counts",
                f"Missing expected cavity counts: {', '.join(str(value) for value in missing_cavity_counts)}",
            )
        )

    if spec.expected_min_configuration_rows and len(result.configuration_rows) < spec.expected_min_configuration_rows:
        score -= 0.12
        issues.append(
            ExtractionIssue(
                "error",
                "too_few_configuration_rows",
                f"Expected at least {spec.expected_min_configuration_rows} configuration rows.",
            )
        )

    if spec.expected_min_insert_arrangements and len(result.pin_components.get("insert_arrangements", [])) < spec.expected_min_insert_arrangements:
        score -= 0.10
        issues.append(
            ExtractionIssue(
                "error",
                "too_few_insert_arrangements",
                f"Expected at least {spec.expected_min_insert_arrangements} insert arrangements.",
            )
        )

    if spec.expected_min_wire_options and len(result.wire_options) < spec.expected_min_wire_options:
        score -= 0.10
        issues.append(
            ExtractionIssue(
                "warning",
                "too_few_wire_options",
                f"Expected at least {spec.expected_min_wire_options} wire options.",
            )
        )

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
    detected_spec_sheet = detect_spec_sheet_from_pages(pages)
    effective_spec_sheet = detected_spec_sheet or args.spec_sheet
    revision_match = REVISION_PATTERN.search(effective_spec_sheet.upper())
    source = ExtractionSource(
        spec_sheet=effective_spec_sheet,
        document_key=args.document_key,
        document_type=document_spec.document_type,
        title=args.title,
        source_url=args.source_url,
        storage_path=args.storage_path or str(args.pdf),
        revision=revision_match.group("revision") if revision_match else None,
    )

    page_summaries = [build_page_summary(page_number, text) for page_number, text in enumerate(pages, start=1)]
    pin_components = parse_pin_components(pages, args.document_key, document_spec.document_type)
    if document_spec.document_type == "plug_receptacle":
        configuration_rows = parse_configuration_rows(pages)
    elif document_spec.document_type == "pcb_tail":
        configuration_rows = parse_pcb_configuration_rows(
            pages,
            {item["cavity_count"] for item in pin_components.get("insert_arrangements", [])} or None,
        )
        if not configuration_rows and pin_components.get("insert_arrangements"):
            configuration_rows = synthesize_pcb_configuration_rows(pin_components)
    else:
        configuration_rows = []
    example_parts = sorted({value for page in page_summaries for value in page.example_parts})
    if pin_components.get("format_example"):
        example_parts.append(pin_components["format_example"])
        example_parts = sorted(dict.fromkeys(example_parts))
    if document_spec.document_type == "mounting_hardware":
        example_parts = sorted(dict.fromkeys(example_parts))

    cavity_counts = sorted({count for page in page_summaries for count in page.cavity_counts})
    if pin_components.get("insert_arrangements"):
        cavity_counts = sorted({item["cavity_count"] for item in pin_components["insert_arrangements"]})

    result = ExtractionResult(
        source=source,
        connector_type=document_spec.connector_type,
        cavity_counts=cavity_counts,
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
        wire_options=parse_wire_options(pages) if document_spec.document_type == "plug_receptacle" else [],
        figure_references=aggregate_figure_references(page_summaries),
        attributes=infer_attributes(source, pages, configuration_rows),
        page_summaries=page_summaries,
        chunks=build_chunks(pages),
    )

    confidence, issues = score_result(document_spec, result)
    validation_checks, fallback_flags = build_validation_checks(document_spec, result)
    if detected_spec_sheet and detected_spec_sheet.upper() != args.spec_sheet.upper():
        issues.append(
            ExtractionIssue(
                "warning",
                "spec_sheet_mismatch",
                f"Input spec sheet {args.spec_sheet} did not match PDF header {detected_spec_sheet}. Using PDF header value.",
            )
        )
    result.confidence_score = round(confidence, 2)
    result.issues.extend(issues)
    result.validation_checks = validation_checks
    result.fallback_flags = fallback_flags
    if confidence < 0.85 or fallback_flags:
        result.llm_fallback_required = True
        result.llm_fallback_reason = (
            "Deterministic extraction raised validation flags."
            if fallback_flags
            else "Low deterministic confidence or missing required structured fields."
        )
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
