#!/usr/bin/env python3
"""Slash-sheet registry and extraction requirements for MIL-DTL-83513."""

from __future__ import annotations

from dataclasses import dataclass


ALLOWED_CAVITY_COUNTS = (9, 15, 21, 25, 31, 37, 51, 100)
ALLOWED_FINISH_CODES = ("A", "C", "K", "N", "P", "T")
EXPECTED_INSERTS = {
    "1": ("A", "B", "C", "D", "E", "F", "G", "H"),
    "2": ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K"),
    "3": ("A", "B", "C", "D", "E", "F", "G", "H"),
    "4": ("A", "B", "C", "D", "E", "F", "G", "H", "J", "K"),
    "6": ("A", "B", "C", "D", "E", "F", "G"),
    "7": ("A", "B", "C", "D", "E", "F", "G"),
    "8": ("A", "B", "C", "D", "E", "F", "G"),
    "9": ("A", "B", "C", "D", "E", "F", "G"),
}
EXPECTED_FINISH_CODES = {
    "1": ALLOWED_FINISH_CODES,
    "2": ALLOWED_FINISH_CODES,
    "3": ALLOWED_FINISH_CODES,
    "4": ALLOWED_FINISH_CODES,
    "6": (),
    "7": (),
    "8": (),
    "9": (),
}


@dataclass(frozen=True)
class DocumentTypeSpec:
    document_type: str
    connector_type: str
    required_fields: tuple[str, ...]
    expected_headers: tuple[str, ...]
    notes: str
    expected_cavity_counts: tuple[int, ...] = ()
    expected_min_configuration_rows: int = 0
    expected_min_insert_arrangements: int = 0
    expected_min_wire_options: int = 0


BASE_SPEC = DocumentTypeSpec(
    document_type="base_spec",
    connector_type="GENERAL_SPECIFICATION",
    required_fields=("spec_sheet", "revision", "title"),
    expected_headers=("REQUIREMENTS", "DIMENSIONS", "ORDERING DATA"),
    expected_min_configuration_rows=0,
    notes="General rules, finish codes, class definitions, and common references.",
)

MOUNTING_HARDWARE = DocumentTypeSpec(
    document_type="mounting_hardware",
    connector_type="MOUNTING_HARDWARE",
    required_fields=("spec_sheet", "revision", "title", "mates_with"),
    expected_headers=("FIGURE", "REQUIREMENTS"),
    expected_min_configuration_rows=0,
    notes="Slash sheet /5 style hardware and accessory references.",
)

SOLDER_PLUG_RECEPTACLE = DocumentTypeSpec(
    document_type="plug_receptacle",
    connector_type="SIGNAL_CONNECTOR",
    required_fields=(
        "spec_sheet",
        "revision",
        "title",
        "connector_type",
        "cavity_counts",
        "dimensions",
        "mates_with",
    ),
    expected_headers=("REQUIREMENTS", "DIMENSIONS", "FIGURE"),
    expected_cavity_counts=ALLOWED_CAVITY_COUNTS,
    expected_min_configuration_rows=8,
    expected_min_insert_arrangements=8,
    notes="Covers solder plug and receptacle slash sheets.",
)

CLASS_P_SOLDER_PLUG_RECEPTACLE = DocumentTypeSpec(
    document_type="plug_receptacle",
    connector_type="SIGNAL_CONNECTOR",
    required_fields=(
        "spec_sheet",
        "revision",
        "title",
        "connector_type",
        "cavity_counts",
        "dimensions",
        "mates_with",
    ),
    expected_headers=("REQUIREMENTS", "DIMENSIONS", "FIGURE"),
    expected_cavity_counts=(9, 15, 21, 25, 31, 37, 51),
    expected_min_configuration_rows=7,
    expected_min_insert_arrangements=7,
    notes="Covers class P solder plug and receptacle slash sheets.",
)

CRIMP_PLUG_RECEPTACLE = DocumentTypeSpec(
    document_type="plug_receptacle",
    connector_type="SIGNAL_CONNECTOR",
    required_fields=(
        "spec_sheet",
        "revision",
        "title",
        "connector_type",
        "cavity_counts",
        "dimensions",
        "mates_with",
    ),
    expected_headers=("HOW TO ORDER", "DIMENSIONS", "FIGURE"),
    expected_cavity_counts=ALLOWED_CAVITY_COUNTS,
    expected_min_configuration_rows=8,
    expected_min_insert_arrangements=8,
    expected_min_wire_options=18,
    notes="Covers solder/crimp plug and receptacle slash sheets.",
)

CLASS_P_CRIMP_PLUG_RECEPTACLE = DocumentTypeSpec(
    document_type="plug_receptacle",
    connector_type="SIGNAL_CONNECTOR",
    required_fields=(
        "spec_sheet",
        "revision",
        "title",
        "connector_type",
        "cavity_counts",
        "dimensions",
        "mates_with",
    ),
    expected_headers=("DIMENSIONS", "FIGURE"),
    expected_cavity_counts=(9, 15, 21, 25, 31, 37, 51),
    expected_min_configuration_rows=7,
    expected_min_insert_arrangements=7,
    expected_min_wire_options=18,
    notes="Covers class P crimp plug and receptacle slash sheets.",
)

PCB_TAIL = DocumentTypeSpec(
    document_type="pcb_tail",
    connector_type="PCB_TAIL_CONNECTOR",
    required_fields=(
        "spec_sheet",
        "revision",
        "title",
        "connector_type",
        "cavity_counts",
        "mates_with",
        "example_parts",
    ),
    expected_headers=("REQUIREMENTS", "FIGURE", "DIMENSIONS"),
    expected_min_configuration_rows=1,
    expected_min_insert_arrangements=1,
    notes="Covers straight/right-angle narrow and standard profile PCB tails.",
)


def document_type_for_key(document_key: str) -> DocumentTypeSpec:
    if document_key == "base":
        return BASE_SPEC
    if document_key == "5":
        return MOUNTING_HARDWARE
    if document_key in {"1", "2"}:
        return SOLDER_PLUG_RECEPTACLE
    if document_key in {"6", "7"}:
        return CLASS_P_SOLDER_PLUG_RECEPTACLE
    if document_key in {"3", "4"}:
        return CRIMP_PLUG_RECEPTACLE
    if document_key in {"8", "9"}:
        return CLASS_P_CRIMP_PLUG_RECEPTACLE
    return PCB_TAIL
