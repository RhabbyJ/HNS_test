#!/usr/bin/env python3
"""Slash-sheet registry and extraction requirements for MIL-DTL-83513."""

from __future__ import annotations

from dataclasses import dataclass


ALLOWED_CAVITY_COUNTS = (9, 15, 21, 25, 31, 37, 51, 100)
ALLOWED_FINISH_CODES = ("A", "C", "K", "N", "P", "T")


@dataclass(frozen=True)
class DocumentTypeSpec:
    document_type: str
    connector_type: str
    required_fields: tuple[str, ...]
    expected_headers: tuple[str, ...]
    notes: str


BASE_SPEC = DocumentTypeSpec(
    document_type="base_spec",
    connector_type="GENERAL_SPECIFICATION",
    required_fields=("spec_sheet", "revision", "title"),
    expected_headers=("REQUIREMENTS", "DIMENSIONS", "ORDERING DATA"),
    notes="General rules, finish codes, class definitions, and common references.",
)

MOUNTING_HARDWARE = DocumentTypeSpec(
    document_type="mounting_hardware",
    connector_type="MOUNTING_HARDWARE",
    required_fields=("spec_sheet", "revision", "title", "dimensions", "mates_with"),
    expected_headers=("DIMENSIONS", "REQUIREMENTS"),
    notes="Slash sheet /5 style hardware and accessory references.",
)

PLUG_RECEPTACLE = DocumentTypeSpec(
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
    notes="Covers solder/crimp plug and receptacle slash sheets.",
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
        "dimensions",
        "mates_with",
        "example_parts",
    ),
    expected_headers=("HOW TO ORDER", "DIMENSIONS", "INSERT ARRANGEMENT"),
    notes="Covers straight/right-angle narrow and standard profile PCB tails.",
)


def document_type_for_key(document_key: str) -> DocumentTypeSpec:
    if document_key == "base":
        return BASE_SPEC
    if document_key == "5":
        return MOUNTING_HARDWARE
    if document_key in {"1", "2", "3", "4", "6", "7", "8", "9"}:
        return PLUG_RECEPTACLE
    return PCB_TAIL
