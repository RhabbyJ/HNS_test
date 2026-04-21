from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class SourceCitation(BaseModel):
    spec_sheet: str
    revision: str | None = None
    storage_path: str | None = None
    source_url: str | None = None
    source_page: int | None = None
    figure_reference: str | None = None


class WireOption(BaseModel):
    wire_type_code: str
    wire_specification: str | None = None
    wire_length_inches: float | None = None
    wire_notes: str | None = None
    is_space_approved: bool = False


class HardwareOption(BaseModel):
    code: str
    description: str


class SearchResult(BaseModel):
    id: str
    spec_family: str
    slash_sheet: str
    name: str
    description: str | None = None
    connector_type: str | None = None
    gender: str | None = None
    contact_type: str | None = None
    cavity_count: int | None = None
    shell_size_letter: str | None = None
    shell_finish_code: str | None = None
    example_full_pin: str | None = None
    citation: SourceCitation


class GroupedSearchResult(BaseModel):
    search_family_key: str
    slash_sheet: str
    connector_type: str | None = None
    cavity_count: int | None = None
    shell_size_letter: str | None = None
    variant_count: int
    available_finish_codes: list[str] = Field(default_factory=list)
    representative_variant: SearchResult
    citation: SourceCitation


class PartDetail(BaseModel):
    id: str
    spec_family: str
    slash_sheet: str
    spec_sheet: str
    name: str
    description: str | None = None
    connector_type: str | None = None
    gender: str | None = None
    contact_type: str | None = None
    cavity_count: int | None = None
    shell_size_letter: str | None = None
    shell_finish_code: str | None = None
    shell_finish_description: str | None = None
    dimensions: dict | None = None
    shell_material: str | None = None
    mates_with: list[str] = Field(default_factory=list)
    mounting_hardware_ref: str | None = None
    hardware_options: list[HardwareOption] = Field(default_factory=list)
    wire_range: str | None = None
    torque_values: list[str] = Field(default_factory=list)
    example_full_pin: str | None = None
    wire_options: list[WireOption] = Field(default_factory=list)
    citation: SourceCitation


class MateCandidate(BaseModel):
    id: str
    spec_sheet: str
    name: str
    slash_sheet: str
    compatibility: Literal["compatible", "review"]
    match_reasons: list[str] = Field(default_factory=list)
    source_spec: str | None = None
    source_page: int | None = None
    confidence_type: Literal["deterministic"] = "deterministic"
    shell_finish_code: str | None = None
    example_full_pin: str | None = None
    gender: str | None = None
    contact_type: str | None = None
    hardware_compatibility: str | None = None
    citation: SourceCitation


class GroupedMateResult(BaseModel):
    mate_family_key: str
    mate_slash_sheet: str
    variant_count: int
    representative_variant: MateCandidate
    variants: list[MateCandidate] = Field(default_factory=list)
    match_reasons: list[str] = Field(default_factory=list)
    source_spec: str | None = None
    source_page: int | None = None
    hardware_compatibility: str | None = None
    confidence_type: Literal["deterministic"] = "deterministic"


class SearchResponse(BaseModel):
    grouped: bool
    items: list[GroupedSearchResult] = Field(default_factory=list)
    raw_variants: list[SearchResult] = Field(default_factory=list)
    total: int


class MateResponse(BaseModel):
    part_id: str
    grouped: bool
    mates: list[GroupedMateResult] = Field(default_factory=list)
    raw_variants: list[MateCandidate] = Field(default_factory=list)
