from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Protocol

from pdf_storage.sync_83513_to_supabase import get_server_key, load_env_file, require_env
from web_app.api.models import (
    GroupedSearchResult,
    GroupedMateResult,
    HardwareOption,
    MateCandidate,
    PartDetail,
    SearchResult,
    SourceCitation,
    WireOption,
)


DEFAULT_ENV_FILE = Path(__file__).resolve().parents[2] / ".env.local"


class ProductRepository(Protocol):
    def search_parts_raw(
        self,
        query: str | None = None,
        slash_sheet: str | None = None,
        cavity_count: int | None = None,
        shell_size_letter: str | None = None,
        shell_finish_code: str | None = None,
        gender: str | None = None,
        contact_type: str | None = None,
        connector_type: str | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> tuple[list[SearchResult], int]:
        ...

    def search_parts_grouped(
        self,
        query: str | None = None,
        slash_sheet: str | None = None,
        cavity_count: int | None = None,
        shell_size_letter: str | None = None,
        shell_finish_code: str | None = None,
        gender: str | None = None,
        contact_type: str | None = None,
        connector_type: str | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> tuple[list[GroupedSearchResult], int]:
        ...

    def get_part(self, part_id: str) -> PartDetail | None:
        ...

    def get_mates(self, part_id: str) -> list[MateCandidate]:
        ...

    def get_grouped_mates(self, part_id: str) -> list[GroupedMateResult]:
        ...


def normalize_slash_sheet(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip().lower()
    if not stripped:
        return None
    if stripped == "base":
        return "base"
    return f"{int(stripped):02d}"


def parse_mate_slash_sheets(values: list[str] | None) -> list[str]:
    if not values:
        return []
    slash_sheets: list[str] = []
    for value in values:
        if "/" not in value:
            continue
        try:
            slash_sheets.append(normalize_slash_sheet(value.split("/", 1)[1]))
        except ValueError:
            continue
    return [item for item in slash_sheets if item]


def search_group_key(row: SearchResult) -> str:
    return ":".join(
        [
            row.slash_sheet,
            row.connector_type or "",
            str(row.cavity_count or ""),
            row.shell_size_letter or "",
            row.name,
        ]
    )


def rank_variant_key(source_part: PartDetail, candidate: dict[str, Any]) -> tuple[int, int, int, int, str, str]:
    opposite_gender = int(bool(candidate.get("gender")) and bool(source_part.gender) and candidate.get("gender") != source_part.gender)
    opposite_contact = int(bool(candidate.get("contact_type")) and bool(source_part.contact_type) and candidate.get("contact_type") != source_part.contact_type)
    exact_shell_size = int(candidate.get("shell_size_letter") == source_part.shell_size_letter)
    evidence_present = int(candidate.get("source_page") is not None)
    revision = candidate.get("revision") or ""
    spec_sheet = candidate.get("spec_sheet") or ""
    return (
        opposite_gender + opposite_contact,
        exact_shell_size,
        evidence_present,
        int(bool(revision)),
        revision,
        spec_sheet,
    )


def hardware_options_from_extra_data(extra_data: Any) -> list[HardwareOption]:
    if not isinstance(extra_data, dict):
        return []
    raw_options = extra_data.get("hardware_options")
    if not isinstance(raw_options, list):
        return []

    options: list[HardwareOption] = []
    for item in raw_options:
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        description = item.get("description")
        if code and description:
            options.append(HardwareOption(code=str(code), description=str(description)))
    return options


def wire_range_from_options(wire_options: list[WireOption]) -> str | None:
    awg_values: set[int] = set()
    for option in wire_options:
        text = " ".join(
            value
            for value in (option.wire_specification, option.wire_notes)
            if value
        )
        upper_text = text.upper()
        for token in ("20", "21", "22", "23", "24", "25", "26", "27", "28", "30"):
            if f"{token} AWG" in upper_text or f"-{token}-" in text or f"-{token}(" in text:
                awg_values.add(int(token))

    if not awg_values:
        return None
    minimum = min(awg_values)
    maximum = max(awg_values)
    if minimum == maximum:
        return f"{minimum} AWG"
    return f"{minimum}-{maximum} AWG"


def format_torque_value(row: dict[str, Any]) -> str:
    minimum = row.get("torque_min_in_lbf")
    maximum = row.get("torque_max_in_lbf")
    pieces: list[str] = []

    if minimum is not None and maximum is not None:
        minimum_value = float(minimum)
        maximum_value = float(maximum)
        if minimum_value == maximum_value:
            pieces.append(f"{minimum_value:g} in-lbf")
        else:
            pieces.append(f"{minimum_value:g}-{maximum_value:g} in-lbf")
    elif row.get("torque_text"):
        pieces.append(str(row["torque_text"]))

    if row.get("fastener_thread"):
        pieces.append(f"for {row['fastener_thread']}")
    if row.get("arrangement_scope"):
        pieces.append(f"({row['arrangement_scope']})")
    if row.get("applies_to") and row.get("torque_min_in_lbf") is None:
        pieces.append(f"per {row['applies_to']}")
    if row.get("spec_sheet"):
        pieces.append(f"[{row['spec_sheet']} p. {row.get('source_page')}]")
    return " ".join(pieces)


def format_effective_torque_value(row: dict[str, Any]) -> str:
    minimum = row.get("torque_min_in_lbf")
    maximum = row.get("torque_max_in_lbf")
    pieces: list[str] = []

    if minimum is not None and maximum is not None:
        minimum_value = float(minimum)
        maximum_value = float(maximum)
        if minimum_value == maximum_value:
            pieces.append(f"{minimum_value:g} in-lbf")
        else:
            pieces.append(f"{minimum_value:g}-{maximum_value:g} in-lbf")

    if row.get("fastener_thread"):
        pieces.append(f"for {row['fastener_thread']}")
    if row.get("arrangement_scope"):
        pieces.append(f"({row['arrangement_scope']})")

    labels: list[str] = []
    if row.get("governing_spec_sheet"):
        labels.append(str(row["governing_spec_sheet"]))
    if row.get("values_inherited"):
        labels.append("inherited")
    if row.get("needs_review"):
        labels.append("needs review")
    elif row.get("values_verified"):
        labels.append("verified")
    if labels:
        pieces.append(f"[{', '.join(labels)}]")
    return " ".join(pieces)


def hardware_compatibility_for(source_part: PartDetail, candidate: dict[str, Any]) -> str | None:
    candidate_hardware_ref = candidate.get("mounting_hardware_ref")
    if source_part.mounting_hardware_ref and candidate_hardware_ref:
        if source_part.mounting_hardware_ref == candidate_hardware_ref:
            return f"Shared hardware reference: {source_part.mounting_hardware_ref}"
        return f"Review hardware references: {source_part.mounting_hardware_ref} and {candidate_hardware_ref}"
    if source_part.mounting_hardware_ref:
        return f"Source part hardware reference: {source_part.mounting_hardware_ref}"
    if candidate_hardware_ref:
        return f"Mate hardware reference: {candidate_hardware_ref}"
    return None


class SupabaseRestRepository:
    def __init__(
        self,
        url: str,
        service_role_key: str,
        schema: str = "public",
    ) -> None:
        self.url = url.rstrip("/")
        self.service_role_key = service_role_key
        self.schema = schema

    @classmethod
    def from_env_file(cls, env_file: Path = DEFAULT_ENV_FILE) -> "SupabaseRestRepository":
        env = load_env_file(env_file)
        supabase_url = require_env(env, "SUPABASE_URL")
        service_role_key = get_server_key(env) or os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or ""
        if not service_role_key:
            raise RuntimeError("Missing SUPABASE_SECRET_KEY or SUPABASE_SERVICE_ROLE_KEY for product API.")
        return cls(url=supabase_url, service_role_key=service_role_key)

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Accept-Profile": self.schema,
        }

    def _request(
        self,
        table_or_view: str,
        *,
        query: list[tuple[str, str]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], int | None]:
        query_string = urllib.parse.urlencode(query or [], doseq=True)
        url = f"{self.url}/rest/v1/{table_or_view}"
        if query_string:
            url = f"{url}?{query_string}"

        request_headers = self._headers()
        if headers:
            request_headers.update(headers)

        request = urllib.request.Request(url, headers=request_headers, method="GET")
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            count_header = response.headers.get("Content-Range")

        total: int | None = None
        if count_header and "/" in count_header:
            _, total_text = count_header.split("/", 1)
            if total_text != "*":
                total = int(total_text)

        payload = json.loads(body) if body else []
        return payload, total

    def _citation_from_row(self, row: dict[str, Any]) -> SourceCitation:
        figure_reference = None
        figure_refs = row.get("figure_references")
        if isinstance(figure_refs, list) and figure_refs:
            first = figure_refs[0]
            if isinstance(first, dict):
                raw_figure_reference = first.get("figure_title") or first.get("figure_number")
                if raw_figure_reference is not None:
                    figure_reference = str(raw_figure_reference)

        return SourceCitation(
            spec_sheet=row.get("spec_sheet") or row.get("source_document") or "",
            revision=row.get("revision"),
            storage_path=row.get("storage_path"),
            source_url=row.get("source_url"),
            source_page=row.get("source_page"),
            figure_reference=figure_reference,
        )

    def _search_result_from_row(self, row: dict[str, Any]) -> SearchResult:
        return SearchResult(
            id=row["id"],
            spec_family=row["spec_family"],
            slash_sheet=row["slash_sheet"],
            name=row["name"],
            description=row.get("description"),
            connector_type=row.get("connector_type"),
            gender=row.get("gender"),
            contact_type=row.get("contact_type"),
            cavity_count=row.get("cavity_count"),
            shell_size_letter=row.get("shell_size_letter"),
            shell_finish_code=row.get("shell_finish_code"),
            example_full_pin=row.get("example_full_pin"),
            citation=self._citation_from_row(row),
        )

    def _mate_candidate_from_row(self, row: dict[str, Any], source_part: PartDetail) -> MateCandidate:
        reasons = [f"Slash sheet {row['slash_sheet']} is listed as a valid mate."]
        if row.get("cavity_count") == source_part.cavity_count:
            reasons.append("Exact cavity count match.")
        if row.get("shell_size_letter") == source_part.shell_size_letter:
            reasons.append("Exact shell size match.")
        if row.get("gender") and source_part.gender and row["gender"] != source_part.gender:
            reasons.append("Opposite gender pairing.")
        if row.get("contact_type") and source_part.contact_type and row["contact_type"] != source_part.contact_type:
            reasons.append("Opposite contact type pairing.")
        if row.get("revision"):
            reasons.append(f"Uses active revision {row['revision']}.")

        citation = self._citation_from_row(row)
        return MateCandidate(
            id=row["id"],
            spec_sheet=row["spec_sheet"],
            name=row["name"],
            slash_sheet=row["slash_sheet"],
            compatibility="compatible",
            match_reasons=reasons,
            source_spec=citation.spec_sheet,
            source_page=citation.source_page,
            shell_finish_code=row.get("shell_finish_code"),
            example_full_pin=row.get("example_full_pin"),
            gender=row.get("gender"),
            contact_type=row.get("contact_type"),
            hardware_compatibility=hardware_compatibility_for(source_part, row),
            citation=citation,
        )

    def _search_filters(
        self,
        query: str | None = None,
        slash_sheet: str | None = None,
        cavity_count: int | None = None,
        shell_size_letter: str | None = None,
        shell_finish_code: str | None = None,
        gender: str | None = None,
        contact_type: str | None = None,
        connector_type: str | None = None,
    ) -> list[tuple[str, str]]:
        filters: list[tuple[str, str]] = [
            ("select", ",".join([
                "id",
                "spec_family",
                "slash_sheet",
                "name",
                "description",
                "connector_type",
                "gender",
                "contact_type",
                "cavity_count",
                "shell_size_letter",
                "shell_finish_code",
                "example_full_pin",
                "source_document",
                "revision",
                "source_url",
                "source_page",
                "figure_references",
            ])),
            ("spec_family", "eq.83513"),
            ("order", "slash_sheet.asc,cavity_count.asc,shell_finish_code.asc,example_full_pin.asc"),
        ]

        normalized_slash_sheet = normalize_slash_sheet(slash_sheet)
        if normalized_slash_sheet:
            filters.append(("slash_sheet", f"eq.{normalized_slash_sheet}"))
        if cavity_count is not None:
            filters.append(("cavity_count", f"eq.{cavity_count}"))
        if shell_size_letter:
            filters.append(("shell_size_letter", f"eq.{shell_size_letter.upper()}"))
        if shell_finish_code:
            filters.append(("shell_finish_code", f"eq.{shell_finish_code.upper()}"))
        if gender:
            filters.append(("gender", f"eq.{urllib.parse.quote(gender.upper(), safe='')}"))
        if contact_type:
            filters.append(("contact_type", f"eq.{urllib.parse.quote(contact_type.upper(), safe='')}"))
        if connector_type:
            filters.append(("connector_type", f"eq.{urllib.parse.quote(connector_type.upper(), safe='')}"))
        if query:
            escaped = query.replace(",", "\\,")
            filters.append(
                (
                    "or",
                    f"(name.ilike.*{escaped}*,description.ilike.*{escaped}*,example_full_pin.ilike.*{escaped}*,spec_sheet.ilike.*{escaped}*)",
                )
            )
        return filters

    def search_parts_raw(
        self,
        query: str | None = None,
        slash_sheet: str | None = None,
        cavity_count: int | None = None,
        shell_size_letter: str | None = None,
        shell_finish_code: str | None = None,
        gender: str | None = None,
        contact_type: str | None = None,
        connector_type: str | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> tuple[list[SearchResult], int]:
        filters = self._search_filters(
            query=query,
            slash_sheet=slash_sheet,
            cavity_count=cavity_count,
            shell_size_letter=shell_size_letter,
            shell_finish_code=shell_finish_code,
            gender=gender,
            contact_type=contact_type,
            connector_type=connector_type,
        )
        filters.extend([("limit", str(limit)), ("offset", str(offset))])

        rows, total = self._request(
            "base_configurations",
            query=filters,
            headers={"Prefer": "count=exact"},
        )
        return [self._search_result_from_row(row) for row in rows], total or len(rows)

    def search_parts_grouped(
        self,
        query: str | None = None,
        slash_sheet: str | None = None,
        cavity_count: int | None = None,
        shell_size_letter: str | None = None,
        shell_finish_code: str | None = None,
        gender: str | None = None,
        contact_type: str | None = None,
        connector_type: str | None = None,
        limit: int = 25,
        offset: int = 0,
    ) -> tuple[list[GroupedSearchResult], int]:
        raw_variants, _ = self.search_parts_raw(
            query=query,
            slash_sheet=slash_sheet,
            cavity_count=cavity_count,
            shell_size_letter=shell_size_letter,
            shell_finish_code=shell_finish_code,
            gender=gender,
            contact_type=contact_type,
            connector_type=connector_type,
            limit=500,
            offset=0,
        )

        grouped_map: dict[str, list[SearchResult]] = {}
        for variant in raw_variants:
            key = search_group_key(variant)
            grouped_map.setdefault(key, [])
            grouped_map[key].append(variant)

        grouped_items: list[GroupedSearchResult] = []
        for key, variants in grouped_map.items():
            representative = variants[0]
            finish_codes = sorted(
                {
                    variant.shell_finish_code
                    for variant in variants
                    if variant.shell_finish_code
                }
            )
            grouped_items.append(
                GroupedSearchResult(
                    search_family_key=key,
                    slash_sheet=representative.slash_sheet,
                    connector_type=representative.connector_type,
                    cavity_count=representative.cavity_count,
                    shell_size_letter=representative.shell_size_letter,
                    variant_count=len(variants),
                    available_finish_codes=finish_codes,
                    representative_variant=representative,
                    citation=representative.citation,
                )
            )

        grouped_items.sort(
            key=lambda item: (
                item.slash_sheet,
                item.cavity_count or 0,
                item.shell_size_letter or "",
                item.representative_variant.name,
            )
        )
        sliced = grouped_items[offset: offset + limit]
        return sliced, len(grouped_items)

    def _wire_options_for_part(self, part_id: str) -> list[WireOption]:
        rows, _ = self._request(
            "hns_wire_options",
            query=[
                ("select", "wire_type_code,wire_specification,wire_length_inches,wire_notes,is_space_approved"),
                ("base_config_id", f"eq.{part_id}"),
                ("order", "wire_type_code.asc"),
            ],
        )
        return [
            WireOption(
                wire_type_code=row["wire_type_code"],
                wire_specification=row.get("wire_specification"),
                wire_length_inches=float(row["wire_length_inches"]) if row.get("wire_length_inches") is not None else None,
                wire_notes=row.get("wire_notes"),
                is_space_approved=bool(row.get("is_space_approved")),
            )
            for row in rows
        ]

    def _torque_values_for_row(self, row: dict[str, Any]) -> list[str]:
        effective_query = [
            ("select", ",".join([
                "spec_sheet",
                "slash_sheet",
                "revision",
                "torque_mode",
                "resolved_profile_code",
                "governing_spec_sheet",
                "governing_revision",
                "values_verified",
                "values_inherited",
                "needs_review",
                "context",
                "fastener_thread",
                "source_thread_label",
                "arrangement_scope",
                "torque_min_in_lbf",
                "torque_max_in_lbf",
                "approval_status",
                "profile_kind",
                "source_of_truth_level",
            ])),
            ("slash_sheet", f"eq.{row['slash_sheet']}"),
            ("order", "context.asc,fastener_thread.asc,arrangement_scope.asc,torque_min_in_lbf.asc"),
        ]
        try:
            effective_rows, _ = self._request("v_83513_torque_effective_facts", query=effective_query)
        except Exception:
            effective_rows = []
        if effective_rows:
            return [format_effective_torque_value(torque_row) for torque_row in effective_rows]

        slash_sheets = [row["slash_sheet"]]
        mounting_ref = row.get("mounting_hardware_ref")
        if isinstance(mounting_ref, str) and mounting_ref.endswith("/5"):
            slash_sheets.append("05")

        query = [
            ("select", ",".join([
                "spec_sheet",
                "slash_sheet",
                "context",
                "applies_to",
                "fastener_thread",
                "arrangement_scope",
                "torque_min_in_lbf",
                "torque_max_in_lbf",
                "torque_text",
                "source_page",
            ])),
            ("spec_family", "eq.83513"),
            ("slash_sheet", f"in.({','.join(dict.fromkeys(slash_sheets))})"),
            ("order", "slash_sheet.asc,context.asc,fastener_thread.asc,source_page.asc"),
        ]
        try:
            rows, _ = self._request("torque_values", query=query)
        except Exception:
            return []
        return [format_torque_value(torque_row) for torque_row in rows]

    def get_part(self, part_id: str) -> PartDetail | None:
        rows, _ = self._request(
            "base_configurations",
            query=[
                ("select", ",".join([
                    "id",
                    "spec_family",
                    "slash_sheet",
                    "spec_sheet",
                    "name",
                    "description",
                    "connector_type",
                    "gender",
                    "contact_type",
                    "cavity_count",
                    "shell_size_letter",
                    "shell_finish_code",
                    "shell_finish_description",
                    "dimensions",
                    "shell_material",
                    "mates_with",
                    "mounting_hardware_ref",
                    "example_full_pin",
                    "source_document",
                    "source_url",
                    "source_page",
                    "revision",
                    "figure_references",
                    "extra_data",
                ])),
                ("id", f"eq.{part_id}"),
                ("limit", "1"),
            ],
        )
        if not rows:
            return None

        row = rows[0]
        wire_options = self._wire_options_for_part(part_id)
        torque_values = self._torque_values_for_row(row)
        return PartDetail(
            id=row["id"],
            spec_family=row["spec_family"],
            slash_sheet=row["slash_sheet"],
            spec_sheet=row["spec_sheet"],
            name=row["name"],
            description=row.get("description"),
            connector_type=row.get("connector_type"),
            gender=row.get("gender"),
            contact_type=row.get("contact_type"),
            cavity_count=row.get("cavity_count"),
            shell_size_letter=row.get("shell_size_letter"),
            shell_finish_code=row.get("shell_finish_code"),
            shell_finish_description=row.get("shell_finish_description"),
            dimensions=row.get("dimensions"),
            shell_material=row.get("shell_material"),
            mates_with=row.get("mates_with") or [],
            mounting_hardware_ref=row.get("mounting_hardware_ref"),
            hardware_options=hardware_options_from_extra_data(row.get("extra_data")),
            wire_range=wire_range_from_options(wire_options),
            torque_values=torque_values,
            example_full_pin=row.get("example_full_pin"),
            wire_options=wire_options,
            citation=self._citation_from_row(row),
        )

    def get_mates(self, part_id: str) -> list[MateCandidate]:
        source_part = self.get_part(part_id)
        if source_part is None:
            return []

        allowed_slash_sheets = parse_mate_slash_sheets(source_part.mates_with)
        if not allowed_slash_sheets:
            return []

        filters: list[tuple[str, str]] = [
            ("select", ",".join([
                "id",
                "spec_sheet",
                "name",
                "slash_sheet",
                "cavity_count",
                "shell_size_letter",
                "connector_type",
                "gender",
                "contact_type",
                "shell_finish_code",
                "example_full_pin",
                "mounting_hardware_ref",
                "source_document",
                "source_url",
                "source_page",
                "revision",
                "figure_references",
            ])),
            ("spec_family", "eq.83513"),
            ("slash_sheet", f"in.({','.join(allowed_slash_sheets)})"),
            ("cavity_count", f"eq.{source_part.cavity_count}"),
            ("order", "slash_sheet.asc,shell_size_letter.asc"),
        ]
        if source_part.shell_size_letter:
            filters.append(("shell_size_letter", f"eq.{source_part.shell_size_letter}"))

        rows, _ = self._request("base_configurations", query=filters)
        ranked_rows = sorted(rows, key=lambda row: rank_variant_key(source_part, row), reverse=True)
        return [self._mate_candidate_from_row(row, source_part) for row in ranked_rows]

    def get_grouped_mates(self, part_id: str) -> list[GroupedMateResult]:
        source_part = self.get_part(part_id)
        if source_part is None:
            return []

        raw_variants = self.get_mates(part_id)
        grouped: dict[str, list[MateCandidate]] = {}
        for variant in raw_variants:
            key = f"{variant.slash_sheet}:{source_part.cavity_count}:{source_part.shell_size_letter or ''}"
            grouped.setdefault(key, [])
            grouped[key].append(variant)

        grouped_results: list[GroupedMateResult] = []
        for family_key, variants in grouped.items():
            representative = variants[0]
            grouped_results.append(
                GroupedMateResult(
                    mate_family_key=family_key,
                    mate_slash_sheet=representative.slash_sheet,
                    variant_count=len(variants),
                    representative_variant=representative,
                    variants=variants,
                    match_reasons=representative.match_reasons,
                    source_spec=representative.source_spec,
                    source_page=representative.source_page,
                    hardware_compatibility=representative.hardware_compatibility,
                )
            )

        grouped_results.sort(
            key=lambda item: (
                int("Exact cavity count match." in item.match_reasons),
                int("Exact shell size match." in item.match_reasons),
                int("Opposite gender pairing." in item.match_reasons) + int("Opposite contact type pairing." in item.match_reasons),
                int(bool(item.source_page)),
                item.representative_variant.spec_sheet,
            ),
            reverse=True,
        )
        return grouped_results


class NotImplementedRepository(SupabaseRestRepository):
    """Compatibility alias until the real repository is wired in everywhere."""
