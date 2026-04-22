#!/usr/bin/env python3
"""Shared helpers for discovering and downloading MIL-DTL-83513 documents."""

from __future__ import annotations

import html
import http.cookiejar
import json
import re
import ssl
import time
import urllib.parse
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


SEARCH_URL = "https://quicksearch.dla.mil/qssearch.aspx"
DOC_DETAILS_URL_TEMPLATE = "https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number={ident_number}"
DATE_FORMAT = "%d-%b-%Y"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Python ASSIST downloader"
BASE_DOC_ID_PATTERN = re.compile(
    r"^MIL-DTL-83513(?:/(?P<slash>\d+))?(?P<revision>[A-Z])(?:\((?P<change>\d+)\))?(?:\s+NOT\s+\d+)?$",
    re.IGNORECASE,
)
REVISION_DESCRIPTION_PATTERN = re.compile(
    r"^Revision\s+(?P<revision>[A-Z])"
    r"(?:\s+Amendment\s+(?P<amendment>\d+)(?:\s*-\s*[A-Za-z ]+)?)?"
    r"(?:\s+\((?P<note>[^)]*)\))?$",
    re.IGNORECASE,
)
REVISION_HISTORY_ROW_PATTERN = re.compile(
    r"<tr[^>]*>.*?ImageRedirector\.aspx\?token=(?P<token>\d+\.\d+).*?</tr>",
    re.IGNORECASE | re.DOTALL,
)
ROW_TEXT_REVISION_PATTERN = re.compile(
    r"(?P<description>Revision\s+[A-Z](?:\s+(?:Amendment|Notice)\s+\d+(?:\s*-\s*[A-Za-z ]+)?)?(?:\s+\([^)]*\))?)\s*[A-Z]?\s*(?P<document_date>\d{2}-[A-Z]{3}-\d{4})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RevisionEntry:
    image_token: str
    description: str
    document_date: datetime
    revision_letter: str


@dataclass(frozen=True)
class ResolvedDownload:
    ident_number: str
    document_key: str
    details_url: str
    image_redirector_url: str
    pdf_url: str
    revision_letter: str
    revision_date: datetime
    revision_description: str


@dataclass(frozen=True)
class DownloadedPdf:
    resolved: ResolvedDownload
    pdf_bytes: bytes


class AssistSession:
    """Stateful HTTP client for ASSIST requests."""

    def __init__(self) -> None:
        cookie_jar = http.cookiejar.CookieJar()
        context = ssl._create_unverified_context()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(cookie_jar),
            urllib.request.HTTPSHandler(context=context),
        )

    def _request(self, url: str, timeout: int, referer: str | None = None) -> tuple[bytes, str]:
        headers = {"User-Agent": USER_AGENT}
        if referer:
            headers["Referer"] = referer

        request = urllib.request.Request(url, headers=headers)
        for attempt in range(3):
            try:
                with self._opener.open(request, timeout=timeout) as response:
                    return response.read(), response.geturl()
            except urllib.error.HTTPError as exc:
                if exc.code not in {403, 429, 500, 502, 503, 504} or attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))

        raise RuntimeError(f"Failed to fetch {url}")

    def fetch_text(self, url: str, timeout: int = 30, referer: str | None = None) -> str:
        payload, _ = self._request(url, timeout=timeout, referer=referer)
        return payload.decode("utf-8", errors="replace")

    def download_bytes(self, url: str, timeout: int = 60, referer: str | None = None) -> tuple[bytes, str]:
        return self._request(url, timeout=timeout, referer=referer)


def fetch_text(url: str, timeout: int = 30) -> str:
    return AssistSession().fetch_text(url, timeout=timeout)


def download_bytes(url: str, timeout: int = 60) -> tuple[bytes, str]:
    return AssistSession().download_bytes(url, timeout=timeout)


def strip_tags(value: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", value)
    return html.unescape(no_tags).replace("\xa0", " ").strip()


def collapse_whitespace(value: str) -> str:
    return " ".join(value.split())


def strip_tags_with_spacing(value: str) -> str:
    no_tags = re.sub(r"<[^>]+>", " ", value)
    return html.unescape(no_tags).replace("\xa0", " ").strip()


def parse_assist_date(value: str) -> datetime:
    return datetime.strptime(value.strip().upper(), DATE_FORMAT)


def is_non_base_revision_description(description: str) -> bool:
    upper_description = description.upper()
    if "NOTICE" in upper_description or "VALIDATION" in upper_description:
        return True
    if "ADMINISTRATIVE" in upper_description or re.search(r"\bADMIN\b", upper_description):
        return True
    return False


def parse_search_doc_id(doc_id: str) -> tuple[str, str] | None:
    normalized = " ".join(doc_id.upper().split())
    match = BASE_DOC_ID_PATTERN.fullmatch(normalized)
    if not match:
        return None

    slash_sheet = match.group("slash")
    document_key = slash_sheet or "base"
    return document_key, match.group("revision")


def build_output_name(document_key: str, revision_letter: str) -> str:
    suffix = storage_document_label(document_key)
    return f"MIL-DTL-83513_{suffix}_rev_{revision_letter}.pdf"


def sort_document_key(document_key: str) -> tuple[int, int]:
    if document_key == "base":
        return (0, 0)
    return (1, int(document_key))


def sort_order_for_document_key(document_key: str) -> int:
    if document_key == "base":
        return 0
    return int(document_key)


def storage_document_label(document_key: str) -> str:
    if document_key == "base":
        return "base"
    return f"{int(document_key):02d}"


def parse_revision_entries(page_html: str) -> list[RevisionEntry]:
    primary_pattern = re.compile(
        r"ImageRedirector\.aspx\?token=(?P<token>\d+\.\d+)',\d+\);\""
        r".*?</td><td[^>]*>(?P<description>.*?)</td>"
        r"<td[^>]*>.*?</td><td[^>]*>(?P<document_date>.*?)</td>",
        re.IGNORECASE | re.DOTALL,
    )

    entries_by_token: dict[str, RevisionEntry] = {}

    for match in primary_pattern.finditer(page_html):
        description = strip_tags(match.group("description"))
        if is_non_base_revision_description(description):
            continue

        revision = REVISION_DESCRIPTION_PATTERN.fullmatch(description)
        if not revision:
            continue

        entry = RevisionEntry(
            image_token=match.group("token"),
            description=description,
            document_date=parse_assist_date(strip_tags(match.group("document_date"))),
            revision_letter=revision.group("revision").upper(),
        )
        entries_by_token[entry.image_token] = entry

    for row_match in REVISION_HISTORY_ROW_PATTERN.finditer(page_html):
        token = row_match.group("token")
        if token in entries_by_token:
            continue

        row_text = collapse_whitespace(strip_tags_with_spacing(row_match.group(0)))
        parsed_row = ROW_TEXT_REVISION_PATTERN.search(row_text)
        if not parsed_row:
            continue

        description = collapse_whitespace(parsed_row.group("description"))
        if is_non_base_revision_description(description):
            continue

        revision_match = re.match(r"Revision\s+([A-Z])", description, re.IGNORECASE)
        if not revision_match:
            continue

        entries_by_token[token] = RevisionEntry(
            image_token=token,
            description=description,
            document_date=parse_assist_date(parsed_row.group("document_date")),
            revision_letter=revision_match.group(1),
        )

    return sorted(entries_by_token.values(), key=lambda entry: entry.document_date, reverse=True)


def latest_base_revision(entries: list[RevisionEntry], expected_revision_letter: str | None = None) -> RevisionEntry:
    if not entries:
        raise RuntimeError("No letter revision or incorporated amendment rows were found on the document page.")

    if expected_revision_letter:
        normalized_expected = expected_revision_letter.upper()
        matching = [entry for entry in entries if entry.revision_letter.upper() == normalized_expected]
        if not matching:
            raise RuntimeError(
                f"Expected revision {normalized_expected} was not found on the document details page."
            )
        return max(matching, key=lambda entry: entry.document_date)

    return max(entries, key=lambda entry: entry.document_date)


def resolve_wmx_url(image_redirector_html: str, base_url: str) -> str:
    match = re.search(r'href="(?P<href>\.\./\.\./WMX/Default\.aspx\?token=\d+)"', image_redirector_html)
    if not match:
        raise RuntimeError("Could not resolve the WMX download URL from the image redirector page.")
    return urllib.parse.urljoin(base_url, match.group("href"))


def details_url_for_ident(ident_number: str) -> str:
    return DOC_DETAILS_URL_TEMPLATE.format(ident_number=ident_number)


def resolve_latest_revision_download(
    ident_number: str,
    document_key: str,
    expected_revision_letter: str | None = None,
    session: AssistSession | None = None,
) -> ResolvedDownload:
    session = session or AssistSession()
    details_url = details_url_for_ident(ident_number)
    details_html = session.fetch_text(details_url)
    revision = latest_base_revision(
        parse_revision_entries(details_html),
        expected_revision_letter=expected_revision_letter,
    )

    image_redirector_url = urllib.parse.urljoin(
        details_url, f"./ImageRedirector.aspx?token={revision.image_token}"
    )
    image_redirector_html = session.fetch_text(image_redirector_url, referer=details_url)
    pdf_url = resolve_wmx_url(image_redirector_html, image_redirector_url)

    return ResolvedDownload(
        ident_number=ident_number,
        document_key=document_key,
        details_url=details_url,
        image_redirector_url=image_redirector_url,
        pdf_url=pdf_url,
        revision_letter=revision.revision_letter,
        revision_date=revision.document_date,
        revision_description=revision.description,
    )


def download_latest_revision_pdf(
    ident_number: str,
    document_key: str,
    output_dir: Path,
    expected_revision_letter: str | None = None,
    session: AssistSession | None = None,
) -> ResolvedDownload:
    downloaded = download_latest_revision_bytes(
        ident_number=ident_number,
        document_key=document_key,
        expected_revision_letter=expected_revision_letter,
        session=session,
    )
    resolved = downloaded.resolved
    pdf_bytes = downloaded.pdf_bytes

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / build_output_name(document_key, resolved.revision_letter)
    output_path.write_bytes(pdf_bytes)

    return resolved


def download_latest_revision_bytes(
    ident_number: str,
    document_key: str,
    expected_revision_letter: str | None = None,
    session: AssistSession | None = None,
) -> DownloadedPdf:
    session = session or AssistSession()
    resolved = resolve_latest_revision_download(
        ident_number,
        document_key,
        expected_revision_letter=expected_revision_letter,
        session=session,
    )
    pdf_bytes, final_pdf_url = session.download_bytes(
        resolved.pdf_url,
        referer=resolved.image_redirector_url,
    )
    if not pdf_bytes.startswith(b"%PDF"):
        raise RuntimeError(f"Unexpected content when downloading {ident_number}; expected PDF bytes.")

    return DownloadedPdf(
        resolved=ResolvedDownload(
            ident_number=resolved.ident_number,
            document_key=resolved.document_key,
            details_url=resolved.details_url,
            image_redirector_url=resolved.image_redirector_url,
            pdf_url=final_pdf_url,
            revision_letter=resolved.revision_letter,
            revision_date=resolved.revision_date,
            revision_description=resolved.revision_description,
        ),
        pdf_bytes=pdf_bytes,
    )


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_catalog(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
