import Link from "next/link";

import { GroupedSearchResult, SearchResponse, SearchResult } from "@/lib/types";

type SearchResultsProps = {
  results: SearchResponse | null;
};

type SearchCardData = {
  key: string;
  href: string;
  pn: string;
  slashSheet: string;
  description: string;
  cavityCount?: number | null;
  gender?: string | null;
  contactType?: string | null;
  variantCount: number;
  availableFinishCodes: string[];
  citationSpecSheet: string;
};

function isGroupedSearchResult(value: GroupedSearchResult | SearchResult): value is GroupedSearchResult {
  return "representative_variant" in value;
}

function toSearchCardData(item: GroupedSearchResult | SearchResult): SearchCardData {
  const representative = isGroupedSearchResult(item) ? item.representative_variant : item;
  const availableFinishCodes = isGroupedSearchResult(item)
    ? item.available_finish_codes
    : representative.shell_finish_code
      ? [representative.shell_finish_code]
      : [];

  return {
    key: isGroupedSearchResult(item) ? item.search_family_key : representative.id,
    href: `/parts/${representative.id}`,
    pn: representative.example_full_pin ?? "Not yet extracted",
    slashSheet: representative.slash_sheet,
    description: representative.description ?? representative.name,
    cavityCount: representative.cavity_count,
    gender: representative.gender,
    contactType: representative.contact_type,
    variantCount: isGroupedSearchResult(item) ? item.variant_count : 1,
    availableFinishCodes,
    citationSpecSheet: representative.citation.spec_sheet,
  };
}

export function SearchResults({ results }: SearchResultsProps) {
  const resultItems = results?.grouped ? results.items : results?.raw_variants ?? [];

  if (!results) {
    return (
      <div className="empty">
        Start with a PN, cavity count, plug/receptacle, or pin/socket. Search results stay tied to
        the extracted source citations so users can trace every result back to the spec.
      </div>
    );
  }

  if (!resultItems.length) {
    return <div className="empty">No parts matched the current filters.</div>;
  }

  return (
    <div className="results">
      <div className="header-row">
        <h2 className="section-heading">Search Results</h2>
        <span className="pill accent">
          {results.total} {results.grouped ? "grouped results" : "raw variants"}
        </span>
      </div>
      {resultItems.map((item) => {
        const card = toSearchCardData(item as GroupedSearchResult | SearchResult);
        return (
        <Link
          key={card.key}
          className="result-card"
          href={card.href}
        >
          <div className="eyebrow">
            Slash {card.slashSheet} | {card.citationSpecSheet}
          </div>
          <h3 className="title">{card.pn}</h3>
          <p className="lead">{card.description}</p>
          <div className="meta">
            {card.cavityCount ? <span className="pill">{card.cavityCount} contacts</span> : null}
            <span className="pill">{card.gender ?? "Not yet extracted"}</span>
            <span className="pill">{card.contactType ?? "Not yet extracted"}</span>
            <span className="pill accent">
              {card.variantCount} {card.variantCount === 1 ? "variant" : "variants"}
            </span>
            {card.availableFinishCodes.length ? (
              <span className="pill">Finishes {card.availableFinishCodes.join(", ")}</span>
            ) : null}
            <span className="pill accent">{card.citationSpecSheet}</span>
          </div>
        </Link>
        );
      })}
    </div>
  );
}
