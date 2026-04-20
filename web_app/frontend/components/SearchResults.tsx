import Link from "next/link";

import { SearchResponse } from "@/lib/types";

type SearchResultsProps = {
  results: SearchResponse | null;
};

export function SearchResults({ results }: SearchResultsProps) {
  if (!results) {
    return (
      <div className="empty">
        Start with a part number, slash sheet, or cavity count. Search results stay tied to
        the extracted source citations so users can trace every result back to the spec.
      </div>
    );
  }

  if (!results.items.length) {
    return <div className="empty">No parts matched the current filters.</div>;
  }

  return (
    <div className="results">
      <div className="header-row">
        <h2 className="section-heading">Search Results</h2>
        <span className="pill accent">{results.total} matching parts</span>
      </div>
      {results.items.map((item) => (
        <Link key={item.id} className="result-card" href={`/parts/${item.id}`}>
          <div className="eyebrow">
            Slash {item.slash_sheet} · {item.connector_type ?? "Connector"}
          </div>
          <h3 className="title">{item.name}</h3>
          <p className="lead">{item.example_full_pin ?? item.description ?? "No example part available."}</p>
          <div className="meta">
            {item.cavity_count ? <span className="pill">{item.cavity_count} contacts</span> : null}
            {item.shell_size_letter ? <span className="pill">Shell {item.shell_size_letter}</span> : null}
            {item.shell_finish_code ? <span className="pill">Finish {item.shell_finish_code}</span> : null}
            <span className="pill accent">{item.citation.spec_sheet}</span>
          </div>
        </Link>
      ))}
    </div>
  );
}
