import Link from "next/link";

import { MateResponse } from "@/lib/types";

type MateResultsProps = {
  mates: MateResponse;
};

export function MateResults({ mates }: MateResultsProps) {
  const notExtracted = "Not yet extracted";

  if (!mates.mates.length) {
    return <div className="empty">No grouped compatible mates were returned for this part.</div>;
  }

  return (
    <div className="results">
      <div className="header-row">
        <h2 className="section-heading">Compatible Mates</h2>
        <span className="pill accent">{mates.mates.length} grouped mate families</span>
      </div>
      {mates.mates.map((mate) => {
        const representative = mate.representative_variant;
        const evidenceHref = representative.citation.source_url;

        return (
          <div className="mate-card" key={mate.mate_family_key}>
            <div className="header-row">
              <div>
                <div className="eyebrow">Slash {mate.mate_slash_sheet}</div>
                <h3 className="title">
                  <Link className="text-link" href={`/parts/${representative.id}`}>
                    {representative.example_full_pin ?? notExtracted}
                  </Link>
                </h3>
              </div>
              <span className="pill accent">{mate.variant_count} variants</span>
            </div>

            <p className="lead">{representative.name}</p>

            <div className="reasons">
              <span className="pill accent">
                Mating connector {representative.spec_sheet}
              </span>
              <span className="pill">{representative.gender ?? notExtracted}</span>
              <span className="pill">{representative.contact_type ?? notExtracted}</span>
              {mate.match_reasons.map((reason) => (
                <span className="pill" key={reason}>
                  {reason}
                </span>
              ))}
              <span className="pill accent">{mate.confidence_type}</span>
            </div>

            <ul className="citation-list">
              <li>Why it matches: {mate.match_reasons.join(" ")}</li>
              <li>Hardware compatibility: {mate.hardware_compatibility ?? notExtracted}</li>
              <li>
                Source evidence:{" "}
                {evidenceHref ? (
                  <a className="text-link" href={evidenceHref} target="_blank" rel="noreferrer">
                    {mate.source_spec ?? representative.citation.spec_sheet}
                  </a>
                ) : (
                  mate.source_spec ?? representative.citation.spec_sheet
                )}
                {mate.source_page ? `, page ${mate.source_page}` : ""}
              </li>
            </ul>

            <details>
              <summary className="pill warning">Show raw variants</summary>
              <ul className="variant-list">
                {mate.variants.map((variant) => (
                  <li key={variant.id}>
                    <Link className="text-link" href={`/parts/${variant.id}`}>
                      {variant.example_full_pin ?? variant.spec_sheet}
                    </Link>
                    {variant.shell_finish_code ? ` - Finish ${variant.shell_finish_code}` : ""}
                  </li>
                ))}
              </ul>
            </details>
          </div>
        );
      })}
    </div>
  );
}
