import { MateResponse } from "@/lib/types";

type MateResultsProps = {
  mates: MateResponse;
};

export function MateResults({ mates }: MateResultsProps) {
  if (!mates.mates.length) {
    return <div className="empty">No grouped compatible mates were returned for this part.</div>;
  }

  return (
    <div className="results">
      <div className="header-row">
        <h2 className="section-heading">Compatible Mates</h2>
        <span className="pill accent">{mates.mates.length} grouped mate families</span>
      </div>
      {mates.mates.map((mate) => (
        <div className="mate-card" key={mate.mate_family_key}>
          <div className="header-row">
            <div>
              <div className="eyebrow">Slash {mate.mate_slash_sheet}</div>
              <h3 className="title">{mate.representative_variant.name}</h3>
            </div>
            <span className="pill accent">{mate.variant_count} variants</span>
          </div>

          <p className="lead">{mate.representative_variant.spec_sheet}</p>

          <div className="reasons">
            {mate.match_reasons.map((reason) => (
              <span className="pill" key={reason}>
                {reason}
              </span>
            ))}
            <span className="pill accent">{mate.confidence_type}</span>
          </div>

          <ul className="citation-list">
            <li>Source spec: {mate.source_spec ?? mate.representative_variant.citation.spec_sheet}</li>
            {mate.source_page ? <li>Source page: {mate.source_page}</li> : null}
          </ul>

          <details>
            <summary className="pill warning">Show raw variants</summary>
            <ul className="variant-list">
              {mate.variants.map((variant) => (
                <li key={variant.id}>
                  {variant.spec_sheet}
                  {variant.shell_finish_code ? ` · Finish ${variant.shell_finish_code}` : ""}
                </li>
              ))}
            </ul>
          </details>
        </div>
      ))}
    </div>
  );
}
