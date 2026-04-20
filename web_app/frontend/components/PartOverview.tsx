import { PartDetail } from "@/lib/types";

type PartOverviewProps = {
  part: PartDetail;
};

export function PartOverview({ part }: PartOverviewProps) {
  const connectorMateRefs = part.mates_with.filter(
    (reference) => reference !== part.mounting_hardware_ref,
  );

  return (
    <div className="detail-card">
      <div className="eyebrow">{part.spec_sheet}</div>
      <h1 className="title">{part.name}</h1>
      <p className="lead">{part.example_full_pin ?? part.description ?? "No example part number available."}</p>

      <div className="meta">
        {part.connector_type ? <span className="pill accent">{part.connector_type}</span> : null}
        {part.cavity_count ? <span className="pill">{part.cavity_count} contacts</span> : null}
        {part.shell_size_letter ? <span className="pill">Shell {part.shell_size_letter}</span> : null}
        {part.shell_finish_code ? <span className="pill">Finish {part.shell_finish_code}</span> : null}
      </div>

      <div className="split" style={{ marginTop: 18 }}>
        <div className="panel panel-pad">
          <h2 className="section-heading">Configuration</h2>
          <ul className="citation-list">
            <li>Gender: {part.gender ?? "Unknown"}</li>
            <li>Contact type: {part.contact_type ?? "Unknown"}</li>
            <li>Shell finish: {part.shell_finish_description ?? part.shell_finish_code ?? "Unknown"}</li>
            <li>
              Compatible connector mates: {connectorMateRefs.join(", ") || "None listed"}
            </li>
            {part.mounting_hardware_ref ? (
              <li>Mounting hardware: {part.mounting_hardware_ref}</li>
            ) : null}
          </ul>
        </div>

        <div className="panel panel-pad">
          <h2 className="section-heading">Source Citation</h2>
          <ul className="citation-list">
            <li>Spec: {part.citation.spec_sheet}</li>
            {part.citation.revision ? <li>Revision: {part.citation.revision}</li> : null}
            {part.citation.source_page ? <li>Source page: {part.citation.source_page}</li> : null}
            {part.citation.source_url ? (
              <li>
                <a href={part.citation.source_url} target="_blank" rel="noreferrer">
                  Open ASSIST source
                </a>
              </li>
            ) : null}
          </ul>
        </div>
      </div>
    </div>
  );
}
