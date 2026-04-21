import { PartDetail } from "@/lib/types";

type PartOverviewProps = {
  part: PartDetail;
};

export function PartOverview({ part }: PartOverviewProps) {
  const connectorMateRefs = part.mates_with.filter(
    (reference) => reference !== part.mounting_hardware_ref,
  );
  const notExtracted = "Not yet extracted";
  const hardwareValues = part.hardware_options.length
    ? part.hardware_options.map((option) => `${option.code}: ${option.description}`).join(", ")
    : part.mounting_hardware_ref ?? notExtracted;
  const torqueValues = part.torque_values.length ? part.torque_values.join(", ") : notExtracted;

  return (
    <div className="stack">
      <div className="detail-card">
        <div className="eyebrow">{part.spec_sheet}</div>
        <h1 className="title">{part.example_full_pin ?? notExtracted}</h1>
        <p className="lead">{part.description ?? part.name}</p>

        <div className="meta">
          <span className="pill accent">PN {part.example_full_pin ?? notExtracted}</span>
          <span className="pill">{part.cavity_count ? `${part.cavity_count} contacts` : notExtracted}</span>
          <span className="pill">{part.gender ?? notExtracted}</span>
          <span className="pill">{part.contact_type ?? notExtracted}</span>
        </div>
      </div>

      <div className="split">
        <div className="panel panel-pad">
          <h2 className="section-heading">Compatibility</h2>
          <ul className="citation-list">
            <li>
              Mating connector: {connectorMateRefs.join(", ") || notExtracted}
            </li>
            <li>Plug/Receptacle: {part.gender ?? notExtracted}</li>
            <li>Pin/Socket: {part.contact_type ?? notExtracted}</li>
            <li>Shell finish: {part.shell_finish_description ?? part.shell_finish_code ?? notExtracted}</li>
          </ul>
        </div>

        <div className="panel panel-pad">
          <h2 className="section-heading">Hardware</h2>
          <ul className="citation-list">
            <li>Jackscrews/jackposts: {hardwareValues}</li>
            <li>Mounting hardware reference: {part.mounting_hardware_ref ?? notExtracted}</li>
          </ul>
        </div>

        <div className="panel panel-pad">
          <h2 className="section-heading">Wire</h2>
          <ul className="citation-list">
            <li>Acceptable wire range: {part.wire_range ?? notExtracted}</li>
            <li>
              Wire options: {part.wire_options.length ? `${part.wire_options.length} extracted` : notExtracted}
            </li>
          </ul>
        </div>

        <div className="panel panel-pad">
          <h2 className="section-heading">Engineering</h2>
          <ul className="citation-list">
            <li>Torque values: {torqueValues}</li>
            <li>Shell material: {part.shell_material ?? notExtracted}</li>
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
