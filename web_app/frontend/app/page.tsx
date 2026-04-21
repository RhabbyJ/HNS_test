import { SearchForm } from "@/components/SearchForm";
import { SearchResults } from "@/components/SearchResults";
import { searchParts } from "@/lib/api";

type HomePageProps = {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
};

function readFirst(value: string | string[] | undefined): string {
  const raw = Array.isArray(value) ? value[0] ?? "" : value ?? "";
  return raw.trim();
}

export default async function HomePage({ searchParams }: HomePageProps) {
  const params = await searchParams;
  const q = readFirst(params.q);
  const slashSheet = readFirst(params.slash_sheet);
  const cavityCount = readFirst(params.cavity_count);
  const gender = readFirst(params.gender);
  const contactType = readFirst(params.contact_type);

  let results = null;
  if (q || slashSheet || cavityCount || gender || contactType) {
    const query = new URLSearchParams();
    if (q) query.set("q", q);
    if (slashSheet) query.set("slash_sheet", slashSheet);
    if (cavityCount) query.set("cavity_count", cavityCount);
    if (gender) query.set("gender", gender);
    if (contactType) query.set("contact_type", contactType);
    results = await searchParts(query);
  }

  return (
    <main>
      <div className="shell">
        <section className="hero">
          <h1>HarnessMate</h1>
          <p>
            Search Micro-D parts, open the extracted detail view, and inspect grouped compatible
            mates with deterministic reasons and source citations.
          </p>
        </section>

        <div className="grid">
          <SearchForm
            initialQuery={q}
            initialCavityCount={cavityCount}
            initialGender={gender}
            initialContactType={contactType}
          />
          <SearchResults results={results} />
        </div>
      </div>
    </main>
  );
}
