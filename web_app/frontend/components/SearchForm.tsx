type SearchFormProps = {
  initialQuery?: string;
  initialSlashSheet?: string;
  initialCavityCount?: string;
};

const slashSheetOptions = [
  "base",
  ...Array.from({ length: 33 }, (_, index) => `${index + 1}`.padStart(2, "0")),
];

export function SearchForm({
  initialQuery = "",
  initialSlashSheet = "",
  initialCavityCount = "",
}: SearchFormProps) {
  return (
    <form className="panel panel-pad stack" action="/" method="get">
      <div>
        <label className="label" htmlFor="q">
          Search
        </label>
        <input
          className="field"
          id="q"
          name="q"
          defaultValue={initialQuery}
          placeholder="M83513/03-A01C or 51-pin plug"
        />
      </div>

      <div>
        <label className="label" htmlFor="slash_sheet">
          Slash Sheet
        </label>
        <select
          className="select"
          id="slash_sheet"
          name="slash_sheet"
          defaultValue={initialSlashSheet}
        >
          <option value="">All</option>
          {slashSheetOptions.map((slashSheet) => (
            <option key={slashSheet} value={slashSheet}>
              {slashSheet === "base" ? "Base" : slashSheet}
            </option>
          ))}
        </select>
      </div>

      <div>
        <label className="label" htmlFor="cavity_count">
          Cavity Count
        </label>
        <input
          className="field"
          id="cavity_count"
          name="cavity_count"
          defaultValue={initialCavityCount}
          placeholder="9, 15, 21, 51, 100"
        />
      </div>

      <button className="button" type="submit">
        Search Parts
      </button>
    </form>
  );
}
