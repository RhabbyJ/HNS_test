type SearchFormProps = {
  initialQuery?: string;
  initialCavityCount?: string;
  initialGender?: string;
  initialContactType?: string;
};

export function SearchForm({
  initialQuery = "",
  initialCavityCount = "",
  initialGender = "",
  initialContactType = "",
}: SearchFormProps) {
  return (
    <form className="panel panel-pad stack" action="/" method="get">
      <div>
        <label className="label" htmlFor="q">
          PN
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

      <div>
        <label className="label" htmlFor="gender">
          Plug/Receptacle
        </label>
        <select
          className="select"
          id="gender"
          name="gender"
          defaultValue={initialGender}
        >
          <option value="">All</option>
          <option value="PLUG">Plug</option>
          <option value="RECEPTACLE">Receptacle</option>
        </select>
      </div>

      <div>
        <label className="label" htmlFor="contact_type">
          Pin/Socket
        </label>
        <select
          className="select"
          id="contact_type"
          name="contact_type"
          defaultValue={initialContactType}
        >
          <option value="">All</option>
          <option value="PIN">Pin</option>
          <option value="SOCKET">Socket</option>
        </select>
      </div>

      <button className="button" type="submit">
        Search Parts
      </button>
    </form>
  );
}
