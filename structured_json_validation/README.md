# Structured JSON + Validation

This folder holds the extraction contract and the generated JSON artifacts that sit between the PDF extractor and the PostgreSQL loaders.

- `m83513_extraction_schema.json`: the intermediate JSON contract
- `outputs/`: generated extraction output files
- `audits/`: manual audit snapshots and batch reports
- `staging/`: non-destructive rebuild snapshots, staged payloads, and diff reports

## Non-Destructive Full Rebuild

Use `rebuild_83513_staging.py` when parser changes need to be validated across the whole family before touching live derived tables.

Fresh extraction and diff:

```powershell
python -m pip install -r requirements.txt
python structured_json_validation\rebuild_83513_staging.py
```

Smoke/diff using existing output JSON only:

```powershell
python structured_json_validation\rebuild_83513_staging.py --skip-extract
```

The script snapshots live Supabase tables, copies the current extraction outputs, builds staged loader payloads locally, rebuilds the torque resolution model locally, and writes `rebuild_diff_report.json`. It does not delete or mutate live tables.
