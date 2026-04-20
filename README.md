# MIL-DTL-83513 Sync

Workflow-aligned repo layout:

- [assist](C:/Users/rjega/HNS_test/assist): ASSIST discovery, details-page parsing, and low-rate/manual download helpers
- [pdf_storage](C:/Users/rjega/HNS_test/pdf_storage): Supabase Storage sync and storage-path reorganization
- [hybrid_extraction](C:/Users/rjega/HNS_test/hybrid_extraction): pdf-first extraction engine and document-class registry
- [structured_json_validation](C:/Users/rjega/HNS_test/structured_json_validation): extraction schema plus generated JSON outputs/audits
- [postgresql](C:/Users/rjega/HNS_test/postgresql): normalized table schema and load scripts
- [compatibility_rules_engine](C:/Users/rjega/HNS_test/compatibility_rules_engine): reserved for compatibility logic
- [web_app](C:/Users/rjega/HNS_test/web_app): FastAPI + Next.js product app

The root `*.py` files are now thin compatibility entrypoints so the existing commands still work.

Primary workflow:

```powershell
python sync_83513_to_supabase.py
```

Current product milestone:

- grouped mate output is the default product path
- raw mate variants are retained for debugging only
- the 10-case golden mate suite passes with:
  - `0` false positives
  - `0` false negatives
  - `10/10` grouped product passes

Phase-1 extraction workflow:

```powershell
python m83513_extraction_engine.py --storage-path mil-dtl-83513/base/MIL-DTL-83513_base_rev_H.pdf --document-key base --spec-sheet MIL-DTL-83513H --title "Connectors, Electrical, Rectangular, Microminiature, Polarized Shell, General Specification for" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33934
```

Generic load workflow:

```powershell
python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_01_extraction_output.json --apply
python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_02_extraction_output.json --apply
python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_base_extraction_output.json --apply
python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_04_extraction_output.json --apply
```

`m83513_load_03.py` remains as a compatibility wrapper for existing `/03` runs, but new work should use `m83513_load_extraction.py`.

Generated runtime artifacts now live under:

- [assist/artifacts](C:/Users/rjega/HNS_test/assist/artifacts)
- [pdf_storage/artifacts](C:/Users/rjega/HNS_test/pdf_storage/artifacts)
- [structured_json_validation/outputs](C:/Users/rjega/HNS_test/structured_json_validation/outputs)
- [structured_json_validation/audits](C:/Users/rjega/HNS_test/structured_json_validation/audits)

To reorganize existing Storage objects into ordered `base/01/02/...` paths:

```powershell
python reorganize_supabase_paths.py --dry-run
python reorganize_supabase_paths.py
```

One-time setup:

```powershell
python -m pip install -r requirements.txt
python -m playwright install chromium
cd web_app/frontend
cmd /c npm.cmd install --cache .npm-cache
```

Required local configuration lives in `.env.local`:

- `SUPABASE_URL`
- `SUPABASE_SECRET_KEY` or `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_STORAGE_BUCKET`
- `SUPABASE_STORAGE_PREFIX`
- `SUPABASE_METADATA_TABLE` (optional, leave blank to skip database upserts)

The Supabase sync does this in one run:

1. Discovers the current active `MIL-DTL-83513` base/slash-sheet records from ASSIST with Playwright.
2. Follows each details page and selects only the latest `Revision <LETTER>` PDF.
3. Uploads the PDF bytes directly to Supabase Storage without writing files to disk.
4. Optionally upserts document metadata into the table from `supabase_schema.sql`.

Normalized extraction data now lands in generic tables rather than family-prefixed tables:

- `base_configurations`
- `hns_wire_options`
- `text_chunks`
- `extraction_runs`

For easier browsing in the Supabase dashboard, use the filtered views:

- `v_83513_documents`
- `v_83513_configurations`
- `v_83513_01_configurations`
- `v_83513_02_configurations`
- `v_83513_base_configurations`
- `v_83513_03_configurations`
- `v_83513_04_configurations`
- `v_83513_03_wire_options`

Useful commands:

```powershell
python sync_83513_to_supabase.py --limit 3
python download_83513_family.py --limit 3
python -m py_compile assist_83513_common.py discover_83513.py m83513_extraction_engine.py sync_83513_to_supabase.py
python compatibility_rules_engine/run_golden_mate_suite.py
python -m uvicorn web_app.api.main:app --host 127.0.0.1 --port 8000
cd web_app/frontend
cmd /c npm.cmd run dev
```
