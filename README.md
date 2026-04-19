# MIL-DTL-83513 Sync

Primary workflow:

```powershell
python sync_83513_to_supabase.py
```

Phase-1 extraction workflow:

```powershell
python m83513_extraction_engine.py --storage-path mil-dtl-83513/base/MIL-DTL-83513_base_rev_H.pdf --document-key base --spec-sheet MIL-DTL-83513H --title "Connectors, Electrical, Rectangular, Microminiature, Polarized Shell, General Specification for" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33934
```

Generic load workflow:

```powershell
python m83513_load_extraction.py --input-json m83513_base_extraction_output.json --apply
python m83513_load_extraction.py --input-json m83513_04_extraction_output.json --apply
```

`m83513_load_03.py` remains as a compatibility wrapper for existing `/03` runs, but new work should use `m83513_load_extraction.py`.

To reorganize existing Storage objects into ordered `base/01/02/...` paths:

```powershell
python reorganize_supabase_paths.py --dry-run
python reorganize_supabase_paths.py
```

One-time setup:

```powershell
python -m pip install -r requirements.txt
python -m playwright install chromium
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
- `v_83513_base_configurations`
- `v_83513_03_configurations`
- `v_83513_04_configurations`
- `v_83513_03_wire_options`

Useful commands:

```powershell
python sync_83513_to_supabase.py --limit 3
python download_83513_family.py --limit 3
python -m py_compile assist_83513_common.py discover_83513.py m83513_extraction_engine.py sync_83513_to_supabase.py
```
