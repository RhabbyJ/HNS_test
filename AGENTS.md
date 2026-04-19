# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Python automation workspace for syncing `MIL-DTL-83513` PDFs from ASSIST into Supabase.

- `sync_83513_to_supabase.py`: primary end-to-end workflow
- `discover_83513.py`: Playwright discovery of current ASSIST documents
- `assist_83513_common.py`: shared HTTP/download parsing helpers
- `reorganize_supabase_paths.py`: one-time bucket path migration to `base/01/02/...`
- `download_83513_family.py`: local download utility for debugging
- `m83513_extraction_engine.py`: phase-1 pdf-first extractor for structured JSON
- `m83513_extraction_registry.py`: slash-sheet type registry and required-field rules
- `m83513_load_extraction.py`: generic class-aware loader for extracted `MIL-DTL-83513` documents
- `m83513_load_03.py`: compatibility wrapper for `MIL-DTL-83513/03`
- `m83513_data_schema.sql`: normalized extraction tables and Supabase views for extracted spec data
- `m83513_extraction_schema.json`: intermediate JSON contract for extraction output
- `supabase_schema.sql`: database schema for `pdf_objects`
- `.env.local`: local secrets and runtime configuration

Generated JSON reports such as `83513_documents.json` and `83513_supabase_sync.json` are runtime artifacts, not source files.

## Current Progress
Current validated state:

- `pdf_objects` and private Storage bucket `mil-spec-pdfs` are populated in Supabase for the current `MIL-DTL-83513` family.
- Actual ASSIST family count is `34` documents: base spec plus slash sheets `/1` through `/33`. The apparent `35th` row on Quick Search is a search-criteria row, not a document.
- Storage and query ordering are normalized around `base`, `01`, `02`, ..., `33`, and `pdf_objects.sort_order` is available for ordered queries.
- Phase-1 `/03` extraction is working with deterministic parsing and no LLM fallback.
- `/03` extraction currently produces:
  - 8 configuration rows
  - 58 wire-option rows
  - 11 text chunks
  - confidence `0.97`
- `/03` loader has already been applied successfully to Supabase:
  - 48 base configuration rows
  - 2784 wire-option rows
  - 11 text-chunk rows
  - 1 extraction-run row
- `base` extraction and generic load are now implemented and applied successfully:
  - 1 base configuration row
  - 54 text chunks
  - 1 extraction-run row
- `/04` extraction and generic load are now implemented and applied successfully:
  - 48 base configuration rows
  - 1056 wire-option rows
  - 9 text chunks
  - 1 extraction-run row
- Normalized extraction tables have been cleaned up to generic names:
  - `base_configurations`
  - `hns_wire_options`
  - `text_chunks`
  - `extraction_runs`
- Supabase browsing should prefer the filtered views:
  - `v_83513_documents`
  - `v_83513_base_configurations`
  - `v_83513_configurations`
  - `v_83513_03_configurations`
  - `v_83513_04_configurations`
  - `v_83513_03_wire_options`

## Session Handoff
Known status at pause:

- `base`, `/03`, and `/04` have a full extraction-to-load path implemented.
- `is_space_approved` in wire options is still heuristic and should be treated as inferred, not authoritative.
- Figure references are good enough for PDF deep links, but not yet curated for polished UI labels.
- Vision LLM is intentionally not part of the default path. Use pdf-first parsing and only add LLM fallback for low-confidence pages or diagram-heavy cases.

Next recommended step: implement `/01` and `/02` on the same shared connector class path, then verify mate-finder compatibility across `/01`-`/04`.

## Build, Test, and Development Commands
- `python -m pip install -r requirements.txt`: install Python dependencies
- `python -m playwright install chromium`: install the browser used for ASSIST discovery
- `python sync_83513_to_supabase.py --limit 3`: safe smoke test against ASSIST and Supabase
- `python sync_83513_to_supabase.py`: full sync run
- `python reorganize_supabase_paths.py --dry-run`: preview storage path moves before applying them
- `python m83513_extraction_engine.py --storage-path mil-dtl-83513/03/MIL-DTL-83513_03_rev_K.pdf --document-key 3 --spec-sheet MIL-DTL-83513/3K --title "Connectors, Electrical, Rectangular, Plug, Microminiature, Polarized Shell, Pin Contacts, Class M, Crimp Type" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33937 --output-json m83513_03_extraction_output.json`: real `/03` extraction run
- `python m83513_extraction_engine.py --storage-path mil-dtl-83513/04/MIL-DTL-83513_04_rev_J.pdf --document-key 4 --spec-sheet MIL-DTL-83513/4J --title "Connectors, Electrical, Rectangular, Receptacle, Microminiature, Polarized Shell, Socket Contacts, Class M, Crimp Type" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33938 --output-json m83513_04_extraction_output.json`: real `/04` extraction run
- `python m83513_load_extraction.py --input-json m83513_base_extraction_output.json`: dry-run base-spec loader preview
- `python m83513_load_extraction.py --input-json m83513_04_extraction_output.json --apply`: write `/04` normalized rows to Supabase
- `python m83513_load_03.py --input-json m83513_03_extraction_output.json --apply`: compatibility path for `/03`
- `python -m py_compile assist_83513_common.py discover_83513.py m83513_extraction_engine.py m83513_load_extraction.py m83513_load_03.py sync_83513_to_supabase.py`: quick syntax validation

## Coding Style & Naming Conventions
Use 4-space indentation and standard Python style. Prefer explicit, descriptive names such as `document_key`, `storage_path`, and `revision_letter`. Keep shared ASSIST parsing logic in `assist_83513_common.py` rather than duplicating regex or HTTP flow in multiple scripts. New filenames should follow the existing lowercase, underscore-separated pattern.

## Working Style
State assumptions briefly before coding when they materially affect the implementation. If multiple reasonable interpretations exist, present them briefly instead of silently choosing one. Prefer the simplest solution that fully satisfies the request. Do not add speculative features, abstractions, configurability, or error handling unless requested. If ambiguity affects correctness, stop and ask.

## Editing Rules
Make surgical changes only. Change only files and lines directly related to the request, do not refactor unrelated code, and match the existing style and structure. Remove imports, variables, or functions made unused by your own edits only. Mention unrelated issues separately without changing them.

## Testing Guidelines
There is no formal test suite yet. Before changing code, define success criteria briefly. For bug fixes, reproduce if practical, then fix, then verify. For new behavior, add or update tests when the repo already uses tests or when a targeted test is the clearest verification. Run the smallest relevant check first, usually `python -m py_compile`, then a limited live run such as `python sync_83513_to_supabase.py --limit 3`. If you add tests, use `tests/` and name files `test_*.py`.

## Security & Configuration Tips
Do not commit real secrets. `.env.local` is ignored and should hold `SUPABASE_URL`, `SUPABASE_SECRET_KEY` or `SUPABASE_SERVICE_ROLE_KEY`, and bucket/table settings. Use private Storage buckets unless public access is explicitly required.

## Commit & Pull Request Guidelines
This repo now has Git initialized but only a minimal history, so conventions should stay simple. Use short, imperative commit subjects such as `Add /03 extraction loader`. PRs should describe the workflow impact, config/schema changes, Supabase migrations, and any live verification performed.

## Verification
A task is not complete until the relevant checks for the changed area pass. In the final response, summarize what changed, how it was verified, and any assumptions or open questions.
