# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Python automation workspace for syncing `MIL-DTL-83513` PDFs from ASSIST into Supabase.

- `assist/`: ASSIST discovery and low-rate download workflow
  - `assist_83513_common.py`: shared HTTP/download parsing helpers
  - `discover_83513.py`: Playwright discovery of current ASSIST documents
  - `download_83513_family.py`: local download utility for debugging
  - `artifacts/83513_documents.json`: discovery catalog output
- `pdf_storage/`: PDF storage workflow
  - `sync_83513_to_supabase.py`: primary end-to-end PDF sync
  - `reorganize_supabase_paths.py`: one-time bucket path migration to `base/01/02/...`
  - `artifacts/83513_supabase_sync.json`: sync report output
- `hybrid_extraction/`: extraction workflow
  - `m83513_extraction_engine.py`: phase-1 pdf-first extractor for structured JSON
  - `m83513_extraction_registry.py`: slash-sheet type registry and required-field rules
- `structured_json_validation/`: extraction output and validation workflow
  - `m83513_extraction_schema.json`: intermediate JSON contract for extraction output
  - `outputs/`: generated extraction JSON files
  - `audits/`: manual audit snapshots and batch reports
- `postgresql/`: PostgreSQL loading workflow
  - `m83513_load_extraction.py`: generic class-aware loader for extracted `MIL-DTL-83513` documents
  - `m83513_load_03.py`: compatibility wrapper for `MIL-DTL-83513/03`
  - `m83513_data_schema.sql`: normalized extraction tables and Supabase views for extracted spec data
  - `supabase_schema.sql`: database schema for `pdf_objects`
- `compatibility_rules_engine/`: golden mate tests and future deterministic + LLM-assisted mate rules
- `web_app/`: FastAPI + Next.js product layer
- `.env.local`: local secrets and runtime configuration

The root `*.py` files are compatibility entrypoints so existing commands like `python sync_83513_to_supabase.py` still work after the workflow reorganization.

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
- `/01` extraction and generic load are now implemented and applied successfully:
  - 48 base configuration rows
  - 0 wire-option rows
  - 8 text chunks
  - 1 extraction-run row
- `/02` extraction and generic load are now implemented and applied successfully:
  - 60 base configuration rows
  - 0 wire-option rows
  - 8 text chunks
  - 1 extraction-run row
- `base` extraction and generic load are now implemented and applied successfully:
  - 1 base configuration row
  - 54 text chunks
  - 1 extraction-run row
- `/04` extraction and generic load are now implemented and applied successfully:
  - 60 base configuration rows
  - 3480 wire-option rows
  - 11 text chunks
  - 1 extraction-run row
- The remaining slash sheets `/05` through `/33` have now been batch extracted and loaded into Supabase.
- `base_configurations` now has coverage for the full current `MIL-DTL-83513` family:
  - `base`
  - `/01` through `/33`
- Representative loaded row counts now include:
  - `/05`: 1 mounting-hardware row
  - `/06`-`/09`: 28 rows each for the class-P connector family
  - multi-row PCB-tail sheets such as `/10`, `/13`, `/16`, `/19`, `/22`, `/25`, `/28`, `/31`: 36 rows each
  - single-arrangement PCB-tail sheets such as `/11`, `/12`, `/14`, `/15`, `/17`, `/18`, `/20`, `/21`, `/23`, `/24`, `/26`, `/27`, `/29`, `/30`, `/32`, `/33`: 6 rows each
- The extractor now emits explicit `validation_checks` and `fallback_flags` so low-confidence pages and class mismatches are visible in JSON output instead of hidden behind the score alone.
- Class-P solder/crimp sheets now use their own 7-arrangement validation profile instead of the class-M 8-arrangement expectation.
- The shared Supabase path now has a REST fallback client for environments where `from supabase import create_client` is unavailable.
- `/04` ASSIST latest-revision resolution is fixed so `Revision K (all previous amendments incorporated)` resolves correctly.
- Normalized extraction tables have been cleaned up to generic names:
  - `base_configurations`
  - `hns_wire_options`
  - `text_chunks`
  - `extraction_runs`
- The product API is now live in `web_app/api` with:
  - `GET /search`
  - `GET /parts/{id}`
  - `GET /parts/{id}/mates`
- The mate endpoint now has two modes:
  - grouped product mode by default
  - raw variant mode for internal/debug validation only
- The search endpoint now also defaults to grouped product results, so finish-code variants collapse into a single result card while raw search remains available for internal/debug use.
- The golden mate suite now exists in `compatibility_rules_engine`:
  - `10` golden cases
  - `0` false positives
  - `0` false negatives
  - `10/10` grouped product passes
- A minimal Next.js frontend now exists in `web_app/frontend` with:
  - search page
  - part detail page
  - grouped mate results
- The frontend build has been verified successfully with `npm run build`.
- The frontend search/detail UX has been hardened for first-user testing:
  - search results tolerate both grouped and legacy raw response shapes
  - slash-sheet filter now supports `base` and `01` through `33`
  - trailing/leading spaces in search input are trimmed
  - part detail now separates compatible connector mates from mounting hardware references such as `MIL-DTL-83513/5`
- Supabase browsing should prefer the filtered views:
  - `v_83513_documents`
  - `v_83513_01_configurations`
  - `v_83513_02_configurations`
  - `v_83513_base_configurations`
  - `v_83513_configurations`
  - `v_83513_03_configurations`
  - `v_83513_04_configurations`
  - `v_83513_03_wire_options`

## Session Handoff
Known status at pause:

- The full `MIL-DTL-83513` family now has an extraction-to-load path implemented.
- `is_space_approved` in wire options is still heuristic and should be treated as inferred, not authoritative.
- Figure references are good enough for PDF deep links, but not yet curated for polished UI labels.
- Some PCB-tail sheets currently load with synthesized single configuration rows and empty `dimensions` JSON when the PDF text does not expose a clean dimensional table in extractable text.
- Vision LLM is intentionally not part of the default path. Use pdf-first parsing and only add LLM fallback for documents or pages that raise `fallback_flags` or remain diagram-heavy after deterministic cleanup.
- Mate logic should currently be treated as trustworthy at the slash-sheet level for `83513`, but raw variant duplication is expected and grouped mode should be the product default.
- Frontend scope is intentionally minimal for now:
  - no auth
  - no chat
  - no BOM builder
  - no complex client state

Next recommended step: run the FastAPI API and the Next.js frontend together, manually validate the first real user flow, and gather feedback before expanding to new families or adding non-core features.

## Build, Test, and Development Commands
- `python -m pip install -r requirements.txt`: install Python dependencies
- `python -m playwright install chromium`: install the browser used for ASSIST discovery
- `python sync_83513_to_supabase.py --limit 3`: safe smoke test against ASSIST and Supabase
- `python sync_83513_to_supabase.py`: full sync run
- `python reorganize_supabase_paths.py --dry-run`: preview storage path moves before applying them
- `python m83513_extraction_engine.py --storage-path mil-dtl-83513/01/MIL-DTL-83513_01_rev_J.pdf --document-key 1 --spec-sheet MIL-DTL-83513/1J --title "Connectors, Electrical, Rectangular, Plug, Microminiature, Polarized Shell, Pin Contacts, Class M, Solder Type" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33935 --output-json structured_json_validation/outputs/m83513_01_extraction_output.json`: real `/01` extraction run
- `python m83513_extraction_engine.py --storage-path mil-dtl-83513/02/MIL-DTL-83513_02_rev_H.pdf --document-key 2 --spec-sheet MIL-DTL-83513/2H --title "Connectors, Electrical, Rectangular, Receptacle, Microminiature, Polarized Shell, Socket Contacts, Class M, Solder Type" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33936 --output-json structured_json_validation/outputs/m83513_02_extraction_output.json`: real `/02` extraction run
- `python m83513_extraction_engine.py --storage-path mil-dtl-83513/03/MIL-DTL-83513_03_rev_K.pdf --document-key 3 --spec-sheet MIL-DTL-83513/3K --title "Connectors, Electrical, Rectangular, Plug, Microminiature, Polarized Shell, Pin Contacts, Class M, Crimp Type" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33937 --output-json structured_json_validation/outputs/m83513_03_extraction_output.json`: real `/03` extraction run
- `python m83513_extraction_engine.py --storage-path mil-dtl-83513/04/MIL-DTL-83513_04_rev_K.pdf --document-key 4 --spec-sheet MIL-DTL-83513/4K --title "Connectors, Electrical, Rectangular, Receptacle, Microminiature, Polarized Shell, Socket Contacts, Class M, Crimp Type" --source-url https://quicksearch.dla.mil/qsDocDetails.aspx?ident_number=33938 --output-json structured_json_validation/outputs/m83513_04_extraction_output.json`: real `/04` extraction run
- `python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_base_extraction_output.json`: dry-run base-spec loader preview
- `python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_01_extraction_output.json --apply`: write `/01` normalized rows to Supabase
- `python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_02_extraction_output.json --apply`: write `/02` normalized rows to Supabase
- `python m83513_load_extraction.py --input-json structured_json_validation/outputs/m83513_04_extraction_output.json --apply`: write `/04` normalized rows to Supabase
- `python m83513_load_03.py --input-json structured_json_validation/outputs/m83513_03_extraction_output.json --apply`: compatibility path for `/03`
- `python -m py_compile assist_83513_common.py discover_83513.py m83513_extraction_engine.py m83513_load_extraction.py m83513_load_03.py sync_83513_to_supabase.py`: quick syntax validation
- `python compatibility_rules_engine/run_golden_mate_suite.py`: run the 10-case golden mate suite against the local FastAPI app
- `python -m uvicorn web_app.api.main:app --host 127.0.0.1 --port 8000`: run the FastAPI product API
- `cd web_app/frontend && cmd /c npm.cmd run dev`: run the Next.js frontend

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
