# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Python automation workspace for syncing `MIL-DTL-83513` PDFs from ASSIST into Supabase.

- `sync_83513_to_supabase.py`: primary end-to-end workflow
- `discover_83513.py`: Playwright discovery of current ASSIST documents
- `assist_83513_common.py`: shared HTTP/download parsing helpers
- `reorganize_supabase_paths.py`: one-time bucket path migration to `base/01/02/...`
- `download_83513_family.py`: local download utility for debugging
- `supabase_schema.sql`: database schema for `pdf_objects`
- `.env.local`: local secrets and runtime configuration

Generated JSON reports such as `83513_documents.json` and `83513_supabase_sync.json` are runtime artifacts, not source files.

## Build, Test, and Development Commands
- `python -m pip install -r requirements.txt`: install Python dependencies
- `python -m playwright install chromium`: install the browser used for ASSIST discovery
- `python sync_83513_to_supabase.py --limit 3`: safe smoke test against ASSIST and Supabase
- `python sync_83513_to_supabase.py`: full sync run
- `python reorganize_supabase_paths.py --dry-run`: preview storage path moves before applying them
- `python -m py_compile assist_83513_common.py discover_83513.py sync_83513_to_supabase.py`: quick syntax validation

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
This folder is not currently a Git repository, so no commit history is available to infer conventions. Use short, imperative commit subjects such as `Add ordered Supabase path migration`. PRs should describe the workflow impact, config/schema changes, and any live verification performed.

## Verification
A task is not complete until the relevant checks for the changed area pass. In the final response, summarize what changed, how it was verified, and any assumptions or open questions.
