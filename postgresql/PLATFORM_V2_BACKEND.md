# Platform V2 Backend

This is the scalable backend target for moving beyond the MIL-DTL-83513 proof of concept.

The v1 system remains the live serving path for now. V2 is additive: new schemas, a release-scoped canonical model, and cold-start payload generation from latest PDFs in Storage.

V2 must prove it can rebuild the correct dataset from source. Do not migrate or copy V1 canonical tables into V2. V1 may be used only as a comparison baseline until V2 has passed a full cold-start rebuild, one additional refresh cycle, and API read cutover.

## Layer Model

### ingest

Owns ASSIST document identity and source text.

- `ingest.documents`
- `ingest.document_chunks`

This layer answers: which PDF, which revision/amendment, which checksum, which pages/chunks.

### extract

Owns parser outputs and evidence. Parser details should stay here and not shape product tables.

- `extract.extraction_runs`
- `extract.extraction_outputs`
- `extract.extraction_evidence`

This layer answers: which parser ran, what JSON it produced, what validation flags/evidence existed.

### catalog

Owns product-domain facts.

- `catalog.configurations`
- `catalog.wire_options`
- `catalog.hardware_options`
- `catalog.torque_profiles`
- `catalog.torque_profile_values`
- `catalog.document_profile_links`
- `catalog.mating_relationships`
- `catalog.fact_evidence`

Rows in `catalog.configurations` represent orderable variants, not physical geometry rows or parser internals.

### publish

Owns dataset releases.

- `publish.releases`
- `publish.active_releases`

The API should query the active release instead of depending on table-level swaps.

### api

Owns serving views.

- `api.v_documents_current`
- `api.v_configurations_current`
- `api.v_wire_options_current`
- `api.v_torque_effective_current`

## Current 83513 Cold-Start Command

Build a v2 release from current ASSIST discovery, latest PDFs in Storage, fresh extraction, and fresh canonicalization:

```powershell
$env:PYTHONPATH='C:\Users\rjega\AppData\Roaming\Python\Python313\site-packages'
python structured_json_validation\cold_start_83513_v2.py `
  --run-id cold_start_v2_83513_proof `
  --release-name 83513-v2-cold-proof
```

Expected key counts:

- `ingest.documents`: 34
- `catalog.configurations`: 630
- `catalog.wire_options`: 6824
- `catalog.torque_profiles`: 3
- `catalog.torque_profile_values`: 9
- `/02` configurations: 60
- `/04` configurations: 60
- `/06-/09` configurations: 7 each, no finish suffixes

The first cold-start proof wrote:

- source manifest: `structured_json_validation/staging/cold_start_v2_83513_proof/source_manifest.json`
- fresh extraction outputs: `structured_json_validation/staging/cold_start_v2_83513_proof/fresh_extraction_outputs/`
- v2 release payloads: `structured_json_validation/staging/cold_start_v2_83513_proof/v2_payloads/`
- acceptance report: `structured_json_validation/staging/cold_start_v2_83513_proof/cold_start_v2_report.json`

Result: 32 gates passed, 0 failed.

## Loading V2 Tables

Apply `postgresql/platform_v2_schema.sql`, then load the cold-start payload:

```powershell
python structured_json_validation\load_platform_v2_release.py `
  --payload-dir structured_json_validation\staging\cold_start_v2_83513_proof\v2_payloads `
  --apply
```

Supabase REST must expose the `ingest`, `extract`, `catalog`, `publish`, and `api` schemas for the REST loader to work. If those schemas are not exposed, use a direct SQL migration/session or enable the schemas before loading.

## Migration Strategy

1. Keep v1 live and stable.
2. Apply `postgresql/platform_v2_schema.sql`.
3. Run the full cold-start V2 rebuild from Storage.
4. Load and publish the V2 release.
5. Compare V2 against PDF acceptance gates and V1 parity queries.
6. Point new API queries at `api.v_*_current`.
7. Run one more real refresh cycle through V2.
8. Dump/archive V1, move it to a legacy schema or leave it read-only for a soak period, then delete V1 in a controlled cleanup.

## Design Rules

- Documents, extraction runs, canonical facts, and publication state are separate layers.
- Orderable variants define catalog rows.
- Geometry and parser tables only annotate catalog rows.
- New MIL spec families should add rows, not new core tables.
- Typed columns are used for facts the app filters/searches often; family-specific detail stays in JSONB plus evidence rows.
