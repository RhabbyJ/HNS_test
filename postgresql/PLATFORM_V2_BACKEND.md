# Platform V2 Backend

This is the scalable backend target for moving beyond the MIL-DTL-83513 proof of concept.

The v1 system remains the live serving path for now. V2 is additive: new schemas, a release-scoped canonical model, and payload generation from the already-verified extraction outputs.

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

## Current 83513 Seed Command

Build a v2 payload from the verified post-release outputs:

```powershell
python structured_json_validation\build_83513_v2_release.py `
  --outputs-dir structured_json_validation\staging\storage_refresh_02_full\staged\outputs `
  --documents-json structured_json_validation\staging\storage_refresh_02_full\snapshot\documents.json `
  --run-id platform_v2_seed_83513 `
  --release-name 83513-v2-poc `
  --created-from-run-id storage_refresh_02_full
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

## Migration Strategy

1. Keep v1 live and stable.
2. Apply `postgresql/platform_v2_schema.sql`.
3. Generate and review the v2 payload for 83513.
4. Add a DB loader for the v2 payloads once the schema is accepted.
5. Point new API queries at `api.v_*_current`.
6. Retire v1 slash-specific views only after the app no longer depends on them.

## Design Rules

- Documents, extraction runs, canonical facts, and publication state are separate layers.
- Orderable variants define catalog rows.
- Geometry and parser tables only annotate catalog rows.
- New MIL spec families should add rows, not new core tables.
- Typed columns are used for facts the app filters/searches often; family-specific detail stays in JSONB plus evidence rows.
