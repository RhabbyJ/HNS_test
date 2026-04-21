# PostgreSQL

This folder contains the normalized database schema and the loader code that maps extraction JSON into queryable tables.

- `m83513_data_schema.sql`: extracted-spec tables and views
- `supabase_schema.sql`: raw `pdf_objects` inventory schema
- `m83513_load_extraction.py`: generic loader
- `m83513_load_03.py`: `/03` compatibility wrapper
- `m83513_load_torque_values.py`: legacy torque-only loader
- `backfill_torque_profile_model.py`: Phase-1 normalized torque profile backfill

`torque_values` is now treated as a legacy/staging source for extracted torque candidates and evidence. The normalized Phase-1 torque model is:

- `document_torque_status`: one row per document with torque mode, audit status, and profile summary.
- `torque_profiles`: shared canonical/provisional torque profiles such as `m83513_05_main`.
- `torque_profile_values`: deduplicated numeric torque facts under each profile.
- `document_torque_profile_map`: document-to-profile mappings.
- `torque_source_evidence`: raw extracted torque text and source metadata.

Run `python postgresql\backfill_torque_profile_model.py --apply` after `torque_values` is loaded to refresh the normalized torque tables without touching connector rows.
