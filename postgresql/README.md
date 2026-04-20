# PostgreSQL

This folder contains the normalized database schema and the loader code that maps extraction JSON into queryable tables.

- `m83513_data_schema.sql`: extracted-spec tables and views
- `supabase_schema.sql`: raw `pdf_objects` inventory schema
- `m83513_load_extraction.py`: generic loader
- `m83513_load_03.py`: `/03` compatibility wrapper
