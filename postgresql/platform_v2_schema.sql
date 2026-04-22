create extension if not exists pgcrypto;

create schema if not exists ingest;
create schema if not exists extract;
create schema if not exists catalog;
create schema if not exists publish;
create schema if not exists api;

create table if not exists ingest.documents (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null,
  slash_sheet text not null default 'base',
  spec_sheet text not null,
  revision text,
  amendment text,
  document_date date,
  title text not null,
  source_url text not null,
  storage_path text not null,
  checksum text,
  source_size_bytes bigint,
  is_latest boolean not null default true,
  status text not null default 'active'
    check (status in ('active', 'superseded', 'archived')),
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (spec_family, slash_sheet, revision, amendment)
);

create table if not exists ingest.document_chunks (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references ingest.documents(id) on delete cascade,
  page_number integer not null,
  chunk_index integer not null,
  text_content text not null,
  layout jsonb,
  created_at timestamptz not null default now(),
  unique (document_id, page_number, chunk_index)
);

create table if not exists extract.extraction_runs (
  id uuid primary key default gen_random_uuid(),
  document_id uuid not null references ingest.documents(id) on delete cascade,
  parser_version text not null,
  registry_version text,
  run_type text not null,
  status text not null default 'completed'
    check (status in ('queued', 'running', 'completed', 'failed')),
  confidence_score numeric(4,2),
  validation_summary jsonb not null default '{}'::jsonb,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now()
);

create table if not exists extract.extraction_outputs (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references extract.extraction_runs(id) on delete cascade,
  document_id uuid not null references ingest.documents(id) on delete cascade,
  output_json jsonb not null,
  issues_json jsonb not null default '[]'::jsonb,
  field_presence jsonb not null default '{}'::jsonb,
  fallback_required boolean not null default false,
  source_hash text,
  created_at timestamptz not null default now(),
  unique (run_id, document_id)
);

create table if not exists extract.extraction_evidence (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references extract.extraction_runs(id) on delete cascade,
  document_id uuid not null references ingest.documents(id) on delete cascade,
  fact_type text not null,
  fact_key text not null,
  page_number integer,
  source_text text,
  source_ref text,
  confidence numeric(4,2),
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists publish.releases (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null,
  release_name text not null,
  created_from_run_id text,
  status text not null default 'draft'
    check (status in ('draft', 'staged', 'published', 'archived')),
  notes text,
  created_at timestamptz not null default now(),
  published_at timestamptz,
  unique (spec_family, release_name)
);

create table if not exists publish.active_releases (
  spec_family text primary key,
  release_id uuid not null references publish.releases(id),
  activated_at timestamptz not null default now()
);

create table if not exists catalog.configurations (
  id uuid primary key default gen_random_uuid(),
  release_id uuid not null references publish.releases(id) on delete cascade,
  document_id uuid not null references ingest.documents(id),
  spec_family text not null,
  slash_sheet text not null,
  spec_sheet text not null,
  revision text,
  part_number_example text,
  connector_type text not null,
  class_code text,
  shell_material text,
  contact_type text,
  gender text,
  termination_style text,
  cavity_count integer,
  insert_arrangement_code text,
  shell_finish_code text,
  shell_size_letter text,
  name text not null,
  description text,
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (release_id, spec_sheet, cavity_count, shell_size_letter, shell_finish_code, insert_arrangement_code, part_number_example)
);

create table if not exists catalog.wire_options (
  id uuid primary key default gen_random_uuid(),
  configuration_id uuid not null references catalog.configurations(id) on delete cascade,
  wire_type_code text not null,
  wire_specification text,
  wire_length_inches numeric(6,2),
  wire_notes text,
  is_space_approved boolean not null default false,
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (configuration_id, wire_type_code)
);

create table if not exists catalog.hardware_options (
  id uuid primary key default gen_random_uuid(),
  configuration_id uuid references catalog.configurations(id) on delete cascade,
  document_id uuid references ingest.documents(id) on delete cascade,
  hardware_code text,
  hardware_type text,
  thread text,
  profile text,
  drive text,
  description text not null,
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists catalog.torque_profiles (
  id uuid primary key default gen_random_uuid(),
  release_id uuid not null references publish.releases(id) on delete cascade,
  profile_code text not null,
  source_document_id uuid references ingest.documents(id),
  governing_document_id uuid references ingest.documents(id),
  profile_status text not null default 'verified',
  profile_kind text not null default 'document_specific',
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (release_id, profile_code)
);

create table if not exists catalog.torque_profile_values (
  id uuid primary key default gen_random_uuid(),
  profile_id uuid not null references catalog.torque_profiles(id) on delete cascade,
  context text not null,
  fastener_thread text,
  source_thread_label text,
  arrangement_scope text,
  torque_min_in_lbf numeric(6,2),
  torque_max_in_lbf numeric(6,2),
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists catalog.document_profile_links (
  id uuid primary key default gen_random_uuid(),
  release_id uuid not null references publish.releases(id) on delete cascade,
  document_id uuid not null references ingest.documents(id),
  profile_id uuid not null references catalog.torque_profiles(id) on delete cascade,
  mapping_type text not null,
  values_inherited boolean not null default false,
  values_verified boolean not null default false,
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (release_id, document_id, profile_id)
);

create table if not exists catalog.mating_relationships (
  id uuid primary key default gen_random_uuid(),
  configuration_id uuid not null references catalog.configurations(id) on delete cascade,
  related_spec_family text,
  related_slash_sheet text,
  relationship_type text not null,
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists catalog.fact_evidence (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null,
  entity_id uuid not null,
  fact_name text not null,
  document_id uuid not null references ingest.documents(id),
  page_number integer,
  source_text text,
  confidence numeric(4,2),
  attributes jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_ingest_documents_family_latest
  on ingest.documents (spec_family, is_latest, slash_sheet);

create index if not exists idx_catalog_configurations_release_lookup
  on catalog.configurations (release_id, spec_family, slash_sheet, cavity_count, connector_type);

create index if not exists idx_catalog_configurations_part_number
  on catalog.configurations (part_number_example);

create index if not exists idx_catalog_wire_options_configuration
  on catalog.wire_options (configuration_id, wire_type_code);

create index if not exists idx_catalog_document_profile_links_lookup
  on catalog.document_profile_links (release_id, document_id, mapping_type);

create or replace view api.v_configurations_current as
select c.*
from catalog.configurations c
join publish.active_releases ar
  on ar.spec_family = c.spec_family
 and ar.release_id = c.release_id;

create or replace view api.v_wire_options_current as
select
  w.*,
  c.release_id,
  c.spec_family,
  c.slash_sheet,
  c.spec_sheet,
  c.part_number_example
from catalog.wire_options w
join catalog.configurations c on c.id = w.configuration_id
join publish.active_releases ar
  on ar.spec_family = c.spec_family
 and ar.release_id = c.release_id;

create or replace view api.v_torque_effective_current as
select
  l.release_id,
  d.spec_family,
  d.slash_sheet,
  d.spec_sheet,
  d.revision,
  l.mapping_type,
  l.values_inherited,
  l.values_verified,
  p.profile_code,
  p.profile_kind,
  p.profile_status,
  v.context,
  v.fastener_thread,
  v.source_thread_label,
  v.arrangement_scope,
  v.torque_min_in_lbf,
  v.torque_max_in_lbf,
  p.attributes as profile_attributes,
  v.attributes as value_attributes
from catalog.document_profile_links l
join ingest.documents d on d.id = l.document_id
join catalog.torque_profiles p on p.id = l.profile_id
join catalog.torque_profile_values v on v.profile_id = p.id
join publish.active_releases ar
  on ar.spec_family = d.spec_family
 and ar.release_id = l.release_id;

create or replace view api.v_documents_current as
select distinct
  d.*
from ingest.documents d
join catalog.configurations c on c.document_id = d.id
join publish.active_releases ar
  on ar.spec_family = d.spec_family
 and ar.release_id = c.release_id;
