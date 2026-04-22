create extension if not exists pgcrypto;

create table if not exists public.base_configurations (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null default '83513',
  spec_sheet text not null,
  slash_sheet text not null,
  connector_type text not null,
  name text not null,
  description text,
  cavity_count integer,
  shell_size_letter char(1),
  shell_size_description text,
  dimensions jsonb,
  shell_material text,
  shell_finish_code char(1),
  shell_finish_description text,
  shell_finish_notes text,
  current_rating_per_contact numeric(4,2),
  contact_type text,
  gender text,
  class text,
  polarization text,
  mates_with text[],
  mounting_hardware_ref text,
  insert_arrangement_ref text,
  source_document text not null,
  source_page integer,
  source_url text not null,
  revision text not null,
  confidence_score numeric(4,2) not null default 1.0,
  extracted_at timestamptz not null default now(),
  is_latest boolean not null default true,
  status text not null default 'active',
  example_full_pin text,
  figure_references jsonb,
  extra_data jsonb,
  unique (spec_sheet, cavity_count, shell_size_letter, shell_finish_code, insert_arrangement_ref)
);

create table if not exists public.hns_wire_options (
  id uuid primary key default gen_random_uuid(),
  base_config_id uuid not null references public.base_configurations(id) on delete cascade,
  wire_type_code text not null,
  wire_specification text,
  wire_length_inches numeric(6,2),
  wire_notes text,
  is_space_approved boolean not null default false
);

create table if not exists public.torque_values (
  id uuid primary key default gen_random_uuid(),
  torque_key text not null unique,
  spec_family text not null default '83513',
  spec_sheet text not null,
  slash_sheet text not null,
  revision text not null,
  context text not null,
  applies_to text,
  fastener_thread text,
  source_thread_label text,
  arrangement_scope text,
  torque_min_in_lbf numeric(6,2),
  torque_max_in_lbf numeric(6,2),
  torque_text text not null,
  source_document text not null,
  source_page integer not null,
  source_url text not null,
  storage_path text not null,
  extracted_at timestamptz not null default now()
);

create table if not exists public.document_torque_status (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null default '83513',
  spec_sheet text not null unique,
  slash_sheet text not null,
  revision text not null,
  torque_profile_code text,
  torque_mode text not null,
  referenced_spec_sheet text,
  extracted_row_count integer not null default 0,
  canonical_row_count integer not null default 0,
  audit_status text not null default 'pending',
  extractor_version text,
  last_extracted_at timestamptz,
  notes text,
  version integer not null default 1,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint document_torque_status_mode_check
    check (torque_mode in ('canonical', 'owns_profile', 'uses_shared_profile', 'references_other_doc', 'no_torque', 'none', 'needs_review', 'direct_numeric')),
  constraint document_torque_status_audit_check
    check (audit_status in ('verified', 'pending', 'needs_review', 'stale'))
);

create table if not exists public.torque_profiles (
  id uuid primary key default gen_random_uuid(),
  profile_code text not null unique,
  profile_name text not null,
  profile_kind text not null default 'provisional',
  source_of_truth_level text not null default 'extracted_numeric_text',
  governing_spec_sheet text,
  governing_revision text,
  source_spec_sheet text not null,
  source_revision text,
  source_page integer,
  profile_status text not null default 'verified',
  approval_status text not null default 'pending',
  approved_by text,
  approved_at timestamptz,
  notes text,
  version integer not null default 1,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint torque_profiles_kind_check
    check (profile_kind in ('canonical', 'shared_derived', 'document_specific', 'provisional')),
  constraint torque_profiles_source_truth_check
    check (source_of_truth_level in ('audited_pdf_table', 'extracted_numeric_text', 'reference_only')),
  constraint torque_profiles_status_check
    check (profile_status in ('verified', 'unverified', 'provisional')),
  constraint torque_profiles_approval_check
    check (approval_status in ('approved', 'pending', 'needs_review', 'rejected'))
);

create table if not exists public.torque_profile_values (
  id uuid primary key default gen_random_uuid(),
  profile_id uuid not null references public.torque_profiles(id) on delete cascade,
  context text not null,
  fastener_thread text,
  source_thread_label text,
  arrangement_scope text,
  torque_min_in_lbf numeric(6,2),
  torque_max_in_lbf numeric(6,2),
  normalized_fact_key text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (profile_id, normalized_fact_key)
);

create table if not exists public.document_torque_profile_map (
  id uuid primary key default gen_random_uuid(),
  spec_sheet text not null references public.document_torque_status(spec_sheet) on delete cascade,
  profile_id uuid not null references public.torque_profiles(id) on delete cascade,
  mapping_type text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint document_torque_profile_map_type_check
    check (mapping_type in ('uses_profile', 'references_profile', 'provisional_profile')),
  unique (spec_sheet, profile_id)
);

create table if not exists public.torque_source_evidence (
  id uuid primary key default gen_random_uuid(),
  spec_sheet text not null,
  slash_sheet text not null,
  revision text not null,
  profile_id uuid references public.torque_profiles(id) on delete set null,
  source_document text not null,
  source_page integer not null,
  source_url text not null,
  storage_path text not null,
  torque_text text not null,
  extracted_context text,
  extracted_fastener_thread text,
  extracted_source_thread_label text,
  extracted_arrangement_scope text,
  extracted_min_in_lbf numeric(6,2),
  extracted_max_in_lbf numeric(6,2),
  extractor_version text,
  extracted_at timestamptz not null default now()
);

create table if not exists public.text_chunks (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null default '83513',
  spec_sheet text not null,
  slash_sheet text not null,
  revision text not null,
  page_number integer not null,
  chunk_index integer not null,
  text_content text not null,
  source_url text not null,
  storage_path text not null,
  created_at timestamptz not null default now(),
  unique (spec_family, spec_sheet, page_number, chunk_index)
);

create table if not exists public.extraction_runs (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null default '83513',
  spec_sheet text not null,
  slash_sheet text not null,
  sort_order integer not null default 0,
  revision text not null,
  extraction_method text not null default 'pdf_first',
  confidence_score numeric(4,2) not null,
  llm_fallback_required boolean not null default false,
  issues jsonb,
  created_at timestamptz not null default now()
);

create table if not exists public.m83513_staged_payload_rows (
  id uuid primary key default gen_random_uuid(),
  run_id text not null,
  table_name text not null,
  slash_sheet text,
  row_data jsonb not null,
  loaded_at timestamptz not null default now()
);

create index if not exists idx_base_configurations_lookup
  on public.base_configurations (spec_family, slash_sheet, cavity_count, connector_type);

create index if not exists idx_base_configurations_example_pin
  on public.base_configurations (example_full_pin);

create index if not exists idx_hns_wire_options_lookup
  on public.hns_wire_options (base_config_id, wire_type_code);

create index if not exists idx_torque_values_lookup
  on public.torque_values (spec_family, slash_sheet, context);

create index if not exists idx_document_torque_status_lookup
  on public.document_torque_status (spec_family, slash_sheet, torque_mode);

create index if not exists idx_torque_profile_values_lookup
  on public.torque_profile_values (profile_id, context, fastener_thread);

create index if not exists idx_document_torque_profile_map_lookup
  on public.document_torque_profile_map (spec_sheet, mapping_type);

create index if not exists idx_torque_source_evidence_lookup
  on public.torque_source_evidence (spec_sheet, source_page);

create index if not exists idx_text_chunks_lookup
  on public.text_chunks (spec_family, slash_sheet, page_number);

create index if not exists idx_extraction_runs_lookup
  on public.extraction_runs (spec_family, sort_order, created_at desc);

create index if not exists idx_m83513_staged_payload_rows_run
  on public.m83513_staged_payload_rows (run_id, table_name, slash_sheet);

create index if not exists idx_m83513_staged_payload_rows_json
  on public.m83513_staged_payload_rows using gin (row_data);

create or replace view public.v_83513_documents as
select
  spec_family,
  slash_sheet,
  sort_order,
  revision_letter,
  document_date,
  title,
  storage_path,
  status,
  source_url
from public.pdf_objects
where spec_family = '83513'
order by sort_order, slash_sheet;

create or replace view public.v_83513_configurations as
select
  spec_family,
  slash_sheet,
  connector_type,
  cavity_count,
  shell_size_letter,
  shell_finish_code,
  name,
  example_full_pin,
  mates_with,
  source_document,
  revision
from public.base_configurations
where spec_family = '83513'
order by slash_sheet, cavity_count, shell_finish_code;

create or replace view public.v_83513_03_configurations as
select *
from public.v_83513_configurations
where slash_sheet = '03';

create or replace view public.v_83513_01_configurations as
select *
from public.v_83513_configurations
where slash_sheet = '01';

create or replace view public.v_83513_02_configurations as
select *
from public.v_83513_configurations
where slash_sheet = '02';

create or replace view public.v_83513_04_configurations as
select *
from public.v_83513_configurations
where slash_sheet = '04';

create or replace view public.v_83513_base_configurations as
select *
from public.v_83513_configurations
where slash_sheet = 'base';

create or replace view public.v_83513_extraction_runs as
select
  spec_family,
  slash_sheet,
  sort_order,
  spec_sheet,
  revision,
  extraction_method,
  confidence_score,
  llm_fallback_required,
  issues,
  created_at
from public.extraction_runs
where spec_family = '83513'
order by sort_order, created_at desc;

create or replace view public.v_83513_03_wire_options as
select
  b.spec_family,
  b.slash_sheet,
  b.example_full_pin,
  b.cavity_count,
  b.shell_finish_code,
  w.wire_type_code,
  w.wire_specification,
  w.wire_length_inches,
  w.is_space_approved,
  w.wire_notes
from public.base_configurations b
join public.hns_wire_options w on w.base_config_id = b.id
where b.spec_family = '83513' and b.slash_sheet = '03'
order by b.cavity_count, b.shell_finish_code, w.wire_type_code;

create or replace view public.v_83513_torque_values as
select
  spec_family,
  slash_sheet,
  spec_sheet,
  revision,
  context,
  applies_to,
  fastener_thread,
  source_thread_label,
  arrangement_scope,
  torque_min_in_lbf,
  torque_max_in_lbf,
  torque_text,
  source_page,
  source_url
from public.torque_values
where spec_family = '83513'
order by slash_sheet, context, fastener_thread, source_page;

create or replace view public.v_83513_torque_document_summary as
select
  spec_family,
  spec_sheet,
  slash_sheet,
  revision,
  torque_mode,
  referenced_spec_sheet,
  torque_profile_code,
  canonical_row_count,
  audit_status,
  last_extracted_at,
  notes
from public.document_torque_status
where spec_family = '83513'
order by case when slash_sheet ~ '^[0-9]+$' then slash_sheet::int else -1 end;

create or replace view public.v_83513_torque_profile_values as
select
  p.profile_code,
  p.profile_name,
  p.source_spec_sheet,
  p.source_revision,
  p.profile_status,
  v.context,
  v.fastener_thread,
  v.source_thread_label,
  v.arrangement_scope,
  v.torque_min_in_lbf,
  v.torque_max_in_lbf,
  v.normalized_fact_key
from public.torque_profiles p
join public.torque_profile_values v on v.profile_id = p.id
where p.profile_code like 'm83513_%'
order by p.profile_code, v.context, v.fastener_thread, v.arrangement_scope;

create or replace view public.v_83513_torque_resolution as
with profile_value_counts as (
  select profile_id, count(*) as value_count
  from public.torque_profile_values
  group by profile_id
),
evidence_counts as (
  select spec_sheet, count(*) as evidence_row_count
  from public.torque_source_evidence
  group by spec_sheet
)
select
  d.spec_family,
  d.spec_sheet,
  d.slash_sheet,
  d.revision,
  d.torque_mode,
  d.audit_status as document_audit_status,
  d.referenced_spec_sheet,
  d.extracted_row_count,
  d.canonical_row_count,
  m.mapping_type,
  p.profile_code as resolved_profile_code,
  p.profile_kind as resolved_profile_kind,
  p.profile_status as resolved_profile_status,
  p.source_of_truth_level as resolved_source_of_truth_level,
  p.approval_status as resolved_approval_status,
  p.governing_spec_sheet,
  p.governing_revision,
  p.source_spec_sheet as profile_source_spec_sheet,
  p.source_revision as profile_source_revision,
  coalesce(v.value_count, 0) as resolved_fact_count,
  coalesce(e.evidence_row_count, 0) as evidence_row_count,
  coalesce(v.value_count, 0) > 0 as has_numeric_values,
  p.approval_status = 'approved' and p.source_of_truth_level = 'audited_pdf_table' as values_verified,
  case
    when p.id is null then false
    when m.mapping_type = 'references_profile' then true
    when p.profile_kind = 'shared_derived' then true
    when d.spec_sheet <> p.source_spec_sheet then true
    else false
  end as values_inherited,
  case
    when p.id is null and d.torque_mode not in ('none', 'no_torque') then true
    when d.audit_status in ('needs_review', 'stale') then true
    when p.approval_status in ('needs_review', 'rejected') then true
    when p.profile_status in ('unverified', 'provisional') then true
    else false
  end as needs_review,
  d.last_extracted_at,
  d.notes
from public.document_torque_status d
left join public.document_torque_profile_map m on m.spec_sheet = d.spec_sheet
left join public.torque_profiles p on p.id = m.profile_id
left join profile_value_counts v on v.profile_id = p.id
left join evidence_counts e on e.spec_sheet = d.spec_sheet
where d.spec_family = '83513' and d.is_active
order by case when d.slash_sheet ~ '^[0-9]+$' then d.slash_sheet::int else -1 end;

create or replace view public.v_83513_torque_effective_facts as
select
  r.spec_sheet,
  r.slash_sheet,
  r.revision,
  r.torque_mode,
  r.resolved_profile_code,
  r.governing_spec_sheet,
  r.governing_revision,
  r.values_verified,
  r.values_inherited,
  r.needs_review,
  v.context,
  v.fastener_thread,
  v.source_thread_label,
  v.arrangement_scope,
  v.torque_min_in_lbf,
  v.torque_max_in_lbf,
  r.resolved_approval_status as approval_status,
  r.resolved_profile_kind as profile_kind,
  r.resolved_source_of_truth_level as source_of_truth_level
from public.v_83513_torque_resolution r
join public.torque_profiles p
  on p.profile_code = r.resolved_profile_code
  and p.is_active
join public.torque_profile_values v on v.profile_id = p.id
where r.resolved_profile_code is not null
order by
  case when r.slash_sheet ~ '^[0-9]+$' then r.slash_sheet::int else -1 end,
  v.context,
  v.fastener_thread,
  v.arrangement_scope,
  v.torque_min_in_lbf,
  v.torque_max_in_lbf;
