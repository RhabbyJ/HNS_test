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

create index if not exists idx_base_configurations_lookup
  on public.base_configurations (spec_family, slash_sheet, cavity_count, connector_type);

create index if not exists idx_base_configurations_example_pin
  on public.base_configurations (example_full_pin);

create index if not exists idx_hns_wire_options_lookup
  on public.hns_wire_options (base_config_id, wire_type_code);

create index if not exists idx_text_chunks_lookup
  on public.text_chunks (spec_family, slash_sheet, page_number);

create index if not exists idx_extraction_runs_lookup
  on public.extraction_runs (spec_family, sort_order, created_at desc);

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
