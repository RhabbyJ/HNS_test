create extension if not exists pgcrypto;

create table if not exists public.m83513_base_configurations (
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
  unique (spec_sheet, cavity_count, shell_size_letter, shell_finish_code)
);

create table if not exists public.m83513_hns_wire_options (
  id uuid primary key default gen_random_uuid(),
  base_config_id uuid not null references public.m83513_base_configurations(id) on delete cascade,
  wire_type_code text not null,
  wire_specification text,
  wire_length_inches numeric(6,2),
  wire_notes text,
  is_space_approved boolean not null default false
);

create table if not exists public.m83513_text_chunks (
  id uuid primary key default gen_random_uuid(),
  spec_sheet text not null,
  slash_sheet text not null,
  revision text not null,
  page_number integer not null,
  chunk_index integer not null,
  text_content text not null,
  source_url text not null,
  storage_path text not null,
  created_at timestamptz not null default now(),
  unique (spec_sheet, page_number, chunk_index)
);

create table if not exists public.m83513_extraction_runs (
  id uuid primary key default gen_random_uuid(),
  spec_sheet text not null,
  slash_sheet text not null,
  revision text not null,
  extraction_method text not null default 'pdf_first',
  confidence_score numeric(4,2) not null,
  llm_fallback_required boolean not null default false,
  issues jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_m83513_base_spec
  on public.m83513_base_configurations (slash_sheet, cavity_count, connector_type);

create index if not exists idx_m83513_chunks_lookup
  on public.m83513_text_chunks (slash_sheet, page_number);
