create extension if not exists pgcrypto;

create table if not exists public.pdf_objects (
  id uuid primary key default gen_random_uuid(),
  spec_family text not null,
  slash_sheet text not null default 'base',
  sort_order integer not null default 0,
  revision_letter text not null,
  document_date date not null,
  title text not null,
  storage_path text not null unique,
  bucket_name text not null default 'mil-spec-pdfs',
  file_size_bytes bigint not null check (file_size_bytes > 0),
  checksum text not null,
  is_latest boolean not null default true,
  status text not null default 'active' check (status in ('active', 'superseded', 'archived')),
  source_url text not null,
  source_ident_number text not null,
  source_doc_id text not null,
  uploaded_at timestamptz not null default now(),
  last_checked_at timestamptz not null default now(),
  unique (spec_family, slash_sheet)
);

create index if not exists idx_pdf_objects_spec_family
  on public.pdf_objects (spec_family);

create index if not exists idx_pdf_objects_family_sort
  on public.pdf_objects (spec_family, sort_order, slash_sheet);

create index if not exists idx_pdf_objects_latest
  on public.pdf_objects (is_latest)
  where is_latest = true;
