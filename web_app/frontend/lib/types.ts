export type SourceCitation = {
  spec_sheet: string;
  revision?: string | null;
  storage_path?: string | null;
  source_url?: string | null;
  source_page?: number | null;
  figure_reference?: string | null;
};

export type SearchResult = {
  id: string;
  spec_family: string;
  slash_sheet: string;
  name: string;
  description?: string | null;
  connector_type?: string | null;
  gender?: string | null;
  contact_type?: string | null;
  cavity_count?: number | null;
  shell_size_letter?: string | null;
  shell_finish_code?: string | null;
  example_full_pin?: string | null;
  citation: SourceCitation;
};

export type GroupedSearchResult = {
  search_family_key: string;
  slash_sheet: string;
  connector_type?: string | null;
  cavity_count?: number | null;
  shell_size_letter?: string | null;
  variant_count: number;
  available_finish_codes: string[];
  representative_variant: SearchResult;
  citation: SourceCitation;
};

export type SearchResponse = {
  grouped: boolean;
  items: GroupedSearchResult[];
  raw_variants: SearchResult[];
  total: number;
};

export type WireOption = {
  wire_type_code: string;
  wire_specification?: string | null;
  wire_length_inches?: number | null;
  wire_notes?: string | null;
  is_space_approved: boolean;
};

export type HardwareOption = {
  code: string;
  description: string;
};

export type PartDetail = {
  id: string;
  spec_family: string;
  slash_sheet: string;
  spec_sheet: string;
  name: string;
  description?: string | null;
  connector_type?: string | null;
  gender?: string | null;
  contact_type?: string | null;
  cavity_count?: number | null;
  shell_size_letter?: string | null;
  shell_finish_code?: string | null;
  shell_finish_description?: string | null;
  dimensions?: Record<string, unknown> | null;
  shell_material?: string | null;
  mates_with: string[];
  mounting_hardware_ref?: string | null;
  hardware_options: HardwareOption[];
  wire_range?: string | null;
  torque_values: string[];
  example_full_pin?: string | null;
  wire_options: WireOption[];
  citation: SourceCitation;
};

export type MateCandidate = {
  id: string;
  spec_sheet: string;
  name: string;
  slash_sheet: string;
  compatibility: "compatible" | "review";
  match_reasons: string[];
  source_spec?: string | null;
  source_page?: number | null;
  confidence_type: "deterministic";
  shell_finish_code?: string | null;
  example_full_pin?: string | null;
  gender?: string | null;
  contact_type?: string | null;
  hardware_compatibility?: string | null;
  citation: SourceCitation;
};

export type GroupedMateResult = {
  mate_family_key: string;
  mate_slash_sheet: string;
  variant_count: number;
  representative_variant: MateCandidate;
  variants: MateCandidate[];
  match_reasons: string[];
  source_spec?: string | null;
  source_page?: number | null;
  hardware_compatibility?: string | null;
  confidence_type: "deterministic";
};

export type MateResponse = {
  part_id: string;
  grouped: boolean;
  mates: GroupedMateResult[];
  raw_variants: MateCandidate[];
};
