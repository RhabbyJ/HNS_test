# Torque Loader Contract

`torque_values` is legacy/staging. New ingestion should treat torque data as three separate layers.

## Layer 1: Evidence

The extractor records what the PDF text/table parser saw.

Target table:

- `torque_source_evidence`

Rules:

- Preserve source document, page, storage path, URL, and raw `torque_text`.
- Store extracted numeric candidates as extracted fields only.
- Evidence rows do not become trusted engineering facts by themselves.
- Evidence ingestion must be safe to rerun without creating duplicate current evidence for the same document run.

## Layer 2: Interpretation

The loader classifies what the document means.

Target tables:

- `document_torque_status`
- `document_torque_profile_map`

Valid document modes:

- `owns_profile`: document owns the governing torque profile.
- `uses_shared_profile`: document uses a shared profile governed by another or representative source.
- `references_other_doc`: document explicitly points to another spec for torque.
- `no_torque`: document has no torque requirement found.
- `needs_review`: evidence exists but cannot be promoted or mapped confidently.

Rules:

- There must be exactly one `document_torque_status` row per spec document.
- Documents may map to zero or one active torque profile in the current product scope.
- Reference statements such as “in accordance with MIL-DTL-83513/5” create a document mapping, not duplicate numeric facts.

## Layer 3: Canonical Engineering Truth

Only audited or intentionally promoted values become reusable facts.

Target tables:

- `torque_profiles`
- `torque_profile_values`

Rules:

- `torque_profile_values` stores deduplicated numeric facts under a profile.
- `normalized_fact_key` is the deterministic dedupe key inside a profile.
- `/05` page 7 is currently the verified canonical source for the audited mounting/mating torque profile.
- Provisional profiles are allowed, but the app must surface their status rather than treating them as verified.

## Promotion Gate

The loader may create or update provisional profiles from extracted numeric evidence, but verified canonical profiles require explicit audit approval.

Promotion should set:

- `profile_kind`
- `source_of_truth_level`
- `profile_status`
- `approval_status`
- `approved_by`
- `approved_at`

## App Read Model

Future app code should read from resolver views, not from the raw profile/evidence tables directly.

The resolver view provides:

- one row per document
- resolved profile
- resolved fact count
- evidence count
- inherited-vs-owned status
- verification/review flags

`v_83513_torque_effective_facts` is the app-facing facts view. It expands the document resolver into one row per effective torque fact per document.

Required fields:

- `spec_sheet`
- `slash_sheet`
- `revision`
- `torque_mode`
- `resolved_profile_code`
- `governing_spec_sheet`
- `governing_revision`
- `values_verified`
- `values_inherited`
- `needs_review`
- `context`
- `fastener_thread`
- `source_thread_label`
- `arrangement_scope`
- `torque_min_in_lbf`
- `torque_max_in_lbf`
- `approval_status`
- `profile_kind`
- `source_of_truth_level`

## State Machine

Current allowed document torque states:

- `none`: no torque evidence or profile mapping is present.
- `references_other_doc`: document references another governing spec/profile for torque.
- `uses_shared_profile`: document uses a shared profile but does not own the governing source.
- `owns_profile`: document owns the governing source profile.
- `needs_review`: document has evidence or a provisional mapping that is not ready for trusted consumption.

Valid normal transitions:

- `none` -> `references_other_doc` when a reference statement is extracted and resolved.
- `none` -> `uses_shared_profile` when evidence maps to an existing shared profile.
- `none` -> `owns_profile` when the document is confirmed as the governing numeric source.
- `none` -> `needs_review` when evidence exists but cannot be classified.
- `references_other_doc` -> `uses_shared_profile` when a reference is replaced by a validated shared applicability rule.
- `references_other_doc` -> `owns_profile` only after confirmed numeric source audit.
- `uses_shared_profile` -> `owns_profile` only if the document becomes the audited governing source for a new profile/version.
- `uses_shared_profile` -> `needs_review` when evidence or governing revision changes.
- `needs_review` -> `references_other_doc`, `uses_shared_profile`, or `owns_profile` only through promotion logic.
- Any active state -> `none` only through explicit review that confirms no torque requirement applies.

Invalid transitions unless explicitly approved in a migration/review note:

- `references_other_doc` -> `owns_profile` without audited numeric source evidence.
- `uses_shared_profile` -> approved/canonical status without promotion approval.
- `needs_review` -> verified/approved status without a promotion decision.
- `none` -> verified profile mapping without evidence or manual audit note.

Mapping rules:

- `references_other_doc` must have exactly one active mapping to the referenced profile.
- `uses_shared_profile` must have exactly one active mapping to a shared or provisional profile.
- `owns_profile` must have exactly one active mapping to an approved or provisional profile.
- `none` must have no active mapping.
- `needs_review` may map to a provisional profile, but must not map to an approved canonical profile unless an explicit override is recorded.

## Promotion Rules

Promotion is the only path from evidence/candidates into canonical profile facts.

Promotion payloads must be explicit:

- `evidence_rows`
- `document_classification`
- `proposed_profile_assignment`
- `proposed_profile_values`
- `promotion_decision`

Promotion decision fields:

- `promotion_batch_id`
- `promoted_from_evidence_count`
- `promotion_reason`
- `review_notes`
- `approved_by`
- `approved_at`

Promotion outcomes:

- `evidence_only`: store source evidence, make no profile changes.
- `provisional_profile`: create or update a provisional profile for review.
- `map_existing_profile`: map the document to an existing active profile.
- `approve_canonical_profile`: create a new approved canonical profile/version from audited evidence.
- `reject_candidate`: keep evidence, mark document/profile as `needs_review` or rejected.

Rules:

- Raw extraction must never write directly into `torque_profile_values` as verified canonical truth.
- New verified facts require `approval_status = 'approved'`, an approver, an approval timestamp, and a promotion batch.
- When a governing revision changes, create a new profile version and deactivate the old active version; do not mutate old approved profile values in place.
- Historical evidence rows remain immutable.

## Parity Validation Before App Cutover

Required checks:

- Compare legacy `torque_values` document coverage against `v_83513_torque_resolution`.
- Compare resolved fact counts per document from `v_83513_torque_effective_facts`.
- Confirm `/01`, `/02`, `/03`, `/04`, `/06`, `/07`, and `/09` resolve to the exact six verified `/05` facts.
- Confirm `/10` through `/33` all resolve to the same two shared provisional facts.
- Confirm `/08` remains isolated and `needs_review = true`.
- Confirm base has no effective torque facts.

## Review Queue

Operations/QA should review:

- documents with `needs_review = true`
- provisional profiles
- documents whose evidence changed after last approval
- unresolved mappings
- documents whose governing spec/revision no longer matches the active profile
