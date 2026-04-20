# Compatibility Rules Engine

This folder contains the deterministic mate-validation hardening work for the product layer.

Current contents:

- `golden_mate_cases_83513.json`: 10 known-good mate test cases
- `run_golden_mate_suite.py`: executes the golden suite against the local FastAPI app

Current validated state:

- `0` false positives
- `0` false negatives
- grouped mate output passes all 10 cases

Planned responsibilities:

- mate validation rules
- keying and arrangement compatibility
- harness/BOM validation rules
- optional fallback prompts for ambiguous compatibility decisions
