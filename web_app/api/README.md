# FastAPI Contract

This package is the starting point for the product-facing API.

Planned first endpoints:

- `GET /health`
- `GET /search`
- `GET /parts/{part_id}`
- `GET /parts/{part_id}/mates`

Current implemented query surface:

- `GET /search?q=&slash_sheet=&cavity_count=&shell_size_letter=&shell_finish_code=&gender=&contact_type=&connector_type=&limit=&offset=`
- `GET /search?...&grouped=false`
- `GET /parts/{part_id}`
- `GET /parts/{part_id}/mates`
- `GET /parts/{part_id}/mates?grouped=false`

Search endpoint behavior:

- default grouped mode returns deduped product cards
- `grouped=false` returns raw normalized variants for debugging and validation
- grouped search collapses finish-code variants within the same slash-sheet/configuration family

Mate endpoint behavior:

- default grouped mode returns deduped mate families for product use
- `grouped=false` returns raw variants for debugging and golden-test validation
- every mate result includes deterministic match reasons and source evidence fields

Design rules:

- every response includes source citations
- mate finding stays deterministic first
- the repository layer should be backed by the normalized PostgreSQL tables, not raw extraction JSON
- LLM assistance stays optional and outside the default request path
