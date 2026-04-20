from __future__ import annotations

from functools import lru_cache

from fastapi import Depends, FastAPI, HTTPException, Query

from web_app.api.models import MateResponse, PartDetail, SearchResponse
from web_app.api.repository import ProductRepository, SupabaseRestRepository


app = FastAPI(
    title="HarnessMate API",
    version="0.1.0",
    description="Product API for MIL-DTL-83513 search, part detail, and mate-finder workflows.",
)


@lru_cache(maxsize=1)
def get_repository() -> ProductRepository:
    return SupabaseRestRepository.from_env_file()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/search", response_model=SearchResponse)
def search(
    q: str | None = Query(default=None, description="Free-text part search."),
    slash_sheet: str | None = Query(default=None, description="Slash sheet filter such as 03 or base."),
    cavity_count: int | None = Query(default=None, description="Exact cavity-count filter."),
    shell_size_letter: str | None = Query(default=None, min_length=1, max_length=1),
    shell_finish_code: str | None = Query(default=None, min_length=1, max_length=1),
    gender: str | None = Query(default=None, description="PLUG or RECEPTACLE."),
    connector_type: str | None = Query(default=None, description="Exact connector type filter."),
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    repository: ProductRepository = Depends(get_repository),
) -> SearchResponse:
    items, total = repository.search_parts(
        query=q,
        slash_sheet=slash_sheet,
        cavity_count=cavity_count,
        shell_size_letter=shell_size_letter,
        shell_finish_code=shell_finish_code,
        gender=gender,
        connector_type=connector_type,
        limit=limit,
        offset=offset,
    )
    return SearchResponse(items=items, total=total)


@app.get("/parts/{part_id}", response_model=PartDetail)
def part_detail(
    part_id: str,
    repository: ProductRepository = Depends(get_repository),
) -> PartDetail:
    part = repository.get_part(part_id)
    if part is None:
        raise HTTPException(status_code=404, detail="Part not found")
    return part


@app.get("/parts/{part_id}/mates", response_model=MateResponse)
def part_mates(
    part_id: str,
    grouped: bool = Query(default=True, description="Return grouped mate families for product use. Set false for raw debug variants."),
    repository: ProductRepository = Depends(get_repository),
) -> MateResponse:
    part = repository.get_part(part_id)
    if part is None:
        raise HTTPException(status_code=404, detail="Part not found")
    if grouped:
        return MateResponse(part_id=part_id, grouped=True, mates=repository.get_grouped_mates(part_id))
    return MateResponse(part_id=part_id, grouped=False, raw_variants=repository.get_mates(part_id))
