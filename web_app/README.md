# FastAPI + Next.js

This folder contains the product application layer.

Responsibilities:

- FastAPI service for filtered lookups, detail pages, and source citations
- Next.js frontend for search, grouped mate results, and part detail pages
- API responses that cite the originating PDF/page for every derived result

Current scaffold:

- [api](C:/Users/rjega/HNS_test/web_app/api): FastAPI contract for search, part detail, and mate-finder endpoints
- [frontend](C:/Users/rjega/HNS_test/web_app/frontend): minimal Next.js product frontend for search, part detail, and grouped mate results

Current product rule:

- grouped mates are the default product output
- raw mate variants stay hidden from the UI and are only for debugging/internal validation
