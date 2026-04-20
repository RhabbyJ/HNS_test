#!/usr/bin/env python3
"""Backward-compatible entrypoint for the extraction engine."""

from hybrid_extraction.m83513_extraction_engine import main


if __name__ == "__main__":
    raise SystemExit(main())
