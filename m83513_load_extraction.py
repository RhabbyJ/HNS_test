#!/usr/bin/env python3
"""Backward-compatible entrypoint for generic extraction loading."""

from postgresql.m83513_load_extraction import main


if __name__ == "__main__":
    raise SystemExit(main())
