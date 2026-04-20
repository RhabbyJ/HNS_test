#!/usr/bin/env python3
"""Backward-compatible entrypoint for storage path reorganization."""

from pdf_storage.reorganize_supabase_paths import main


if __name__ == "__main__":
    raise SystemExit(main())
