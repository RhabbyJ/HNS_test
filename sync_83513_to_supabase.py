#!/usr/bin/env python3
"""Backward-compatible entrypoint for Supabase PDF sync."""

from pdf_storage.sync_83513_to_supabase import main


if __name__ == "__main__":
    raise SystemExit(main())
