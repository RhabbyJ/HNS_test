#!/usr/bin/env python3
"""Backward-compatible entrypoint for local family downloads."""

from assist.download_83513_family import main


if __name__ == "__main__":
    raise SystemExit(main())
