#!/usr/bin/env python3
"""Backward-compatible entrypoint for ASSIST discovery."""

from assist.discover_83513 import main


if __name__ == "__main__":
    raise SystemExit(main())
