#!/usr/bin/env python3
"""Backward-compatible entrypoint for MIL-DTL-83513/03 loading."""

from postgresql.m83513_load_03 import main


if __name__ == "__main__":
    raise SystemExit(main())
