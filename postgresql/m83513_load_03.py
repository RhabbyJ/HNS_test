#!/usr/bin/env python3
"""Backward-compatible wrapper for loading MIL-DTL-83513/03 extraction output."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from postgresql.m83513_load_extraction import main as generic_main


def main() -> int:
    if "--input-json" in sys.argv:
        try:
            input_path = Path(sys.argv[sys.argv.index("--input-json") + 1])
            extraction = json.loads(input_path.read_text(encoding="utf-8"))
            if extraction["source"]["document_key"] != "3":
                raise RuntimeError("m83513_load_03.py only supports slash sheet /03. Use m83513_load_extraction.py for other documents.")
        except Exception as exc:  # pragma: no cover
            print(f"Loader failed: {exc}", file=sys.stderr)
            return 1
    return generic_main()


if __name__ == "__main__":
    raise SystemExit(main())
