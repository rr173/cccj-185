#!/usr/bin/env python3
"""Process entry point usable under Python isolated mode (-I).

Environment variables are ignored by -I, so paths are derived from this
file's location and inserted explicitly before importing the package.
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "vendor"))
sys.path.insert(0, ROOT)

from barcode_pipeline.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
