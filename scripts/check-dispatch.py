#!/usr/bin/env python3
"""Pre-commit hook: verify all registered countries dispatch correctly."""

import sys
from pathlib import Path

# Ensure engine's src/ takes precedence over any globally installed legalize
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
for _k in [k for k in sys.modules if k.startswith("legalize")]:
    del sys.modules[_k]

from legalize.countries import (
    get_client_class,
    get_discovery_class,
    get_metadata_parser,
    get_text_parser,
    supported_countries,
)

errors = []

for code in supported_countries():
    c = get_client_class(code)
    if not hasattr(c, "create"):
        errors.append(f"{code}: {c.__name__} missing create()")
    if not hasattr(c, "get_text"):
        errors.append(f"{code}: {c.__name__} missing get_text()")
    if not hasattr(c, "get_metadata"):
        errors.append(f"{code}: {c.__name__} missing get_metadata()")

    t = get_text_parser(code)
    if not hasattr(t, "parse_text"):
        errors.append(f"{code}: {type(t).__name__} missing parse_text()")

    m = get_metadata_parser(code)
    if not hasattr(m, "parse"):
        errors.append(f"{code}: {type(m).__name__} missing parse()")

    d = get_discovery_class(code)
    if not hasattr(d, "discover_all"):
        errors.append(f"{code}: {d.__name__} missing discover_all()")

if errors:
    print("Country dispatch errors:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

print(f"All {len(supported_countries())} countries dispatch OK")
