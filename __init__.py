"""processing/parsers package init.

Auto-discovers and imports every parser plugin module in this package so
each one's @PARSER_REGISTRY.register(...) decorator fires at import time.
This means adding a new parser file (audio_parser.py, image_parser.py,
video_parser.py, or any future *_parser.py / module you drop in here) never
requires touching this file again.

If you already have explicit imports here (e.g. `from . import csv_parser`),
it's safe to replace them with this - explicit imports are just a subset of
what this loop already covers. Non-parser helper modules like normalize.py
are imported too, which is harmless (they don't register anything).
"""
import importlib
import logging
import pkgutil

logger = logging.getLogger(__name__)

_package_name = __name__
_package_path = __path__

for _finder, _module_name, _is_pkg in pkgutil.iter_modules(_package_path):
    if _module_name.startswith("_"):
        continue  # skip private/dunder modules
    try:
        importlib.import_module(f"{_package_name}.{_module_name}")
    except Exception:
        logger.exception(f"Failed to import parser module '{_module_name}'")

del _finder, _module_name, _is_pkg, _package_name, _package_path
