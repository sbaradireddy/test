"""Parser dispatcher - resolves the right BaseParser plugin by file extension.

Importing this module registers every built-in parser plugin (pdf, docx,
html, csv, text) with `PARSER_REGISTRY` as a side effect. New formats can be
added by dropping a new module here that registers under a new name/
extension, with zero changes to `DocumentParser` itself.
"""
import logging
from pathlib import Path
from typing import Optional

from custom_rag_pipeline_framework.core.registry import PARSER_REGISTRY

# Importing submodules registers each parser plugin as a side effect.
from custom_rag_pipeline_framework.processing.parsers import (  # noqa: F401
    pdf_parser,
    docx_parser,
    html_parser,
    csv_parser,
    text_parser,
)

logger = logging.getLogger(__name__)

_EXTENSION_MAP = {
    ".pdf": "pdf",
    ".txt": "text",
    ".docx": "docx",
    ".html": "html",
    ".htm": "html",
    ".csv": "csv",
}


class DocumentParser:
    """Parses various file formats into normalized plain text via the plugin registry."""

    def parse(self, content_bytes: bytes, filename: str) -> Optional[str]:
        ext = Path(filename).suffix.lower()
        plugin_name = _EXTENSION_MAP.get(ext)
        if not plugin_name:
            logger.warning(f"Unsupported format: {ext} for {filename}")
            return None
        try:
            parser = PARSER_REGISTRY.create(plugin_name)
            return parser.parse(content_bytes, filename)
        except Exception as e:
            logger.error(f"Parse error for {filename}: {e}")
            return None
