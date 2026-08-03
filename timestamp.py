"""Timestamp chunking strategy: groups timestamped transcript lines (as
produced by audio_parser.py / video_parser.py, e.g. "[00:01:02 - 00:01:05]
some speech") up to chunk_size without ever splitting a single timestamped
line across two chunks. Falls back to RecursiveChunker for non-timestamped
text, so it's safe to register as the default chunker for media sources.
"""
import re
from typing import List

from custom_rag_pipeline_framework.core.interfaces import BaseChunker
from custom_rag_pipeline_framework.core.registry import CHUNKER_REGISTRY
from custom_rag_pipeline_framework.processing.chunkers.recursive import RecursiveChunker

# Matches "[00:01:02]" or "[00:01:02 - 00:01:05]" or "[01:02]" (mm:ss) styles
# emitted by AudioParser/VideoParser.
TIMESTAMP_LINE = re.compile(
    r'^\[\d{1,2}:\d{2}(?::\d{2})?(?:\s*-\s*\d{1,2}:\d{2}(?::\d{2})?)?\]'
)


@CHUNKER_REGISTRY.register("timestamp")
class TimestampChunker(BaseChunker):
    """Recommended default for audio/video transcript text."""

    def chunk(self, text: str) -> List[str]:
        size = self.config.chunk_size
        lines = [l for l in text.split("\n") if l.strip()]

        timestamped_count = sum(1 for l in lines if TIMESTAMP_LINE.match(l.strip()))
        if not lines or timestamped_count < max(1, len(lines) // 4):
            # Doesn't look like timestamped transcript content (e.g. a
            # "Detected text:" block from an image) - defer to the general
            # recursive strategy instead of guessing.
            return RecursiveChunker(self.config).chunk(text)

        chunks: List[str] = []
        current = ""

        for line in lines:
            candidate = current + "\n" + line if current else line
            if len(candidate) <= size:
                current = candidate
                continue

            if current:
                chunks.append(current)

            if len(line) > size:
                # A single timestamped line longer than chunk_size (rare) -
                # fall back to sub-splitting just that line.
                chunks.extend(RecursiveChunker(self.config)._split(line, ["\n", ". ", " "]))
                current = ""
            else:
                current = line

        if current:
            chunks.append(current)

        return [c.strip() for c in chunks if c.strip()]
