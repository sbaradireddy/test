"""Media-aware quality validation: wraps QualityValidator with heuristics for
the failure modes specific to transcribed/OCR'd/captioned media - a silent
audio track, a video whose speech-to-text produced almost nothing, or an
image where neither label detection nor OCR found anything worth indexing.
Falls through to all of QualityValidator's existing checks (length,
encoding, duplicates, information density) unchanged.
"""
import logging
from pathlib import Path

from custom_rag_pipeline_framework.core.registry import VALIDATOR_REGISTRY
from custom_rag_pipeline_framework.processing.validators.quality_validator import (
    QualityValidator,
    ValidationResult,
)
from custom_rag_pipeline_framework.processing.parsers.audio_parser import AudioParser
from custom_rag_pipeline_framework.processing.parsers.image_parser import ImageParser
from custom_rag_pipeline_framework.processing.parsers.video_parser import VideoParser

logger = logging.getLogger(__name__)

_AUDIO_EXT = set(AudioParser.extensions)
_VIDEO_EXT = set(VideoParser.extensions)
_IMAGE_EXT = set(ImageParser.extensions)


@VALIDATOR_REGISTRY.register("media_quality")
class MediaQualityValidator:
    """Length/encoding/duplicate/density checks (via QualityValidator) plus
    media-specific sanity checks for audio, video, and image derived text."""

    def __init__(self, config):
        self.config = config
        self._base = QualityValidator(config)

    def validate(self, content: str, filename: str) -> ValidationResult:
        base_result = self._base.validate(content, filename)
        if not base_result.is_valid:
            return base_result

        ext = Path(filename).suffix.lower()

        if ext in _AUDIO_EXT | _VIDEO_EXT:
            min_words = getattr(self.config, "media_min_word_count", 5)
            word_count = len(content.split())
            if word_count < min_words:
                return ValidationResult(
                    False,
                    f"Transcript too sparse: {word_count} words "
                    f"(likely silent track or failed transcription)",
                )

        if ext in _IMAGE_EXT:
            has_recognized_section = any(
                marker in content
                for marker in ("Image contents:", "Detected text:", "Description:")
            )
            if not has_recognized_section:
                return ValidationResult(
                    False, "No recognizable image content (no labels, text, or caption)"
                )

        logger.info(f"Media-validated: {filename} ({len(content)} chars)")
        return base_result

    def reset_seen_hashes(self):
        """Delegate duplicate-tracking reset to the wrapped QualityValidator."""
        self._base.reset_seen_hashes()
