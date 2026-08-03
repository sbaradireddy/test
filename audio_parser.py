"""Audio parsing plugin - transcribes audio files to text via local Whisper inference.

No external API dependency required (offline, no per-call cost). Produces a
timestamped transcript that flows straight into the existing content_cleanup /
chunking / validation / privacy / tagging stages unchanged.
"""
import logging
import os
import tempfile

from custom_rag_pipeline_framework.core.interfaces import BaseParser
from custom_rag_pipeline_framework.core.registry import PARSER_REGISTRY

logger = logging.getLogger(__name__)

# Process-wide model cache so we don't reload the model per file in a batch run.
_MODEL_CACHE = {}


def _get_whisper_model(model_size: str, device: str, compute_type: str):
    """Lazily load and cache a faster-whisper model."""
    key = (model_size, device, compute_type)
    if key not in _MODEL_CACHE:
        from faster_whisper import WhisperModel  # pip install faster-whisper
        logger.info(f"Loading whisper model '{model_size}' ({device}/{compute_type})")
        _MODEL_CACHE[key] = WhisperModel(model_size, device=device, compute_type=compute_type)
    return _MODEL_CACHE[key]


def _format_ts(seconds: float) -> str:
    """Format seconds as HH:MM:SS."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


@PARSER_REGISTRY.register("audio")
class AudioParser(BaseParser):
    """Transcribes audio content to plain text via faster-whisper."""

    extensions = [".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac", ".wma"]

    def __init__(self, config=None):
        # Optional config, mirroring PiiProcessor/QualityValidator style. Falls
        # back to sane defaults so this also works if instantiated bare, like
        # TextParser()/CsvParser() are elsewhere in the framework.
        self.config = config

    def parse(self, content_bytes: bytes, filename: str) -> str:
        model_size = getattr(self.config, "whisper_model_size", "base") if self.config else "base"
        device = getattr(self.config, "whisper_device", "cpu") if self.config else "cpu"
        compute_type = getattr(self.config, "whisper_compute_type", "int8") if self.config else "int8"

        suffix = os.path.splitext(filename)[1] or ".mp3"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
            tmp.write(content_bytes)
            tmp.flush()

            model = _get_whisper_model(model_size, device, compute_type)
            segments, info = model.transcribe(tmp.name, beam_size=5, vad_filter=True)

            lines = []
            for seg in segments:
                ts = f"[{_format_ts(seg.start)} - {_format_ts(seg.end)}]"
                text = seg.text.strip()
                if text:
                    lines.append(f"{ts} {text}")

        if not lines:
            logger.info(f"No speech detected in {filename}")
            return ""

        transcript = "\n".join(lines)
        logger.info(
            f"Transcribed {filename}: {len(lines)} segments, "
            f"language={info.language} ({info.language_probability:.2f})"
        )
        return transcript
