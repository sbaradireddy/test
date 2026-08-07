"""Audio parsing plugin - transcribes audio files to text via Amazon Bedrock
(Amazon Nova 2 Lite multimodal understanding, Converse API).

Uses a managed Bedrock model instead of a locally-hosted Whisper model - no
GPU/CPU transcription workload to run inside the pipeline container, no
model weights to bake into the image, and it scales with Bedrock rather
than with container CPU. Trade-off worth knowing: Nova's Converse API
returns a single transcript, not per-segment start/end timestamps the way
faster-whisper's segment objects did, so output here isn't wrapped in
"[HH:MM:SS - HH:MM:SS]" lines. TimestampChunker (processing/chunkers/
timestamp.py) already degrades gracefully to RecursiveChunker for
non-timestamped text, so chunking still works fine - it just chunks this
transcript the same way it would chunk a document. If you rely on
AudioPiiProcessor's segment-level audio muting in pii_processor.py, that
class keeps its own local Whisper loader specifically for the timestamps
that feature needs - see the comment there.
"""
import logging
import os

from custom_rag_pipeline_framework.core.interfaces import BaseParser
from custom_rag_pipeline_framework.core.registry import PARSER_REGISTRY

logger = logging.getLogger(__name__)

# Audio formats Nova's Converse API accepts natively for the "audio" content
# block. ".m4a" has no direct enum value - it's an MP4 container with AAC
# audio, so it's mapped to "mp4" below.
_NOVA_AUDIO_FORMATS = {"mp3", "opus", "wav", "aac", "flac", "mp4", "ogg", "mkv"}
_FORMAT_ALIASES = {"m4a": "mp4"}

# Inline request payloads over ~25MB should go through an S3 URI instead of
# raw bytes (general Bedrock multimodal guidance) - this is a soft warning
# threshold, not a hard block, since the actual limit can vary by region/model.
_INLINE_BYTES_WARN_THRESHOLD = 25 * 1024 * 1024

_DEFAULT_TRANSCRIBE_PROMPT = (
    "Transcribe this audio recording verbatim. Output only the spoken "
    "words as plain text with standard punctuation - no commentary, no "
    "timestamps, no speaker labels unless multiple speakers are clearly "
    "distinguishable. If there is no discernible speech, respond with "
    "exactly: [NO_SPEECH_DETECTED]"
)


@PARSER_REGISTRY.register("audio")
class AudioParser(BaseParser):
    """Transcribes audio content to plain text via Amazon Bedrock (Nova 2 Lite)."""

    extensions = [".mp3", ".wav", ".m4a", ".flac", ".ogg", ".aac"]

    def __init__(self, config=None):
        self.config = config
        self._bedrock = None

    @property
    def bedrock(self):
        if self._bedrock is None:
            import boto3
            from botocore.config import Config

            region = getattr(self.config, "region", None) if self.config else None
            # Bedrock recommends extended timeouts for multimodal inference;
            # audio transcription requests can run well past the SDK's 60s default.
            self._bedrock = boto3.client(
                "bedrock-runtime",
                region_name=region,
                config=Config(connect_timeout=3600, read_timeout=3600),
            )
        return self._bedrock

    def parse(self, content_bytes: bytes, filename: str) -> str:
        model_id = getattr(self.config, "bedrock_audio_model_id", "us.amazon.nova-2-lite-v1:0") \
            if self.config else "us.amazon.nova-2-lite-v1:0"
        prompt = getattr(self.config, "audio_transcribe_prompt", _DEFAULT_TRANSCRIBE_PROMPT) \
            if self.config else _DEFAULT_TRANSCRIBE_PROMPT

        audio_format = self._resolve_format(filename)
        if audio_format is None:
            logger.warning(f"Unsupported audio format for Bedrock transcription: {filename}")
            return ""

        if len(content_bytes) > _INLINE_BYTES_WARN_THRESHOLD:
            logger.warning(
                f"{filename} is {len(content_bytes) / (1024 * 1024):.1f}MB - "
                f"inline Bedrock payloads over ~25MB may fail; consider an "
                f"S3 URI source for large audio files."
            )

        try:
            response = self.bedrock.converse(
                modelId=model_id,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"audio": {"format": audio_format, "source": {"bytes": content_bytes}}},
                            {"text": prompt},
                        ],
                    }
                ],
            )
        except Exception as exc:
            logger.warning(f"Bedrock audio transcription failed for {filename}: {exc}")
            return ""

        transcript = self._extract_text(response).strip()
        if not transcript or transcript == "[NO_SPEECH_DETECTED]":
            logger.info(f"No speech detected in {filename}")
            return ""

        logger.info(f"Transcribed {filename} via Bedrock ({model_id}): {len(transcript)} chars")
        return transcript

    def _resolve_format(self, filename: str):
        ext = os.path.splitext(filename)[1].lstrip(".").lower()
        fmt = _FORMAT_ALIASES.get(ext, ext)
        return fmt if fmt in _NOVA_AUDIO_FORMATS else None

    def _extract_text(self, response: dict) -> str:
        try:
            content = response["output"]["message"]["content"]
            return "".join(block["text"] for block in content if "text" in block)
        except (KeyError, IndexError):
            return ""
