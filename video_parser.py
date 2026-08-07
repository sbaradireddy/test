"""Video parsing plugin - produces a text transcript plus periodic visual-scene
descriptions from a video file, by delegating to AudioParser and ImageParser.

Requires the ffmpeg binary to be present on PATH (used for audio extraction
and keyframe sampling only - no heavy video ML dependency needed).
"""
import logging
import os
import subprocess
import tempfile

from custom_rag_pipeline_framework.core.interfaces import BaseParser
from custom_rag_pipeline_framework.core.registry import PARSER_REGISTRY
from custom_rag_pipeline_framework.processing.parsers.audio_parser import AudioParser
from custom_rag_pipeline_framework.processing.parsers.image_parser import ImageParser

logger = logging.getLogger(__name__)


@PARSER_REGISTRY.register("video")
class VideoParser(BaseParser):
    """Extracts a speech transcript and sampled visual-scene descriptions from video."""

    extensions = [".mp4", ".mov", ".avi", ".mkv", ".webm"]

    def __init__(self, config=None):
        self.config = config

    def parse(self, content_bytes: bytes, filename: str) -> str:
        frame_interval = getattr(self.config, "frame_interval_seconds", 30) if self.config else 30
        analyze_frames = getattr(self.config, "analyze_video_frames", True) if self.config else True

        suffix = os.path.splitext(filename)[1] or ".mp4"
        with tempfile.TemporaryDirectory() as workdir:
            video_path = os.path.join(workdir, f"input{suffix}")
            with open(video_path, "wb") as f:
                f.write(content_bytes)

            sections = []

            audio_path = os.path.join(workdir, "audio.wav")
            if self._extract_audio(video_path, audio_path):
                with open(audio_path, "rb") as f:
                    transcript = AudioParser(self.config).parse(f.read(), "audio.wav")
                if transcript:
                    sections.append("Transcript:\n" + transcript)
            else:
                logger.info(f"No audio track extracted from {filename}")

            if analyze_frames:
                frame_descriptions = self._analyze_frames(video_path, workdir, frame_interval)
                if frame_descriptions:
                    sections.append("Visual scenes:\n" + "\n".join(frame_descriptions))

        if not sections:
            logger.info(f"No content extracted from video {filename}")
            return ""

        logger.info(f"Parsed video {filename}: {len(sections)} content section(s)")
        return "\n\n".join(sections)

    def _extract_audio(self, video_path: str, audio_path: str) -> bool:
        try:
            result = subprocess.run(
                [
                    "ffmpeg", "-y", "-i", video_path,
                    "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", audio_path,
                ],
                capture_output=True, timeout=600,
            )
            return result.returncode == 0 and os.path.exists(audio_path) and os.path.getsize(audio_path) > 0
        except Exception as exc:
            logger.warning(f"ffmpeg audio extraction failed: {exc}")
            return False

    def _analyze_frames(self, video_path: str, workdir: str, interval_seconds: int):
        frame_pattern = os.path.join(workdir, "frame_%04d.jpg")
        try:
            subprocess.run(
                [
                    "ffmpeg", "-y", "-i", video_path,
                    "-vf", f"fps=1/{interval_seconds}",
                    "-qscale:v", "4", frame_pattern,
                ],
                capture_output=True, timeout=600, check=True,
            )
        except Exception as exc:
            logger.warning(f"ffmpeg frame extraction failed: {exc}")
            return []

        frame_files = sorted(f for f in os.listdir(workdir) if f.startswith("frame_"))
        image_parser = ImageParser(self.config)
        descriptions = []
        for idx, frame_file in enumerate(frame_files):
            timestamp = idx * interval_seconds
            with open(os.path.join(workdir, frame_file), "rb") as f:
                description = image_parser.parse(f.read(), frame_file)
            if description:
                ts = f"[{timestamp // 60:02d}:{timestamp % 60:02d}]"
                descriptions.append(f"{ts} {description.splitlines()[0]}")
        return descriptions
