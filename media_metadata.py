"""Media-specific metadata extraction for audio/image/video assets.

Output is meant to be passed straight into tagger.generate_chunk_metadata(...)
/ tagger.tag_chunks(...) via their existing extra_metadata: Dict parameter -
no changes needed to tagger.py itself.
"""
import io
import json
import logging
import os
import subprocess
import tempfile

logger = logging.getLogger(__name__)


def get_av_metadata(content_bytes: bytes, filename: str) -> dict:
    """Probe an audio/video file with ffprobe. Returns duration/resolution/codec info."""
    suffix = os.path.splitext(filename)[1] or ".bin"
    metadata = {}
    with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
        tmp.write(content_bytes)
        tmp.flush()
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "quiet", "-print_format", "json",
                    "-show_format", "-show_streams", tmp.name,
                ],
                capture_output=True, timeout=60, check=True,
            )
            probe = json.loads(result.stdout)
            fmt = probe.get("format", {})
            if "duration" in fmt:
                metadata["duration_seconds"] = round(float(fmt["duration"]), 2)

            for stream in probe.get("streams", []):
                if stream.get("codec_type") == "video" and "width" not in metadata:
                    metadata["width"] = stream.get("width")
                    metadata["height"] = stream.get("height")
                    metadata["video_codec"] = stream.get("codec_name")
                if stream.get("codec_type") == "audio" and "audio_codec" not in metadata:
                    metadata["audio_codec"] = stream.get("codec_name")
                    metadata["sample_rate"] = stream.get("sample_rate")
        except Exception as exc:
            logger.warning(f"ffprobe metadata extraction failed for {filename}: {exc}")
    return metadata


def get_image_metadata(content_bytes: bytes) -> dict:
    """Extract width/height/format from an image using Pillow."""
    try:
        from PIL import Image  # pip install Pillow
        with Image.open(io.BytesIO(content_bytes)) as img:
            return {"width": img.width, "height": img.height, "image_format": img.format}
    except Exception as exc:
        logger.warning(f"Image metadata extraction failed: {exc}")
        return {}
