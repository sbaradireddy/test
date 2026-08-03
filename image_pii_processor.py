"""Image-specific PII processing: detects and blurs faces in image bytes.

This complements pii_processor.py rather than replacing it - PiiProcessor
handles textual PII in the OCR/label/caption text that ImageParser produces
(that flow is already unchanged and automatic). ImagePiiProcessor handles
the visual PII a text redactor can never see: an actual face in the pixels.
Same opt-in style as PiiProcessor.process(..., use_comprehend=False) -
disabled by default via self.config.redact_faces_in_images.
"""
import io
import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)


class ImagePiiProcessor:
    """Detects faces via AWS Rekognition and blurs them in the image bytes."""

    def __init__(self, config):
        self.config = config
        self._rekognition = None

    @property
    def rekognition(self):
        if self._rekognition is None:
            import boto3
            region = getattr(self.config, "region", None)
            self._rekognition = boto3.client("rekognition", region_name=region)
        return self._rekognition

    def process(self, image_bytes: bytes, redact_faces: bool = None) -> Tuple[bytes, List[dict]]:
        """Redact faces from an image. Returns (image_bytes, detections).

        If redaction is disabled (default) or no faces are found, the
        original image_bytes are returned unchanged, mirroring
        PiiProcessor.process()'s (text, []) passthrough when redact_pii=False.
        """
        if redact_faces is None:
            redact_faces = getattr(self.config, "redact_faces_in_images", False)
        if not redact_faces:
            return image_bytes, []

        detections = self._detect_faces(image_bytes)
        if not detections:
            return image_bytes, []

        redacted_bytes = self._blur_regions(image_bytes, detections)
        logger.info(f"Redacted {len(detections)} face(s) from image")
        return redacted_bytes, detections

    def _detect_faces(self, image_bytes: bytes) -> List[dict]:
        try:
            response = self.rekognition.detect_faces(Image={"Bytes": image_bytes})
            return [
                {"type": "FACE", "bounding_box": f["BoundingBox"]}
                for f in response.get("FaceDetails", [])
            ]
        except Exception as exc:
            logger.warning(f"Rekognition face detection failed: {exc}")
            return []

    def _blur_regions(self, image_bytes: bytes, detections: List[dict]) -> bytes:
        try:
            from PIL import Image, ImageFilter  # pip install Pillow

            img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
            width, height = img.size

            for det in detections:
                box = det["bounding_box"]
                left = max(0, int(box["Left"] * width))
                top = max(0, int(box["Top"] * height))
                right = min(width, int(left + box["Width"] * width))
                bottom = min(height, int(top + box["Height"] * height))
                if right <= left or bottom <= top:
                    continue

                region = img.crop((left, top, right, bottom))
                blurred = region.filter(ImageFilter.GaussianBlur(radius=25))
                img.paste(blurred, (left, top))

            out = io.BytesIO()
            img.save(out, format="JPEG", quality=90)
            return out.getvalue()
        except Exception as exc:
            logger.warning(f"Face blurring failed, returning original image: {exc}")
            return image_bytes
