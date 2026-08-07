"""Image parsing plugin - converts images into RAG-searchable text.

Combines AWS Rekognition label detection + OCR text detection (cheap, fast),
with an optional richer natural-language caption from a multimodal Bedrock
model when self.config.use_bedrock_captioning is enabled - same opt-in
pattern as PiiProcessor's use_comprehend flag.
"""
import logging

from custom_rag_pipeline_framework.core.interfaces import BaseParser
from custom_rag_pipeline_framework.core.registry import PARSER_REGISTRY

logger = logging.getLogger(__name__)


@PARSER_REGISTRY.register("image")
class ImageParser(BaseParser):
    """Extracts labels, detected text, and (optionally) a natural-language caption."""

    extensions = [".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff", ".bmp"]

    def __init__(self, config=None):
        self.config = config
        self._rekognition = None

    @property
    def rekognition(self):
        if self._rekognition is None:
            import boto3
            region = getattr(self.config, "region", None) if self.config else None
            self._rekognition = boto3.client("rekognition", region_name=region)
        return self._rekognition

    def parse(self, content_bytes: bytes, filename: str) -> str:
        min_confidence = getattr(self.config, "rekognition_min_confidence", 80) if self.config else 80
        max_labels = getattr(self.config, "rekognition_max_labels", 20) if self.config else 20
        use_caption = getattr(self.config, "use_bedrock_captioning", False) if self.config else False

        sections = []

        if use_caption:
            caption = self._bedrock_caption(content_bytes)
            if caption:
                sections.append(f"Description: {caption}")

        labels = self._detect_labels(content_bytes, max_labels, min_confidence)
        if labels:
            sections.append("Image contents: " + ", ".join(labels))

        text_lines = self._detect_text(content_bytes, min_confidence)
        if text_lines:
            sections.append("Detected text:\n" + "\n".join(text_lines))

        if not sections:
            logger.info(f"No content extracted from image {filename}")
            return ""

        logger.info(f"Parsed image {filename}: {len(sections)} content section(s)")
        return "\n\n".join(sections)

    def _detect_labels(self, image_bytes: bytes, max_labels: int, min_confidence: float):
        try:
            response = self.rekognition.detect_labels(
                Image={"Bytes": image_bytes},
                MaxLabels=max_labels,
                MinConfidence=min_confidence,
            )
            return [label["Name"] for label in response.get("Labels", [])]
        except Exception as exc:
            logger.warning(f"Rekognition label detection failed: {exc}")
            return []

    def _detect_text(self, image_bytes: bytes, min_confidence: float):
        try:
            response = self.rekognition.detect_text(Image={"Bytes": image_bytes})
            return [
                d["DetectedText"]
                for d in response.get("TextDetections", [])
                if d.get("Type") == "LINE" and d.get("Confidence", 0) >= min_confidence
            ]
        except Exception as exc:
            logger.warning(f"Rekognition text detection failed: {exc}")
            return []

    def _bedrock_caption(self, image_bytes: bytes) -> str:
        """Optional richer caption via a multimodal Bedrock model (e.g. Claude)."""
        try:
            import base64
            import json
            import boto3

            region = getattr(self.config, "region", None) if self.config else None
            model_id = getattr(
                self.config, "bedrock_model_id", "anthropic.claude-3-haiku-20240307-v1:0"
            )
            bedrock = boto3.client("bedrock-runtime", region_name=region)

            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 300,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": "image/jpeg",
                                    "data": base64.b64encode(image_bytes).decode("utf-8"),
                                },
                            },
                            {
                                "type": "text",
                                "text": "Describe this image factually in 2-3 sentences for a search index.",
                            },
                        ],
                    }
                ],
            }
            response = bedrock.invoke_model(modelId=model_id, body=json.dumps(body))
            payload = json.loads(response["body"].read())
            return payload["content"][0]["text"].strip()
        except Exception as exc:
            logger.warning(f"Bedrock captioning failed: {exc}")
            return ""
