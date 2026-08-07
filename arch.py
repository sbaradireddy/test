"""RAG Data Pipeline - Main Orchestrator.

Wires together a source, processing plugins (parser/validator/privacy/
chunker), a target, and a state tracker - all resolved dynamically via the
plugin registry (Phase 1-2) from a validated `PipelineConfig` (Phase 3).
Every run emits structured logs, EMF metrics, and a persisted audit trail
(Phase 4).
"""
import logging
import time
from typing import Dict, Tuple

from custom_rag_pipeline_framework.core.config import LoadStrategy, PipelineConfig
from custom_rag_pipeline_framework.core.registry import (
    SOURCE_REGISTRY,
    STATE_TRACKER_REGISTRY,
    TARGET_REGISTRY,
)
from custom_rag_pipeline_framework.observability.audit import RunAuditTrail
from custom_rag_pipeline_framework.observability.metrics import MetricsRecorder
from custom_rag_pipeline_framework.processing.chunkers import create_chunker
from custom_rag_pipeline_framework.processing.cleanup.content_cleanup import cleanup_content, simplify_tables
from custom_rag_pipeline_framework.processing.metadata.tagger import tag_chunks
from custom_rag_pipeline_framework.processing.parsers import DocumentParser
from custom_rag_pipeline_framework.processing.parsers.normalize import normalize_text
from custom_rag_pipeline_framework.processing.privacy.pii_processor import PiiProcessor
from custom_rag_pipeline_framework.processing.validators.quality_validator import QualityValidator

# Importing connector/state submodules registers each plugin as a side effect.
from custom_rag_pipeline_framework.connectors.sources import s3_source  # noqa: F401
from custom_rag_pipeline_framework.connectors.targets import s3_vectors_target, opensearch_target  # noqa: F401
from custom_rag_pipeline_framework.state import dynamodb_tracker, s3_manifest_tracker  # noqa: F401

logger = logging.getLogger(__name__)


class RAGDataPipeline:
    """Main pipeline orchestrator - resolves all components via config + registries."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.source = SOURCE_REGISTRY.create("s3", config)
        self.parser = DocumentParser()
        self.validator = QualityValidator(config)
        self.privacy = PiiProcessor(config)
        self.chunker = create_chunker(config)
        self.state = STATE_TRACKER_REGISTRY.create(config.state_backend.value, config)
        self.target = TARGET_REGISTRY.create(config.target_type.value, config)
        self.metrics = MetricsRecorder(namespace="RAGDataPipeline", dimensions={"pipeline": "custom_rag"})
        self.audit = RunAuditTrail()

    def run(self) -> Dict:
        """Execute the pipeline based on load strategy."""
        logger.info(
            f"Starting pipeline run={self.audit.run_id} "
            f"strategy={self.config.load_strategy.value} target={self.config.target_type.value}"
        )
        start = time.time()

        files = self.source.list_files()
        logger.info(f"Found {len(files)} files in s3://{self.config.source_bucket}/{self.config.source_prefix}")

        processed = skipped = failed = 0

        for file_obj in files:
            key = file_obj["Key"]
            etag = file_obj["ETag"].strip('"')

            if self.config.load_strategy == LoadStrategy.UPSERT_NEW and not self.state.is_file_changed(key, etag):
                skipped += 1
                self.audit.record_file(key, "skipped")
                continue

            try:
                chunks_written, pii_count = self._process_file(key, etag, file_obj["Size"])
                processed += 1
                self.audit.record_file(key, "processed", chunks_written=chunks_written, pii_redacted=pii_count)
            except Exception as e:
                logger.error(f"Failed to process {key}: {e}")
                failed += 1
                self.audit.record_file(key, "failed", reason=str(e))

        elapsed = time.time() - start

        self.metrics.put("FilesProcessed", processed)
        self.metrics.put("FilesSkipped", skipped)
        self.metrics.put("FilesFailed", failed)
        self.metrics.put("RunDurationSeconds", elapsed, unit="Seconds")
        self.metrics.flush()

        self.audit.finalize(self.config.model_dump(mode="json"))
        audit_bucket = self.config.state_bucket or self.config.target_bucket
        if audit_bucket:
            self.audit.write_to_s3(bucket=audit_bucket, region=self.config.region)

        result = {
            "run_id": self.audit.run_id,
            "processed": processed,
            "skipped": skipped,
            "failed": failed,
            "elapsed_seconds": elapsed,
        }
        logger.info(f"Pipeline complete: {result}")
        return result

    def _process_file(self, key: str, etag: str, file_size: int) -> Tuple[int, int]:
        """Full processing pipeline for a single file. Returns (chunks_written, pii_detected)."""
        logger.info(f"Processing: {key}")

        content_bytes = self.source.read_file(key)

        text = self.parser.parse(content_bytes, key)
        if not text:
            raise ValueError(f"Failed to parse: {key}")
        text = normalize_text(text)

        validation = self.validator.validate(text, key)
        if not validation.is_valid:
            logger.warning(f"Validation failed for {key}: {validation.reason}")
            return 0, 0

        text = cleanup_content(text)
        text = simplify_tables(text)

        text, pii_detections = self.privacy.process(text)

        chunks = self.chunker.chunk(text)
        if not chunks:
            logger.warning(f"No chunks generated for {key}")
            return 0, len(pii_detections)

        tagged_chunks = tag_chunks(
            chunks=chunks,
            source_key=key,
            source_bucket=self.config.source_bucket,
            file_size=file_size,
            content_hash=validation.content_hash,
            extra_metadata={"pii_redacted": len(pii_detections) > 0},
        )

        self.target.write(tagged_chunks, key)
        self.state.update_file_state(key, etag, validation.content_hash)

        return len(chunks), len(pii_detections)
