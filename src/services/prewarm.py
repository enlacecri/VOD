import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple

from redis import Redis
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.core.config import settings
from src.core.queues import (
    QUEUE_BATCH,
    QUEUE_PRIORITY,
    QUEUE_LEGACY,
    get_redis_connection,
    get_queue,
)
from src.models.asset import Asset
from src.models.job import Job
from src.models.enums import VideoStatus, JobStatus
from src.models.ranking_snapshot import RankingSnapshot, RankingSnapshotItem
from src.models.prewarm_run import PrewarmRun
from src.services.ranking.base import (
    RankingProvider,
    RankingResult,
    RankingItem,
    DuplicateEnlaceIdError,
)
from src.services.job_dispatch import batch_enqueue_asset

logger = logging.getLogger(__name__)


class ItemClassification(str, Enum):
    CANDIDATE_BATCH = "CANDIDATE_BATCH"
    SKIP_ALREADY_READY = "SKIP_ALREADY_READY"
    SKIP_ALREADY_ACTIVE = "SKIP_ALREADY_ACTIVE"
    SKIP_ALREADY_BATCH = "SKIP_ALREADY_BATCH"
    SKIP_FAILED = "SKIP_FAILED"
    SKIP_LEGACY_STATE = "SKIP_LEGACY_STATE"
    NOT_IN_CATALOG = "NOT_IN_CATALOG"
    SKIP_UNSUPPORTED = "SKIP_UNSUPPORTED"


@dataclass
class ClassifiedRankingItem:
    ranking_item: RankingItem
    classification: ItemClassification
    asset: Optional[Asset] = None
    active_job: Optional[Job] = None
    detail: str = ""


@dataclass
class PrewarmPlanResult:
    provider_source: str
    target_top_n: int
    ranking_size: int
    matched_assets: int
    already_ready: int
    active: int
    cold_candidates: int
    failed_skipped: int
    missing_catalog: int
    legacy_skipped: int
    batch_queue_depth: int
    queue_capacity: int
    would_enqueue_now: int
    classified_items: List[ClassifiedRankingItem] = field(default_factory=list)
    ranking_result: Optional[RankingResult] = None


@dataclass
class PrewarmRunResult:
    plan: PrewarmPlanResult
    run_id: Optional[uuid.UUID] = None
    snapshot_id: Optional[uuid.UUID] = None
    enqueued_count: int = 0
    recoverable_pending_count: int = 0
    batch_queue_depth_after: int = 0
    status: str = "COMPLETED"
    error: Optional[str] = None
    duration_seconds: float = 0.0
    dispatch_results: List[Dict[str, Any]] = field(default_factory=list)


class PrewarmPlanner:
    """
    Central service for Dynamic TOP N and Batch Prewarming.
    
    Adheres strictly to the architectural constraints:
    - Never executes ffmpeg, ffprobe, subprocess, or transcoding directly.
    - PostgreSQL is the single source of truth for technical asset status.
    - Zero N+1 queries: performs bulk chunked queries for Assets and active Jobs.
    - Dry-run / plan operations are 100% pure (0 DB writes, 0 Redis mutations).
    - Enqueue limits and backpressure protect the vod_batch queue.
    - Dispatches exclusively through Phase 3 batch_enqueue_asset.
    """

    def __init__(
        self,
        db: Session,
        redis_conn: Optional[Redis] = None,
        chunk_size: int = 1000,
    ):
        self.db = db
        self.redis_conn = redis_conn
        self.chunk_size = chunk_size

    def _get_redis(self) -> Redis:
        return self.redis_conn or get_redis_connection()

    def _fetch_assets_in_bulk(self, enlace_ids: List[str]) -> Dict[str, Asset]:
        """Bulk-fetches assets for the given enlace_ids in chunks to avoid N+1 queries."""
        assets_by_enlace_id: Dict[str, Asset] = {}
        for i in range(0, len(enlace_ids), self.chunk_size):
            chunk = enlace_ids[i:i + self.chunk_size]
            assets = self.db.query(Asset).filter(Asset.enlace_id.in_(chunk)).all()
            for asset in assets:
                assets_by_enlace_id[asset.enlace_id] = asset
        return assets_by_enlace_id

    def _fetch_active_jobs_in_bulk(self, asset_ids: List[uuid.UUID]) -> Dict[uuid.UUID, Job]:
        """Bulk-fetches active (PENDING/PROCESSING) jobs for the given asset IDs."""
        jobs_by_asset_id: Dict[uuid.UUID, Job] = {}
        for i in range(0, len(asset_ids), self.chunk_size):
            chunk = asset_ids[i:i + self.chunk_size]
            active_jobs = self.db.query(Job).filter(
                Job.asset_id.in_(chunk),
                Job.status.in_([JobStatus.PENDING, JobStatus.PROCESSING])
            ).all()
            for job in active_jobs:
                # If multiple active jobs exist (should not happen), keep the latest
                if job.asset_id not in jobs_by_asset_id or job.created_at > jobs_by_asset_id[job.asset_id].created_at:
                    jobs_by_asset_id[job.asset_id] = job
        return jobs_by_asset_id

    def classify_items(
        self,
        ranking_result: RankingResult,
        target_top_n: int,
    ) -> Tuple[List[ClassifiedRankingItem], Dict[str, int]]:
        """
        Classifies ranked items by cross-referencing PostgreSQL state in bulk without N+1 queries.
        Validates that duplicate enlace_ids are rejected prior to execution.
        """
        ranking_result.validate_unique_enlace_ids()

        top_items = ranking_result.items[:target_top_n]
        enlace_ids = [item.enlace_id for item in top_items]

        # 1. Bulk query assets
        asset_by_enlace = self._fetch_assets_in_bulk(enlace_ids)

        # 2. Bulk query active jobs for matched assets
        matched_asset_ids = [asset.id for asset in asset_by_enlace.values()]
        active_job_by_asset = self._fetch_active_jobs_in_bulk(matched_asset_ids)

        classified: List[ClassifiedRankingItem] = []
        metrics = {
            "matched_assets": 0,
            "already_ready": 0,
            "active": 0,
            "cold_candidates": 0,
            "failed_skipped": 0,
            "missing_catalog": 0,
            "legacy_skipped": 0,
        }

        for item in top_items:
            asset = asset_by_enlace.get(item.enlace_id)
            if not asset:
                metrics["missing_catalog"] += 1
                classified.append(ClassifiedRankingItem(
                    ranking_item=item,
                    classification=ItemClassification.NOT_IN_CATALOG,
                    detail="Content asset does not exist in PostgreSQL catalog."
                ))
                continue

            metrics["matched_assets"] += 1
            active_job = active_job_by_asset.get(asset.id)

            if asset.status == VideoStatus.READY:
                metrics["already_ready"] += 1
                classified.append(ClassifiedRankingItem(
                    ranking_item=item,
                    classification=ItemClassification.SKIP_ALREADY_READY,
                    asset=asset,
                    active_job=active_job,
                    detail="Asset is already published and READY."
                ))
            elif asset.status in (VideoStatus.PLAYABLE, VideoStatus.VALIDATING, VideoStatus.PROCESSING):
                metrics["active"] += 1
                classified.append(ClassifiedRankingItem(
                    ranking_item=item,
                    classification=ItemClassification.SKIP_ALREADY_ACTIVE,
                    asset=asset,
                    active_job=active_job,
                    detail=f"Asset is actively processing/playable in state {asset.status.value}."
                ))
            elif asset.status == VideoStatus.QUEUED:
                if active_job and active_job.queue_name == QUEUE_BATCH:
                    metrics["active"] += 1
                    classified.append(ClassifiedRankingItem(
                        ranking_item=item,
                        classification=ItemClassification.SKIP_ALREADY_BATCH,
                        asset=asset,
                        active_job=active_job,
                        detail="Asset is already QUEUED in vod_batch."
                    ))
                else:
                    metrics["active"] += 1
                    classified.append(ClassifiedRankingItem(
                        ranking_item=item,
                        classification=ItemClassification.SKIP_ALREADY_ACTIVE,
                        asset=asset,
                        active_job=active_job,
                        detail=f"Asset is already QUEUED in queue '{active_job.queue_name if active_job else 'unknown'}'."
                    ))
            elif asset.status == VideoStatus.FAILED:
                metrics["failed_skipped"] += 1
                classified.append(ClassifiedRankingItem(
                    ranking_item=item,
                    classification=ItemClassification.SKIP_FAILED,
                    asset=asset,
                    active_job=active_job,
                    detail="Asset is in FAILED state. Requires explicit operator retry."
                ))
            elif asset.status in (VideoStatus.CREATED, VideoStatus.PROBING):
                # Legacy pipeline states - never convert to batch
                if active_job:
                    metrics["active"] += 1
                    classified.append(ClassifiedRankingItem(
                        ranking_item=item,
                        classification=ItemClassification.SKIP_ALREADY_ACTIVE,
                        asset=asset,
                        active_job=active_job,
                        detail=f"Asset in legacy status {asset.status.value} has active job."
                    ))
                else:
                    metrics["legacy_skipped"] += 1
                    classified.append(ClassifiedRankingItem(
                        ranking_item=item,
                        classification=ItemClassification.SKIP_LEGACY_STATE,
                        asset=asset,
                        active_job=None,
                        detail=f"Asset is in legacy status {asset.status.value}. Only COLD is eligible for batch."
                    ))
            elif asset.status == VideoStatus.COLD:
                if active_job:
                    metrics["active"] += 1
                    classified.append(ClassifiedRankingItem(
                        ranking_item=item,
                        classification=ItemClassification.SKIP_ALREADY_ACTIVE,
                        asset=asset,
                        active_job=active_job,
                        detail=f"COLD asset already has active job {active_job.id}."
                    ))
                else:
                    metrics["cold_candidates"] += 1
                    classified.append(ClassifiedRankingItem(
                        ranking_item=item,
                        classification=ItemClassification.CANDIDATE_BATCH,
                        asset=asset,
                        active_job=None,
                        detail="Eligible COLD candidate for batch prewarming."
                    ))
            else:
                classified.append(ClassifiedRankingItem(
                    ranking_item=item,
                    classification=ItemClassification.SKIP_UNSUPPORTED,
                    asset=asset,
                    active_job=active_job,
                    detail=f"Unsupported status {asset.status.value}."
                ))

        return classified, metrics

    def plan_prewarm(
        self,
        provider: RankingProvider,
        target_top_n: Optional[int] = None,
        enqueue_limit: Optional[int] = None,
        max_queue_depth: Optional[int] = None,
    ) -> PrewarmPlanResult:
        """
        100% Dry-Run Planning.
        Inspects ranking, queries PostgreSQL assets in bulk, checks Redis queue depth,
        and computes theoretical candidates to enqueue.
        NO database writes, NO Job inserts, NO Redis mutations.
        """
        top_n = target_top_n or settings.VOD_PREWARM_TOP_N
        limit = enqueue_limit or settings.VOD_PREWARM_ENQUEUE_LIMIT
        max_depth = max_queue_depth or settings.VOD_BATCH_MAX_QUEUE_DEPTH

        ranking_result = provider.get_ranking(limit=top_n)
        classified_items, metrics = self.classify_items(ranking_result, target_top_n=top_n)

        # Inspect batch queue depth in Redis (read-only)
        conn = self._get_redis()
        try:
            batch_q = get_queue(QUEUE_BATCH, connection=conn)
            batch_queue_depth = batch_q.count
        except Exception as e:
            logger.warning(f"Failed to query Redis batch queue depth: {e}. Assuming queue depth 0.")
            batch_queue_depth = 0

        queue_capacity = max(0, max_depth - batch_queue_depth)
        cold_count = metrics["cold_candidates"]
        would_enqueue = min(limit, queue_capacity, cold_count)

        return PrewarmPlanResult(
            provider_source=ranking_result.source,
            target_top_n=top_n,
            ranking_size=len(ranking_result.items),
            matched_assets=metrics["matched_assets"],
            already_ready=metrics["already_ready"],
            active=metrics["active"],
            cold_candidates=cold_count,
            failed_skipped=metrics["failed_skipped"],
            missing_catalog=metrics["missing_catalog"],
            legacy_skipped=metrics["legacy_skipped"],
            batch_queue_depth=batch_queue_depth,
            queue_capacity=queue_capacity,
            would_enqueue_now=would_enqueue,
            classified_items=classified_items,
            ranking_result=ranking_result,
        )

    def run_prewarm(
        self,
        provider: RankingProvider,
        target_top_n: Optional[int] = None,
        enqueue_limit: Optional[int] = None,
        max_queue_depth: Optional[int] = None,
        dry_run: bool = False,
    ) -> PrewarmRunResult:
        """
        Executes prewarming.
        If dry_run=True, delegates to plan_prewarm without writing anything to DB or Redis.
        If dry_run=False:
          1. Persists ranking snapshot and snapshot items.
          2. Creates prewarm_runs record (status=RUNNING).
          3. Dispatches up to actual_enqueue_limit COLD candidates to vod_batch.
          4. Handles partial errors gracefully.
          5. Updates prewarm_runs record with final metrics.
        """
        start_time = datetime.now(timezone.utc)
        plan = self.plan_prewarm(
            provider=provider,
            target_top_n=target_top_n,
            enqueue_limit=enqueue_limit,
            max_queue_depth=max_queue_depth,
        )

        if dry_run:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            return PrewarmRunResult(
                plan=plan,
                enqueued_count=0,
                recoverable_pending_count=0,
                batch_queue_depth_after=plan.batch_queue_depth,
                status="DRY_RUN",
                duration_seconds=duration,
            )

        # 1. Create RankingSnapshot in PostgreSQL
        rr = plan.ranking_result
        snapshot = RankingSnapshot(
            source=plan.provider_source,
            period_start=rr.period_start if rr else None,
            period_end=rr.period_end if rr else None,
            generated_at=datetime.now(timezone.utc),
            item_count=len(rr.items) if rr else 0,
            extra_metadata=rr.extra_metadata if rr else {},
        )
        self.db.add(snapshot)
        self.db.flush()

        # Insert snapshot items
        if rr and rr.items:
            for item in rr.items:
                self.db.add(RankingSnapshotItem(
                    snapshot_id=snapshot.id,
                    enlace_id=item.enlace_id,
                    rank=item.rank,
                    score=item.score,
                ))
        self.db.commit()
        self.db.refresh(snapshot)

        # 2. Create PrewarmRun record
        prewarm_run = PrewarmRun(
            ranking_snapshot_id=snapshot.id,
            started_at=start_time,
            target_top_n=plan.target_top_n,
            enqueue_limit=enqueue_limit or settings.VOD_PREWARM_ENQUEUE_LIMIT,
            batch_queue_depth_before=plan.batch_queue_depth,
            examined=plan.ranking_size,
            matched=plan.matched_assets,
            already_ready=plan.already_ready,
            active=plan.active,
            cold_candidates=plan.cold_candidates,
            failed_skipped=plan.failed_skipped,
            missing_catalog=plan.missing_catalog,
            enqueued=0,
            status="RUNNING",
        )
        self.db.add(prewarm_run)
        self.db.commit()
        self.db.refresh(prewarm_run)

        # 3. Identify candidates to enqueue
        eligible_candidates = [
            c for c in plan.classified_items
            if c.classification == ItemClassification.CANDIDATE_BATCH and c.asset is not None
        ]
        candidates_to_dispatch = eligible_candidates[:plan.would_enqueue_now]

        conn = self._get_redis()
        enqueued_count = 0
        recoverable_pending_count = 0
        dispatch_results: List[Dict[str, Any]] = []
        dispatch_errors: List[str] = []

        # 4. Dispatch each candidate
        for candidate in candidates_to_dispatch:
            asset = candidate.asset
            try:
                res = batch_enqueue_asset(
                    asset_identifier=asset.vod_uuid,
                    db=self.db,
                    redis_conn=conn,
                )
                dispatch_results.append(res)
                res_status = res.get("status")
                if res_status == "ENQUEUED_BATCH":
                    enqueued_count += 1
                elif res_status == "ENQUEUED_BATCH_REDIS_OFFLINE":
                    # Job was safely committed as PENDING in PostgreSQL for reconciler
                    recoverable_pending_count += 1
                    enqueued_count += 1
                else:
                    logger.warning(f"Candidate {asset.enlace_id} returned unexpected status: {res}")
            except Exception as e:
                logger.error(f"Error dispatching candidate {asset.enlace_id}: {e}", exc_info=True)
                dispatch_errors.append(f"{asset.enlace_id}: {e}")
                # Check if PostgreSQL job was created nonetheless
                self.db.rollback()
                pending_job = self.db.query(Job).filter(
                    Job.asset_id == asset.id,
                    Job.status == JobStatus.PENDING,
                    Job.queue_name == QUEUE_BATCH
                ).first()
                if pending_job:
                    recoverable_pending_count += 1
                    enqueued_count += 1

        # 5. Measure depth after
        try:
            batch_q = get_queue(QUEUE_BATCH, connection=conn)
            batch_queue_depth_after = batch_q.count
        except Exception:
            batch_queue_depth_after = plan.batch_queue_depth + enqueued_count

        # 6. Determine final run status
        if dispatch_errors:
            if enqueued_count > 0:
                final_status = "PARTIAL"
            else:
                final_status = "FAILED"
        else:
            final_status = "COMPLETED"

        finish_time = datetime.now(timezone.utc)
        duration = (finish_time - start_time).total_seconds()

        prewarm_run.finished_at = finish_time
        prewarm_run.batch_queue_depth_after = batch_queue_depth_after
        prewarm_run.enqueued = enqueued_count
        prewarm_run.status = final_status
        prewarm_run.error = "; ".join(dispatch_errors) if dispatch_errors else None

        self.db.commit()
        self.db.refresh(prewarm_run)

        return PrewarmRunResult(
            plan=plan,
            run_id=prewarm_run.id,
            snapshot_id=snapshot.id,
            enqueued_count=enqueued_count,
            recoverable_pending_count=recoverable_pending_count,
            batch_queue_depth_after=batch_queue_depth_after,
            status=final_status,
            error=prewarm_run.error,
            duration_seconds=duration,
            dispatch_results=dispatch_results,
        )
