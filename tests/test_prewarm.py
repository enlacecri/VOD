import json
import uuid
import subprocess
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest
from redis import Redis, exceptions as redis_exceptions
from sqlalchemy.orm import Session
from sqlalchemy import func

from src.core.config import settings
from src.core.queues import (
    QUEUE_BATCH,
    QUEUE_PRIORITY,
    QUEUE_LEGACY,
    get_queue,
)
from src.models.asset import Asset
from src.models.job import Job
from src.models.ranking_snapshot import RankingSnapshot, RankingSnapshotItem
from src.models.prewarm_run import PrewarmRun
from src.models.enums import VideoStatus, JobType, JobStatus
from src.services.ranking import (
    RankingItem,
    RankingResult,
    RankingProvider,
    RankingValidationError,
    DuplicateEnlaceIdError,
    StaticRankingProvider,
    BigQueryRankingProvider,
)
from src.services.prewarm import (
    PrewarmPlanner,
    ItemClassification,
    PrewarmPlanResult,
    PrewarmRunResult,
)
from src.services.job_dispatch import batch_enqueue_asset, promote_batch_to_priority


@pytest.fixture
def redis_conn():
    conn = Redis.from_url(settings.REDIS_URL)
    conn.flushdb()
    try:
        yield conn
    finally:
        conn.flushdb()
        conn.close()


# ==============================================================================
# 1. Ranking Provider Interface & Validation Tests
# ==============================================================================

def test_ranking_item_creation_and_validation():
    item = RankingItem(enlace_id="PREDI-001", rank=1, score=123.45)
    assert item.enlace_id == "PREDI-001"
    assert item.rank == 1
    assert item.score == 123.45

    # Score is optional
    item_no_score = RankingItem(enlace_id="PREDI-002", rank=2)
    assert item_no_score.score is None

    # Invalid enlace_id
    with pytest.raises(RankingValidationError):
        RankingItem(enlace_id="", rank=1)

    # Invalid rank (< 1)
    with pytest.raises(RankingValidationError):
        RankingItem(enlace_id="PREDI-003", rank=0)


def test_ranking_provider_interface():
    class CustomProvider(RankingProvider):
        def get_ranking(self, limit=None):
            items = [
                RankingItem(enlace_id="PREDI-A", rank=1),
                RankingItem(enlace_id="PREDI-B", rank=2),
            ]
            if limit:
                items = items[:limit]
            return RankingResult(items=items, source="custom")

    prov = CustomProvider()
    res = prov.get_ranking(limit=1)
    assert len(res.items) == 1
    assert res.items[0].enlace_id == "PREDI-A"
    assert prov.get_ranked_items(limit=2) == [
        RankingItem(enlace_id="PREDI-A", rank=1),
        RankingItem(enlace_id="PREDI-B", rank=2),
    ]


def test_static_ranking_provider_json_file(tmp_path):
    data = [
        {"enlace_id": "PREDI-B", "rank": 2, "score": 80.0},
        {"enlace_id": "PREDI-A", "rank": 1, "score": 100.0},
    ]
    file_path = tmp_path / "ranking.json"
    file_path.write_text(json.dumps(data))

    prov = StaticRankingProvider(str(file_path))
    res = prov.get_ranking()
    assert res.source == "static"
    assert len(res.items) == 2
    # Preserves sort order by rank
    assert res.items[0].enlace_id == "PREDI-A"
    assert res.items[0].rank == 1
    assert res.items[1].enlace_id == "PREDI-B"
    assert res.items[1].rank == 2


def test_static_ranking_provider_limit():
    data = [
        {"enlace_id": f"PREDI-{i:03d}", "rank": i, "score": 100 - i}
        for i in range(1, 11)
    ]
    prov = StaticRankingProvider(data)
    res = prov.get_ranking(limit=3)
    assert len(res.items) == 3
    assert [x.rank for x in res.items] == [1, 2, 3]


def test_ranking_allows_repeated_ranks():
    """Requirement: ranking allows ties (duplicate ranks) without errors."""
    data = [
        {"enlace_id": "PREDI-TIE1", "rank": 1, "score": 100.0},
        {"enlace_id": "PREDI-TIE2", "rank": 1, "score": 100.0},
        {"enlace_id": "PREDI-TIE3", "rank": 2, "score": 90.0},
    ]
    prov = StaticRankingProvider(data)
    res = prov.get_ranking()
    assert len(res.items) == 3
    assert res.items[0].rank == 1
    assert res.items[1].rank == 1
    assert res.items[2].rank == 2


def test_duplicate_enlace_id_rejected():
    """Requirement: duplicate enlace_id in same ranking is rejected before execution."""
    data = [
        {"enlace_id": "PREDI-DUP", "rank": 1, "score": 100.0},
        {"enlace_id": "PREDI-DUP", "rank": 2, "score": 90.0},
    ]
    prov = StaticRankingProvider(data)
    with pytest.raises(DuplicateEnlaceIdError):
        prov.get_ranking()


def test_bigquery_provider_placeholder():
    """Requirement: BigQuery provider placeholder documents contract and raises NotImplementedError."""
    bq_prov = BigQueryRankingProvider(project_id="test-proj")
    with pytest.raises(NotImplementedError) as exc_info:
        bq_prov.get_ranking()
    assert "placeholder contract" in str(exc_info.value)


# ==============================================================================
# 2. Bulk Database Queries (Anti-N+1 Verification)
# ==============================================================================

def test_bulk_queries_prevent_n_plus_one(db_session: Session):
    """
    Requirement 3:
    1 query for bulk Assets + 1 query for bulk active Jobs, regardless of ranking size!
    """
    # Create 10 assets
    for i in range(1, 11):
        db_session.add(Asset(
            vod_uuid=uuid.uuid4(),
            enlace_id=f"PREDI-N1-{i:03d}",
            source_uri=f"test{i}.mp4",
            status=VideoStatus.COLD
        ))
    db_session.commit()

    ranking_items = [
        {"enlace_id": f"PREDI-N1-{i:03d}", "rank": i}
        for i in range(1, 11)
    ]
    prov = StaticRankingProvider(ranking_items)
    planner = PrewarmPlanner(db=db_session)

    # Monitor query executions during classify_items
    query_log = []
    from sqlalchemy import event

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        query_log.append(statement)

    event.listen(db_session.bind, "before_cursor_execute", before_cursor_execute)
    try:
        rr = prov.get_ranking()
        classified, metrics = planner.classify_items(rr, target_top_n=10)
        assert len(classified) == 10
        assert metrics["matched_assets"] == 10
        assert metrics["cold_candidates"] == 10

        # Should be exactly 1 query for assets (SELECT ... FROM assets WHERE enlace_id IN (...))
        # and 1 query for jobs (SELECT ... FROM jobs WHERE asset_id IN (...))
        asset_queries = [q for q in query_log if "FROM assets" in q or "from assets" in q.lower()]
        job_queries = [q for q in query_log if "FROM jobs" in q or "from jobs" in q.lower()]

        assert len(asset_queries) == 1
        assert len(job_queries) == 1
    finally:
        event.remove(db_session.bind, "before_cursor_execute", before_cursor_execute)


# ==============================================================================
# 3. Status Classification Tests
# ==============================================================================

def test_classification_all_statuses(db_session: Session):
    """
    Validates classification of all statuses:
    READY -> SKIP_ALREADY_READY
    COLD -> CANDIDATE_BATCH
    PROCESSING -> SKIP_ALREADY_ACTIVE
    PLAYABLE -> SKIP_ALREADY_ACTIVE
    VALIDATING -> SKIP_ALREADY_ACTIVE
    QUEUED in batch -> SKIP_ALREADY_BATCH
    QUEUED in priority -> SKIP_ALREADY_ACTIVE
    FAILED -> SKIP_FAILED
    CREATED/PROBING active -> SKIP_ALREADY_ACTIVE
    CREATED/PROBING without job -> SKIP_LEGACY_STATE (never CANDIDATE_BATCH)
    Missing in catalog -> NOT_IN_CATALOG
    """
    assets = [
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C01", source_uri="1.mp4", status=VideoStatus.READY),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C02", source_uri="2.mp4", status=VideoStatus.COLD),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C03", source_uri="3.mp4", status=VideoStatus.PROCESSING),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C04", source_uri="4.mp4", status=VideoStatus.PLAYABLE),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C05", source_uri="5.mp4", status=VideoStatus.VALIDATING),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C06", source_uri="6.mp4", status=VideoStatus.QUEUED),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C07", source_uri="7.mp4", status=VideoStatus.QUEUED),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C08", source_uri="8.mp4", status=VideoStatus.FAILED),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C09", source_uri="9.mp4", status=VideoStatus.CREATED),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C10", source_uri="10.mp4", status=VideoStatus.CREATED),
        Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C11", source_uri="11.mp4", status=VideoStatus.PROBING),
    ]
    for a in assets:
        db_session.add(a)
    db_session.commit()

    # Add active jobs
    # PREDI-C06 is queued in vod_batch
    db_session.add(Job(asset_id=assets[5].id, type=JobType.TRANSCODE, status=JobStatus.PENDING, queue_name=QUEUE_BATCH))
    # PREDI-C07 is queued in vod_priority
    db_session.add(Job(asset_id=assets[6].id, type=JobType.TRANSCODE, status=JobStatus.PENDING, queue_name=QUEUE_PRIORITY))
    # PREDI-C09 has active legacy job
    db_session.add(Job(asset_id=assets[8].id, type=JobType.PROBE, status=JobStatus.PROCESSING, queue_name=QUEUE_LEGACY))
    # PREDI-C10 has NO active job (orphan legacy)
    # PREDI-C11 has NO active job (orphan legacy)
    db_session.commit()

    items = [
        {"enlace_id": f"PREDI-C{i:02d}", "rank": i}
        for i in range(1, 12)
    ]
    items.append({"enlace_id": "PREDI-MISSING", "rank": 12})

    prov = StaticRankingProvider(items)
    planner = PrewarmPlanner(db=db_session)
    classified, metrics = planner.classify_items(prov.get_ranking(), target_top_n=20)

    class_map = {c.ranking_item.enlace_id: c.classification for c in classified}

    assert class_map["PREDI-C01"] == ItemClassification.SKIP_ALREADY_READY
    assert class_map["PREDI-C02"] == ItemClassification.CANDIDATE_BATCH
    assert class_map["PREDI-C03"] == ItemClassification.SKIP_ALREADY_ACTIVE
    assert class_map["PREDI-C04"] == ItemClassification.SKIP_ALREADY_ACTIVE
    assert class_map["PREDI-C05"] == ItemClassification.SKIP_ALREADY_ACTIVE
    assert class_map["PREDI-C06"] == ItemClassification.SKIP_ALREADY_BATCH
    assert class_map["PREDI-C07"] == ItemClassification.SKIP_ALREADY_ACTIVE
    assert class_map["PREDI-C08"] == ItemClassification.SKIP_FAILED
    assert class_map["PREDI-C09"] == ItemClassification.SKIP_ALREADY_ACTIVE
    assert class_map["PREDI-C10"] == ItemClassification.SKIP_LEGACY_STATE
    assert class_map["PREDI-C11"] == ItemClassification.SKIP_LEGACY_STATE
    assert class_map["PREDI-MISSING"] == ItemClassification.NOT_IN_CATALOG

    # ONLY PREDI-C02 is CANDIDATE_BATCH!
    assert metrics["cold_candidates"] == 1
    assert metrics["already_ready"] == 1
    assert metrics["failed_skipped"] == 1
    assert metrics["missing_catalog"] == 1
    assert metrics["legacy_skipped"] == 2


# ==============================================================================
# 4. Dry-Run & Plan Purity Tests
# ==============================================================================

def test_prewarm_plan_is_100_percent_pure(db_session: Session, redis_conn: Redis):
    """
    Requirement 7 & 19:
    prewarm-plan and prewarm-run --dry-run must perform:
    0 DB inserts, 0 DB updates, 0 Jobs, 0 snapshots, 0 prewarm_runs, 0 Redis mutations.
    """
    # Setup assets
    for i in range(1, 6):
        db_session.add(Asset(
            vod_uuid=uuid.uuid4(),
            enlace_id=f"PREDI-DRY-{i:03d}",
            source_uri=f"dry{i}.mp4",
            status=VideoStatus.COLD
        ))
    db_session.commit()

    initial_assets_count = db_session.query(Asset).count()
    initial_jobs_count = db_session.query(Job).count()
    initial_snapshots_count = db_session.query(RankingSnapshot).count()
    initial_runs_count = db_session.query(PrewarmRun).count()

    items = [{"enlace_id": f"PREDI-DRY-{i:03d}", "rank": i} for i in range(1, 6)]
    prov = StaticRankingProvider(items)
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    # 1. Test plan_prewarm
    plan_res = planner.plan_prewarm(provider=prov, target_top_n=5, enqueue_limit=2)
    assert plan_res.cold_candidates == 5
    assert plan_res.would_enqueue_now == 2

    # Verify zero DB changes
    assert db_session.query(Asset).count() == initial_assets_count
    assert db_session.query(Job).count() == initial_jobs_count
    assert db_session.query(RankingSnapshot).count() == initial_snapshots_count
    assert db_session.query(PrewarmRun).count() == initial_runs_count
    # Verify zero Redis changes
    assert get_queue(QUEUE_BATCH, redis_conn).count == 0

    # 2. Test run_prewarm with dry_run=True
    run_res = planner.run_prewarm(provider=prov, target_top_n=5, enqueue_limit=2, dry_run=True)
    assert run_res.status == "DRY_RUN"
    assert run_res.enqueued_count == 0

    assert db_session.query(Asset).count() == initial_assets_count
    assert db_session.query(Job).count() == initial_jobs_count
    assert db_session.query(RankingSnapshot).count() == initial_snapshots_count
    assert db_session.query(PrewarmRun).count() == initial_runs_count
    assert get_queue(QUEUE_BATCH, redis_conn).count == 0


# ==============================================================================
# 5. Enqueue Limits & Backpressure Tests
# ==============================================================================

def test_enqueue_limit_respected(db_session: Session, redis_conn: Redis):
    """
    Requirement 14:
    If cold_candidates = 5 and limit = 2, only 2 are enqueued.
    """
    for i in range(1, 6):
        db_session.add(Asset(
            vod_uuid=uuid.uuid4(),
            enlace_id=f"PREDI-LIM-{i:03d}",
            source_uri=f"lim{i}.mp4",
            status=VideoStatus.COLD
        ))
    db_session.commit()

    items = [{"enlace_id": f"PREDI-LIM-{i:03d}", "rank": i} for i in range(1, 6)]
    prov = StaticRankingProvider(items)
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    run_res = planner.run_prewarm(provider=prov, target_top_n=5, enqueue_limit=2, max_queue_depth=50)
    assert run_res.enqueued_count == 2
    assert get_queue(QUEUE_BATCH, redis_conn).count == 2

    # Exactly 2 Jobs created in vod_batch
    jobs = db_session.query(Job).filter(Job.queue_name == QUEUE_BATCH).all()
    assert len(jobs) == 2


def test_backpressure_respected(db_session: Session, redis_conn: Redis):
    """
    Requirement 15:
    max_queue_depth = 5, current queue depth = 4, enqueue_limit = 3.
    available_capacity = 1.
    Only 1 job is enqueued.
    """
    # Put 4 dummy jobs in vod_batch
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    for i in range(4):
        q_batch.enqueue("os.getpid", job_id=f"dummy-job-{i}")
    assert q_batch.count == 4

    for i in range(1, 5):
        db_session.add(Asset(
            vod_uuid=uuid.uuid4(),
            enlace_id=f"PREDI-BP-{i:03d}",
            source_uri=f"bp{i}.mp4",
            status=VideoStatus.COLD
        ))
    db_session.commit()

    items = [{"enlace_id": f"PREDI-BP-{i:03d}", "rank": i} for i in range(1, 5)]
    prov = StaticRankingProvider(items)
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    # With max_queue_depth = 5 and current depth = 4, capacity = 1
    plan_res = planner.plan_prewarm(provider=prov, target_top_n=5, enqueue_limit=3, max_queue_depth=5)
    assert plan_res.batch_queue_depth == 4
    assert plan_res.queue_capacity == 1
    assert plan_res.would_enqueue_now == 1

    run_res = planner.run_prewarm(provider=prov, target_top_n=5, enqueue_limit=3, max_queue_depth=5)
    assert run_res.enqueued_count == 1
    assert q_batch.count == 5  # 4 initial + 1 new


def test_backpressure_full_queue_enqueues_zero(db_session: Session, redis_conn: Redis):
    """If vod_batch is at max depth, 0 jobs are enqueued."""
    q_batch = get_queue(QUEUE_BATCH, redis_conn)
    for i in range(2):
        q_batch.enqueue("os.getpid", job_id=f"full-job-{i}")
    assert q_batch.count == 2

    asset = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-FULL", source_uri="full.mp4", status=VideoStatus.COLD)
    db_session.add(asset)
    db_session.commit()

    prov = StaticRankingProvider([{"enlace_id": "PREDI-FULL", "rank": 1}])
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    run_res = planner.run_prewarm(provider=prov, target_top_n=1, enqueue_limit=5, max_queue_depth=2)
    assert run_res.enqueued_count == 0
    assert q_batch.count == 2
    # Asset remains COLD
    db_session.refresh(asset)
    assert asset.status == VideoStatus.COLD


# ==============================================================================
# 6. Real Run Persistence & Second Run Continuation Tests
# ==============================================================================

def test_real_run_persists_snapshot_and_audit(db_session: Session, redis_conn: Redis):
    """
    Requirement 28, 30:
    Real run persists RankingSnapshot and PrewarmRun in PostgreSQL.
    """
    asset = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-AUDIT", source_uri="a.mp4", status=VideoStatus.COLD)
    db_session.add(asset)
    db_session.commit()

    prov = StaticRankingProvider([{"enlace_id": "PREDI-AUDIT", "rank": 1, "score": 999.0}])
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    run_res = planner.run_prewarm(provider=prov, target_top_n=1, enqueue_limit=1)
    assert run_res.status == "COMPLETED"
    assert run_res.enqueued_count == 1

    # Check snapshot in DB
    snapshot = db_session.query(RankingSnapshot).filter(RankingSnapshot.id == run_res.snapshot_id).first()
    assert snapshot is not None
    assert snapshot.source == "static"
    assert snapshot.item_count == 1
    assert len(snapshot.items) == 1
    assert snapshot.items[0].enlace_id == "PREDI-AUDIT"
    assert snapshot.items[0].rank == 1
    assert snapshot.items[0].score == 999.0

    # Check prewarm_run in DB
    run_record = db_session.query(PrewarmRun).filter(PrewarmRun.id == run_res.run_id).first()
    assert run_record is not None
    assert run_record.ranking_snapshot_id == snapshot.id
    assert run_record.status == "COMPLETED"
    assert run_record.enqueued == 1
    assert run_record.cold_candidates == 1
    assert run_record.finished_at is not None


def test_second_run_advances_without_duplication(db_session: Session, redis_conn: Redis):
    """
    Requirement 25, 26:
    Run 1 with limit 2 enqueues items 1 and 2.
    Run 2 with limit 2 enqueues items 3 and 4, without duplicating 1 and 2.
    """
    assets = [
        Asset(vod_uuid=uuid.uuid4(), enlace_id=f"PREDI-SEQ-{i:03d}", source_uri=f"s{i}.mp4", status=VideoStatus.COLD)
        for i in range(1, 5)
    ]
    for a in assets:
        db_session.add(a)
    db_session.commit()

    items = [{"enlace_id": f"PREDI-SEQ-{i:03d}", "rank": i} for i in range(1, 5)]
    prov = StaticRankingProvider(items)
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    # First run: limit 2
    run1 = planner.run_prewarm(provider=prov, target_top_n=4, enqueue_limit=2)
    assert run1.enqueued_count == 2
    db_session.expire_all()
    # Assets 1 and 2 are QUEUED
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-SEQ-001").first().status == VideoStatus.QUEUED
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-SEQ-002").first().status == VideoStatus.QUEUED
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-SEQ-003").first().status == VideoStatus.COLD

    # Second run: limit 2
    run2 = planner.run_prewarm(provider=prov, target_top_n=4, enqueue_limit=2)
    assert run2.enqueued_count == 2
    db_session.expire_all()
    # Assets 3 and 4 are now QUEUED
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-SEQ-003").first().status == VideoStatus.QUEUED
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-SEQ-004").first().status == VideoStatus.QUEUED

    # Total jobs across the 4 assets is exactly 4 (1 per asset, never duplicated!)
    total_jobs = db_session.query(Job).filter(Job.queue_name == QUEUE_BATCH).count()
    assert total_jobs == 4


def test_ranking_change_preserves_ready_outside_top(db_session: Session, redis_conn: Redis):
    """
    Requirement 27:
    Asset A, B, C are READY.
    New ranking has A, X, Y. (B and C fell out of TOP).
    B and C remain READY (no eviction).
    """
    asset_a = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-A", source_uri="a.mp4", status=VideoStatus.READY)
    asset_b = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-B", source_uri="b.mp4", status=VideoStatus.READY)
    asset_c = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-C", source_uri="c.mp4", status=VideoStatus.READY)
    asset_x = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-X", source_uri="x.mp4", status=VideoStatus.COLD)
    for a in [asset_a, asset_b, asset_c, asset_x]:
        db_session.add(a)
    db_session.commit()

    # New ranking only has A and X
    new_ranking = [
        {"enlace_id": "PREDI-A", "rank": 1},
        {"enlace_id": "PREDI-X", "rank": 2},
    ]
    prov = StaticRankingProvider(new_ranking)
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    run_res = planner.run_prewarm(provider=prov, target_top_n=2, enqueue_limit=10)
    assert run_res.enqueued_count == 1  # Only X enqueued
    db_session.expire_all()

    # B and C are untouched and still READY
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-B").first().status == VideoStatus.READY
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-C").first().status == VideoStatus.READY
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-A").first().status == VideoStatus.READY
    assert db_session.query(Asset).filter(Asset.enlace_id == "PREDI-X").first().status == VideoStatus.QUEUED


# ==============================================================================
# 7. Media Isolation & Worker Promotion Integration Tests
# ==============================================================================

def test_planner_never_executes_ffmpeg_or_ffprobe(db_session: Session, redis_conn: Redis):
    """
    Requirement 31, 32:
    Prewarm plan and run NEVER execute subprocess, ffmpeg, or ffprobe.
    """
    asset = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-NO-FFMPEG", source_uri="test.mp4", status=VideoStatus.COLD)
    db_session.add(asset)
    db_session.commit()

    prov = StaticRankingProvider([{"enlace_id": "PREDI-NO-FFMPEG", "rank": 1}])
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    with patch("subprocess.run") as mock_subproc_run, \
         patch("subprocess.Popen") as mock_subproc_popen:
        planner.plan_prewarm(provider=prov, target_top_n=1)
        planner.run_prewarm(provider=prov, target_top_n=1)

        assert mock_subproc_run.call_count == 0
        assert mock_subproc_popen.call_count == 0


def test_batch_to_priority_promotion_integration(db_session: Session, redis_conn: Redis):
    """
    Requirement 33:
    Asset is enqueued by prewarm-run into vod_batch.
    When playback is requested, promote_batch_to_priority safely promotes it to vod_priority.
    """
    asset = Asset(vod_uuid=uuid.uuid4(), enlace_id="PREDI-PROMOTE", source_uri="p.mp4", status=VideoStatus.COLD)
    db_session.add(asset)
    db_session.commit()

    prov = StaticRankingProvider([{"enlace_id": "PREDI-PROMOTE", "rank": 1}])
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    # 1. Run prewarm -> enqueues to vod_batch
    planner.run_prewarm(provider=prov, target_top_n=1)
    db_session.expire_all()

    asset_db = db_session.query(Asset).filter(Asset.enlace_id == "PREDI-PROMOTE").first()
    assert asset_db.status == VideoStatus.QUEUED
    batch_job = db_session.query(Job).filter(Job.asset_id == asset_db.id).first()
    assert batch_job.queue_name == QUEUE_BATCH
    assert batch_job.status == JobStatus.PENDING

    # 2. User requests playback -> triggers Phase 3 promotion
    res = promote_batch_to_priority(
        active_job=batch_job,
        asset=asset_db,
        db=db_session,
        redis_conn=redis_conn,
    )
    assert res["action"] == "PROMOTED"
    db_session.refresh(batch_job)
    assert batch_job.queue_name == QUEUE_PRIORITY
    assert get_queue(QUEUE_PRIORITY, redis_conn).count == 1
    assert get_queue(QUEUE_BATCH, redis_conn).count == 0


def test_redis_failure_recovery_creates_no_duplicate_job(db_session: Session, redis_conn: Redis):
    """
    Clarification 5 & 10:
    When Redis is offline during dispatch, Job remains PENDING in PostgreSQL.
    A second dispatch attempt recognizes the active PENDING job and does NOT create a duplicate job.
    """
    vod_uuid = uuid.uuid4()
    asset = Asset(vod_uuid=vod_uuid, enlace_id="PREDI-OFFLINE", source_uri="off.mp4", status=VideoStatus.COLD)
    db_session.add(asset)
    db_session.commit()

    prov = StaticRankingProvider([{"enlace_id": "PREDI-OFFLINE", "rank": 1}])
    planner = PrewarmPlanner(db=db_session, redis_conn=redis_conn)

    # Simulate Redis connection failure during enqueue
    from rq import Queue as RQQueue
    with patch.object(RQQueue, "enqueue", side_effect=redis_exceptions.ConnectionError("Redis down")):
        run1 = planner.run_prewarm(provider=prov, target_top_n=1, enqueue_limit=1)

    assert run1.recoverable_pending_count == 1
    assert run1.enqueued_count == 1

    # Asset is QUEUED, Job is PENDING in DB
    db_session.expire_all()
    jobs = db_session.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.PENDING
    assert jobs[0].queue_name == QUEUE_BATCH

    # Second run: asset already has active job, classify as SKIP_ALREADY_BATCH / SKIP_ALREADY_ACTIVE
    run2 = planner.run_prewarm(provider=prov, target_top_n=1, enqueue_limit=1)
    assert run2.enqueued_count == 0

    # Total jobs for this asset is STILL exactly 1!
    db_session.expire_all()
    jobs_after = db_session.query(Job).filter(Job.asset_id == asset.id).all()
    assert len(jobs_after) == 1
