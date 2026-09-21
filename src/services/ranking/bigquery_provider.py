from datetime import datetime
from typing import Optional, Dict, Any

from src.services.ranking.base import RankingProvider, RankingResult


class BigQueryRankingProvider(RankingProvider):
    """
    Placeholder contract for future BigQuery Ranking Provider integration.

    Future Contract Requirements:
    -----------------------------
    1. Authentication:
       - Uses Google Cloud ADC (Application Default Credentials) or Service Account key.
       - No credentials or dependencies are bundled in Phase 4.

    2. Query & Metrics:
       - Queries an analytical view/table over a sliding window (e.g. past 7 days).
       - Evaluates popularity based on metric (e.g. views, watch time, unique players).
       - Aggregates by canonical `enlace_id`.

    3. Returned Contract:
       - Must yield items with:
         - `enlace_id`: str (matching PostgreSQL assets.enlace_id)
         - `rank`: int (1-based ranking, ties allowed)
         - `score`: Optional[float] (the raw aggregated metric value)
       - `period_start`: datetime (start of analytical window)
       - `period_end`: datetime (end of analytical window)
       - `source`: "bigquery"

    4. Prewarm Planner Integration:
       - When implemented, this class will inherit from `RankingProvider` and fulfill
         `get_ranking(limit: Optional[int]) -> RankingResult`.
       - The Prewarm Planner requires ZERO modifications to switch from Static to BigQuery.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset: Optional[str] = None,
        table_or_view: Optional[str] = None,
        sliding_window_days: int = 7,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ):
        self.project_id = project_id
        self.dataset = dataset
        self.table_or_view = table_or_view
        self.sliding_window_days = sliding_window_days
        self.extra_metadata = extra_metadata or {}

    def get_ranking(self, limit: Optional[int] = None) -> RankingResult:
        raise NotImplementedError(
            "BigQueryRankingProvider is a placeholder contract for future phases. "
            "BigQuery dependencies and credentials are not configured in Phase 4. "
            "Use StaticRankingProvider for local execution and testing."
        )
