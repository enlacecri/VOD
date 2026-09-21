import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

from src.services.ranking.base import (
    RankingProvider,
    RankingResult,
    RankingItem,
    RankingValidationError,
)


class StaticRankingProvider(RankingProvider):
    """
    Local ranking provider loading rankings from JSON files, JSON strings, or Python lists.
    Used for local development, test automation, and controlled fixtures.
    """

    def __init__(
        self,
        source_data: Union[str, os.PathLike, List[Dict[str, Any]]],
        source: str = "static",
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ):
        self.source = source
        self.period_start = period_start
        self.period_end = period_end
        self.extra_metadata = extra_metadata or {}
        self._raw_data = self._load_data(source_data)
        self._parsed_items = self._parse_items(self._raw_data)

    def _load_data(self, source_data: Union[str, os.PathLike, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if isinstance(source_data, list):
            return source_data

        str_val = str(source_data).strip()
        # Check if it's an existing file path
        if os.path.exists(str_val):
            with open(str_val, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    raise RankingValidationError(f"Expected JSON array in ranking file '{str_val}', got {type(data).__name__}.")
                return data

        # Check if it's JSON string
        if str_val.startswith("[") and str_val.endswith("]"):
            try:
                data = json.loads(str_val)
                if not isinstance(data, list):
                    raise RankingValidationError("Expected JSON array for ranking data.")
                return data
            except json.JSONDecodeError as e:
                raise RankingValidationError(f"Invalid JSON string: {e}")

        raise FileNotFoundError(f"Ranking source file not found: '{str_val}'")

    def _parse_items(self, raw_items: List[Dict[str, Any]]) -> List[RankingItem]:
        items: List[RankingItem] = []
        for idx, entry in enumerate(raw_items):
            if not isinstance(entry, dict):
                raise RankingValidationError(f"Item at index {idx} must be a dictionary, got {type(entry).__name__}.")
            if "enlace_id" not in entry:
                raise RankingValidationError(f"Missing required 'enlace_id' in item at index {idx}: {entry}")
            if "rank" not in entry:
                raise RankingValidationError(f"Missing required 'rank' in item at index {idx}: {entry}")

            item = RankingItem(
                enlace_id=str(entry["enlace_id"]).strip(),
                rank=int(entry["rank"]),
                score=float(entry["score"]) if entry.get("score") is not None else None,
            )
            items.append(item)

        # Deterministic stable sort by rank ascending
        items.sort(key=lambda x: x.rank)
        return items

    def get_ranking(self, limit: Optional[int] = None) -> RankingResult:
        items_to_return = self._parsed_items
        if limit is not None and limit > 0:
            items_to_return = items_to_return[:limit]

        result = RankingResult(
            items=items_to_return,
            source=self.source,
            period_start=self.period_start,
            period_end=self.period_end,
            extra_metadata=dict(self.extra_metadata),
        )
        result.validate_unique_enlace_ids()
        return result
