from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any


class RankingValidationError(ValueError):
    """Raised when ranking items are structurally invalid or violate constraints."""
    pass


class DuplicateEnlaceIdError(RankingValidationError):
    """Raised when an enlace_id appears more than once within the same ranking dataset."""
    pass


@dataclass(frozen=True)
class RankingItem:
    """
    Individual item in a ranking.
    - enlace_id: Unique content identifier in the catalog.
    - rank: Position in the ranking (1-indexed). Note: ties are allowed (identical ranks).
    - score: Optional numeric score (views, watch time, composite metric, etc.).
    """
    enlace_id: str
    rank: int
    score: Optional[float] = None

    def __post_init__(self):
        if not self.enlace_id or not isinstance(self.enlace_id, str):
            raise RankingValidationError("enlace_id must be a non-empty string.")
        if not isinstance(self.rank, int) or self.rank < 1:
            raise RankingValidationError(f"rank must be a positive integer >= 1 (got {self.rank}).")
        if self.score is not None and not isinstance(self.score, (int, float)):
            raise RankingValidationError(f"score must be a numeric value or None (got {self.score}).")


@dataclass
class RankingResult:
    """
    Encapsulates the complete response from a RankingProvider.
    """
    items: List[RankingItem]
    source: str
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    extra_metadata: Optional[Dict[str, Any]] = field(default_factory=dict)

    def validate_unique_enlace_ids(self) -> None:
        """Ensures each enlace_id appears at most once in this ranking result."""
        seen = set()
        duplicates = set()
        for item in self.items:
            if item.enlace_id in seen:
                duplicates.add(item.enlace_id)
            seen.add(item.enlace_id)
        if duplicates:
            raise DuplicateEnlaceIdError(
                f"Duplicate enlace_id(s) detected in ranking: {sorted(list(duplicates))}. "
                f"Each content asset must appear at most once per snapshot."
            )


class RankingProvider(ABC):
    """
    Abstract interface for all Ranking Providers.
    Decouples ranking generation/retrieval from the Prewarm Planner.
    """

    @abstractmethod
    def get_ranking(self, limit: Optional[int] = None) -> RankingResult:
        """
        Fetch the ranking dataset, optionally limited to the top `limit` items.
        Must return items ordered deterministically by rank ascending.
        """
        pass

    def get_ranked_items(self, limit: Optional[int] = None) -> List[RankingItem]:
        """Convenience method returning directly the list of RankingItem objects."""
        return self.get_ranking(limit=limit).items
