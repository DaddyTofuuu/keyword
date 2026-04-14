from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


DISCOVERY_COLUMNS = [
    "seed_keyword",
    "keyword",
    "source",
    "provider",
    "rank",
    "score_hint",
]


class BaseKeywordDiscoveryProvider(ABC):
    """
    Base interface for keyword discovery providers.
    """

    provider_name = "base"
    source_name = "base"

    def __init__(self, top_n: int = 20):
        self.top_n = top_n

    @abstractmethod
    def collect(self, seed_keyword: str) -> pd.DataFrame:
        """
        Return a candidate DataFrame for the given seed keyword.
        """

    def empty_result(self) -> pd.DataFrame:
        return pd.DataFrame(columns=DISCOVERY_COLUMNS)

    def normalize_keyword(self, keyword: Any) -> str:
        return str(keyword).strip()

    def build_rows(
        self,
        seed_keyword: str,
        keywords: list[str],
        score_hints: list[float] | None = None,
    ) -> pd.DataFrame:
        normalized_seed = self.normalize_keyword(seed_keyword)
        rows: list[dict[str, Any]] = []

        for idx, keyword in enumerate(keywords[: self.top_n], start=1):
            normalized_keyword = self.normalize_keyword(keyword)
            if not normalized_keyword:
                continue

            score_hint = None
            if score_hints is not None and idx - 1 < len(score_hints):
                score_hint = score_hints[idx - 1]

            rows.append(
                {
                    "seed_keyword": normalized_seed,
                    "keyword": normalized_keyword,
                    "source": self.source_name,
                    "provider": self.provider_name,
                    "rank": idx,
                    "score_hint": score_hint,
                }
            )

        if not rows:
            return self.empty_result()

        return pd.DataFrame(rows, columns=DISCOVERY_COLUMNS)
