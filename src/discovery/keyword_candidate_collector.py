from __future__ import annotations

from typing import Iterable

import pandas as pd

from projects.keyword.src.discovery.base import BaseKeywordDiscoveryProvider, DISCOVERY_COLUMNS
from projects.keyword.src.discovery.google_trends_provider import GoogleTrendsProvider
from projects.keyword.src.discovery.naver_autocomplete_provider import NaverAutocompleteProvider


class KeywordCandidateCollector:
    """
    Aggregate keyword candidates from one or more discovery providers.
    """

    def __init__(
        self,
        providers: Iterable[BaseKeywordDiscoveryProvider] | None = None,
        top_n: int = 20,
    ):
        if providers is None:
            providers = [
                NaverAutocompleteProvider(top_n=10),
                GoogleTrendsProvider(top_n=20),
            ]

        self.providers = list(providers)
        self.top_n = top_n

    @staticmethod
    def _empty_result() -> pd.DataFrame:
        return pd.DataFrame(columns=DISCOVERY_COLUMNS)

    @staticmethod
    def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        for column in DISCOVERY_COLUMNS:
            if column not in result.columns:
                result[column] = pd.NA

        result["seed_keyword"] = result["seed_keyword"].astype(str).str.strip()
        result["keyword"] = result["keyword"].astype(str).str.strip()
        result["source"] = result["source"].astype(str).str.strip()
        result["provider"] = result["provider"].astype(str).str.strip()
        result["rank"] = pd.to_numeric(result["rank"], errors="coerce")
        result["score_hint"] = pd.to_numeric(result["score_hint"], errors="coerce")

        result = result[result["keyword"] != ""].copy()
        return result.loc[:, DISCOVERY_COLUMNS]

    def _collect_provider_frames(self, seed_keyword: str) -> list[pd.DataFrame]:
        frames: list[pd.DataFrame] = []

        for provider in self.providers:
            df = provider.collect(seed_keyword)
            if df.empty:
                continue
            frames.append(self._normalize_frame(df))

        return frames

    def collect_dataframe(self, seed_keyword: str) -> pd.DataFrame:
        frames = self._collect_provider_frames(seed_keyword)
        if not frames:
            return self._empty_result()

        combined = pd.concat(frames, ignore_index=True)
        if combined.empty:
            return self._empty_result()

        combined = combined.sort_values(
            by=["rank", "provider", "source", "keyword"],
            ascending=[True, True, True, True],
            na_position="last",
        ).reset_index(drop=True)

        combined = combined.drop_duplicates(subset=["provider", "keyword"], keep="first").reset_index(drop=True)

        if len(combined) > self.top_n:
            combined = combined.head(self.top_n).reset_index(drop=True)

        return combined.loc[:, DISCOVERY_COLUMNS]

    def collect_keywords(self, seed_keyword: str) -> list[str]:
        combined = self.collect_dataframe(seed_keyword)
        if combined.empty:
            return []
        return combined["keyword"].astype(str).tolist()

    def collect(self, seed_keyword: str) -> pd.DataFrame:
        return self.collect_dataframe(seed_keyword)
