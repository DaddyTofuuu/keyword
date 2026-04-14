from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd


@dataclass(slots=True)
class KeywordCleanerConfig:
    remove_exact_seed: bool = False
    min_length: int | None = None
    max_length: int | None = None
    noisy_tokens: tuple[str, ...] = field(default_factory=tuple)
    collapse_internal_whitespace: bool = True


class KeywordCleaner:
    """
    Normalize and filter keyword candidates deterministically.
    """

    REQUIRED_COLUMNS = [
        "seed_keyword",
        "keyword",
        "source",
        "provider",
        "rank",
        "score_hint",
    ]

    def __init__(self, config: KeywordCleanerConfig | None = None):
        self.config = config or KeywordCleanerConfig()

    @staticmethod
    def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for column in KeywordCleaner.REQUIRED_COLUMNS:
            if column not in result.columns:
                result[column] = pd.NA
        return result

    def _normalize_text(self, value: object) -> str:
        text = "" if value is None else str(value)
        text = text.strip()

        if self.config.collapse_internal_whitespace:
            text = re.sub(r"\s+", " ", text)

        return text

    def _is_valid_keyword(
        self,
        keyword: str,
        *,
        seed_keyword: str | None = None,
    ) -> bool:
        if not keyword:
            return False

        if self.config.remove_exact_seed and seed_keyword and keyword == seed_keyword:
            return False

        if self.config.min_length is not None and len(keyword) < self.config.min_length:
            return False

        if self.config.max_length is not None and len(keyword) > self.config.max_length:
            return False

        if self.config.noisy_tokens and any(token in keyword for token in self.config.noisy_tokens):
            return False

        return True

    def clean_keywords(
        self,
        keywords: Iterable[str],
        *,
        seed_keyword: str | None = None,
    ) -> list[str]:
        normalized_seed = self._normalize_text(seed_keyword) if seed_keyword is not None else None
        cleaned: list[str] = []
        seen: set[str] = set()

        for keyword in keywords:
            normalized_keyword = self._normalize_text(keyword)

            if not self._is_valid_keyword(
                normalized_keyword,
                seed_keyword=normalized_seed,
            ):
                continue

            if normalized_keyword in seen:
                continue

            seen.add(normalized_keyword)
            cleaned.append(normalized_keyword)

        return cleaned

    def clean_dataframe(
        self,
        df: pd.DataFrame,
        *,
        seed_keyword: str | None = None,
    ) -> pd.DataFrame:
        result = self._ensure_columns(df)

        result["seed_keyword"] = result["seed_keyword"].map(self._normalize_text)
        result["keyword"] = result["keyword"].map(self._normalize_text)
        result["source"] = result["source"].map(self._normalize_text)
        result["provider"] = result["provider"].map(self._normalize_text)
        result["rank"] = pd.to_numeric(result["rank"], errors="coerce")
        result["score_hint"] = pd.to_numeric(result["score_hint"], errors="coerce")

        effective_seed = self._normalize_text(seed_keyword) if seed_keyword is not None else None
        if effective_seed is None and "seed_keyword" in result.columns and not result.empty:
            seed_values = result["seed_keyword"].dropna().astype(str)
            effective_seed = seed_values.iloc[0] if not seed_values.empty else None

        if result.empty:
            return result.loc[:, self.REQUIRED_COLUMNS]

        valid_mask = result["keyword"].map(
            lambda keyword: self._is_valid_keyword(
                keyword,
                seed_keyword=effective_seed,
            )
        )
        result = result.loc[valid_mask].copy()

        result = result.sort_values(
            by=["rank", "provider", "source", "keyword"],
            ascending=[True, True, True, True],
            na_position="last",
        ).reset_index(drop=True)

        result = result.drop_duplicates(subset=["keyword"], keep="first").reset_index(drop=True)
        return result.loc[:, self.REQUIRED_COLUMNS]

    def clean(
        self,
        data: pd.DataFrame | Iterable[str],
        *,
        seed_keyword: str | None = None,
    ) -> pd.DataFrame | list[str]:
        if isinstance(data, pd.DataFrame):
            return self.clean_dataframe(data, seed_keyword=seed_keyword)
        return self.clean_keywords(data, seed_keyword=seed_keyword)
