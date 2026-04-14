from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(slots=True)
class KeywordScoringConfig:
    weights: dict[str, float] = field(
        default_factory=lambda: {
            "search_volume": 0.30,
            "trend": 0.20,
            "competition": 0.15,
            "ctr": 0.15,
            "keyword_quality": 0.20,
        }
    )


class KeywordScorer:
    """
    Interpretable rule-based scoring for keyword ranking.
    """

    def __init__(self, config: KeywordScoringConfig | None = None):
        self.config = config or KeywordScoringConfig()

    @staticmethod
    def _normalize_series(series: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        valid = numeric.dropna()

        if valid.empty:
            return pd.Series(0.0, index=series.index, dtype=float)

        min_value = valid.min()
        max_value = valid.max()

        if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
            normalized = pd.Series(0.0, index=series.index, dtype=float)
            normalized.loc[valid.index] = 1.0
            return normalized

        normalized = (numeric - min_value) / (max_value - min_value)
        return normalized.fillna(0.0)

    @staticmethod
    def _inverse_normalize_series(series: pd.Series) -> pd.Series:
        return 1.0 - KeywordScorer._normalize_series(series)

    def _score_search_volume(self, df: pd.DataFrame) -> pd.Series:
        if "log_search_volume" in df.columns:
            return self._normalize_series(df["log_search_volume"])
        if "naver_total_qc" in df.columns:
            return self._normalize_series(df["naver_total_qc"])
        return pd.Series(0.0, index=df.index, dtype=float)

    def _score_trend(self, df: pd.DataFrame) -> pd.Series:
        if "log_trend_signal" in df.columns:
            return self._normalize_series(df["log_trend_signal"])
        if "trend_signal" in df.columns:
            return self._normalize_series(df["trend_signal"])
        return pd.Series(0.0, index=df.index, dtype=float)

    def _score_competition(self, df: pd.DataFrame) -> pd.Series:
        if "compIdx" in df.columns:
            return self._inverse_normalize_series(df["compIdx"])
        return pd.Series(0.0, index=df.index, dtype=float)

    def _score_ctr(self, df: pd.DataFrame) -> pd.Series:
        ctr_mean_score = self._normalize_series(df["ctr_mean"]) if "ctr_mean" in df.columns else pd.Series(0.0, index=df.index, dtype=float)
        click_total_score = self._normalize_series(df["log_click_total"]) if "log_click_total" in df.columns else pd.Series(0.0, index=df.index, dtype=float)
        return (ctr_mean_score * 0.6) + (click_total_score * 0.4)

    def _score_keyword_quality(self, df: pd.DataFrame) -> pd.Series:
        rank_score = self._normalize_series(df["rank_inverse"]) if "rank_inverse" in df.columns else pd.Series(0.0, index=df.index, dtype=float)
        discovery_score = self._normalize_series(df["discovery_score"]) if "discovery_score" in df.columns else pd.Series(0.0, index=df.index, dtype=float)
        completeness_score = self._normalize_series(df["data_completeness"]) if "data_completeness" in df.columns else pd.Series(0.0, index=df.index, dtype=float)

        if "keyword_length" in df.columns:
            keyword_length = pd.to_numeric(df["keyword_length"], errors="coerce")
            ideal_length_score = (1.0 - ((keyword_length - 8).abs() / 8.0)).clip(lower=0.0, upper=1.0).fillna(0.0)
        else:
            ideal_length_score = pd.Series(0.0, index=df.index, dtype=float)

        return (
            rank_score * 0.30
            + discovery_score * 0.30
            + completeness_score * 0.20
            + ideal_length_score * 0.20
        )

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        if result.empty:
            result["search_volume_score"] = pd.Series(dtype=float)
            result["trend_score"] = pd.Series(dtype=float)
            result["competition_score"] = pd.Series(dtype=float)
            result["ctr_score"] = pd.Series(dtype=float)
            result["keyword_quality_score"] = pd.Series(dtype=float)
            result["total_score"] = pd.Series(dtype=float)
            result["final_rank"] = pd.Series(dtype="Int64")
            return result

        result["search_volume_score"] = self._score_search_volume(result)
        result["trend_score"] = self._score_trend(result)
        result["competition_score"] = self._score_competition(result)
        result["ctr_score"] = self._score_ctr(result)
        result["keyword_quality_score"] = self._score_keyword_quality(result)

        weights = self.config.weights
        result["total_score"] = (
            result["search_volume_score"] * weights["search_volume"]
            + result["trend_score"] * weights["trend"]
            + result["competition_score"] * weights["competition"]
            + result["ctr_score"] * weights["ctr"]
            + result["keyword_quality_score"] * weights["keyword_quality"]
        )

        result = result.sort_values(
            by=["total_score", "search_volume_score", "trend_score", "keyword_quality_score"],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)
        result["final_rank"] = range(1, len(result) + 1)

        return result
