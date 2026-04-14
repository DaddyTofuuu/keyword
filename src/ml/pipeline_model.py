from __future__ import annotations

import numpy as np
import pandas as pd


class KeywordScoringModel:
    """
    Min-Max 기반 Keyword Ranking Model
    """

    def __init__(self):
        # 🔥 weight (튜닝 대상)
        self.weights = {
            "volume": 0.6,
            "trend": 0.2,
            "ratio": 0.1,
            "rank": 0.1,
        }

    # =========================
    # 🔥 Min-Max Scaling
    # =========================
    @staticmethod
    def _minmax(series: pd.Series) -> pd.Series:
        min_val = series.min()
        max_val = series.max()

        if pd.isna(min_val) or pd.isna(max_val) or min_val == max_val:
            return pd.Series(0.0, index=series.index)

        return (series - min_val) / (max_val - min_val + 1e-9)

    # =========================
    # 🔥 scoring
    # =========================
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        result_df = df.copy()

        # -------------------------
        # 기본 feature
        # -------------------------
        volume = result_df["naver_total_qc"].fillna(0)
        trend = result_df["naver_index"].fillna(0)
        ratio = result_df["mobile_ratio"].fillna(0)
        rank = result_df["rank"].fillna(999)

        # -------------------------
        # 🔥 Min-Max 정규화
        # -------------------------
        vol_norm = self._minmax(volume)
        trend_norm = self._minmax(trend)
        ratio_norm = self._minmax(ratio)

        # rank는 낮을수록 좋음 → invert
        rank_norm = 1 - self._minmax(rank)

        # -------------------------
        # 🔥 scoring (핵심)
        # -------------------------
        result_df["score"] = (
            vol_norm * self.weights["volume"]
            + trend_norm * self.weights["trend"]
            + ratio_norm * self.weights["ratio"]
            + rank_norm * self.weights["rank"]
        )

        # -------------------------
        # 🔥 confidence weighting
        # -------------------------
        if "confidence" in result_df.columns:
            weight_map = {
                "A": 1.0,
                "B": 0.85,
                "C": 0.7,
            }

            result_df["confidence_weight"] = (
                result_df["confidence"]
                .map(weight_map)
                .fillna(0.7)
            )

            result_df["final_score"] = result_df["score"] * result_df["confidence_weight"]
        else:
            result_df["final_score"] = result_df["score"]

        # -------------------------
        # 🔥 ranking
        # -------------------------
        result_df = result_df.sort_values(
            by="final_score",
            ascending=False
        ).reset_index(drop=True)

        return result_df