from __future__ import annotations

import numpy as np
import pandas as pd


class FeatureEngineer:
    """
    Build deterministic ranking-oriented features for keyword prioritization.
    """

    NUMERIC_COLUMNS = [
        "rank",
        "score_hint",
        "discovery_score",
        "monthlyPcQcCnt",
        "monthlyMobileQcCnt",
        "monthlyAvePcClkCnt",
        "monthlyAveMobileClkCnt",
        "monthlyAvePcCtr",
        "monthlyAveMobileCtr",
        "plAvgDepth",
        "compIdx",
        "naver_index",
    ]

    @staticmethod
    def _to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def _add_keyword_length(df: pd.DataFrame) -> pd.DataFrame:
        if "keyword" in df.columns:
            df["keyword_length"] = df["keyword"].astype(str).str.len()
            df["keyword_word_count"] = df["keyword"].astype(str).str.split().str.len()
        else:
            df["keyword_length"] = np.nan
            df["keyword_word_count"] = np.nan
        return df

    @staticmethod
    def _add_search_volume_features(df: pd.DataFrame) -> pd.DataFrame:
        has_pc = "monthlyPcQcCnt" in df.columns
        has_mobile = "monthlyMobileQcCnt" in df.columns

        pc = df["monthlyPcQcCnt"].fillna(0) if has_pc else pd.Series(0, index=df.index)
        mobile = df["monthlyMobileQcCnt"].fillna(0) if has_mobile else pd.Series(0, index=df.index)

        if has_pc or has_mobile:
            df["naver_total_qc"] = pc + mobile
            df["predicted_search_volume"] = df["naver_total_qc"]
            df["has_search_volume"] = (df["naver_total_qc"] > 0).astype(int)
            df["log_search_volume"] = np.log1p(df["naver_total_qc"])
        else:
            df["naver_total_qc"] = np.nan
            df["predicted_search_volume"] = np.nan
            df["has_search_volume"] = 0
            df["log_search_volume"] = np.nan

        return df

    @staticmethod
    def _add_device_ratio_features(df: pd.DataFrame) -> pd.DataFrame:
        if {"monthlyPcQcCnt", "monthlyMobileQcCnt"}.issubset(df.columns):
            total = df["monthlyPcQcCnt"].fillna(0) + df["monthlyMobileQcCnt"].fillna(0)
            safe_total = total.replace(0, np.nan)

            df["mobile_ratio"] = df["monthlyMobileQcCnt"].fillna(0) / safe_total
            df["pc_ratio"] = df["monthlyPcQcCnt"].fillna(0) / safe_total
            df["device_ratio_gap"] = (df["mobile_ratio"] - df["pc_ratio"]).abs()
        else:
            df["mobile_ratio"] = np.nan
            df["pc_ratio"] = np.nan
            df["device_ratio_gap"] = np.nan

        return df

    @staticmethod
    def _add_trend_features(df: pd.DataFrame) -> pd.DataFrame:
        if "naver_index" in df.columns:
            df["trend_signal"] = df["naver_index"]
            df["log_trend_signal"] = np.log1p(df["naver_index"].clip(lower=0))
            df["has_trend_data"] = df["naver_index"].notna().astype(int)
        else:
            df["trend_signal"] = np.nan
            df["log_trend_signal"] = np.nan
            df["has_trend_data"] = 0

        return df

    @staticmethod
    def _add_ctr_features(df: pd.DataFrame) -> pd.DataFrame:
        if {"monthlyAvePcCtr", "monthlyAveMobileCtr"}.issubset(df.columns):
            df["ctr_mean"] = df[["monthlyAvePcCtr", "monthlyAveMobileCtr"]].mean(axis=1)
            df["ctr_gap"] = (
                df["monthlyAveMobileCtr"].fillna(0) - df["monthlyAvePcCtr"].fillna(0)
            ).abs()
        else:
            df["ctr_mean"] = np.nan
            df["ctr_gap"] = np.nan

        if {"monthlyAvePcClkCnt", "monthlyAveMobileClkCnt"}.issubset(df.columns):
            df["click_total"] = (
                df["monthlyAvePcClkCnt"].fillna(0) + df["monthlyAveMobileClkCnt"].fillna(0)
            )
            df["log_click_total"] = np.log1p(df["click_total"])
        else:
            df["click_total"] = np.nan
            df["log_click_total"] = np.nan

        return df

    @staticmethod
    def _add_source_features(df: pd.DataFrame) -> pd.DataFrame:
        if "provider" in df.columns:
            provider_counts = (
                df["provider"]
                .fillna("")
                .astype(str)
                .str.split("|")
                .str.len()
            )
            df["provider_diversity"] = provider_counts.replace(0, 1)
        else:
            df["provider_diversity"] = np.nan

        if "source" in df.columns:
            source_counts = (
                df["source"]
                .fillna("")
                .astype(str)
                .str.split("|")
                .str.len()
            )
            df["source_diversity"] = source_counts.replace(0, 1)
        else:
            df["source_diversity"] = np.nan

        return df

    @staticmethod
    def _add_rank_features(df: pd.DataFrame) -> pd.DataFrame:
        if "rank" in df.columns:
            safe_rank = df["rank"].fillna(999).clip(lower=1)
            df["rank_inverse"] = 1 / safe_rank
        else:
            df["rank_inverse"] = np.nan

        return df

    @staticmethod
    def _add_data_completeness(df: pd.DataFrame) -> pd.DataFrame:
        signals = pd.DataFrame(index=df.index)

        signals["has_discovery_score"] = df["discovery_score"].notna().astype(int) if "discovery_score" in df.columns else 0
        signals["has_search_volume"] = df["naver_total_qc"].notna().astype(int) if "naver_total_qc" in df.columns else 0
        signals["has_trend_signal"] = df["trend_signal"].notna().astype(int) if "trend_signal" in df.columns else 0
        signals["has_ctr_mean"] = df["ctr_mean"].notna().astype(int) if "ctr_mean" in df.columns else 0

        df["data_completeness"] = signals.sum(axis=1)
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        work_df = df.copy()

        work_df = self._to_numeric(work_df, self.NUMERIC_COLUMNS)
        work_df = self._add_keyword_length(work_df)
        work_df = self._add_search_volume_features(work_df)
        work_df = self._add_device_ratio_features(work_df)
        work_df = self._add_trend_features(work_df)
        work_df = self._add_ctr_features(work_df)
        work_df = self._add_source_features(work_df)
        work_df = self._add_rank_features(work_df)
        work_df = self._add_data_completeness(work_df)

        return work_df
