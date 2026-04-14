from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from projects.keyword.src.collectors.api_collector import APICollector
from projects.keyword.src.clients.google_anchor_client import GoogleAnchorClient
from projects.keyword.src.config import Settings
from projects.keyword.src.discovery.keyword_candidate_collector import KeywordCandidateCollector
from projects.keyword.src.processors.keyword_cleaner import KeywordCleaner
from projects.keyword.src.utils.io import save_dataframe


@dataclass(slots=True)
class PipelineRunConfig:
    top_n: int = 30
    timeout: int = 20
    save_outputs: bool = False
    output_dir: Path | None = None
    related_weight_naver: float = 0.7
    related_weight_trend: float = 0.3
    save_to_db: bool = False


@dataclass(slots=True)
class PipelineRunResult:
    seed_keyword: str
    status: str
    messages: list[str] = field(default_factory=list)
    candidates_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    related_keywords_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    autocomplete_keywords_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    related_trend_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    autocomplete_trend_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    summary_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    ranked_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    saved_paths: dict[str, Path] = field(default_factory=dict)


@dataclass(slots=True)
class ParallelPipelineResult:
    keywords: list[str]
    results: dict[str, PipelineRunResult]
    combined_ranked_df: pd.DataFrame
    combined_summary_df: pd.DataFrame
    succeeded_keywords: list[str]
    failed_keywords: list[str]


def run_parallel(
    keywords: list[str],
    config: PipelineRunConfig | None = None,
    *,
    max_workers: int | None = None,
    enrich: bool = True,
) -> ParallelPipelineResult:
    unique_keywords = list(dict.fromkeys(kw.strip() for kw in keywords if kw.strip()))
    if not unique_keywords:
        raise ValueError("keywords must contain at least one non-empty string.")

    workers = min(max_workers or len(unique_keywords), len(unique_keywords), 8)
    results: dict[str, PipelineRunResult] = {}

    def _run_one(kw: str) -> tuple[str, PipelineRunResult]:
        runner = PipelineRunner(config=config)
        try:
            return kw, runner.run(kw, enrich=enrich)
        except Exception as exc:
            return kw, PipelineRunResult(
                seed_keyword=kw,
                status="failed",
                messages=[f"Pipeline failed: {exc}"],
            )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_run_one, kw): kw for kw in unique_keywords}
        for future in as_completed(futures):
            kw, result = future.result()
            results[kw] = result

    succeeded = [kw for kw in unique_keywords if results[kw].status == "completed"]
    failed = [kw for kw in unique_keywords if results[kw].status != "completed"]

    ranked_frames: list[pd.DataFrame] = []
    summary_frames: list[pd.DataFrame] = []
    for kw in unique_keywords:
        r = results[kw]
        if not r.ranked_df.empty:
            df = r.ranked_df.copy()
            if "seed_keyword" in df.columns:
                df["seed_keyword"] = df["seed_keyword"].fillna(kw)
                ordered_columns = ["seed_keyword"] + [col for col in df.columns if col != "seed_keyword"]
                df = df.loc[:, ordered_columns]
            else:
                df.insert(0, "seed_keyword", kw)
            ranked_frames.append(df)
        if not r.summary_df.empty:
            df = r.summary_df.copy()
            if "seed_keyword" in df.columns:
                df["seed_keyword"] = df["seed_keyword"].fillna(kw)
                ordered_columns = ["seed_keyword"] + [col for col in df.columns if col != "seed_keyword"]
                df = df.loc[:, ordered_columns]
            else:
                df.insert(0, "seed_keyword", kw)
            summary_frames.append(df)

    combined_ranked_df = pd.concat(ranked_frames, ignore_index=True) if ranked_frames else pd.DataFrame()
    if not combined_ranked_df.empty and "keyword" in combined_ranked_df.columns:
        _score_col = "weighted_score" if "weighted_score" in combined_ranked_df.columns else "predicted_search_volume"
        _vol_col = "predicted_search_volume" if "predicted_search_volume" in combined_ranked_df.columns else _score_col
        combined_ranked_df = (
            combined_ranked_df
            .sort_values([_score_col, _vol_col], ascending=[False, False])
            .drop_duplicates(subset=["keyword"], keep="first")
            .reset_index(drop=True)
        )
    combined_summary_df = pd.concat(summary_frames, ignore_index=True) if summary_frames else pd.DataFrame()

    return ParallelPipelineResult(
        keywords=unique_keywords,
        results=results,
        combined_ranked_df=combined_ranked_df,
        combined_summary_df=combined_summary_df,
        succeeded_keywords=succeeded,
        failed_keywords=failed,
    )


class PipelineRunner:
    """
    Two-track keyword pipeline:
    - related keywords: Google Trends candidates + Naver SA/DataLab weighted rank
    - autocomplete keywords: Naver autocomplete candidates + Naver SA/DataLab rank
    """

    RELATED_PROVIDER = "google_trends"
    AUTOCOMPLETE_PROVIDER = "naver_autocomplete"
    WINDOW_DAYS = (1, 7, 30)
    TREND_TARGET_COLUMNS = (
        "trend_avg_1d",
        "trend_avg_7d",
        "trend_avg_30d",
        "trend_growth_1d",
        "trend_growth_7d",
        "trend_growth_30d",
    )

    def __init__(self, config: PipelineRunConfig | None = None):
        self.config = config or PipelineRunConfig()
        self.collector = KeywordCandidateCollector(top_n=self.config.top_n)
        self.cleaner = KeywordCleaner()
        self.api_collector = APICollector(timeout=self.config.timeout)
        self.google_anchor_client = GoogleAnchorClient()

    @staticmethod
    def _normalize_seed_keyword(seed_keyword: str) -> str:
        return str(seed_keyword).strip()

    @staticmethod
    def _normalize_keywords(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        work = df.copy()
        work["keyword"] = work["keyword"].astype(str).str.strip()
        work = work[work["keyword"] != ""].copy()
        return work

    @staticmethod
    def _safe_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @staticmethod
    def _minmax(series: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.dropna().empty:
            return pd.Series(0.0, index=series.index)
        min_v = numeric.min()
        max_v = numeric.max()
        if pd.isna(min_v) or pd.isna(max_v) or min_v == max_v:
            return pd.Series(1.0, index=series.index)
        return ((numeric - min_v) / (max_v - min_v)).fillna(0.0)

    @staticmethod
    def _resolve_recent_30d() -> tuple[str, str]:
        end = date.today()
        start = end - timedelta(days=29)
        return start.isoformat(), end.isoformat()

    @staticmethod
    def _build_seed_fallback(seed_keyword: str, provider: str) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "seed_keyword": seed_keyword,
                    "keyword": seed_keyword,
                    "source": provider,
                    "provider": provider,
                    "rank": 1,
                    "score_hint": pd.NA,
                }
            ]
        )

    def _resolve_output_dir(self) -> Path:
        output_dir = self.config.output_dir if self.config.output_dir is not None else Settings.OUTPUT_DIR
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _split_candidates(self, candidates_df: pd.DataFrame, seed_keyword: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        work = self._normalize_keywords(candidates_df)

        related_df = work[work["provider"] == self.RELATED_PROVIDER].copy()
        autocomplete_df = work[work["provider"] == self.AUTOCOMPLETE_PROVIDER].copy()

        related_df = self.cleaner.clean_dataframe(related_df, seed_keyword=seed_keyword)
        autocomplete_df = self.cleaner.clean_dataframe(autocomplete_df, seed_keyword=seed_keyword)

        if related_df.empty:
            related_df = self._build_seed_fallback(seed_keyword, self.RELATED_PROVIDER)
        if autocomplete_df.empty:
            autocomplete_df = self._build_seed_fallback(seed_keyword, self.AUTOCOMPLETE_PROVIDER)

        return related_df, autocomplete_df

    def _collect_enrichment(self, keywords: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
        start_date, end_date = self._resolve_recent_30d()
        return self.api_collector.collect_all(
            keywords=keywords,
            start_date=start_date,
            end_date=end_date,
            time_unit="date",
        )

    def _build_trend_metrics(self, trend_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        if trend_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        work = trend_df.copy()
        work["period"] = pd.to_datetime(work["period"], errors="coerce")
        work["naver_index"] = pd.to_numeric(work["naver_index"], errors="coerce")
        work = work.dropna(subset=["keyword", "period"]).copy()
        if work.empty:
            return pd.DataFrame(), pd.DataFrame()

        max_period = work["period"].max()
        metric_frames: list[pd.DataFrame] = []

        for window in self.WINDOW_DAYS:
            cutoff = max_period - pd.Timedelta(days=window - 1)
            subset = work[work["period"] >= cutoff].copy()
            if subset.empty:
                continue

            subset = subset.sort_values(["keyword", "period"]).reset_index(drop=True)
            grouped = subset.groupby("keyword", as_index=False)

            frame = grouped.agg(
                **{
                    f"trend_avg_{window}d": ("naver_index", "mean"),
                    f"trend_max_{window}d": ("naver_index", "max"),
                    f"trend_min_{window}d": ("naver_index", "min"),
                    f"trend_last_{window}d": ("naver_index", "last"),
                }
            )

            first_values = grouped["naver_index"].first().rename(columns={"naver_index": f"trend_first_{window}d"})
            frame = frame.merge(first_values, on="keyword", how="left")
            frame[f"trend_growth_{window}d"] = frame[f"trend_last_{window}d"] - frame[f"trend_first_{window}d"]
            metric_frames.append(frame)

        if not metric_frames:
            return pd.DataFrame(), work

        merged = metric_frames[0]
        for frame in metric_frames[1:]:
            merged = merged.merge(frame, on="keyword", how="outer")

        work["period"] = work["period"].dt.strftime("%Y-%m-%d")
        return merged, work

    def _build_inferred_trend_timeseries(
        self,
        trend_metrics_df: pd.DataFrame,
        trend_timeseries_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if trend_metrics_df.empty:
            return trend_timeseries_df

        observed = trend_timeseries_df.copy()
        observed_keywords: set[str] = set()
        if not observed.empty and "keyword" in observed.columns:
            observed_keywords = set(observed["keyword"].astype(str).tolist())

        inferred_metrics = trend_metrics_df.copy()
        if "trend_data_source" in inferred_metrics.columns:
            inferred_metrics = inferred_metrics[inferred_metrics["trend_data_source"] == "ml_inferred"].copy()
        if inferred_metrics.empty:
            return observed

        max_period = None
        if not observed.empty and "period" in observed.columns:
            max_period = pd.to_datetime(observed["period"], errors="coerce").max()
        if pd.isna(max_period) or max_period is None:
            _, end_date = self._resolve_recent_30d()
            max_period = pd.to_datetime(end_date)

        rows: list[dict[str, object]] = []
        for record in inferred_metrics.to_dict("records"):
            keyword = str(record.get("keyword", "")).strip()
            if not keyword or keyword in observed_keywords:
                continue

            first_30 = float(pd.to_numeric(record.get("trend_first_30d"), errors="coerce") or 0.0)
            first_7 = float(pd.to_numeric(record.get("trend_first_7d"), errors="coerce") or first_30)
            last_1 = pd.to_numeric(record.get("trend_last_1d"), errors="coerce")
            last_7 = pd.to_numeric(record.get("trend_last_7d"), errors="coerce")
            last_30 = pd.to_numeric(record.get("trend_last_30d"), errors="coerce")
            last_value = last_1
            if pd.isna(last_value):
                last_value = last_7
            if pd.isna(last_value):
                last_value = last_30
            if pd.isna(last_value):
                last_value = 0.0
            last_value = float(last_value)

            anchor_indices = [0, 23, 29]
            anchor_values = [max(0.0, first_30), max(0.0, first_7), max(0.0, last_value)]
            synthetic_values = np.interp(np.arange(30), anchor_indices, anchor_values)

            for offset, value in enumerate(synthetic_values):
                period = (max_period - pd.Timedelta(days=29 - offset)).strftime("%Y-%m-%d")
                rows.append(
                    {
                        "period": period,
                        "naver_index": float(max(0.0, value)),
                        "keyword": keyword,
                        "trend_data_source": "ml_inferred",
                    }
                )

        if not rows:
            return observed

        inferred_ts = pd.DataFrame(rows)
        if observed.empty:
            return inferred_ts

        combined = pd.concat([observed, inferred_ts], ignore_index=True, sort=False)
        combined = combined.drop_duplicates(subset=["keyword", "period"], keep="first").reset_index(drop=True)
        return combined

    def _build_searchad_metrics(self, searchad_df: pd.DataFrame) -> pd.DataFrame:
        if searchad_df.empty:
            return pd.DataFrame(columns=["keyword", "monthlyPcQcCnt", "monthlyMobileQcCnt", "naver_total_qc", "mobile_ratio", "pc_ratio"])

        work = searchad_df.copy()
        work = self._normalize_keywords(work)
        work = self._safe_numeric(
            work,
            [
                "monthlyPcQcCnt",
                "monthlyMobileQcCnt",
                "naver_total_qc",
                "mobile_ratio",
                "pc_ratio",
            ],
        )

        if "naver_total_qc" not in work.columns:
            pc = work["monthlyPcQcCnt"] if "monthlyPcQcCnt" in work.columns else 0
            mobile = work["monthlyMobileQcCnt"] if "monthlyMobileQcCnt" in work.columns else 0
            work["naver_total_qc"] = pc + mobile

        columns = [
            col
            for col in ["keyword", "monthlyPcQcCnt", "monthlyMobileQcCnt", "naver_total_qc", "mobile_ratio", "pc_ratio"]
            if col in work.columns
        ]
        return work.loc[:, columns].drop_duplicates(subset=["keyword"], keep="first").reset_index(drop=True)

    def _load_google_anchor_metrics(self) -> pd.DataFrame:
        return self.google_anchor_client.load()

    def _apply_google_anchor_scaling(
        self,
        ranked_df: pd.DataFrame,
        google_anchor_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int]:
        if ranked_df.empty or google_anchor_df.empty:
            return ranked_df, 0

        work = ranked_df.merge(google_anchor_df, on="keyword", how="left")
        if "google_absolute_volume" not in work.columns:
            return work, 0

        work["google_absolute_volume"] = pd.to_numeric(work["google_absolute_volume"], errors="coerce")
        eligible_mask = work["google_absolute_volume"].notna() & work.get("trend_avg_30d", pd.Series(index=work.index, dtype=float)).notna()
        eligible_mask &= pd.to_numeric(work.get("trend_avg_30d"), errors="coerce").fillna(0) > 0

        if not eligible_mask.any():
            return work, 0

        work["google_anchor_scale"] = np.nan
        work.loc[eligible_mask, "google_anchor_scale"] = (
            work.loc[eligible_mask, "google_absolute_volume"] / work.loc[eligible_mask, "trend_avg_30d"]
        )

        for window in (1, 7, 30):
            trend_col = f"trend_avg_{window}d"
            scaled_col = f"anchored_search_volume_{window}d"
            if trend_col not in work.columns:
                work[scaled_col] = np.nan
                continue
            work[scaled_col] = pd.to_numeric(work[trend_col], errors="coerce") * work["google_anchor_scale"]
            work[scaled_col] = pd.to_numeric(work[scaled_col], errors="coerce").clip(lower=0.0)

        if "anchored_search_volume_30d" in work.columns:
            anchored_mask = work["anchored_search_volume_30d"].notna()
            work["predicted_search_volume"] = np.where(
                anchored_mask,
                work["anchored_search_volume_30d"],
                pd.to_numeric(work.get("predicted_search_volume"), errors="coerce").fillna(0.0),
            )
            work["search_volume_source"] = np.where(
                anchored_mask,
                "google_anchor_scaled",
                "naver_searchad",
            )
        else:
            work["search_volume_source"] = "naver_searchad"

        work["trend_reference"] = pd.to_numeric(work.get("trend_avg_30d"), errors="coerce").fillna(0.0)
        work["naver_score_norm"] = self._minmax(pd.to_numeric(work["predicted_search_volume"], errors="coerce").fillna(0.0))
        work["trend_score_norm"] = self._minmax(work["trend_reference"])
        weight_naver = float(self.config.related_weight_naver)
        weight_trend = float(self.config.related_weight_trend)
        work["weighted_score"] = work["naver_score_norm"] * weight_naver + work["trend_score_norm"] * weight_trend
        work = work.sort_values(
            by=["weighted_score", "predicted_search_volume", "trend_reference", "rank"],
            ascending=[False, False, False, True],
            na_position="last",
        ).reset_index(drop=True)
        work["final_rank"] = range(1, len(work) + 1)

        return work, int(eligible_mask.sum())

    @staticmethod
    def _build_trend_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
        work = df.copy()

        if "keyword" not in work.columns:
            work["keyword"] = ""
        if "provider" not in work.columns:
            work["provider"] = "unknown"
        if "source" not in work.columns:
            work["source"] = "unknown"
        if "rank" not in work.columns:
            work["rank"] = 999.0
        if "score_hint" not in work.columns:
            work["score_hint"] = 0.0
        if "naver_total_qc" not in work.columns:
            work["naver_total_qc"] = 0.0
        if "mobile_ratio" not in work.columns:
            work["mobile_ratio"] = 0.0
        if "pc_ratio" not in work.columns:
            work["pc_ratio"] = 0.0

        work["rank"] = pd.to_numeric(work["rank"], errors="coerce").fillna(999.0).clip(lower=1.0)
        work["score_hint"] = pd.to_numeric(work["score_hint"], errors="coerce").fillna(0.0)
        work["naver_total_qc"] = pd.to_numeric(work["naver_total_qc"], errors="coerce").fillna(0.0).clip(lower=0.0)
        work["mobile_ratio"] = pd.to_numeric(work["mobile_ratio"], errors="coerce").fillna(0.0).clip(lower=0.0)
        work["pc_ratio"] = pd.to_numeric(work["pc_ratio"], errors="coerce").fillna(0.0).clip(lower=0.0)

        feature_df = pd.DataFrame(index=work.index)
        feature_df["log_naver_total_qc"] = np.log1p(work["naver_total_qc"])
        feature_df["mobile_ratio"] = work["mobile_ratio"]
        feature_df["pc_ratio"] = work["pc_ratio"]
        feature_df["rank_inverse"] = 1.0 / work["rank"]
        feature_df["score_hint"] = work["score_hint"]
        feature_df["keyword_length"] = work["keyword"].astype(str).str.len().astype(float)

        provider_dummies = pd.get_dummies(work["provider"].astype(str), prefix="provider", dtype=float)
        source_dummies = pd.get_dummies(work["source"].astype(str), prefix="source", dtype=float)
        feature_df = pd.concat([feature_df, provider_dummies, source_dummies], axis=1)
        return feature_df

    @staticmethod
    def _align_feature_columns(train_df: pd.DataFrame, predict_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        train = train_df.copy()
        predict = predict_df.copy()
        all_columns = sorted(set(train.columns).union(predict.columns))

        for col in all_columns:
            if col not in train.columns:
                train[col] = 0.0
            if col not in predict.columns:
                predict[col] = 0.0

        train = train.loc[:, all_columns].astype(float)
        predict = predict.loc[:, all_columns].astype(float)
        return train, predict

    @staticmethod
    def _fit_and_predict_target(
        x_train: pd.DataFrame,
        y_train: pd.Series,
        x_predict: pd.DataFrame,
    ) -> np.ndarray:
        if x_predict.empty:
            return np.array([], dtype=float)

        numeric_target = pd.to_numeric(y_train, errors="coerce")
        valid_mask = numeric_target.notna()
        if valid_mask.sum() == 0:
            return np.zeros(len(x_predict), dtype=float)

        if valid_mask.sum() < 3:
            baseline = float(numeric_target[valid_mask].median())
            return np.full(len(x_predict), baseline, dtype=float)

        x_obs = x_train.loc[valid_mask].to_numpy(dtype=float)
        y_obs = numeric_target.loc[valid_mask].to_numpy(dtype=float)
        x_pred = x_predict.to_numpy(dtype=float)

        x_obs = np.hstack([np.ones((x_obs.shape[0], 1)), x_obs])
        x_pred = np.hstack([np.ones((x_pred.shape[0], 1)), x_pred])

        beta, *_ = np.linalg.lstsq(x_obs, y_obs, rcond=None)
        return x_pred @ beta

    def _infer_missing_trend_metrics(
        self,
        candidates_df: pd.DataFrame,
        searchad_metrics_df: pd.DataFrame,
        trend_metrics_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, int]:
        base = candidates_df.copy()
        base = base.drop_duplicates(subset=["keyword"], keep="first").reset_index(drop=True)
        if base.empty:
            return trend_metrics_df, 0

        merged = base.merge(searchad_metrics_df, on="keyword", how="left")
        if trend_metrics_df.empty:
            trend_metrics_df = pd.DataFrame(columns=["keyword"])
        merged = merged.merge(trend_metrics_df, on="keyword", how="left")
        for column in self.TREND_TARGET_COLUMNS:
            if column not in merged.columns:
                merged[column] = np.nan

        observed_mask = merged.get("trend_avg_30d", pd.Series(index=merged.index, dtype=float)).notna()
        missing_mask = ~observed_mask

        observed_count = int(observed_mask.sum())
        missing_count = int(missing_mask.sum())
        if missing_count == 0 or observed_count == 0:
            augmented = trend_metrics_df.copy()
            if not augmented.empty and "trend_data_source" not in augmented.columns:
                augmented["trend_data_source"] = "naver_datalab"
            return augmented, 0

        feature_df = self._build_trend_feature_frame(merged)
        train_x = feature_df.loc[observed_mask].reset_index(drop=True)
        predict_x = feature_df.loc[missing_mask].reset_index(drop=True)
        train_x, predict_x = self._align_feature_columns(train_x, predict_x)

        inferred = merged.loc[missing_mask, ["keyword"]].reset_index(drop=True).copy()
        for column in self.TREND_TARGET_COLUMNS:
            inferred[column] = self._fit_and_predict_target(
                train_x,
                merged.loc[observed_mask, column].reset_index(drop=True),
                predict_x,
            )

        for window in self.WINDOW_DAYS:
            avg_col = f"trend_avg_{window}d"
            growth_col = f"trend_growth_{window}d"
            inferred[avg_col] = pd.to_numeric(inferred[avg_col], errors="coerce").fillna(0.0).clip(lower=0.0)
            inferred[growth_col] = pd.to_numeric(inferred[growth_col], errors="coerce").fillna(0.0)

            inferred[f"trend_last_{window}d"] = (inferred[avg_col] + (inferred[growth_col] / 2.0)).clip(lower=0.0)
            inferred[f"trend_first_{window}d"] = (inferred[f"trend_last_{window}d"] - inferred[growth_col]).clip(lower=0.0)
            inferred[f"trend_max_{window}d"] = inferred[
                [avg_col, f"trend_first_{window}d", f"trend_last_{window}d"]
            ].max(axis=1)
            inferred[f"trend_min_{window}d"] = inferred[
                [avg_col, f"trend_first_{window}d", f"trend_last_{window}d"]
            ].min(axis=1)

        inferred["trend_data_source"] = "ml_inferred"

        observed = trend_metrics_df.copy()
        if not observed.empty:
            observed["trend_data_source"] = observed.get("trend_data_source", "naver_datalab")

        augmented = pd.concat([observed, inferred], ignore_index=True, sort=False)
        augmented = augmented.drop_duplicates(subset=["keyword"], keep="first").reset_index(drop=True)
        return augmented, missing_count

    def _build_category_rank(
        self,
        category_label: str,
        candidates_df: pd.DataFrame,
        searchad_metrics_df: pd.DataFrame,
        trend_metrics_df: pd.DataFrame,
        *,
        weight_naver: float,
        weight_trend: float,
        top_n: int,
    ) -> pd.DataFrame:
        base = candidates_df.copy()
        base = base.drop_duplicates(subset=["keyword"], keep="first").reset_index(drop=True)

        if searchad_metrics_df.empty or "keyword" not in searchad_metrics_df.columns:
            searchad_metrics_df = pd.DataFrame(columns=["keyword"])
        if trend_metrics_df.empty or "keyword" not in trend_metrics_df.columns:
            trend_metrics_df = pd.DataFrame(columns=["keyword"])

        merged = base.merge(searchad_metrics_df, on="keyword", how="left")
        merged = merged.merge(trend_metrics_df, on="keyword", how="left")

        merged = self._safe_numeric(
            merged,
            [
                "monthlyPcQcCnt",
                "monthlyMobileQcCnt",
                "naver_total_qc",
                "mobile_ratio",
                "pc_ratio",
                "trend_avg_1d",
                "trend_avg_7d",
                "trend_avg_30d",
                "trend_growth_1d",
                "trend_growth_7d",
                "trend_growth_30d",
            ],
        )

        if "naver_total_qc" not in merged.columns:
            merged["naver_total_qc"] = 0.0

        merged["predicted_search_volume"] = merged["naver_total_qc"].fillna(0.0)
        merged["trend_reference"] = merged.get("trend_avg_30d", pd.Series(0.0, index=merged.index)).fillna(0.0)

        merged["naver_score_norm"] = self._minmax(merged["predicted_search_volume"])
        merged["trend_score_norm"] = self._minmax(merged["trend_reference"])
        merged["weighted_score"] = (
            merged["naver_score_norm"] * weight_naver + merged["trend_score_norm"] * weight_trend
        )

        merged = merged.sort_values(
            by=["weighted_score", "predicted_search_volume", "trend_reference", "rank"],
            ascending=[False, False, False, True],
            na_position="last",
        ).reset_index(drop=True)

        merged = merged.head(top_n).reset_index(drop=True)
        merged["final_rank"] = range(1, len(merged) + 1)
        merged["category"] = category_label

        return merged

    @staticmethod
    def _build_summary(related_df: pd.DataFrame, autocomplete_df: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for category_label, frame in (
            ("related", related_df),
            ("autocomplete", autocomplete_df),
        ):
            if frame.empty:
                rows.append(
                    {
                        "category": category_label,
                        "avg_search_volume": 0.0,
                        "top_growth_keyword_30d": "-",
                        "top_growth_value_30d": 0.0,
                    }
                )
                continue

            avg_volume = pd.to_numeric(frame.get("predicted_search_volume"), errors="coerce").fillna(0).mean()

            growth_col = "trend_growth_30d"
            if growth_col in frame.columns and frame[growth_col].notna().any():
                top_row = frame.sort_values(growth_col, ascending=False).iloc[0]
                top_keyword = str(top_row.get("keyword", "-"))
                top_growth = float(pd.to_numeric(top_row.get(growth_col), errors="coerce") or 0.0)
            else:
                top_keyword = "-"
                top_growth = 0.0

            rows.append(
                {
                    "category": category_label,
                    "avg_search_volume": float(avg_volume),
                    "top_growth_keyword_30d": top_keyword,
                    "top_growth_value_30d": top_growth,
                }
            )

        return pd.DataFrame(rows)

    def _save_stage_outputs(self, result: PipelineRunResult) -> dict[str, Path]:
        output_dir = self._resolve_output_dir()
        saved_paths: dict[str, Path] = {}

        stage_map = {
            "candidates": result.candidates_df,
            "related_keywords": result.related_keywords_df,
            "autocomplete_keywords": result.autocomplete_keywords_df,
            "related_trends": result.related_trend_df,
            "autocomplete_trends": result.autocomplete_trend_df,
            "summary": result.summary_df,
        }

        for stage_name, df in stage_map.items():
            if df.empty:
                continue
            path = output_dir / f"{stage_name}.csv"
            save_dataframe(df, path)
            saved_paths[stage_name] = path

        return saved_paths

    def run(
        self,
        seed_keyword: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        enrich: bool = True,
    ) -> PipelineRunResult:
        normalized_seed = self._normalize_seed_keyword(seed_keyword)
        if not normalized_seed:
            raise ValueError("seed_keyword must not be empty.")

        import uuid as _uuid
        from projects.keyword.src.store import KeywordStore, get_engine

        run_id = str(_uuid.uuid4())
        _store: "KeywordStore | None" = None
        if self.config.save_to_db:
            try:
                _store = KeywordStore(get_engine())
                _store.save_run(run_id, normalized_seed, {
                    "top_n": self.config.top_n,
                    "timeout": self.config.timeout,
                    "related_weight_naver": self.config.related_weight_naver,
                    "related_weight_trend": self.config.related_weight_trend,
                })
            except Exception as _db_exc:
                import warnings
                warnings.warn(f"[KeywordStore] save_run failed: {_db_exc}")
                _store = None

        result = PipelineRunResult(
            seed_keyword=normalized_seed,
            status="running",
            messages=[f"Pipeline started for '{normalized_seed}'."],
        )

        try:
            result.messages.append("Collecting candidate keywords.")
            raw_candidates_df = self.collector.collect_dataframe(normalized_seed)
            if raw_candidates_df.empty:
                raw_candidates_df = pd.concat(
                    [
                        self._build_seed_fallback(normalized_seed, self.RELATED_PROVIDER),
                        self._build_seed_fallback(normalized_seed, self.AUTOCOMPLETE_PROVIDER),
                    ],
                    ignore_index=True,
                )

            result.candidates_df = raw_candidates_df

            if _store:
                try:
                    _cands = raw_candidates_df.copy()
                    _cands["track"] = _cands["provider"].map({
                        self.RELATED_PROVIDER: "related",
                        self.AUTOCOMPLETE_PROVIDER: "autocomplete",
                    }).fillna("unknown")
                    _store.save_candidates(run_id, _cands)
                except Exception as _db_exc:
                    import warnings
                    warnings.warn(f"[KeywordStore] save_candidates failed: {_db_exc}")

            related_candidates_df, autocomplete_candidates_df = self._split_candidates(
                raw_candidates_df,
                normalized_seed,
            )

            all_keywords = (
                pd.concat(
                    [related_candidates_df["keyword"], autocomplete_candidates_df["keyword"]],
                    ignore_index=True,
                )
                .astype(str)
                .str.strip()
                .drop_duplicates()
                .tolist()
            )

            searchad_df = pd.DataFrame()
            datalab_df = pd.DataFrame()
            if enrich:
                result.messages.append("Enriching keywords with SearchAd and DataLab (recent 30 days, daily).")
                searchad_df, datalab_df = self._collect_enrichment(all_keywords)
                if _store:
                    try:
                        _store.save_sa_raw(run_id, normalized_seed, searchad_df)
                    except Exception as _db_exc:
                        import warnings
                        warnings.warn(f"[KeywordStore] save_sa_raw failed: {_db_exc}")
            else:
                result.messages.append("Skipping SearchAd/DataLab enrichment.")

            searchad_metrics_df = self._build_searchad_metrics(searchad_df)
            google_anchor_df = self._load_google_anchor_metrics()
            if not google_anchor_df.empty:
                result.messages.append(
                    f"Loaded Google absolute-volume anchors for {len(google_anchor_df)} keywords."
                )
            trend_metrics_df, trend_timeseries_df = self._build_trend_metrics(datalab_df)
            full_candidates_df = pd.concat([related_candidates_df, autocomplete_candidates_df], ignore_index=True)
            trend_metrics_df, inferred_count = self._infer_missing_trend_metrics(
                full_candidates_df,
                searchad_metrics_df,
                trend_metrics_df,
            )
            trend_timeseries_df = self._build_inferred_trend_timeseries(
                trend_metrics_df,
                trend_timeseries_df,
            )
            if inferred_count:
                result.messages.append(
                    f"Inferred recent 1d/7d/30d trend metrics for {inferred_count} keywords without DataLab timeseries."
                )

            related_ranked = self._build_category_rank(
                "related",
                related_candidates_df,
                searchad_metrics_df,
                trend_metrics_df,
                weight_naver=self.config.related_weight_naver,
                weight_trend=self.config.related_weight_trend,
                top_n=20,
            )
            related_ranked, related_anchor_count = self._apply_google_anchor_scaling(related_ranked, google_anchor_df)
            autocomplete_ranked = self._build_category_rank(
                "autocomplete",
                autocomplete_candidates_df,
                searchad_metrics_df,
                trend_metrics_df,
                weight_naver=self.config.related_weight_naver,
                weight_trend=self.config.related_weight_trend,
                top_n=20,
            )
            autocomplete_ranked, autocomplete_anchor_count = self._apply_google_anchor_scaling(
                autocomplete_ranked,
                google_anchor_df,
            )
            anchor_count = related_anchor_count + autocomplete_anchor_count
            if anchor_count:
                result.messages.append(
                    f"Scaled Naver relative trend into absolute search volume for {anchor_count} keywords using Google anchors."
                )

            if not trend_timeseries_df.empty:
                related_keys = set(related_ranked["keyword"].astype(str).tolist())
                autocomplete_keys = set(autocomplete_ranked["keyword"].astype(str).tolist())
                result.related_trend_df = trend_timeseries_df[
                    trend_timeseries_df["keyword"].astype(str).isin(related_keys)
                ].copy()
                result.autocomplete_trend_df = trend_timeseries_df[
                    trend_timeseries_df["keyword"].astype(str).isin(autocomplete_keys)
                ].copy()

            result.related_keywords_df = related_ranked
            result.autocomplete_keywords_df = autocomplete_ranked
            result.ranked_df = pd.concat([related_ranked, autocomplete_ranked], ignore_index=True)

            if _store:
                try:
                    _store.save_metrics(run_id, result.ranked_df)
                    _store.save_trend_daily(run_id, normalized_seed, result.related_trend_df, track="related")
                    _store.save_trend_daily(run_id, normalized_seed, result.autocomplete_trend_df, track="autocomplete")
                except Exception as _db_exc:
                    import warnings
                    warnings.warn(f"[KeywordStore] save_metrics/trend failed: {_db_exc}")

            result.summary_df = self._build_summary(related_ranked, autocomplete_ranked)

            if self.config.save_outputs:
                result.messages.append("Saving pipeline outputs.")
                result.saved_paths = self._save_stage_outputs(result)

            result.status = "completed"
            if _store:
                try:
                    _store.update_run_status(run_id, "success", candidate_count=len(raw_candidates_df))
                except Exception as _db_exc:
                    import warnings
                    warnings.warn(f"[KeywordStore] update_run_status failed: {_db_exc}")
            result.messages.append("Pipeline completed successfully.")
            return result

        except Exception as exc:
            result.status = "failed"
            result.messages.append(f"Pipeline failed: {exc}")
            if _store:
                try:
                    _store.update_run_status(run_id, "failed", error_message=str(exc))
                except Exception:
                    pass
            raise
