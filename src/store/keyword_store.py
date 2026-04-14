from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_engine(database: str = "keywords") -> Engine:
    """Build a SQLAlchemy engine from MYSQL_* environment variables.

    The ``database`` parameter lets callers override the schema name; it
    defaults to ``keywords`` (the keyword warehouse schema).
    """
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    db = os.getenv("KEYWORD_MYSQL_DATABASE") or os.getenv("MYSQL_DB") or database

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)


# camelCase SA API field → snake_case DB column
_SA_COLUMN_MAP: dict[str, str] = {
    "monthlyPcQcCnt":       "monthly_pc_qc_cnt",
    "monthlyMobileQcCnt":   "monthly_mobile_qc_cnt",
    "naver_total_qc":       "monthly_total_qc",
    "monthlyAvePcClkCnt":   "monthly_avg_pc_clk",
    "monthlyAveMobileClkCnt": "monthly_avg_mobile_clk",
    "monthlyAvePcCtr":      "monthly_avg_pc_ctr",
    "monthlyAveMobileCtr":  "monthly_avg_mobile_ctr",
    "plAvgDepth":           "pl_avg_depth",
    "compIdx":              "comp_idx",
    "mobile_ratio":         "mobile_ratio",
    "pc_ratio":             "pc_ratio",
}

# ranked_df DataFrame column → keyword_metrics DB column
_METRICS_COLUMN_MAP: dict[str, str] = {
    "monthlyPcQcCnt":            "monthly_pc_qc_cnt",
    "monthlyMobileQcCnt":        "monthly_mobile_qc_cnt",
    "naver_total_qc":            "monthly_total_qc",
    "mobile_ratio":              "mobile_ratio",
    "pc_ratio":                  "pc_ratio",
    "rank":                      "discovery_rank",
    "category":                  "track",
    "anchored_search_volume_1d":  "anchored_search_volume_1d",
    "anchored_search_volume_7d":  "anchored_search_volume_7d",
    "anchored_search_volume_30d": "anchored_search_volume_30d",
}

# DB columns in keyword_metrics that pass through unchanged from ranked_df
_METRICS_PASSTHROUGH = {
    "track", "provider", "discovery_rank",
    "monthly_pc_qc_cnt", "monthly_mobile_qc_cnt", "monthly_total_qc",
    "mobile_ratio", "pc_ratio",
    "trend_avg_1d",  "trend_max_1d",  "trend_min_1d",  "trend_first_1d",  "trend_last_1d",  "trend_growth_1d",
    "trend_avg_7d",  "trend_max_7d",  "trend_min_7d",  "trend_first_7d",  "trend_last_7d",  "trend_growth_7d",
    "trend_avg_30d", "trend_max_30d", "trend_min_30d", "trend_first_30d", "trend_last_30d", "trend_growth_30d",
    "trend_data_source",
    "google_absolute_volume", "google_anchor_scale",
    "anchored_search_volume_1d", "anchored_search_volume_7d", "anchored_search_volume_30d",
    "search_volume_source",
    "predicted_search_volume", "naver_score_norm", "trend_score_norm",
    "weighted_score", "final_rank",
}


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def _scalar(value: Any) -> Any:
    """Convert numpy scalars / NA to plain Python types."""
    if pd.isna(value) if not isinstance(value, (list, dict)) else False:
        return None
    try:
        import numpy as np  # noqa: PLC0415
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return None if pd.isna(value) else float(value)
        if isinstance(value, (np.bool_,)):
            return bool(value)
    except ImportError:
        pass
    return value


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to list of dicts with Python-native types."""
    records = df.where(df.notna(), other=None).to_dict("records")
    return [
        {k: _scalar(v) for k, v in row.items()}
        for row in records
    ]


def _quote_identifier(identifier: str) -> str:
    """Quote a MySQL identifier for dynamically generated SQL."""
    escaped = str(identifier).replace("`", "``")
    return f"`{escaped}`"


class KeywordStore:
    """
    MySQL persistence layer for the keyword pipeline.
    Targets the `keywords` schema (see sql/keyword_warehouse.sql).

    Usage::
        from sqlalchemy import create_engine
        engine = create_engine("mysql+pymysql://user:pw@host/keywords?charset=utf8mb4")
        store = KeywordStore(engine)
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------ #
    # pipeline_runs
    # ------------------------------------------------------------------ #

    def save_run(
        self,
        run_id: str,
        seed_keyword: str,
        config: dict,
    ) -> None:
        """INSERT into pipeline_runs with status='running', started_at=now."""
        stmt = text(
            """
            INSERT INTO pipeline_runs
                (run_id, seed_keyword, started_at, status, config)
            VALUES
                (:run_id, :seed_keyword, :started_at, 'running', :config)
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {
                "run_id":       run_id,
                "seed_keyword": seed_keyword,
                "started_at":   _now_utc(),
                "config":       json.dumps(config, ensure_ascii=False, default=str),
            })

    def update_run_status(
        self,
        run_id: str,
        status: str,
        candidate_count: int | None = None,
        error_message: str | None = None,
    ) -> None:
        """UPDATE pipeline_runs.status / finished_at / candidate_count / error_message."""
        stmt = text(
            """
            UPDATE pipeline_runs
            SET
                status          = :status,
                finished_at     = :finished_at,
                candidate_count = COALESCE(:candidate_count, candidate_count),
                error_message   = COALESCE(:error_message,   error_message)
            WHERE run_id = :run_id
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {
                "run_id":          run_id,
                "status":          status,
                "finished_at":     _now_utc(),
                "candidate_count": candidate_count,
                "error_message":   error_message,
            })

    # ------------------------------------------------------------------ #
    # keyword_candidates
    # ------------------------------------------------------------------ #

    def save_candidates(
        self,
        run_id: str,
        candidates_df: pd.DataFrame,
    ) -> None:
        """INSERT keyword_candidates rows from candidates_df."""
        if candidates_df.empty:
            return

        df = candidates_df.copy()
        df["run_id"] = run_id

        db_columns = [
            "run_id", "seed_keyword", "keyword", "track",
            "provider", "rank", "score_hint", "discovery_score",
        ]
        # keep only columns that exist in the DataFrame
        present = [c for c in db_columns if c in df.columns]
        df = df[present]
        quoted_columns = ", ".join(_quote_identifier(column) for column in present)

        stmt = text(
            f"""
            INSERT INTO keyword_candidates
                ({quoted_columns})
            VALUES
                ({', '.join(':' + c for c in present)})
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, _df_to_records(df))

    # ------------------------------------------------------------------ #
    # keyword_sa_raw
    # ------------------------------------------------------------------ #

    def save_sa_raw(
        self,
        run_id: str,
        seed_keyword: str,
        searchad_df: pd.DataFrame,
    ) -> None:
        """INSERT keyword_sa_raw rows from the full SA API response DataFrame."""
        if searchad_df.empty:
            return

        df = searchad_df.copy()

        # rename camelCase → snake_case
        df = df.rename(columns=_SA_COLUMN_MAP)

        df["run_id"] = run_id
        df["seed_keyword"] = seed_keyword

        db_columns = [
            "run_id", "seed_keyword", "keyword",
            "monthly_pc_qc_cnt", "monthly_mobile_qc_cnt", "monthly_total_qc",
            "monthly_avg_pc_clk", "monthly_avg_mobile_clk",
            "monthly_avg_pc_ctr", "monthly_avg_mobile_ctr",
            "pl_avg_depth", "comp_idx", "mobile_ratio", "pc_ratio",
        ]
        present = [c for c in db_columns if c in df.columns]
        df = df[present]
        quoted_columns = ", ".join(_quote_identifier(column) for column in present)

        stmt = text(
            f"""
            INSERT INTO keyword_sa_raw
                ({quoted_columns})
            VALUES
                ({', '.join(':' + c for c in present)})
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, _df_to_records(df))

    # ------------------------------------------------------------------ #
    # keyword_metrics
    # ------------------------------------------------------------------ #

    def save_metrics(
        self,
        run_id: str,
        ranked_df: pd.DataFrame,
    ) -> None:
        """INSERT keyword_metrics rows from final ranked_df."""
        if ranked_df.empty:
            return

        df = ranked_df.copy()

        # ranked_df uses 'category' for track; normalise to 'track'
        if "track" not in df.columns and "category" in df.columns:
            df["track"] = df["category"]

        # 'rank' → 'discovery_rank'
        if "discovery_rank" not in df.columns and "rank" in df.columns:
            df["discovery_rank"] = df["rank"]

        # camelCase SA fields → snake_case
        df = df.rename(columns={
            "monthlyPcQcCnt":   "monthly_pc_qc_cnt",
            "monthlyMobileQcCnt": "monthly_mobile_qc_cnt",
            "naver_total_qc":   "monthly_total_qc",
        })

        df["run_id"] = run_id

        # seed_keyword: use column if present, else will be missing (safe — it's added below)
        if "seed_keyword" not in df.columns:
            # ranked_df doesn't always carry seed_keyword; caller should add it
            df["seed_keyword"] = ""

        db_columns = [
            "run_id", "seed_keyword", "keyword", "track", "provider", "discovery_rank",
            "monthly_pc_qc_cnt", "monthly_mobile_qc_cnt", "monthly_total_qc",
            "mobile_ratio", "pc_ratio",
            "trend_avg_1d",  "trend_max_1d",  "trend_min_1d",  "trend_first_1d",  "trend_last_1d",  "trend_growth_1d",
            "trend_avg_7d",  "trend_max_7d",  "trend_min_7d",  "trend_first_7d",  "trend_last_7d",  "trend_growth_7d",
            "trend_avg_30d", "trend_max_30d", "trend_min_30d", "trend_first_30d", "trend_last_30d", "trend_growth_30d",
            "trend_data_source",
            "google_absolute_volume", "google_anchor_scale",
            "anchored_search_volume_1d", "anchored_search_volume_7d", "anchored_search_volume_30d",
            "search_volume_source",
            "predicted_search_volume", "naver_score_norm", "trend_score_norm",
            "weighted_score", "final_rank",
        ]
        present = [c for c in db_columns if c in df.columns]
        df = df[present]
        quoted_columns = ", ".join(_quote_identifier(column) for column in present)

        stmt = text(
            f"""
            INSERT INTO keyword_metrics
                ({quoted_columns})
            VALUES
                ({', '.join(':' + c for c in present)})
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, _df_to_records(df))

    # ------------------------------------------------------------------ #
    # keyword_trend_daily
    # ------------------------------------------------------------------ #

    def save_trend_daily(
        self,
        run_id: str,
        seed_keyword: str,
        trend_df: pd.DataFrame,
        track: str,
    ) -> None:
        """INSERT keyword_trend_daily rows from trend timeseries DataFrame."""
        if trend_df.empty:
            return

        df = trend_df.copy()
        df["run_id"] = run_id
        df["seed_keyword"] = seed_keyword
        df["track"] = track

        # period (YYYY-MM-DD string) → trend_date
        if "trend_date" not in df.columns and "period" in df.columns:
            df["trend_date"] = df["period"]

        # naver_index → trend_index
        if "trend_index" not in df.columns and "naver_index" in df.columns:
            df["trend_index"] = df["naver_index"]

        # data_source: prefer trend_data_source if available
        if "data_source" not in df.columns:
            if "trend_data_source" in df.columns:
                df["data_source"] = df["trend_data_source"]
            else:
                df["data_source"] = "naver_datalab"
        df["data_source"] = (
            df["data_source"]
            .replace("", None)
            .fillna("naver_datalab")
        )

        db_columns = [
            "run_id", "seed_keyword", "keyword", "track",
            "trend_date", "trend_index", "data_source",
        ]
        present = [c for c in db_columns if c in df.columns]
        df = df[present]
        dedupe_keys = [c for c in ("keyword", "track", "trend_date") if c in df.columns]
        if dedupe_keys:
            df = df.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)
        quoted_columns = ", ".join(_quote_identifier(column) for column in present)

        stmt = text(
            f"""
            INSERT INTO keyword_trend_daily
                ({quoted_columns})
            VALUES
                ({', '.join(':' + c for c in present)})
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, _df_to_records(df))
