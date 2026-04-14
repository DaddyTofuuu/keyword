from __future__ import annotations

import pandas as pd


class KeywordMerger:
    """
    Merge candidate keywords with SearchAd and DataLab results.
    """

    STABLE_METADATA_COLUMNS = [
        "seed_keyword",
        "keyword",
        "source",
        "provider",
        "rank",
        "score_hint",
        "discovery_score",
    ]

    @staticmethod
    def _copy_dataframe(df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        return df.copy()

    @staticmethod
    def _normalize_keyword_column(
        df: pd.DataFrame,
        column_name: str = "keyword",
    ) -> pd.DataFrame:
        if df.empty or column_name not in df.columns:
            return df

        df[column_name] = df[column_name].astype(str).str.strip()
        df = df[df[column_name] != ""].copy()
        return df

    @staticmethod
    def _dedupe_by_keyword(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "keyword" not in df.columns:
            return df

        return df.drop_duplicates(subset=["keyword"], keep="first").reset_index(drop=True)

    @staticmethod
    def _to_numeric_if_present(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        for col in columns:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .replace("None", None)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _prepare_candidates(self, df_candidates: pd.DataFrame) -> pd.DataFrame:
        candidates = self._copy_dataframe(df_candidates)
        candidates = self._normalize_keyword_column(candidates, "keyword")

        if candidates.empty:
            raise ValueError("Candidate keyword DataFrame is empty.")

        candidates = self._to_numeric_if_present(
            candidates,
            ["rank", "score_hint", "discovery_score"],
        )
        candidates = self._dedupe_by_keyword(candidates)
        return candidates

    def _prepare_searchad(self, df_searchad: pd.DataFrame | None) -> pd.DataFrame:
        searchad = self._copy_dataframe(df_searchad)
        searchad = self._normalize_keyword_column(searchad, "keyword")
        searchad = self._dedupe_by_keyword(searchad)
        return searchad

    def _prepare_datalab(self, df_datalab: pd.DataFrame | None) -> pd.DataFrame:
        datalab = self._copy_dataframe(df_datalab)
        datalab = self._normalize_keyword_column(datalab, "keyword")

        if datalab.empty:
            return datalab

        if "period" in datalab.columns:
            datalab["period"] = pd.to_datetime(datalab["period"], errors="coerce")
            datalab = (
                datalab.sort_values(["keyword", "period"], na_position="last")
                .groupby("keyword", as_index=False)
                .tail(1)
                .reset_index(drop=True)
            )
            if "period" in datalab.columns:
                datalab["period"] = datalab["period"].dt.strftime("%Y-%m-%d")
        else:
            datalab = self._dedupe_by_keyword(datalab)

        return datalab

    def merge(
        self,
        df_candidates: pd.DataFrame,
        df_searchad: pd.DataFrame | None = None,
        df_datalab: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        candidates = self._prepare_candidates(df_candidates)
        searchad = self._prepare_searchad(df_searchad)
        datalab = self._prepare_datalab(df_datalab)

        merged = candidates.copy()

        if not searchad.empty:
            merged = merged.merge(
                searchad,
                on="keyword",
                how="left",
                suffixes=("", "_sa"),
            )

        if not datalab.empty:
            merged = merged.merge(
                datalab,
                on="keyword",
                how="left",
                suffixes=("", "_dl"),
            )

        metadata_columns = [
            column for column in self.STABLE_METADATA_COLUMNS if column in merged.columns
        ]
        other_columns = [
            column for column in merged.columns if column not in metadata_columns
        ]
        merged = merged.loc[:, metadata_columns + other_columns]

        if "rank" in merged.columns:
            merged = merged.sort_values("rank", na_position="last").reset_index(drop=True)

        return merged
