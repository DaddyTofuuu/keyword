from __future__ import annotations

from pathlib import Path

import pandas as pd

from projects.keyword.src.config import Settings


class GoogleAnchorClient:
    """
    Load Google absolute keyword volumes from a local CSV file.

    Expected columns:
    - keyword
    - google_absolute_volume
    Optional aliases:
    - avg_monthly_searches
    - google_search_volume
    - absolute_volume
    """

    VOLUME_ALIASES = (
        "google_absolute_volume",
        "avg_monthly_searches",
        "google_search_volume",
        "absolute_volume",
    )

    def __init__(self, csv_path: str | None = None):
        self.csv_path = Path(csv_path or Settings.GOOGLE_ABSOLUTE_VOLUME_CSV)

    def load(self) -> pd.DataFrame:
        if not self.csv_path.exists():
            return pd.DataFrame(columns=["keyword", "google_absolute_volume"])

        df = pd.read_csv(self.csv_path, encoding="utf-8-sig")
        if df.empty:
            return pd.DataFrame(columns=["keyword", "google_absolute_volume"])

        work = df.copy()
        if "keyword" not in work.columns:
            return pd.DataFrame(columns=["keyword", "google_absolute_volume"])

        volume_column = next((col for col in self.VOLUME_ALIASES if col in work.columns), None)
        if volume_column is None:
            return pd.DataFrame(columns=["keyword", "google_absolute_volume"])

        work["keyword"] = work["keyword"].astype(str).str.strip()
        work["google_absolute_volume"] = pd.to_numeric(work[volume_column], errors="coerce")
        work = work.dropna(subset=["keyword", "google_absolute_volume"]).copy()
        work = work[work["keyword"] != ""].copy()

        return work.loc[:, ["keyword", "google_absolute_volume"]].drop_duplicates(
            subset=["keyword"],
            keep="first",
        ).reset_index(drop=True)
