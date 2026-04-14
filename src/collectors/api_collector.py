from __future__ import annotations

import time
from typing import Iterable

import pandas as pd
from requests.exceptions import RequestException

from projects.keyword.src.clients.naver_datalab_client import NaverDataLabClient
from projects.keyword.src.clients.naver_sa_client import NaverSearchAdClient


class APICollector:
    """
    Collect SearchAd/DataLab results for expanded keywords.
    """

    def __init__(self, timeout: int = 20):
        self.sa_client = NaverSearchAdClient(timeout=timeout)
        self.datalab_client = NaverDataLabClient(timeout=timeout)
        self.retry_count = 4
        self.retry_sleep_sec = 1.0

    @staticmethod
    def _sanitize_searchad_keyword(keyword: str) -> str:
        # SearchAd hintKeywords uses comma as delimiter, so normalize commas inside a keyword.
        return str(keyword).replace(",", " ").strip()

    @staticmethod
    def _is_http_400(error: RequestException) -> bool:
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        return status_code == 400

    @staticmethod
    def _is_http_429(error: RequestException) -> bool:
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
        return status_code == 429

    def _compute_retry_sleep(self, attempt: int, error: RequestException | None = None) -> float:
        base_sleep = self.retry_sleep_sec * (2 ** attempt)
        if error is not None and self._is_http_429(error):
            response = getattr(error, "response", None)
            retry_after = getattr(response, "headers", {}).get("Retry-After") if response is not None else None
            if retry_after:
                try:
                    return max(float(retry_after), base_sleep)
                except ValueError:
                    pass
            return max(base_sleep, 5.0)
        return base_sleep

    def _request_searchad_chunk(self, chunk: list[str]) -> pd.DataFrame:
        hint_keywords = ",".join(chunk)
        last_error: RequestException | None = None

        for attempt in range(self.retry_count + 1):
            try:
                df = self.sa_client.get_related_keywords(
                    hint_keywords=hint_keywords,
                    show_detail=1,
                )
                if not df.empty and "keyword" not in df.columns and "relKeyword" in df.columns:
                    df = df.rename(columns={"relKeyword": "keyword"})
                return df
            except RequestException as e:
                last_error = e
                if attempt < self.retry_count:
                    time.sleep(self._compute_retry_sleep(attempt, e))

        if last_error is not None:
            raise last_error

        return pd.DataFrame()

    def _collect_searchad_chunk_with_fallback(self, chunk: list[str]) -> list[pd.DataFrame]:
        try:
            df = self._request_searchad_chunk(chunk)
            return [df] if not df.empty else []
        except RequestException as e:
            if self._is_http_400(e) and len(chunk) > 1:
                mid = len(chunk) // 2
                left = chunk[:mid]
                right = chunk[mid:]
                frames: list[pd.DataFrame] = []
                if left:
                    frames.extend(self._collect_searchad_chunk_with_fallback(left))
                if right:
                    frames.extend(self._collect_searchad_chunk_with_fallback(right))
                return frames

            if len(chunk) == 1:
                original = chunk[0]
                compact = original.replace(" ", "")
                if compact and compact != original:
                    try:
                        compact_df = self._request_searchad_chunk([compact])
                        if not compact_df.empty:
                            if "keyword" in compact_df.columns:
                                compact_df["keyword"] = original
                            return [compact_df]
                    except RequestException:
                        pass

                print(f"[APICollector][SearchAd] keyword skipped due to request error: '{chunk[0]}' | {e}")
            else:
                print(f"[APICollector][SearchAd] chunk request failed: {e}")
            return []

    @staticmethod
    def _normalize_keywords(keywords: Iterable[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for kw in keywords:
            v = APICollector._sanitize_searchad_keyword(str(kw).strip())
            if not v:
                continue
            if v in seen:
                continue
            seen.add(v)
            normalized.append(v)
        return normalized

    def collect_searchad(self, keywords: list[str]) -> pd.DataFrame:
        normalized = self._normalize_keywords(keywords)
        if not normalized:
            return pd.DataFrame()

        chunk_size = 5
        frames: list[pd.DataFrame] = []

        for i in range(0, len(normalized), chunk_size):
            chunk = normalized[i : i + chunk_size]
            frames.extend(self._collect_searchad_chunk_with_fallback(chunk))

        frames = [frame for frame in frames if not frame.empty and not frame.dropna(axis=1, how="all").empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)

    def collect_datalab(
        self,
        keywords: list[str],
        start_date: str,
        end_date: str,
        time_unit: str = "month",
    ) -> pd.DataFrame:
        normalized = self._normalize_keywords(keywords)
        frames: list[pd.DataFrame] = []

        for kw in normalized:
            df = pd.DataFrame()
            last_error: RequestException | None = None
            for attempt in range(self.retry_count + 1):
                try:
                    df = self.datalab_client.get_search_trend(
                        keyword=kw,
                        start_date=start_date,
                        end_date=end_date,
                        time_unit=time_unit,
                    )
                    last_error = None
                    break
                except RequestException as e:
                    last_error = e
                    if attempt < self.retry_count:
                        time.sleep(self._compute_retry_sleep(attempt, e))
            if last_error is not None:
                print(f"[APICollector][DataLab] keyword='{kw}' request failed: {last_error}")
                continue
            if not df.empty:
                if "ratio" in df.columns and "naver_index" not in df.columns:
                    df = df.rename(columns={"ratio": "naver_index"})
                frames.append(df)

        frames = [frame for frame in frames if not frame.empty and not frame.dropna(axis=1, how="all").empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def collect_all(
        self,
        keywords: list[str],
        start_date: str,
        end_date: str,
        time_unit: str = "month",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        sa_df = self.collect_searchad(keywords)
        dl_df = self.collect_datalab(
            keywords=keywords,
            start_date=start_date,
            end_date=end_date,
            time_unit=time_unit,
        )
        return sa_df, dl_df
