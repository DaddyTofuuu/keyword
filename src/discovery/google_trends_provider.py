from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from requests.exceptions import RequestException

from projects.keyword.src.discovery.base import BaseKeywordDiscoveryProvider


class GoogleTrendsProvider(BaseKeywordDiscoveryProvider):
    """
    Collect keyword candidates from Google Trends autocomplete endpoint.
    """

    provider_name = "google_trends"
    source_name = "google_trends"
    AUTOCOMPLETE_URL = "https://trends.google.com/trends/api/autocomplete/{keyword}"

    def __init__(self, top_n: int = 20, timeout: int = 10, language: str = "ko"):
        super().__init__(top_n=top_n)
        self.timeout = timeout
        self.language = language
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
            }
        )

    def _build_url(self, seed_keyword: str) -> str:
        return self.AUTOCOMPLETE_URL.format(keyword=requests.utils.quote(seed_keyword))

    def _build_params(self) -> dict[str, Any]:
        return {
            "hl": self.language,
        }

    @staticmethod
    def _strip_xssi_prefix(payload: str) -> str:
        normalized = payload.strip()
        xssi_prefix = ")]}',"
        if normalized.startswith(xssi_prefix):
            return normalized[len(xssi_prefix) :].strip()
        return normalized

    def _request(self, seed_keyword: str) -> dict[str, Any] | None:
        try:
            response = self.session.get(
                self._build_url(seed_keyword),
                params=self._build_params(),
                timeout=self.timeout,
            )
            response.raise_for_status()
            cleaned = self._strip_xssi_prefix(response.text)
            return requests.models.complexjson.loads(cleaned)
        except (RequestException, ValueError):
            return None

    def _extract_keywords(self, payload: dict[str, Any]) -> list[str]:
        default = payload.get("default")
        if not isinstance(default, dict):
            return []

        extracted: list[str] = []
        for key in ("topics", "queries"):
            items = default.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                candidate = item.get("title") or item.get("query")
                normalized = self.normalize_keyword(candidate)
                if normalized:
                    extracted.append(normalized)

        deduped: list[str] = []
        seen: set[str] = set()
        for keyword in extracted:
            if keyword in seen:
                continue
            seen.add(keyword)
            deduped.append(keyword)
        return deduped[: self.top_n]

    def collect_keywords(self, seed_keyword: str) -> list[str]:
        normalized_seed = self.normalize_keyword(seed_keyword)
        if not normalized_seed:
            return []

        payload = self._request(normalized_seed)
        if not payload:
            return []

        keywords = self._extract_keywords(payload)
        return [keyword for keyword in keywords if keyword != normalized_seed]

    def collect(self, seed_keyword: str) -> pd.DataFrame:
        keywords = self.collect_keywords(seed_keyword)
        return self.build_rows(
            seed_keyword=seed_keyword,
            keywords=keywords,
        )

