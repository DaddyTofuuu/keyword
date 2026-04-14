from __future__ import annotations

from typing import Any

import pandas as pd
import requests
from requests import Response
from requests.exceptions import RequestException

from projects.keyword.src.discovery.base import BaseKeywordDiscoveryProvider


class NaverAutocompleteProvider(BaseKeywordDiscoveryProvider):
    """
    Collect keyword candidates from Naver's autocomplete endpoint.
    """

    provider_name = "naver_autocomplete"
    source_name = "naver_autocomplete"
    AUTOCOMPLETE_URL = "https://ac.search.naver.com/nx/ac"

    def __init__(self, top_n: int = 20, timeout: int = 10):
        super().__init__(top_n=top_n)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://search.naver.com/",
            }
        )

    def _build_params(self, seed_keyword: str) -> dict[str, Any]:
        return {
            "q": seed_keyword,
            "con": 1,
            "frm": "nx",
            "ans": 2,
            "r_format": "json",
            "r_enc": "UTF-8",
            "r_unicode": 0,
            "t_koreng": 1,
            "run": 2,
            "rev": 4,
            "q_enc": "UTF-8",
            "st": 100,
        }

    def _request(self, seed_keyword: str) -> dict[str, Any] | list[Any] | None:
        try:
            response = self.session.get(
                self.AUTOCOMPLETE_URL,
                params=self._build_params(seed_keyword),
                timeout=self.timeout,
            )
            response.raise_for_status()
            return self._parse_json(response)
        except (RequestException, ValueError):
            return None

    @staticmethod
    def _parse_json(response: Response) -> dict[str, Any] | list[Any]:
        payload = response.text.strip()

        if payload.startswith("_callback(") and payload.endswith(")"):
            payload = payload[len("_callback(") : -1]

        return response.json() if payload == response.text.strip() else requests.models.complexjson.loads(payload)

    def _extract_keywords(self, data: dict[str, Any] | list[Any]) -> list[str]:
        if isinstance(data, dict):
            items = data.get("items", [])
            keywords: list[str] = []

            for group in items:
                if not isinstance(group, list):
                    continue
                for item in group:
                    keyword = self._extract_keyword_from_item(item)
                    if keyword:
                        keywords.append(keyword)

            return keywords

        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
            keywords = []
            for item in data[1]:
                keyword = self._extract_keyword_from_item(item)
                if keyword:
                    keywords.append(keyword)
            return keywords

        return []

    def _extract_keyword_from_item(self, item: Any) -> str:
        if isinstance(item, str):
            return self.normalize_keyword(item)

        if isinstance(item, list) and item:
            return self.normalize_keyword(item[0])

        return ""

    def collect_keywords(self, seed_keyword: str) -> list[str]:
        normalized_seed = self.normalize_keyword(seed_keyword)
        if not normalized_seed:
            return []

        data = self._request(normalized_seed)
        if data is None:
            return []

        keywords = self._extract_keywords(data)
        keywords = [self.normalize_keyword(keyword) for keyword in keywords]
        keywords = [keyword for keyword in keywords if keyword]

        deduped: list[str] = []
        seen: set[str] = set()

        for keyword in keywords:
            if keyword in seen:
                continue
            seen.add(keyword)
            deduped.append(keyword)

        return deduped[: self.top_n]

    def collect(self, seed_keyword: str) -> pd.DataFrame:
        keywords = self.collect_keywords(seed_keyword)
        return self.build_rows(
            seed_keyword=seed_keyword,
            keywords=keywords,
        )
