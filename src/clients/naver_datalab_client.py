from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from projects.keyword.src.config import Settings


class NaverDataLabClient:
    """
    Naver DataLab Search Trend API client

    주요 역할:
    - 검색어 트렌드 조회
    - 기간별 ratio(상대 검색 지수) 수집
    - device / gender / ages 조건 확장 가능
    """

    BASE_URL = "https://openapi.naver.com/v1/datalab/search"

    def __init__(self, timeout: int = 20):
        self.client_id = Settings.NAVER_CLIENT_ID
        self.client_secret = Settings.NAVER_CLIENT_SECRET
        self.timeout = timeout
        self.session = requests.Session()

    def _build_headers(self) -> Dict[str, str]:
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Content-Type": "application/json",
        }

    def post(self, body: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.post(
            self.BASE_URL,
            headers=self._build_headers(),
            json=body,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_search_trend(
        self,
        keyword: str,
        start_date: str,
        end_date: str,
        time_unit: str = "month",
        device: Optional[str] = None,
        gender: Optional[str] = None,
        ages: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        body: Dict[str, Any] = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "keywordGroups": [
                {
                    "groupName": keyword,
                    "keywords": [keyword],
                }
            ],
        }

        if device:
            body["device"] = device
        if gender:
            body["gender"] = gender
        if ages:
            body["ages"] = ages

        data = self.post(body)
        results = data.get("results", [])

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results[0].get("data", []))
        if df.empty:
            return df

        df["keyword"] = keyword
        if "ratio" in df.columns:
            df["ratio"] = pd.to_numeric(df["ratio"], errors="coerce")

        return df


if __name__ == "__main__":
    client = NaverDataLabClient()

    try:
        df = client.get_search_trend(
            keyword="비뇨기과",
            start_date="2025-04-01",
            end_date="2026-03-31",
            time_unit="month",
        )

        print("=== DataLab Result ===")
        print(df.tail(12))

        if not df.empty:
            print("\n=== Columns ===")
            print(df.columns.tolist())

    except Exception as e:
        print("오류:", e)