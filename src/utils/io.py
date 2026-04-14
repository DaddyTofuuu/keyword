from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
from pandas.errors import EmptyDataError


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def save_dataframe(
    df: pd.DataFrame,
    path: Path,
    encoding: str = "utf-8-sig",
) -> None:
    ensure_parent_dir(path)
    df.to_csv(path, index=False, encoding=encoding)


def load_dataframe(
    path: Path,
    encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    try:
        return pd.read_csv(path, encoding=encoding)
    except EmptyDataError:
        return pd.DataFrame()


def save_keywords_list(
    keywords: list[str],
    path: Path,
    seed_keyword: str,
    source: str = "seed_fallback",
) -> pd.DataFrame:
    rows = []
    for idx, keyword in enumerate(keywords, start=1):
        rows.append(
            {
                "seed_keyword": seed_keyword,
                "keyword": keyword,
                "source": source,
                "rank": idx,
            }
        )

    df = pd.DataFrame(rows)
    save_dataframe(df, path)
    return df


def append_log_line(
    path: Path,
    line: str,
    encoding: str = "utf-8",
) -> None:
    ensure_parent_dir(path)
    with open(path, "a", encoding=encoding) as f:
        f.write(line.rstrip() + "\n")


def save_text(
    text: str,
    path: Path,
    encoding: str = "utf-8",
) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding=encoding) as f:
        f.write(text)


def load_text(
    path: Path,
    encoding: str = "utf-8",
) -> str:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    with open(path, "r", encoding=encoding) as f:
        return f.read()


def save_iterable_lines(
    items: Iterable[str],
    path: Path,
    encoding: str = "utf-8",
) -> None:
    ensure_parent_dir(path)
    with open(path, "w", encoding=encoding) as f:
        for item in items:
            f.write(str(item).rstrip() + "\n")
