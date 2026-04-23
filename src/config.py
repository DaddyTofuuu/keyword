from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


CURRENT_FILE = Path(__file__).resolve()
PROJECT_DIR = CURRENT_FILE.parents[1]
WORKSPACE_DIR = PROJECT_DIR.parent
ENV_CANDIDATES = [
    PROJECT_DIR / ".env",
    WORKSPACE_DIR / ".env",
]

for env_path in ENV_CANDIDATES:
    if env_path.exists():
        load_dotenv(env_path, override=False)

ENV_PATH = next((path for path in ENV_CANDIDATES if path.exists()), ENV_CANDIDATES[0])


class Settings:
    # base paths
    ROOT_DIR = WORKSPACE_DIR
    PROJECT_DIR = PROJECT_DIR
    SRC_DIR = PROJECT_DIR / "src"

    # data paths
    DATA_DIR = PROJECT_DIR / "data"
    RAW_DIR = DATA_DIR / "raw"
    PROCESSED_DIR = DATA_DIR / "processed"

    # output paths
    OUTPUT_DIR = PROJECT_DIR / "output"
    PREDICTIONS_DIR = OUTPUT_DIR / "predictions"

    # env paths
    ENV_PATH = ENV_PATH

    # Google absolute-volume anchor
    GOOGLE_ABSOLUTE_VOLUME_CSV = os.getenv(
        "GOOGLE_ABSOLUTE_VOLUME_CSV",
        str(DATA_DIR / "raw" / "google_absolute_volume.csv"),
    )

    # Naver SearchAd API
    NAVER_SA_API_KEY = os.getenv("NAVER_SA_API_KEY")
    NAVER_SA_SECRET_KEY = os.getenv("NAVER_SA_SECRET_KEY")
    NAVER_SA_CUSTOMER_ID = os.getenv("NAVER_SA_CUSTOMER_ID")

    # Naver Open API
    NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
    NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

    @classmethod
    def ensure_directories(cls) -> None:
        directories = [
            cls.DATA_DIR,
            cls.RAW_DIR,
            cls.PROCESSED_DIR,
            cls.OUTPUT_DIR,
            cls.PREDICTIONS_DIR,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate_common_env(cls) -> None:
        missing = []

        required_env = {
            "NAVER_SA_API_KEY": cls.NAVER_SA_API_KEY,
            "NAVER_SA_SECRET_KEY": cls.NAVER_SA_SECRET_KEY,
            "NAVER_SA_CUSTOMER_ID": cls.NAVER_SA_CUSTOMER_ID,
            "NAVER_CLIENT_ID": cls.NAVER_CLIENT_ID,
            "NAVER_CLIENT_SECRET": cls.NAVER_CLIENT_SECRET,
        }

        for key, value in required_env.items():
            if not value:
                missing.append(key)

        if missing:
            raise EnvironmentError(
                "Missing required environment variables: "
                + ", ".join(missing)
            )

    @classmethod
    def print_summary(cls) -> None:
        print("[CONFIG]")
        print(f"ROOT_DIR        : {cls.ROOT_DIR}")
        print(f"PROJECT_DIR     : {cls.PROJECT_DIR}")
        print(f"ENV_PATH        : {cls.ENV_PATH}")
        print(f"RAW_DIR         : {cls.RAW_DIR}")
        print(f"PROCESSED_DIR   : {cls.PROCESSED_DIR}")
        print(f"PREDICTIONS_DIR : {cls.PREDICTIONS_DIR}")
