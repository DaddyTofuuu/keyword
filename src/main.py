from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import sys

def run_streamlit(port: int) -> int:
    streamlit_app_path = Path("projects/keyword/streamlit_app.py")
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(streamlit_app_path),
        "--server.address",
        "0.0.0.0",
        "--server.port",
        "8502",
        str(port),
    ]
    completed = subprocess.run(command, check=False)
    return int(completed.returncode)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keyword project Streamlit launcher")
    parser.add_argument("--port", type=int, default=8501)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_streamlit(port=args.port)


if __name__ == "__main__":
    raise SystemExit(main())
