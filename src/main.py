from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_streamlit(port: int) -> int:
    streamlit_app_path = PROJECT_ROOT / "streamlit_app.py"
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(streamlit_app_path),
        "--server.address",
        "0.0.0.0",
        "--server.port",
        str(port),
    ]
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    return int(completed.returncode)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Keyword project Streamlit launcher")
    parser.add_argument("--port", type=int, default=8502)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_streamlit(port=args.port)


if __name__ == "__main__":
    raise SystemExit(main())
