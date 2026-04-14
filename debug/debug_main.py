from __future__ import annotations

from projects.keyword.src.services.pipeline_runner import PipelineRunConfig, PipelineRunner


def main() -> None:
    runner = PipelineRunner(
        PipelineRunConfig(
            top_n=10,
            save_outputs=False,
        )
    )

    result = runner.run(
        seed_keyword="diet",
        start_date="2025-04-01",
        end_date="2026-03-31",
        enrich=False,
    )

    print(f"status: {result.status}")
    for message in result.messages:
        print(f"- {message}")

    print("\nCandidates")
    print(result.candidates_df.head(10).to_string(index=False))

    print("\nRanked")
    print(result.ranked_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
