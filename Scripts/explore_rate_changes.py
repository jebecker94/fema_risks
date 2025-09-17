"""Explore FEMA policy rate changes by policy year."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterable, Tuple

import matplotlib.pyplot as plt
import polars as pl


def prepare_directories(base_path: Path) -> Tuple[Path, Path]:
    """Create and return the data and figure directories for the project."""

    data_dir = base_path / "Data"
    figures_dir = base_path / "Figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir, figures_dir


def load_policy_frame(data_dir: Path) -> pl.LazyFrame:
    """Load the FEMA policy dataset as a lazy Polars frame."""

    return pl.scan_parquet(data_dir / "FimaNfipPoliciesV2.parquet")


def preprocess_policies(lazy_frame: pl.LazyFrame, state: str) -> pl.DataFrame:
    """Filter and engineer features needed for rate change exploration."""

    return (
        lazy_frame.with_columns(
            pl.col("policyEffectiveDate").dt.year().alias("Year"),
            pl.col("policyEffectiveDate").dt.month().alias("Month"),
        )
        .filter(
            (pl.col("Year") >= 2021),
            (pl.col("Month") > 3) | (pl.col("Year") > 2021),
            (pl.col("propertyState").is_in([state])),
            (pl.col("occupancyType").is_in([1, 11])),
            (pl.col("censusTract") != ""),
            (pl.col("ratedFloodZone") != ""),
            (pl.col("originalNBDate").dt.year() < 2021)
            | (pl.col("originalNBDate").dt.month() < 4),
        )
        .with_columns(
            (pl.col("policyEffectiveDate") - dt.datetime(2021, 3, 1))
            .dt.total_days()
            .alias("DaysSinceMarch2021"),
        )
        .with_columns(
            (pl.col("DaysSinceMarch2021") / 365).floor().alias("YearsSinceMarch2021"),
        )
        .collect()
    )


def compute_policy_cost_changes(df: pl.DataFrame, policy_year: int) -> pl.DataFrame:
    """Compute policy cost changes between year zero and a specified policy year."""

    df_original = df.filter(pl.col("YearsSinceMarch2021") == 0).select(
        ["censusTract", "originalNBDate", "policyCost"]
    )
    df_new = df.filter(pl.col("YearsSinceMarch2021") == policy_year).select(
        ["censusTract", "originalNBDate", "policyCost"]
    )
    joined = df_original.join(
        df_new,
        on=["censusTract", "originalNBDate"],
        how="inner",
        suffix=f"_year_{policy_year}",
    )
    return (
        joined.with_columns(
            (
                pl.col(f"policyCost_year_{policy_year}") - pl.col("policyCost")
            ).alias(f"PolicyCostChange_year_{policy_year}"),
            (
                (
                    pl.col(f"policyCost_year_{policy_year}")
                    - pl.col("policyCost")
                )
                / pl.col("policyCost")
            ).alias(f"PolicyCostChangePercent_year_{policy_year}"),
        )
        .with_columns(
            pl.when(pl.col(f"PolicyCostChangePercent_year_{policy_year}") > 2)
            .then(2)
            .otherwise(pl.col(f"PolicyCostChangePercent_year_{policy_year}"))
            .alias(f"PolicyCostChangePercent_year_{policy_year}"),
        )
    )


def plot_histogram(series: pl.Series, title: str, bins: int, output_path: Path | None = None) -> None:
    """Plot a histogram for the provided Polars series."""

    fig = plt.figure(figsize=(10, 5))
    plt.hist(series.to_numpy(), bins=bins)
    plt.title(title)
    plt.xlabel("Policy Cost Change Percent")
    plt.ylabel("Frequency")
    if output_path is not None:
        plt.savefig(output_path)
    plt.show()
    plt.close(fig)


def explore_rate_changes(state: str, policy_years: Iterable[int]) -> None:
    """Explore rate changes and create histograms for the requested policy years."""

    base_path = Path(__file__).parent.parent
    data_dir, figures_dir = prepare_directories(base_path)
    df = preprocess_policies(load_policy_frame(data_dir), state)

    first_year_changes = compute_policy_cost_changes(df, 1)
    plot_histogram(
        first_year_changes["PolicyCostChangePercent_year_1"],
        "Policy Cost Change Percent",
        bins=100,
    )

    for policy_year in policy_years:
        change_df = compute_policy_cost_changes(df, policy_year)
        plot_histogram(
            change_df[f"PolicyCostChangePercent_year_{policy_year}"],
            f"Policy Cost Change Percent for Policy Year {policy_year}",
            bins=100,
            output_path=figures_dir
            / f"{state}_policy_cost_change_percent_year_{policy_year}.png",
        )


def main() -> None:
    """Execute the full rate change exploration workflow for Florida policies."""

    explore_rate_changes("FL", policy_years=range(1, 5))


if __name__ == "__main__":
    main()
