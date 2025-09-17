"""Import FEMA policy data, clean it, and compute census tract summaries."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence, Tuple

import polars as pl


def prepare_data_directory(base_path: Path) -> Path:
    """Ensure the shared data directory exists and return its path."""

    data_dir = base_path / "Data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def load_policy_lazy_frame(data_dir: Path) -> pl.LazyFrame:
    """Load the FEMA policy dataset as a lazy frame for efficient filtering."""

    return pl.scan_parquet(data_dir / "FimaNfipPoliciesV2.parquet")


def prepare_policy_dataframe(lazy_frame: pl.LazyFrame) -> pl.DataFrame:
    """Create the filtered policy DataFrame with temporal columns."""

    return (
        lazy_frame.with_columns(
            pl.col("policyEffectiveDate").dt.year().alias("Year"),
            pl.col("policyEffectiveDate").dt.month().alias("Month"),
        )
        .filter(
            (pl.col("Year") >= 2021) & (pl.col("Year") <= 2022),
            (pl.col("Month") > 3) | (pl.col("Year") > 2021),
            (pl.col("Month") < 4) | (pl.col("Year") < 2022),
            (pl.col("occupancyType").is_in([1, 11])),
            (pl.col("rateMethod") != "RatingEngine"),
            (pl.col("censusTract") != ""),
            (pl.col("ratedFloodZone") != ""),
            (pl.col("subsidizedRateType").is_in(["N", "P"])),
        )
        .collect()
    )


def replace_zero_rates(df: pl.DataFrame, rate_columns: Sequence[str]) -> pl.DataFrame:
    """Replace all-zero rate combinations with missing values in the provided columns."""

    cleaned = df.with_columns(
        pl.when(
            (pl.col("basicBuildingRate") == 0)
            & (pl.col("additionalBuildingRate") == 0)
            & (pl.col("basicContentsRate") == 0)
            & (pl.col("AdditionalContentsRate") == 0)
        )
        .then(1)
        .otherwise(0)
        .alias("i_AllZeroRates")
    )
    for rate_column in rate_columns:
        cleaned = cleaned.with_columns(
            pl.when(pl.col("i_AllZeroRates") == 1)
            .then(None)
            .otherwise(pl.col(rate_column))
            .alias(rate_column)
        )
    return cleaned


def determine_flood_zone(df: pl.DataFrame) -> pl.DataFrame:
    """Attach simplified flood zone labels to the policy DataFrame."""

    a_cols = [f"A{str(x).zfill(2)}" for x in range(1, 31)]
    v_cols = [f"V{str(x).zfill(2)}" for x in range(1, 31)]
    return df.with_columns(
        pl.when(pl.col("ratedFloodZone").is_in(a_cols))
        .then(pl.lit("A"))
        .when(pl.col("ratedFloodZone").is_in(v_cols))
        .then(pl.lit("V"))
        .when(pl.col("ratedFloodZone").is_in(["AHB"]))
        .then(pl.lit("AH"))
        .when(pl.col("ratedFloodZone").is_in(["AOB"]))
        .then(pl.lit("AO"))
        .otherwise(pl.col("ratedFloodZone"))
        .alias("FloodZoneType")
    )


def compute_tabulations(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Compute counts by year/month and subsidized rate type."""

    ct = df.group_by(["Year", "Month"]).agg(pl.count()).sort(["Year", "Month"])
    srt = df.group_by("subsidizedRateType").agg(pl.count()).sort("subsidizedRateType")
    return ct, srt


def compute_census_tract_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate average rate metrics by census tract and flood zone."""

    enriched = df.with_columns(
        pl.col("basicBuildingRate")
        .cast(pl.Float64)
        .mean()
        .over(["censusTract", "FloodZoneType"])
        .alias("AverageBasicBuildingRate"),
        pl.col("additionalBuildingRate")
        .cast(pl.Float64)
        .mean()
        .over(["censusTract", "FloodZoneType"])
        .alias("AverageAdditionalBuildingRate"),
        pl.col("basicContentsRate")
        .cast(pl.Float64)
        .mean()
        .over(["censusTract", "FloodZoneType"])
        .alias("AverageBasicContentsRate"),
        pl.col("AdditionalContentsRate")
        .cast(pl.Float64)
        .mean()
        .over(["censusTract", "FloodZoneType"])
        .alias("AverageAdditionalContentsRate"),
    )
    return enriched.select(
        pl.col("censusTract"),
        pl.col("FloodZoneType"),
        pl.col("AverageBasicBuildingRate"),
        pl.col("AverageAdditionalBuildingRate"),
        pl.col("AverageBasicContentsRate"),
        pl.col("AverageAdditionalContentsRate"),
    ).unique()


def process_policies(base_path: Path) -> pl.DataFrame:
    """Run the full data import and cleaning pipeline."""

    data_dir = prepare_data_directory(base_path)
    rate_columns: Tuple[str, ...] = (
        "basicBuildingRate",
        "additionalBuildingRate",
        "basicContentsRate",
        "AdditionalContentsRate",
    )
    df = prepare_policy_dataframe(load_policy_lazy_frame(data_dir))
    df = replace_zero_rates(df, rate_columns)
    df = determine_flood_zone(df)
    compute_tabulations(df)  # Keeps computation available for interactive use
    return compute_census_tract_summary(df)


def main() -> None:
    """Save the census tract summary statistics to disk."""

    base_path = Path(__file__).parent.parent
    tract_summary = process_policies(base_path)
    data_dir = base_path / "Data"
    tract_summary.write_parquet(data_dir / "census_tract_summary_stats.parquet")


if __name__ == "__main__":
    main()
