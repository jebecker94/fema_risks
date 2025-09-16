# Import required libraries
import polars as pl
from pathlib import Path

# Define the path to the CSV file
data_dir = Path(__file__).parent.parent / "Data"
csv_file = data_dir / "FimaNfipPoliciesV2.parquet"

# Load data
df = pl.scan_parquet(csv_file)

# Create year and month
df = df.with_columns(
    pl.col('policyEffectiveDate').dt.year().alias('Year'),
    pl.col('policyEffectiveDate').dt.month().alias('Month')
)

# Filter Data
df = df.filter(
    (pl.col('Year') >= 2021) & (pl.col('Year') <= 2022),
    (pl.col('Month') > 3) | (pl.col('Year') > 2021),
    (pl.col('Month') < 4) | (pl.col('Year') < 2022),
    (pl.col('occupancyType').is_in([1,11])),
    (pl.col('rateMethod') != "RatingEngine"),
    (pl.col('censusTract') != ""),
    (pl.col('ratedFloodZone') != ""),
    (pl.col('subsidizedRateType').is_in(["N","P"]))
)

# Collect data
df = df.collect()

# Replace All Zeros Observations with Missings
df = df.with_columns(
    pl.when((pl.col('basicBuildingRate') == 0) &
            (pl.col('additionalBuildingRate') == 0) &
            (pl.col('basicContentsRate') == 0) &
            (pl.col('AdditionalContentsRate') == 0))
    .then(1)
    .otherwise(0)
    .alias('i_AllZeroRates')
)
for rate_column in ['basicBuildingRate', 'additionalBuildingRate', 'basicContentsRate', 'AdditionalContentsRate']:
    df = df.with_columns(
        pl.when(pl.col('i_AllZeroRates') == 1)
        .then(None)
        .otherwise(pl.col(rate_column))
        .alias(rate_column)
    )

# Get Flood Hazard Area Names
a_cols = [f'A{str(x).zfill(2)}' for x in range(1, 30+1)]
v_cols = [f'V{str(x).zfill(2)}' for x in range(1, 30+1)]
df = df.with_columns(
    pl.when(pl.col('ratedFloodZone').is_in(a_cols))
    .then(pl.lit('A'))
    .when(pl.col('ratedFloodZone').is_in(v_cols))
    .then(pl.lit('V'))
    .when(pl.col('ratedFloodZone').is_in(['AHB']))
    .then(pl.lit('AH'))
    .when(pl.col('ratedFloodZone').is_in(['AOB']))
    .then(pl.lit('AO'))
    .otherwise(pl.col('ratedFloodZone'))
    .alias('FloodZoneType')
)
del a_cols, v_cols

# Tabulations
ct = df.group_by(['Year','Month']).agg(pl.count()).sort(['Year','Month'])
srt = df.group_by('subsidizedRateType').agg(pl.count()).sort('subsidizedRateType')

# Census Tract Summary
df = df.with_columns(
    pl.col('basicBuildingRate').cast(pl.Float64).mean().over(['censusTract','FloodZoneType']).alias('AverageBasicBuildingRate'),
    pl.col('additionalBuildingRate').cast(pl.Float64).mean().over(['censusTract','FloodZoneType']).alias('AverageAdditionalBuildingRate'),
    pl.col('basicContentsRate').cast(pl.Float64).mean().over(['censusTract','FloodZoneType']).alias('AverageBasicContentsRate'),
    pl.col('AdditionalContentsRate').cast(pl.Float64).mean().over(['censusTract','FloodZoneType']).alias('AverageAdditionalContentsRate')
)

# Tract Summary
tract_summary = df.select(
    pl.col('censusTract'),
    pl.col('FloodZoneType'),
    pl.col('AverageBasicBuildingRate'),
    pl.col('AverageAdditionalBuildingRate'),
    pl.col('AverageBasicContentsRate'),
    pl.col('AverageAdditionalContentsRate')
).unique()

# Save
tract_summary.write_parquet(data_dir / 'census_tract_summary_stats.parquet')