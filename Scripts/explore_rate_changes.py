# Import required libraries
import polars as pl
from pathlib import Path
import datetime
import matplotlib.pyplot as plt

# Set directories
data_dir = Path(__file__).parent.parent / "Data"
figures_dir = Path(__file__).parent.parent / "Figures"

# Create directories if they don't exist
figures_dir.mkdir(parents=True, exist_ok=True)
data_dir.mkdir(parents=True, exist_ok=True)

# Set state
state = 'FL'

# Load data
csv_file = data_dir / "FimaNfipPoliciesV2.parquet"
df = pl.scan_parquet(csv_file)

# Create year and month
df = df.with_columns(
    pl.col('policyEffectiveDate').dt.year().alias('Year'),
    pl.col('policyEffectiveDate').dt.month().alias('Month')
)

# Filter Data
df = df.filter(
    (pl.col('Year') >= 2021),
    (pl.col('Month') > 3) | (pl.col('Year') > 2021),
    (pl.col('propertyState').is_in(['FL'])),
    (pl.col('occupancyType').is_in([1,11])),
    (pl.col('censusTract') != ""),
    (pl.col('ratedFloodZone') != ""),
    (pl.col('originalNBDate').dt.year() < 2021) | (pl.col('originalNBDate').dt.month() < 4)
)

# Years since march 2021
df = df.with_columns(
    (pl.col('policyEffectiveDate') - datetime.datetime(2021, 3, 1)).dt.total_days().alias('DaysSinceMarch2021'),
)
df = df.with_columns(
    (pl.col('DaysSinceMarch2021') / 365).floor().alias('YearsSinceMarch2021'),
)

# Collect data
df = df.collect()

# Merge years 0 and 1
df0 = df.filter(pl.col('YearsSinceMarch2021') == 0).select(['censusTract','originalNBDate','policyCost'])
df1 = df.filter(pl.col('YearsSinceMarch2021') == 1).select(['censusTract','originalNBDate','policyCost'])
df01 = df0.join(df1, on=['censusTract','originalNBDate'], how='inner', suffix='_01')
df01 = df01.with_columns(
    (pl.col('policyCost_01') - pl.col('policyCost')).alias('PolicyCostChange'),
    ((pl.col('policyCost_01') - pl.col('policyCost')) / pl.col('policyCost')).alias('PolicyCostChangePercent')
)

# Truncate change percent at 200%
df01 = df01.with_columns(
    pl.when(pl.col('PolicyCostChangePercent') > 2)
    .then(2)
    .otherwise(pl.col('PolicyCostChangePercent'))
    .alias('PolicyCostChangePercent')
)

# Plot histogram of policy cost changes
fig = plt.figure(figsize=(10, 5))
plt.hist(df01['PolicyCostChangePercent'], bins=100)
plt.show()

# Plot increases for policy years 1-4
for policy_year in range(1, 5):
    df_original = df.filter(pl.col('YearsSinceMarch2021') == 0).select(['censusTract','originalNBDate','policyCost'])
    df_new = df.filter(pl.col('YearsSinceMarch2021') == policy_year).select(['censusTract','originalNBDate','policyCost'])
    df_change = df_original.join(df_new, on=['censusTract','originalNBDate'], how='inner', suffix=f'_year_{policy_year}')
    df_change = df_change.with_columns(
        (pl.col(f'policyCost_year_{policy_year}') - pl.col('policyCost')).alias(f'PolicyCostChange_year_{policy_year}'),
        ((pl.col(f'policyCost_year_{policy_year}') - pl.col('policyCost')) / pl.col('policyCost')).alias(f'PolicyCostChangePercent_year_{policy_year}')
    )
    df_change = df_change.with_columns(
        pl.when(pl.col(f'PolicyCostChangePercent_year_{policy_year}') > 2)
        .then(2)
        .otherwise(pl.col(f'PolicyCostChangePercent_year_{policy_year}'))
        .alias(f'PolicyCostChangePercent_year_{policy_year}')
    )
    fig = plt.figure(figsize=(10, 5))
    plt.hist(df_change[f'PolicyCostChangePercent_year_{policy_year}'], bins=100)
    plt.title(f'Policy Cost Change Percent for Policy Year {policy_year}')
    plt.xlabel('Policy Cost Change Percent')
    plt.ylabel('Frequency')
    plt.savefig(figures_dir / f'{state}_policy_cost_change_percent_year_{policy_year}.png')
    plt.show()
