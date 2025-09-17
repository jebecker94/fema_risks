# FEMA NFIP Risk Analysis Toolkit

## Project overview
This repository houses early-stage tooling for analyzing FEMA's National Flood Insurance Program (NFIP) policy data. The current focus is on building reusable data-preparation steps that will support a forthcoming nationwide study of policy rate changes under Risk Rating 2.0. Although some exploratory scripts were first drafted for individual states, all reusable components now emphasize tract-level and policy-level transformations that can scale to the entire United States.

Key capabilities implemented so far include:

- Converting FEMA-published HTML data dictionaries into machine-readable formats for downstream validation.
- Importing, filtering, and cleaning NFIP policy records with Polars, including rate normalization and tract-level summaries.
- Exploring historical policy cost changes and producing diagnostic visualizations for iterative analysis.

## Repository layout

```
.
├── Dictionaries/             # Canonical NFIP data dictionary (HTML, CSV, Parquet)
├── Scripts/                  # Reusable processing and exploration scripts
│   ├── convert_html_tables.py         # Convert HTML tables to Parquet/CSV (supports dictionaries)
│   ├── import_and_clean_fema_data.py  # Build tract-level rate summaries from policy parquet files
│   └── explore_rate_changes.py        # Visualize policy cost changes over successive years
├── pyproject.toml            # Project metadata and runtime dependencies (Python 3.13+)
└── uv.lock                   # Locked dependency versions for reproducible environments
```

## Data inputs
- **NFIP Policy Dictionary** (`Dictionaries/nfip_policies_data_dictionary.*`): Derived from FEMA's HTML data dictionary using the conversion utility. Provides authoritative field descriptions for validation.
- **Policy records** (`Data/FimaNfipPoliciesV2.parquet`): Expected input for analysis scripts. The parquet file is not tracked in source control; place it under `Data/` before running the workflows.

## Getting started
1. Install Python 3.13 (the project targets the latest CPython tooling).
2. Set up dependencies. The repository is configured for [uv](https://github.com/astral-sh/uv), but any PEP 517 installer will work:
   ```bash
   uv pip install -e .
   ```
3. Verify the scripts compile:
   ```bash
   python -m compileall Scripts
   ```

## Script usage

### Convert FEMA HTML tables
```
python Scripts/convert_html_tables.py --input-dir Dictionaries --output-dir Dictionaries
```
Reads each HTML table in `--input-dir`, strips responsive label artifacts, and writes matching `.parquet` and `.csv` files alongside the source. Useful for refreshing the NFIP data dictionary when FEMA publishes updates.

### Prepare tract-level policy metrics
```
python Scripts/import_and_clean_fema_data.py
```
Loads `Data/FimaNfipPoliciesV2.parquet`, filters policies to post–March 2021 renewals, normalizes flood zone labels, replaces invalid zero-rate combinations with nulls, and exports `Data/census_tract_summary_stats.parquet`. The tract summary includes average building and contents rates split by simplified flood zone types, providing a foundation for nationwide comparisons.

### Explore policy cost changes
```
python Scripts/explore_rate_changes.py
```
Produces histograms of percentage policy cost changes between initial Risk Rating 2.0 premiums and later renewal years. The current default targets Florida (`FL`) for iteration, but the workflow accepts any state code and is designed to scale to national analysis by adjusting the input parameter set.

## Roadmap toward the nationwide analysis
- Integrate state-agnostic parameterization across exploration scripts and harmonize outputs for all states.
- Extend the tract-level aggregation to include coverage counts and other distribution metrics needed for national reporting.
- Automate report generation (tables and figures) to summarize findings across the United States once nationwide inputs are available.

## Contributing
Contributions should follow the Polars-first data-processing approach established in the current scripts. Please keep helper functions short, document new utilities with module-level docstrings, and run `ruff format` / `ruff check --fix` before submitting pull requests.
