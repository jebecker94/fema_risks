#!/usr/bin/env python
"""
Convert HTML tables in a directory to Parquet and CSV using Pandas.

Assumptions:
- Each HTML file contains a single <table> element (as provided by the user).
- Outputs are written alongside the source HTML by default, with the same stem
  and .parquet / .csv extensions.

Usage:
  python Scripts/convert_html_tables.py \
    --input-dir Dictionaries \
    --output-dir Dictionaries

Dependencies:
- pandas (for HTML parsing) + one of: lxml, html5lib, or beautifulsoup4
- pyarrow or fastparquet (for writing Parquet)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import re


def find_html_files(directory: Path) -> List[Path]:
    return sorted([p for p in directory.glob("*.html") if p.is_file()])


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def flatten_pandas_columns(columns: Iterable) -> List[str]:
    # Pandas read_html may yield MultiIndex columns if the table has multiple header rows.
    # Convert any non-scalar column name to a single string.
    flat: List[str] = []
    for col in columns:
        if isinstance(col, tuple):
            flat.append("_".join([str(part) for part in col if part is not None and str(part) != ""]))
        else:
            flat.append(str(col))
    return flat


def strip_label_prefix(text: str) -> str:
    # Remove known responsive labels like "Name", "Title", "Type", "Description", "Is Searchable"
    # when they are embedded like "Name <value>" due to small-screen tablesaw markup.
    if not isinstance(text, str):
        return text
    pattern = r"^(Name|Title|Type|Description|Is\s+Searchable)\s+"
    return re.sub(pattern, "", text).strip()


def remove_embedded_labels(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    for col in cleaned.columns:
        cleaned[col] = cleaned[col].apply(strip_label_prefix)
    return cleaned


def read_html_to_pandas(html_path: Path) -> pd.DataFrame:
    try:
        tables = pd.read_html(html_path)
    except ValueError as exc:  # No tables found
        raise RuntimeError(f"No <table> elements found in {html_path}") from exc

    if not tables:
        raise RuntimeError(f"No <table> elements found in {html_path}")

    df = tables[0]

    # Normalize potentially complex column headers
    df.columns = flatten_pandas_columns(df.columns)

    # Clean responsive table labels that are embedded in cell text
    df = remove_embedded_labels(df)

    return df


def write_outputs(df: pd.DataFrame, out_base: Path) -> None:
    parquet_path = out_base.with_suffix(".parquet")
    csv_path = out_base.with_suffix(".csv")

    # Write Parquet
    df.to_parquet(parquet_path.as_posix(), index=False)

    # Write CSV
    df.to_csv(csv_path.as_posix(), index=False)


def convert_directory(input_dir: Path, output_dir: Path) -> None:
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    ensure_output_dir(output_dir)

    html_files = find_html_files(input_dir)
    if not html_files:
        print(f"No .html files found in {input_dir}")
        return

    for html_file in html_files:
        try:
            df = read_html_to_pandas(html_file)
            out_base = (output_dir / html_file.stem)
            write_outputs(df, out_base)
            print(f"Converted {html_file.name} -> {out_base.with_suffix('.parquet').name}, {out_base.with_suffix('.csv').name}")
        except Exception as exc:
            print(f"Failed to convert {html_file}: {exc}")


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert HTML tables to Parquet and CSV using Pandas")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("Dictionaries"),
        help="Directory containing HTML files (default: Dictionaries)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write outputs (default: same as --input-dir)",
    )
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    input_dir: Path = args.input_dir
    output_dir: Path = args.output_dir or input_dir
    try:
        convert_directory(input_dir.resolve(), output_dir.resolve())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
