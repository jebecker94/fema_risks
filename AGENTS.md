# Project Guidance for `fema_risks`

## Coding Practices
- Target Python 3.13 syntax and typing features when helpful.
- Prefer [Polars](https://www.pola.rs/) dataframes for tabular work. Only fall back to pandas when a required feature is missing in Polars.
- Keep functions under ~50 lines when practical and favor small, testable helpers.
- Place shared utilities under `Scripts/` unless a clearer module structure emerges.
- Document new scripts with a short module-level docstring describing inputs/outputs.

## Data Sources
- Raw FEMA policy dictionaries live in the `Dictionaries/` directory.
- The canonical schema for policy records is stored in `Dictionaries/nfip_policies_data_dictionary.csv`. Use it to validate field names, types, and descriptions, but avoid copying the entire dictionary text into source files or commit messages.
- Treat CSV column names as case-sensitive; mirror them exactly in code and documentation.

## Analysis Assets
- Visualization scripts should emit figures to `Reports/` (create the folder if needed) with descriptive filenames like `YYYYMMDD_<topic>.png`.
- When generating derived datasets, store them under `Data/processed/` and include a README describing provenance.

## Testing & Tooling
- Before committing, run `uv pip install -e .` if new dependencies are introduced.
- Use `python -m compileall Scripts` as a lightweight lint to catch syntax errors.
- Format code with `ruff format` and enforce linting via `ruff check --fix`.

## Pull Requests
- Summaries should emphasize data inputs, processing steps, and any assumptions about FEMA policy semantics.

