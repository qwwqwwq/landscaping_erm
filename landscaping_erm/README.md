# Landscaping ERM dbt Project

dbt project for transforming raw BBIT database exports into clean, analytics-ready tables.

## Datasets

| Dataset | Purpose |
|---------|---------|
| `erm_data` | Source - raw tables from BBIT database (163 tables) |
| `erm_dbt_staging` | Views - 1:1 with source, cleaned/renamed columns |
| `erm_dbt_intermediate` | Views - business logic, joins between staging models |
| `erm_dbt_marts` | Tables - final tables for dashboards/reports |

## Project Structure

```
landscaping_erm/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # BigQuery connection settings
├── models/
│   ├── staging/
│   │   └── _sources.yml     # Source definitions (163 tables from erm_data)
│   ├── intermediate/        # Business logic models
│   └── marts/               # Final consumption models
├── macros/                  # Reusable SQL macros
├── seeds/                   # Static CSV data
├── snapshots/               # SCD Type 2 snapshots
├── tests/                   # Custom data tests
└── analyses/                # Ad-hoc analytical queries
```

## Usage

Run from this directory:

```bash
# Validate configuration
uv run dbt debug --profiles-dir .

# Run all models
uv run dbt run --profiles-dir .

# Run tests
uv run dbt test --profiles-dir .

# Generate docs
uv run dbt docs generate --profiles-dir .
```

## Connection

Uses OAuth authentication to BigQuery project `landscaping-erm`. Ensure you're authenticated:

```bash
gcloud auth application-default login
```
