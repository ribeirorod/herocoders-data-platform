# Home Challenge — Senior Analytics Engineer @ HeroCoders

Layered SQL analytics platform built on Snowflake + dbt, with raw data ingested from S3.

---

## Prerequisites

### Accounts
- **AWS** account with permissions to create S3 buckets and IAM resources
- **Snowflake** account with a database, warehouse, and user configured

### Tools
- [Terraform](https://developer.hashicorp.com/terraform/install) — infrastructure provisioning
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) — configured with `aws configure`
- Python 3.9+

### Python dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### dbt profile
Copy `profiles.example.yml` to `~/.dbt/profiles.yml` (or your local dbt profiles path) and fill in your Snowflake credentials:

```bash
cp profiles.example.yml ~/.dbt/profiles.yml
```

---

## Project structure

```
home-task/
  eda/
    explore.py              ← profile sources + infer pandera schemas
    schemas_to_dbt.py       ← bootstrap dbt schema.yml from pandera (one-time)

data-platform/
  models/
    staging/                ← stg_<model>.sql + stg_<model>.yml per source
    intermediate/           ← int_<model>.sql
    marts/                  ← mart_<model>.sql
  macros/                   ← shared SQL utilities
  seeds/                    ← static reference data (country codes, legal suffixes)

analysis.ipynb              ← exploratory analysis + business questions
```
