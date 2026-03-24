"""
HeroCoders Data Platform — Exploratory Data Analysis
======================================================
Step 1: ydata-profiling  → automated schema inference + quality overview per source
Step 2: pandera          → infer schema objects to use as raw layer data contracts

Run from project root:
    uv run python home-task/eda/explore.py
"""

from pathlib import Path
import pandas as pd
import pandera as pa
from ydata_profiling import ProfileReport

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR    = Path(__file__).parent.parent
DATA_DIR    = BASE_DIR / "data"
REPORTS_DIR = Path(__file__).parent / "reports"
SCHEMAS_DIR = Path(__file__).parent / "schemas"

REPORTS_DIR.mkdir(exist_ok=True)
SCHEMAS_DIR.mkdir(exist_ok=True)

SOURCES = {
    "marketplace_licenses":     DATA_DIR / "marketplace_licenses.csv",
    "marketplace_transactions": DATA_DIR / "marketplace_transactions.csv",
    "amplitude_events":         DATA_DIR / "amplitude_events.csv",
}

# ---------------------------------------------------------------------------
# Step 1 — Load & profile each source
# ---------------------------------------------------------------------------
def load(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def profile(name: str, df: pd.DataFrame) -> None:
    print(f"  Profiling {name} ({len(df):,} rows x {len(df.columns)} cols)...")
    report = ProfileReport(
        df,
        title=f"HeroCoders — {name}",
        explorative=True,
        minimal=False,
    )
    out = REPORTS_DIR / f"{name}.html"
    report.to_file(out)
    print(f"  -> {out}")


# ---------------------------------------------------------------------------
# Step 2 — Infer pandera schema (raw layer data contract)
# ---------------------------------------------------------------------------
def infer_schema(name: str, df: pd.DataFrame) -> None:
    print(f"  Inferring schema for {name}...")
    schema = pa.infer_schema(df)
    out = SCHEMAS_DIR / f"{name}_schema.py"
    out.write_text(schema.to_script())
    print(f"  -> {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    for name, path in SOURCES.items():
        print(f"\n{'='*60}")
        print(f" {name}")
        print(f"{'='*60}")
        df = load(path)
        profile(name, df)
        infer_schema(name, df)

    print("\nDone.")
    print("  HTML profiles   -> home-task/eda/reports/")
    print("  Pandera schemas -> home-task/eda/schemas/")
