import os
from pathlib import Path
import pandas as pd

def filter_by_state(input_csv: Path, filtered_dir: Path, state_col: str):
    df = pd.read_csv(input_csv)
    if state_col not in df.columns:
        raise KeyError(f"Column '{state_col}' not found in {input_csv}")
    filtered_dir.mkdir(parents=True, exist_ok=True)
    # split into one file per state
    for state, group in df.groupby(state_col):
        out_file = filtered_dir / f"{state}.csv"
        group.to_csv(out_file, index=False)
    print(f"Filtered files written to {filtered_dir}")

def group_summary(input_csv: Path, summary_csv: Path, state_col: str):
    df = pd.read_csv(input_csv)
    # example aggregation: mean of numeric columns per state
    summary = df.groupby(state_col).mean(numeric_only=True).reset_index()
    summary.to_csv(summary_csv, index=False)
    print(f"Grouped summary written to {summary_csv}")

def main():
    base = Path("d:/VsCode_Projects/ENG4040/Treating_BR_Weather")
    input_csv = base / "data/Grouped/combined_dataset.csv"
    filtered_dir = base / "data/Filtered"
    summary_csv = filtered_dir / "grouped_summary.csv"
    state_col = "state"  # adjust to actual column name
    filter_by_state(input_csv, filtered_dir, state_col)
    group_summary(input_csv, summary_csv, state_col)

if __name__ == "__main__":
    main()
