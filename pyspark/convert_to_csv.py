# Converts the Parquet output folders downloaded from HDFS into CSV files for Power BI.
# Requires: python -m pip install pandas pyarrow

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "parquet_data"
OUTPUT_DIR = BASE_DIR / "output_csv"
OUTPUT_DIR.mkdir(exist_ok=True)

datasets = [
    "product_demand",
    "sales_summary",
    "monthly_demand",
    "regional_demand",
    "product_ranking",
    "inventory_priority",
    "priority_summary"
]

for dataset in datasets:
    input_path = INPUT_DIR / dataset
    output_path = OUTPUT_DIR / (dataset + ".csv")

    print("\nProcessing: " + dataset)

    if not input_path.exists():
        print("ERROR: Folder not found: " + str(input_path))
        continue

    try:
        df = pd.read_parquet(input_path, engine="pyarrow")
        df.to_csv(output_path, index=False)

        print("Successfully converted: " + dataset)
        print("Rows: " + str(len(df)))
        print("Columns: " + str(len(df.columns)))
        print("Saved to: " + str(output_path))

    except Exception as e:
        print("ERROR converting " + dataset + ": " + str(e))

print("\nAll dataset conversions completed.")
