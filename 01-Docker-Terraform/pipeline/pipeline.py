import sys
import os
import datetime as dt
import polars as pl
from pathlib import Path

if len(sys.argv) > 1:
    # Parse the string from sys.argv (e.g., '2025-02-09')
    load_date = dt.datetime.strptime(sys.argv[1], '%Y-%m-%d').strftime('%Y-%m-%d')
else:
    load_date = dt.datetime.today().strftime('%Y-%m-%d')    

print("Pipeline is running with Python version:", sys.version)

# Example of a simple pipeline step: reading data from a CSV file
def step_one(path):
    data = pl.scan_csv(path)
    n_rows = data.select(pl.len()).collect().item()
    print( f"Step one:\tRead data from {path} with {n_rows} records")
    return data

# Example of another pipeline step: transforming the data
def step_two(data, load_date= load_date):
    # Example transformation
    # load_date = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    transformed_data = data.with_columns(pl.lit(load_date).alias("load_date"))
    # transformed_data.show()
    n_rows = transformed_data.select(pl.len()).collect().item()
    print( f"Step two:\tTransformed data with {n_rows} records")
    return transformed_data

# Example of a final pipeline step: writing the data to a Parquet file
def step_three(data, output_path):
    data.sink_parquet(output_path, compression='snappy')
    print(f"Step three:\tSuccessfully wrote data to {output_path}")

# Run the pipeline
def run_pipeline(input_path, output_path):
    data = step_one(input_path)
    transformed_data = step_two(data)
    step_three(transformed_data, output_path)

if __name__ == "__main__":
    base_dir = Path(__file__).parent
    input_path = base_dir / "uber.csv" # Example input path
    input_path = str(input_path)  # Convert Path object to string for local libraries
    output_path = base_dir / "uber.parquet"  # Example output path
    output_path = str(output_path)  # Convert Path object to string for local libraries

    run_pipeline(input_path, output_path)
