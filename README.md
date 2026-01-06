# JSONL to Parquet Converter

A lightweight Python utility to streamingly convert JSONL (JSON Lines) files to Parquet format using Polars. This tool is designed to handle large datasets and variable schemas with minimal memory footprint.

## Features

- **Streaming Conversion**: Uses Polars' lazy API and `sink_parquet` to process files that are larger than available RAM.
- **Variable Schema Support**: By default, scans the entire file to ensure all keys across all lines are included in the Parquet schema.
- **Efficient Performance**: Leverages the high-performance Polars library for fast processing.

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt` (primarily `polars` and `pyarrow`).

## Installation

1. Clone or download this repository.
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the script from the command line by providing the input JSONL file and the desired output Parquet path.

```bash
python main.py <input_file.jsonl> <output_file.parquet>
```

### Options

- `--infer-length`: Specify the number of rows to scan for schema inference. 
  - If omitted (default), it scans the **entire file** to ensure a unified schema, which is safest for JSONL files where keys might not be present in every line or appear later in the file.
  - Set a specific integer (e.g., `--infer-length 1000`) for faster inference on very large files if you are certain the schema is consistent.

### Example

```bash
python main.py test.jsonl output.parquet
```

## How it Works

The utility uses `pl.scan_ndjson()` to create a `LazyFrame`, which defines the transformation plan without loading data into memory. The `sink_parquet()` method then executes this plan, streaming the data from the source to the destination in chunks.
