import polars as pl
import argparse
import sys
import os

def convert_jsonl_to_parquet(input_file: str, output_file: str, infer_schema_length: int = None):
    """
    Converts a JSONL (JSON Lines) file to Parquet format in a streaming fashion.
    
    This approach uses Polars' lazy API, which handles schema inference and 
    writing without loading the entire file into memory, making it suitable 
    for large files and limited memory environments.
    
    Args:
        input_file: Path to the source .jsonl file.
        output_file: Path where the .parquet file will be saved.
        infer_schema_length: Number of rows to scan for schema inference. 
                            If None, the entire file is scanned once to determine 
                            the unified schema (variable schema support).
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    print(f"Scanning {input_file} to infer schema...")
    
    # scan_ndjson creates a LazyFrame. 
    # infer_schema_length=None ensures all lines are scanned for keys, 
    # supporting variable schemas where new columns might appear late in the file.
    lazy_df = pl.scan_ndjson(input_file, infer_schema_length=infer_schema_length)
    
    print(f"Streaming data from {input_file} to {output_file}...")
    
    # sink_parquet executes the lazy plan and writes to disk in chunks, 
    # maintaining a low memory footprint.
    lazy_df.sink_parquet(output_file)
    
    print(f"Successfully converted {input_file} to {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description="Streamingly convert JSONL files with variable schema to Parquet."
    )
    parser.add_argument("input", help="Path to the input .jsonl file")
    parser.add_argument("output", help="Path to the output .parquet file")
    parser.add_argument(
        "--infer-length", 
        type=int, 
        default=None,
        help="Rows to scan for schema (default: None - scans entire file for full schema safety)"
    )

    args = parser.parse_args()

    try:
        convert_jsonl_to_parquet(args.input, args.output, args.infer_length)
    except Exception as e:
        print(f"Error during conversion: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
