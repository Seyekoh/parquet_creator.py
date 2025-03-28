#!/usr/bin/env python3
"""
parquet_creator.py

Script that will create a Parquet file from a .csv file.

The script will read the input file, convert it to a DataFrame,
and then save it as a Parquet file. The user can specify the
input file path and the output Parquet file path.

Input file formats supported: .csv
Output file format: .parquet

Usage:
    python parquet_creator.py <input_file> <output_file>

Requirements:
- pandas
- pyarrow (for Parquet support)

.csv header:
Category,Criterion

Example:
    Input CSV:
        Category,Criterion
        Size,Large
        Size,Medium
        Size,Small
        Color,Red
        Color,Blue

    Output Parquet as json:
        {"Category": "Size", "Criterion": "Large"}
        {"Category": "Size", "Criterion": "Medium"}
        {"Category": "Size", "Criterion": "Small"}
        {"Category": "Color", "Criterion": "Red"}
        {"Category": "Color", "Criterion": "Blue"}

Author: Seyekoh
Date: 27-March-2025
"""
import sys
import pandas as panda

def main():
    # Check for proper number of command line arguments
    if (len(sys.argv) != 3):
        print("Usage: python parquet_creator.py <input_file> <output_file>")
        print("Example: python parquet_creator.py input.csv output.parquet")
        sys.exit(1)

    # Verify input file extension
    input_file = sys.argv[1]
    if not (input_file.endswith('.csv')):
        print("Error: Input file must be a .csv file.")
        sys.exit(1)

    # Verify output file extension
    output_file = sys.argv[2]
    if not output_file.endswith('.parquet'):
        print("Error: Output file must be a .parquet file.")
        sys.exit(1)

    # Read the input file into a DataFrame
    try:
        df = panda.read_csv(input_file)
    except Exception as e:
        print(f"Error reading input file: {e}")
        sys.exit(1)

    # Save the DataFrame as a Parquet file
    try:
        df.to_parquet(output_file, index=False)
        print(f"Successfully created Parquet file: {output_file}")
    except Exception as e:
        print(f"Error saving Parquet file: {e}")
        sys.exit(1)

# Allow the script to be run directly
if __name__ == "__main__":
    main()
