import argparse
from pathlib import Path

def parse_args():
    """
    Parses command-line arguments for the EDA tool.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Generate an Excel EDA report from a CSV file.")

    parser.add_argument(
        "input_file",
        type=str,
        help="Path to the input CSV file."
    )

    parser.add_argument(
        "-o", "--output",
        dest="output_file",
        type=str,
        help="Path to the output Excel file. If not provided, a default name will be generated."
    )

    parser.add_argument(
        "--intermediate",
        action="store_true",
        default=False,
        help="Save intermediate metrics to 'metrics.json'. Default is False."
    )

    parser.add_argument(
        "--dask-args",
        type=str,
        default=None,
        help="Optional JSON string of arguments to pass to dask.dataframe.read_csv (e.g., '{\"sep\": \";\"}')."
    )

    return parser.parse_args()
