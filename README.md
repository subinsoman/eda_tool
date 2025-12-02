# EDA Tool

A powerful CLI tool for Exploratory Data Analysis (EDA) and automated Excel report generation.

## Features

- **Automated Metrics Calculation**: Calculates a wide range of statistical metrics for numerical, categorical, and date columns.
- **Excel Report Generation**: Generates a comprehensive Excel report with:
    - Summary statistics.
    - Detailed column metrics.
    - Individual column analysis.
    - Dynamic charts.
    - Data definitions.
- **Performance**: Uses Dask for efficient processing of large datasets.
- **CLI Interface**: Easy-to-use command-line interface.

## Installation

You can install the package using pip:

```bash
pip install subinsoman-eda-tool
```

## Usage

Run the tool from the command line:

```bash
eda-tool input.csv --output report.xlsx
```

### Arguments

- `input_file`: Path to the input CSV file (required).
- `--output`, `-o`: Path to the output Excel file (optional, defaults to `[input_stem]_metrics_report_[timestamp].xlsx`).
- `--intermediate`, `-i`: Save intermediate metrics to `metrics.json` (optional).

### Example

```bash
eda-tool data.csv -o my_report.xlsx -i
```

## Requirements

- Python 3.8+
- dask[complete]
- numpy
- openpyxl
- scipy
- tqdm
