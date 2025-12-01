from setuptools import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="eda_tool",
    version="0.1.0",
    description="A tool for Exploratory Data Analysis and Excel report generation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    py_modules=["excel", "cli_utils"],
    install_requires=[
        "dask[complete]==2024.1.0",
        "pandas==2.1.4",
        "numpy==1.26.3",
        "openpyxl==3.1.2",
        "scipy==1.11.4",
        "tqdm==4.66.1",
    ],
    entry_points={
        "console_scripts": [
            "eda-tool=excel:main",
        ],
    },
    python_requires=">=3.8",
)
