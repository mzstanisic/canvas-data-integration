# import os
import glob
import datetime
import logging
from pathlib import Path
from dap.dap_types import Format

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=Path(__file__).parent
    / "../logs/"
    / datetime.datetime.now().strftime("%Y-%m-%d.log"),
    encoding="utf-8",
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(module)s.py - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_format(config_format: dict = {"output_format":"csv"}) -> Format:
    """
    Accepts a selected output format for files from config.yml,
    and returns the corresponding DAP output format type.
    
    :param config_format: The desired format for the data files, specified in the config file: `CSV`, `JSONL`, `TSV`, or `Parquet`
    :returns: Corresponding DAP format type.
    """

    if not isinstance(config_format, dict):
        logger.warning(f"Type mismatch for parameter `config_format`, expected dict: {type(config_format)}")
        logger.info("Defaulting to CSV.")
        config_format={"output_format":"csv"}
    if not config_format.get("output_format"):
        logger.warning(f"Dictionary `config_format` does not contain an `output_format` key: {config_format}")
        logger.info("Defaulting to CSV.")

    config_format = config_format.get("output_format") or "csv"
    config_format = config_format.lower().strip()

    match config_format:
        case "csv":
            return Format.CSV
        case "json" | "jsonl":
            return Format.JSONL
        case "tsv":
            return Format.TSV
        case "parquet":
            return Format.Parquet
        case _:
            logger.warning(f"Specified format does not exist, expected one of (CSV, JSONL, TSV, Parquet): {config_format}")
            logger.info("Defaulting to CSV.")
            return Format.CSV



#TODO: Remove
def temp_file_rename(output_directory: str, table: str, format, filename):
    """
    Renames the temp file created by canvas.get_canvas_data() to the Canvas table name
    for dataframe import.

    :param table: A Canvas table: https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#datasets
    :param output_directory: The output directory for the generated data files.
    :param format: The data format for the generated data files.
    """

    match format:
        case Format.CSV:
            pattern = 'part-*.csv'
            new_file_name = table + '.csv'
        case Format.JSONL:
            pattern = 'part-*.json'
            new_file_name = table + '.json'
        case Format.TSV:
            pattern = 'part-*.tsv'
            new_file_name = table + '.tsv'
        case Format.Parquet:
            pattern = 'part-*.parquet'
            new_file_name = table + '.parquet'
        case _:
            logger.warning(f"Specified format does not exist, expected one of (CSV, JSONL, TSV, Parquet): {format}")
            logger.info("No files renamed.")
            return

    # files = glob.glob(os.path.join(output_directory, pattern))
    files = glob.glob(Path.joinpath(output_directory, pattern))

    if files:
        # Assuming you only expect one file that matches the pattern
        old_path = files[0]
        new_path = Path.joinpath(output_directory, new_file_name)
        
        # Rename the file
        try:
            os.rename(old_path, new_path)
            print(f"File renamed successfully from '{old_path}' to '{new_path}'")
        except FileNotFoundError:
            print(f"Error: The file '{old_path}' does not exist.")
        except PermissionError:
            print(f"Error: Permission denied while renaming '{old_path}'.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
            print("No files matching the pattern were found.")