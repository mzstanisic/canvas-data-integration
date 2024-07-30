import os
import glob
import datetime
import logging
from dap.dap_types import Format

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.dirname(__file__)
    + "/../logs/"
    + datetime.datetime.now().strftime("%Y-%m-%d.log"),
    encoding="utf-8",
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s %(module)s.py - %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def get_format(config_format={"output_format":"csv"}):
    """
    Accepts a selected output format for files from config.yml,
    and returns the corresponding DAP output format type.
    
    :param config_format: The desired format for the data files: `CSV`, `JSONL`, `TSV`, or `Parquet`
    :returns: Corresponding DAP format type.
    """

    if not isinstance(config_format, dict):
        logger.warning("Parameter passed is not a dictionary. Defaulting to CSV.")
        config_format={"output_format":"csv"}
    if not config_format.get("output_format"):
        logger.warning("Config does not contain an 'output_format' entry. Defaulting to CSV.")

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
            logger.warning("Specified config format does not exist. Defaulting to CSV.")
            return Format.CSV



def temp_file_rename(output_directory: str, table: str): #TODO: format, add logs, checks, etc.
    """
    Renames the temp file created by canvas.get_canvas_data() to the Canvas table name
    for dataframe import.

    :param table: A Canvas table: https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#datasets
    :param output_directory: The output directory for the generated data files.
    """

    pattern = 'part-*.csv'
    files = glob.glob(os.path.join(output_directory, pattern))
    # Check if any files were found
    if files:
        # Assuming you only expect one file that matches the pattern
        old_path = files[0]  # Get the first matching file
        
        # Define the new file name
        new_file_name = table + '.csv'
        new_path = os.path.join(output_directory, new_file_name)
        
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