"""
Imports the JSON Line files into pandas dataframes, flattens them,
and extracts only the selected columns for each table for further operations.
"""

import logging
from pathlib import Path
import pandas as pd
import config

logger = logging.getLogger(__name__)


def flatten_and_select_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Flattens nested JSON data in a DataFrame and selects only specified columns.

    :param1 df (pd.Dataframe): The DataFrame containing JSON data.
    :param2 columns (List[str]): The list of columns to retain after flattening.
    :return: A new DataFrame with flattened JSON data and selected columns.
    """
    flat_df = pd.json_normalize(df.to_dict(orient="records"))
    selected_columns = [col for col in columns if col in flat_df.columns]
    filtered_df = flat_df[selected_columns]

    return filtered_df


def process_file(json_file: Path, dataframes: dict, columns_to_keep: list) -> None:
    """
    Helper function to process a single JSON file and store the DataFrame in the dictionary.

    :param1 json_file (Path): The path to the JSON file.
    :param2 dataframes (dict): The dictionary to store DataFrames with file stems as keys.
    :param3 columns_to_keep (list): The columns to keep for each table.
    """
    try:
        df = pd.read_json(
            json_file, encoding="utf-8", lines=True
        )  # Canvas outputs into JSON Lines format
        if not df.empty:
            stem = json_file.stem
            df = flatten_and_select_columns(df, columns_to_keep)
            dataframes[stem] = df
            logger.info(
                "Loaded JSON file %s into DataFrame with key: %s.", json_file, stem
            )
        else:
            logger.warning("No data loaded from %s.", json_file)
    except Exception as e:
        logger.error("Failed to process file %s. Error: %s", json_file, e)
        raise RuntimeError(f"Failed to process file {json_file}") from e


def load_and_process_json_files(directory: Path, columns_mapping: dict) -> dict:
    """
    Reads all JSON files in the specified directory into DataFrames, flattens them,
    and selects only specified columns.

    :param1 directory (str): The path to the directory containing JSON files.
    :param2 columns_mapping (dict): A dictionary where keys are JSON file name stems
    and values are lists of columns to keep.
    :return: A dictionary where keys are the JSON file name stems and values are
    filtered DataFrames.
    """
    if not directory.is_dir():
        logger.error("The path %s is not a valid directory.", directory)
        raise ValueError(f"The path {directory} is not a valid directory.")

    json_files = list(directory.glob("*.json"))

    if not json_files:
        logger.error("No JSON files found in directory: %s", directory)
        raise FileNotFoundError(f"No JSON files found in directory: {directory}")

    dataframes = {}

    for json_file in json_files:
        stem = json_file.stem
        columns_to_keep = columns_mapping.get(stem).get("fields")
        process_file(json_file, dataframes, columns_to_keep)

    return dataframes


def rename_dataframe_columns(dataframes: dict) -> dict:
    """
    Renames columns in each DataFrame in the dictionary to include the DataFrame's key as a prefix.

    :param dataframes (dict): Dictionary of dataframes to have their columns renamed.
    :return: Dictionary of dataframes with renamed columns.
    """

    for key, df in dataframes.items():
        if not isinstance(df, pd.DataFrame):
            logger.warning("Skipping key '%s' as the value is not a DataFrame.", key)
            continue

        try:
            # Create a dictionary to map old column names to new column names
            new_column_names = {}

            for column in df.columns:
                # Split column name on the dot and create a new name with the DataFrame key as prefix
                if "." in column:
                    prefix, name = column.split(".", 1)
                    new_name = f"{key}_{name}"
                else:
                    # Handle columns without a dot
                    new_name = f"{key}_{column}"

                new_column_names[column] = new_name

            # Rename columns
            dataframes[key] = df.rename(columns=new_column_names)

        except Exception as e:
            logger.error(
                "Failed to rename columns for dataframe with key '%s'. Error: %s",
                key,
                e,
            )
            raise RuntimeError(f"Failed to rename columns for dataframe with key '{key}'") from e

    return dataframes


def export_to_final(user_config: dict, dataframes: dict):
    """
    Exports a list of dataframes into CSV files in the final data directory.

    :param1 user_config (dict): The user config.
    :param2 dataframes (dict): Dataframes to be exported.
    """
    user_config.final_path.mkdir(parents=True, exist_ok=True)

    for key, df in dataframes.items():
        final_dir = user_config.final_path / f"{key}.csv"
        df.to_csv(final_dir, index=False, encoding="utf-8")
        logger.info("%s created successfully.", final_dir)


def main(user_config: dict) -> dict:
    """
    Main function to load and process JSON files into DataFrames.

    :return: A dictionary of DataFrames processed from JSON files.
    """
    # load and process JSON files into DataFrames
    if user_config.str_format.lower() == "jsonl":
        json_path = user_config.temp_path / user_config.str_format.lower()
        dataframes = load_and_process_json_files(json_path, user_config.canvas_tables)

    # rename the selected dataframe columns for further processing
    dataframes = rename_dataframe_columns(dataframes)

    # save CSV files to data/final
    export_to_final(user_config, dataframes)

    return dataframes


if __name__ == "__main__":
    run_config = config.get_config()
    final_dataframes = main(run_config)
    print(f"\n-----data_transformer.py-----\n{final_dataframes}")
