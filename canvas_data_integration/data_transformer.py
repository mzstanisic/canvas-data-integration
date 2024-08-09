"""
Imports the JSON Line files into pandas dataframes, flattens them,
and extracts only the selected columns for each table for further operations.
"""

import config
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def flatten_and_select_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Flattens nested JSON data in a DataFrame and selects only specified columns.

    :param1 df (pd.Dataframe): The DataFrame containing JSON data.
    :param columns (List[str]): The list of columns to retain after flattening.
    :return: A new DataFrame with flattened JSON data and selected columns.
    """
    flat_df = pd.json_normalize(df.to_dict(orient='records'))
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
        df = pd.read_json(json_file, encoding="utf-8", lines=True) # Canvas outputs into JSON Lines format
        if not df.empty:
            stem = json_file.stem
            df = flatten_and_select_columns(df, columns_to_keep)
            dataframes[stem] = df
            logger.info(f"Loaded JSON file {json_file} into DataFrame with key: {stem}.")
        else:
            logger.warning(f"No data loaded from {json_file}.")
    except Exception as e:
        logger.error(f"Failed to process file {json_file}. Error: {e}")


def load_and_process_json_files(directory: Path, columns_mapping: dict) -> dict:
    """
    Reads all JSON files in the specified directory into DataFrames, flattens them,
    and selects only specified columns.

    :param1 directory (str): The path to the directory containing JSON files.
    :param2 columns_mapping (dict): A dictionary where keys are JSON file name stems and values are lists of columns to keep.
    :return dataframes: A dictionary where keys are the JSON file name stems and values are filtered DataFrames.
    """
    if not directory.is_dir():
        logger.error(f"The path {directory} is not a valid directory.")
        raise ValueError(f"The path {directory} is not a valid directory.")
    
    json_files = [file for file in directory.glob("*.json")]

    dataframes = {}

    for json_file in json_files:
        stem = json_file.stem
        columns_to_keep = columns_mapping.get(stem).get("fields")
        process_file(json_file, dataframes, columns_to_keep)

    return dataframes


def rename_dataframe_columns(dataframes: dict) -> dict:
    """
    Renames columns in each DataFrame in the dictionary to include the DataFrame's key as a prefix.
    """
    dataframes_bu = dataframes.copy()  # backup original dataframes in case of failure
    try:
        for key, df in dataframes.items():
            # create a dictionary to map old column names to new column names
            new_column_names = {}
            
            for column in df.columns:
                # split column name on the dot and create a new name with the DataFrame key as prefix
                if '.' in column:
                    prefix, name = column.split('.', 1)
                    new_name = f"{key}_{name}"
                    new_column_names[column] = new_name
                else:
                    # handle columns without a dot
                    new_name = f"{key}_{column}"
                    new_column_names[column] = new_name
            
            # rename columns
            dataframes[key] = df.rename(columns=new_column_names)

        logger.info("Dataframe columns renamed successfully.")
        return dataframes
    except Exception as e:
        logger.error(f"Failed to rename dataframe columns. Error: {e}")
        return dataframes_bu


def export_to_final(config, dataframes):
    """
    """
    config.final_path.mkdir(parents=True, exist_ok=True)

    for key, df in dataframes.items():
        final_dir = config.final_path / f"{key}.csv"
        df.to_csv(final_dir, index=False, encoding="utf-8")
        logger.info(f"{final_dir} created successfully.")


def main(config: dict) -> dict:
    """
    Main function to load and process JSON files into DataFrames.

    :return: A dictionary of DataFrames processed from JSON files.
    """
    # load and process JSON files into DataFrames
    if config.str_format.lower() == 'jsonl':
        json_path = config.temp_path / config.str_format.lower()
        dataframes = load_and_process_json_files(json_path, config.canvas_tables)

    # TODO: repeat option for CSV, TSV, and Parquet once implemented

    # rename the selected dataframe columns for further processing
    dataframes = rename_dataframe_columns(dataframes)

    # save CSV files to data/final
    export_to_final(config, dataframes)

    return dataframes


if __name__ == "__main__":
    user_config = config.get_config()
    dataframes = main(user_config)
    print(f"\n-----data_transformer.py-----\n{dataframes}")