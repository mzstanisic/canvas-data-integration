"""
processing.py

Imports the JSON Line files into pandas dataframes, flattens them,
and extracts only the selected columns for each table for further operations.
"""

import asyncio
import config
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)


def flatten_and_select_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
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
        df = pd.read_json(json_file, lines=True) # Canvas outputs into JSON Lines format
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
    # path = Path(directory)
    if not directory.is_dir():
        raise ValueError(f"The path {directory} is not a valid directory.")
    
    json_files = [file for file in directory.glob("*.json")]

    dataframes = {}
    # tasks = []

    for json_file in json_files:
        stem = json_file.stem
        columns_to_keep = columns_mapping.get(stem, [])
        process_file(json_file, dataframes, columns_to_keep)
        # tasks.append(asyncio.create_task(process_file(json_file, dataframes, columns_to_keep)))

    # await asyncio.gather(*tasks)
    return dataframes


def main(config: dict) -> dict:
    """
    Main function to load and process JSON files into DataFrames.

    :return: A dictionary of DataFrames processed from JSON files.
    """
    # TODO: replace with proper config directories
    # directory = r"C:\Users\stanisim\Desktop\canvas-data-integration\data\temp\json"
    # f_directory = r"C:\Users\stanisim\Desktop\canvas-data-integration\data\final"
    # merged = r"C:\Users\stanisim\Desktop\canvas-data-integration\data\final\FINAL.csv"

    # define column mappings for each JSON file
    # columns_mapping = {
    #     'enrollment_terms': ['key.id', 'value.sis_source_id', 'value.workflow_state', 'meta.ts'],
    #     'courses': ['key.id', 'value.sis_source_id', 'value.name', 'value.enrollment_term_id', 'value.workflow_state', 'value.is_public', 'meta.ts'],
    #     'course_sections': ['key.id', 'value.name', 'value.course_id', 'value.workflow_state', 'meta.ts'],
    #     'enrollments': ['key.id', 'value.last_activity_at', 'value.total_activity_time', 'value.course_section_id', 'value.course_id', 'value.role_id', 'value.user_id', 'value.sis_pseudonym_id', 'value.workflow_state', 'value.type', 'meta.ts'],
    #     'users': ['key.id', 'value.workflow_state', 'value.name', 'meta.ts'],
    #     'pseudonyms': ['key.id', 'value.user_id', 'value.sis_user_id', 'value.unique_id', 'value.workflow_state', 'meta.ts'],
    #     'scores': ['key.id', 'value.current_score', 'value.enrollment_id', 'value.workflow_state', 'value.course_score', 'meta.ts']
    # }

    # load and process JSON files into DataFrames
    if config.str_format.lower() == 'jsonl':
        dataframes = load_and_process_json_files(config.temp_path, config.canvas_tables)

    # TODO: repeat option for CSV, TSV, and Parquet

    return dataframes


if __name__ == "__main__":
    user_config = config.get_config()
    main(user_config)