"""
Provides utility functions to the rest of the modules in the the canvas_data_integration package.
"""

import logging
from pathlib import Path
import config

logger = logging.getLogger(__name__)


def empty_temp(temp_path: Path) -> None:
    """
    Empties the data/temp folder of data files.

    :param temp_path: Path to the directory that stores the temporary data files.
    :return: None
    """

    file_extensions = [".csv", ".json", ".tsv", ".parquet"]
    files = [p for p in temp_path.rglob("*") if p.suffix in file_extensions]

    print(files)

    for file in files:
        if file.is_file():
            file.unlink()
            logger.info("Deleted file: %s", file)


if __name__ == "__main__":
    user_config = config.get_config()
    empty_temp(user_config.temp_path)
