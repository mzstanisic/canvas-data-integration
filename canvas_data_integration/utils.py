"""
Provides utility functions to the rest of the modules in the canvas_data_integration package.
"""

import logging
from pathlib import Path
from datetime import datetime, timedelta

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


def clean_old_logs(log_dir: Path, days: int = 30) -> None:
    """
    Remove old logs from the log folder to prevent buildup.

    :param1 log_dir (Path): The path to the log directory.
    :param2 days (int): The number of days to keep logs for.
    :return: None
    """
    cutoff_date = datetime.now() - timedelta(days=days)
    for log_file in log_dir.glob("*.log"):
        file_date = datetime.strptime(log_file.stem, "%Y-%m-%d")
        if file_date < cutoff_date:
            log_file.unlink()
            logger.info("Deleted old log file: %s", log_file)
