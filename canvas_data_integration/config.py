"""
config.py
"""
import yaml
import logging
import datetime
from pathlib import Path
from dap.dap_types import Format

logger = logging.getLogger(__name__)

# config the logger
logging.basicConfig(
    filename=Path(__file__).parent
    / "../logs/"
    / datetime.datetime.now().strftime("%Y-%m-%d.log"),
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s :: %(levelname)-8s :: %(module)s.%(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

class Config:
    def __init__(self, final_path: Path, temp_path: Path, format: str, canvas_format: Format):
        """
        Initializes the Config object with the given properties.

        :param1 final_path: The path where final output files are stored.
        :param2 temp_path: The path where temporary files are stored.
        :param3 format: The format for the Canvas data files (string representation).
        :param4 canvas_format: The format for the Canvas data files.
        """
        self.final_path = final_path
        self.temp_path = temp_path
        self.format = format
        self.canvas_format = canvas_format

    def __repr__(self):
        """
        Provides a string representation of the Config object.
        """
        return (f"Config(final_path={self.final_path}, "
                f"temp_path={self.temp_path}, "
                f"format='{self.format}', "
                f"canvas_format='{self.canvas_format}')")


def get_format(config_format: dict = {"canvas_format":"json"}) -> Format:
    """
    Accepts a selected output format for files from config.yml,
    and returns the corresponding DAP Canvas format type.
    
    :param1 config_format (dict): The desired format for the data files, specified in the config file: `CSV`, `JSONL`, `TSV`, or `Parquet`
    :returns: Corresponding DAP format type.
    """

    if not isinstance(config_format, dict):
        logger.warning(f"Type mismatch for parameter `config_format`, expected dict: {type(config_format)}")
        logger.info("Defaulting to JSON.")
        config_format={"canvas_format":"json"}
    if not config_format.get("canvas_format"):
        logger.warning(f"Dictionary `config_format` does not contain an `canvas_format` key: {config_format}")
        logger.info("Defaulting to JSON.")

    config_format = config_format.get("canvas_format") or "json"
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
            logger.info("Defaulting to JSON.")
            return Format.JSONL
        

def get_config() -> Config:
    """
    Looks for a valid config.yml file in the base project directory.
    If there is none, uses defaults.

    :returns: A Config object that includes a data format and paths.
    """
    config_path = Path(__file__).parent / '../config.yml'

    if not config_path.is_file():
        logger.warning(f"The path {config_path} is not a valid file. Using defaults.")
        config = {
            "temp_path":"../data/temp",
            "final_path":"../data/final",
            "canvas_format":"json"
        }
        canvas_format = Format.JSONL
    else:
        config = yaml.safe_load(open(config_path))
        if config.get("temp_path") and config.get("final_path"):
            canvas_format = get_format(config)
        else:
            logger.warning(f"The config does not have entries for temp and final paths. Using defaults.")
            config = {
                "temp_path":"../data/temp",
                "final_path":"../data/final",
                "canvas_format":"json"
            }
            canvas_format = Format.JSONL

    config = Config(
        final_path = Path(__file__).parent / config.get('final_path'),
        temp_path = Path(__file__).parent / config.get('temp_path'),
        format = config.get('canvas_format'),
        canvas_format = canvas_format
    )

    return config
