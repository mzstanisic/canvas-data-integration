import os
import datetime
import logging
from dap.dap_types import Format

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.dirname(__file__) + "/../logs/" + datetime.datetime.now().strftime('%Y-%m-%d.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s %(module)s.py - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

def get_format(config_format={"output_format":"csv"}):
    """The get_format function accepts the selected output format for files from config.yml,
    and returns the corresponding format type."""

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
