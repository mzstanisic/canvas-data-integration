"""
Retrieve config values from the config.yml file in the base project directory,
and environment variables from either the system or an included .env file.
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
import yaml
from dotenv import load_dotenv
from dap.dap_types import Format, Mode
import utils

# setup the logger
logger = logging.getLogger(__name__)
log_path = Path(__file__).parent / "../logs/"
log_path.mkdir(parents=True, exist_ok=True)

# config the logger
logging.basicConfig(
    filename=log_path / datetime.now().strftime("%Y-%m-%d.log"),
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s :: %(levelname)-8s :: %(module)s.%(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Config:
    """
    Class for the user Config object. Contains information from the config.yaml
    and .env file if present.
    """

    def __init__(
        self,
        final_path: Path,
        temp_path: Path,
        batch_size: int,
        past_days: int,
        log_retention_period: int,
        str_format: str,
        canvas_format: Format,
        canvas_tables: dict,
        db_host: str,
        db_port: int,
        db_service: str,
        dap_api_url: str,
        dap_client_id: str,
        dap_client_secret: str,
        oracle_username: str,
        oracle_password: str,
    ):
        """
        Initializes the Config object with the given properties.

        :param final_path: The path where final output files are stored.
        :param temp_path: The path where temporary files are stored.
        :param batch_size: The batch size for merging records into the database.
        :param past_days: How many days in the past to search for updated records.
        :param log_retention_period: How many days to keeps logs for.
        :param str_format: The format for the Canvas data files (string representation).
        :param canvas_format: The format for the Canvas data files.
        :param db_host: The host address of the database.
        :param db_port: The port number of the database.
        :param db_service: The service name of the database.
        :param dap_api_url: The API URL for DAP.
        :param dap_client_id: The client ID for DAP.
        :param dap_client_secret: The client secret for DAP.
        :param oracle_username: The username for Oracle database.
        :param oracle_password: The password for Oracle database.
        """
        self.final_path = final_path
        self.temp_path = temp_path
        self.batch_size = batch_size or 10000
        self.past_days = past_days or 3
        self.log_retention_period = log_retention_period or 30
        self.str_format = str_format
        self.canvas_format = canvas_format
        self.canvas_tables = canvas_tables
        self.db_host = db_host
        self.db_port = db_port
        self.db_service = db_service
        self.dap_api_url = dap_api_url
        self.dap_client_id = dap_client_id
        self.dap_client_secret = dap_client_secret
        self.oracle_username = oracle_username
        self.oracle_password = oracle_password

        # canvas_extractor.py: lays out nested fixed-cardinality fields into several columns
        # JSON and Parquet don't accept a mode parameter
        self.canvas_mode = (
            None if canvas_format in {Format.JSONL, Format.Parquet} else Mode.expanded
        )

        # for incremental DAP calls, the date range of past x days to retrieve data from
        self.last_seen = datetime.now(timezone.utc) - timedelta(days=past_days)

    def __repr__(self):
        """
        Provides a string representation of the Config object.
        """
        return (
            f"Config(final_path={self.final_path}\n"
            f"temp_path={self.temp_path}\n"
            f"batch_size={self.batch_size}\n"
            f"past_days={self.past_days}\n"
            f"log_retention_period={self.log_retention_period}\n"
            f"format='{self.str_format}'\n"
            f"canvas_format='{self.canvas_format}'\n"
            f"canvas_mode='{self.canvas_mode}'\n"
            f"canvas_tables='{self.canvas_tables}'\n"
            f"db_host='{self.db_host}'\n"
            f"db_port={self.db_port}\n"
            f"db_service='{self.db_service}'\n"
            f"dap_api_url='{self.dap_api_url}'\n"
            f"dap_client_id='{self.dap_client_id}'\n"
            f"dap_client_secret='{self.dap_client_secret}'\n"
            f"canvas_mode={self.canvas_mode}\n"
            f"last_seen={self.last_seen}\n"
            f"oracle_username='{self.oracle_username}')"
        )


def get_format(config_format: str = "JSONL") -> Format:
    """
    Accepts a selected output format for files from config.yml,
    and returns the corresponding DAP Canvas format type.

    :param1 config_format (dict): The desired format for the data files, specified
    in the config file: `CSV`, `JSONL`, `TSV`, or `Parquet`
    :returns: Corresponding DAP format type.
    """

    config_format = config_format or "JSONL"
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
            logger.warning(
                "Specified format does not exist, expected one of (CSV, JSONL, TSV, Parquet): %s",
                config_format,
            )
            logger.info("Defaulting to JSONL.")
            return Format.JSONL


def validate_config(config_path: Path) -> dict:
    """
    Retrieve config settings from `config.yml` and validate them.

    :param1 config_path (Path): The path to the config file.
    :return (dict): A dictionary with values from the config file.
    """

    # check that the config path provided is a file
    if not config_path.is_file():
        logger.error(
            "The path %s is not a valid file. Cannot proceed without Canvas table data. "
            + "Add a config.yml file to the base directory.",
            config_path,
        )
        raise FileNotFoundError(
            f"The path {config_path} is not a valid file. Cannot proceed without Canvas table data. "
            + "Add a config.yml file to the base directory."
        )

    # load the config.yml file
    config = yaml.safe_load(open(config_path, encoding="utf-8"))

    # check each optional configuration option and provide a default if it is empty
    if config.get("temp_path") is None:
        config["temp_path"] = "../data/temp"
        logger.warning(
            "Configuration field 'temp_path' in config.yml is empty. Using default: %s",
            config["temp_path"],
        )
    if config.get("final_path") is None:
        config["final_path"] = "../data/final"
        logger.warning(
            "Configuration field 'final_path' in config.yml is empty. Using default: %s",
            config["final_path"],
        )
    if config.get("canvas_format") is None:
        config["canvas_format"] = Format.JSONL
        logger.warning(
            "Configuration field 'canvas_format' in config.yml is empty. Using default: %s",
            config["canvas_format"],
        )
    else:
        config["canvas_format"] = get_format(config.get("canvas_format"))
    if config.get("batch_size") is None:
        config["batch_size"] = 10000
        logger.warning(
            "Configuration field 'batch_size' in config.yml is empty. Using default: %s",
            config["batch_size"],
        )
    if config.get("past_days") is None:
        config["past_days"] = 3
        logger.warning(
            "Configuration field 'past_days' in config.yml is empty. Using default: %s",
            config["past_days"],
        )
    if config.get("log_retention_period") is None:
        config["log_retention_period"] = 30
        logger.warning(
            "Configuration field 'log_retention_period' in config.yml is empty. Using default: %s",
            config["log_retention_period"],
        )

    if config.get("canvas_tables") is None:
        logger.error(
            "'canvas_tables' configuration dictionary in config.yml is empty. Cannot proceed."
        )
        raise RuntimeError(
            "'canvas_tables' configuration dictionary in config.yml is empty. Cannot proceed."
        )

    # check the mandatory canvas_tables entry in the config
    if isinstance(config.get("canvas_tables"), dict):
        for key in config.get("canvas_tables").keys():
            table = config.get("canvas_tables").get(key)
            if isinstance(table, dict):
                if (
                    table.get("query_type") is None
                    or table.get("fields") is None
                    or table.get("db_query") is None
                ):
                    logger.error(
                        "'canvas_tables' table '%s' configuration dictionary in config.yml is missing one of 'query_type': (incremental or snapshot), "
                        + "'fields': [list of canvas table fields to retrieve], or 'db_query': (merge query for the Oracle table destination). Cannot proceed.",
                        key,
                    )
                    raise RuntimeError(
                        f"'canvas_tables' table '{key}' configuration dictionary in config.yml is missing one of 'query_type': (incremental or snapshot), "
                        + "'fields': [list of canvas table fields to retrieve], or 'db_query': (merge query for the Oracle table destination). Cannot proceed."
                    )
            else:
                logger.error(
                    "'canvas_tables' table '%s' configuration dictionary in config.yml is not structured as a dictionary. Cannot proceed.",
                    key,
                )
                raise RuntimeError(
                    f"'canvas_tables' table '{key}' configuration dictionary in config.yml is not structured as a dictionary. Cannot proceed."
                )
    else:
        logger.error(
            "'canvas_tables' configuration dictionary in config.yml is not structured as a dictionary. Cannot proceed."
        )
        raise RuntimeError(
            "'canvas_tables' configuration dictionary in config.yml is not structured as a dictionary. Cannot proceed."
        )

    return config


def validate_env(env_path: Path) -> dict:
    """
    Retrieve environment variables trying the system first, then a `.env` file.
    Finally, validate them.

    :param1 env_path (Path): The path to the environment file.
    :return (dict): A dictionary with values from the environment file.
    """
    env = {
        "dap_api_url": None,
        "dap_client_id": None,
        "dap_client_secret": None,
        "db_host": None,
        "db_port": None,
        "db_service": None,
        "oracle_username": None,
        "oracle_password": None,
    }

    env["dap_api_url"] = os.environ.get("DAP_API_URL")
    env["dap_client_id"] = os.environ.get("DAP_CLIENT_ID")
    env["dap_client_secret"] = os.environ.get("DAP_CLIENT_SECRET")
    env["db_host"] = os.environ.get("DB_HOST")
    env["db_port"] = os.environ.get("DB_PORT")
    env["db_service"] = os.environ.get("DB_SERVICE")
    env["oracle_username"] = os.environ.get("ORACLE_USERNAME")
    env["oracle_password"] = os.environ.get("ORACLE_PASSWORD")

    # if no system environment variables, check file
    if None in env.values():
        logger.warning("No system environment variables found. Trying .env file...")

        if not env_path.is_file():
            logger.error(
                "The path %s is not a valid file. Cannot proceed without DAP and Oracle authentication data. "
                + "Add system environment variables or a .env file to the resources directory.",
                env_path,
            )
            raise FileNotFoundError(
                f"The path {env_path} is not a valid file. Cannot proceed without DAP and Oracle authentication data. "
                + "Add system environment variables or a .env file to the resources directory."
            )
        else:
            load_dotenv(env_path)
            env["dap_api_url"] = os.environ.get("DAP_API_URL")
            env["dap_client_id"] = os.environ.get("DAP_CLIENT_ID")
            env["dap_client_secret"] = os.environ.get("DAP_CLIENT_SECRET")
            env["db_host"] = os.environ.get("DB_HOST")
            env["db_port"] = os.environ.get("DB_PORT")
            env["db_service"] = os.environ.get("DB_SERVICE")
            env["oracle_username"] = os.environ.get("ORACLE_USERNAME")
            env["oracle_password"] = os.environ.get("ORACLE_PASSWORD")

            if None in env.values():
                logger.error(
                    "Some or all .env file variables are empty. Update %s to include all necessary environment variables.",
                    env_path,
                )
                raise RuntimeError(
                    f"Some or all .env file variables are empty. Update {env_path} to include all necessary environment variables."
                )
            else:
                logger.info("Retrieved environment variables from .env file.")
                return env
    else:
        logger.info("Obtained environment variables from system.")
        return env


def get_config() -> Config:
    """
    Looks for a valid config.yml file in the base project directory.
    If there is none, uses defaults.

    :returns: A Config object that includes a data format and paths.
    """
    config_path = Path(__file__).parent / "../config.yml"
    config = validate_config(config_path)

    # Development only. In production, use system environment variables
    env_path = Path(__file__).parent / "../resources/.env"
    env = validate_env(env_path)

    config = Config(
        final_path=Path(__file__).parent / config.get("final_path"),
        temp_path=Path(__file__).parent / config.get("temp_path"),
        batch_size=config.get("batch_size"),
        past_days=config.get("past_days"),
        log_retention_period=config.get("log_retention_period"),
        str_format=config.get("canvas_format").name,  # string representation of format
        canvas_format=config.get("canvas_format"),  # actual format
        canvas_tables=config.get("canvas_tables"),
        db_host=env.get("db_host"),
        db_port=env.get("db_port"),
        db_service=env.get("db_service"),
        dap_api_url=env.get("dap_api_url"),
        dap_client_id=env.get("dap_client_id"),
        dap_client_secret=env.get("dap_client_secret"),
        oracle_username=env.get("oracle_username"),
        oracle_password=env.get("oracle_password"),
    )

    # clean old logs
    utils.clean_old_logs(log_path, config.log_retention_period)

    return config


if __name__ == "__main__":
    user_config = get_config()
    print(f"\n-----config.py-----\n{user_config.__repr__}")
