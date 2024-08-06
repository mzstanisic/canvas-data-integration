"""
config.py
"""
import os
import yaml
import logging
import datetime
from pathlib import Path
from dotenv import load_dotenv
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
    def __init__(self, 
                 final_path: Path,
                 temp_path: Path,
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
                 oracle_password: str):
        """
        Initializes the Config object with the given properties.

        :param final_path: The path where final output files are stored.
        :param temp_path: The path where temporary files are stored.
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

    def __repr__(self):
        """
        Provides a string representation of the Config object.
        """
        return (f"Config(final_path={self.final_path}, "
                f"temp_path={self.temp_path}, "
                f"format='{self.str_format}', "
                f"canvas_format='{self.canvas_format}', "
                f"canvas_tables='{self.canvas_tables}', "
                f"db_host='{self.db_host}', "
                f"db_port={self.db_port}, "
                f"db_service='{self.db_service}', "
                f"dap_api_url='{self.dap_api_url}', "
                f"dap_client_id='{self.dap_client_id}', "
                f"dap_client_secret='{self.dap_client_secret}', "
                f"oracle_username='{self.oracle_username}')")


def get_format(config_format: str = "JSONL") -> Format:
    """
    Accepts a selected output format for files from config.yml,
    and returns the corresponding DAP Canvas format type.
    
    :param1 config_format (dict): The desired format for the data files, specified in the config file: `CSV`, `JSONL`, `TSV`, or `Parquet`
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
            logger.warning(f"Specified format does not exist, expected one of (CSV, JSONL, TSV, Parquet): {config_format}")
            logger.info("Defaulting to JSONL.")
            return Format.JSONL
        

def validate_config(config_path: Path) -> dict:
    """
    """
    if not config_path.is_file():
        logger.error(f"The path {config_path} is not a valid file. Cannot proceed without database connection data. "
                         + "Add a config.yml file to the base directory.")
        raise FileNotFoundError(f"The path {config_path} is not a valid file. Cannot proceed without database connection data. "
                                + "Add a config.yml file to the base directory.")
    else:
        config = yaml.safe_load(open(config_path))
        
        if config.get("db_host") == None or config.get("db_port") == None or config.get("db_service") == None: 
            logger.error(f"Some or all config.yml database connection fields are empty. Update {config_path} to include all database connection fields.")
            raise Exception(f"Some or all config.yml database connection fields are empty. Update {config_path} to include all database connection fields.")
        
        if config.get("temp_path") == None or config.get("final_path") == None or config.get("canvas_format") == None:
            logger.warning(f"Some or all optional configuration fields in config.yml are empty. Using defaults.")
            config["temp_path"] = "../data/temp"
            config["final_path"] = "../data/final"
            config["canvas_format"] = Format.JSONL
            
        if config.get("canvas_tables") == None:
            logger.warning(f"Canvas table configuration dictionary in config.yml is empty. Using defaults.")
            config["canvas_tables"] = {
                'course_sections': ['key.id', 'value.name', 'value.course_id', 'value.workflow_state', 'meta.ts'],
                'courses': ['key.id', 'value.sis_source_id', 'value.name', 'value.enrollment_term_id', 'value.workflow_state', 'value.is_public', 'meta.ts'],
                'enrollment_terms': ['key.id', 'value.sis_source_id', 'value.workflow_state', 'meta.ts'],
                'enrollments': ['key.id', 'value.last_activity_at', 'value.total_activity_time', 'value.course_section_id', 'value.course_id', 'value.role_id', 'value.user_id', 'value.sis_pseudonym_id', 'value.workflow_state', 'value.type', 'meta.ts'],
                'pseudonyms': ['key.id', 'value.user_id', 'value.sis_user_id', 'value.unique_id', 'value.workflow_state', 'meta.ts'],
                'scores': ['key.id', 'value.current_score', 'value.enrollment_id', 'value.workflow_state', 'value.course_score', 'meta.ts'],
                'users': ['key.id', 'value.workflow_state', 'value.name', 'meta.ts'],
                }

        config["canvas_format"] = get_format(config.get("canvas_format"))
        return config


def validate_env(env_path: Path) -> dict:
    """
    get environment variables, try system first, then .env file
    """
    env = {
        'dap_api_url':None,
        'dap_client_id':None,
        'dap_client_secret':None,
        'oracle_username':None,
        'oracle_password':None
    }

    env["dap_api_url"] = os.environ.get("DAP_API_URL")
    env["dap_client_id"] = os.environ.get("DAP_CLIENT_ID")
    env["dap_client_secret"] = os.environ.get("DAP_CLIENT_SECRET")
    env["oracle_username"] = os.environ.get("ORACLE_USERNAME")
    env["oracle_password"] = os.environ.get("ORACLE_PASSWORD")

    # if no system environment variables, check file
    if None in env.values():
        logger.warning("No system environment variables found. Trying .env file...")

        if not env_path.is_file():
            logger.error(f"The path {env_path} is not a valid file. Cannot proceed without DAP and Oracle authentication data. "
                         + "Add system environment variables or a .env file to the resources directory.")
            raise FileNotFoundError(f"The path {env_path} is not a valid file. Cannot proceed without DAP and Oracle authentication data. "
                                    + "Add system environment variables or a .env file to the resources directory.")
        else:
            load_dotenv(env_path)
            env["dap_api_url"] = os.environ.get("DAP_API_URL")
            env["dap_client_id"] = os.environ.get("DAP_CLIENT_ID")
            env["dap_client_secret"] = os.environ.get("DAP_CLIENT_SECRET")
            env["oracle_username"] = os.environ.get("ORACLE_USERNAME")
            env["oracle_password"] = os.environ.get("ORACLE_PASSWORD")

            if None in env.values():
                logger.error(f"Some or all .env file variables are empty. Update {env_path} to include all necessary environment variables.")
                raise Exception(f"Some or all .env file variables are empty. Update {env_path} to include all necessary environment variables.")
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
    config_path = Path(__file__).parent / '../config.yml'
    config = validate_config(config_path)

    env_path = Path(__file__).parent / '../resources/.env'  # Development only. In production, use system environment variables
    env = validate_env(env_path)

    config = Config(
        final_path = Path(__file__).parent / config.get('final_path'),
        temp_path = Path(__file__).parent / config.get('temp_path'),
        str_format = config.get('canvas_format').name,          # string representation of format
        canvas_format = config.get('canvas_format'),            # actual format
        canvas_tables = config.get("canvas_tables"),
        db_host = config.get('db_host'),
        db_port = config.get('db_port'),
        db_service = config.get('db_service'),
        dap_api_url = env.get('dap_api_url'),
        dap_client_id = env.get('dap_client_id'),
        dap_client_secret = env.get('dap_client_secret'),
        oracle_username = env.get('oracle_username'),
        oracle_password = env.get('oracle_password')
    )

    return config


if __name__ == "__main__":
    user_config = get_config()
    print(user_config.__repr__)
