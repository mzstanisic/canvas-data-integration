"""
Uses predefined SQL statements to merge pulled records from Canvas CSV files into
predefined Oracle tables.
"""

import csv
import logging
from pathlib import Path
import oracledb
import config

logger = logging.getLogger(__name__)


def update_table_with_csv(user_config: dict, csv_file: Path) -> None:
    """
    Update or insert records from the CSV file into the database table.

    :param1 user_config (dict): The user config.
    :param2 csv_fiel (Path): The Path to the csv_file.
    :return: None
    """

    sql = user_config.canvas_tables.get(csv_file.stem).get("db_query")
    num_columns = len(user_config.canvas_tables.get(csv_file.stem).get("fields"))

    with oracledb.connect(
        user=user_config.db_username,
        password=user_config.db_password,
        host=user_config.db_host,
        port=user_config.db_port,
        service_name=user_config.db_service,
    ) as connection:

        with connection.cursor() as cursor:

            with open(csv_file, "r", encoding="utf-8") as csv_stream:
                csv_reader = csv.reader(csv_stream, delimiter=",")

                # skip the header row
                next(csv_reader)

                data = []
                parameters = []
                records_affected = 0
                for line in csv_reader:
                    i = 0
                    while i < num_columns:
                        parameters.append(line[i])
                        i += 1

                    data_tuple = tuple(parameters)
                    data.append(data_tuple)
                    parameters = []
                    if len(data) % user_config.batch_size == 0:
                        cursor.executemany(
                            sql, data, batcherrors=True, arraydmlrowcounts=True
                        )
                        data = []
                        records_affected += sum(cursor.getarraydmlrowcounts())
                if data:
                    cursor.executemany(
                        sql, data, batcherrors=True, arraydmlrowcounts=True
                    )
                    records_affected += sum(cursor.getarraydmlrowcounts())

            connection.commit()
            logger.info(
                "Table [canvas_%s] had [%s] rows updated or inserted.",
                csv_file.stem,
                records_affected,
            )

            for error in cursor.getbatcherrors():
                logger.error("Error %s at row offset %s", error.message, error.offset)


def main(user_config: dict) -> None:
    """
    Main function to process CSV files and update the database.

    :param1 user_config (dict): The user config.
    :return: None
    """
    if not user_config.final_path.is_dir():
        logger.error("The path %s is not a valid directory.", user_config.final_path)
        raise ValueError(f"The path {user_config.final_path} is not a valid directory.")

    csv_files = [file for file in user_config.final_path.glob("*.csv")]

    for csv_file in csv_files:
        try:
            if csv_file.stem in user_config.canvas_tables.keys():
                update_table_with_csv(user_config, csv_file)
        except Exception as e:
            logger.error("An error occurred: %s", e)
            raise e


if __name__ == "__main__":
    run_config = config.get_config()
    main(run_config)
