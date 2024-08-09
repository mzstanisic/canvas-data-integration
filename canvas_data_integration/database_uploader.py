"""
database_uploader.py

"""
import csv
import config
import logging
import oracledb
from pathlib import Path

logger = logging.getLogger(__name__)


def update_table_with_csv(config: dict, csv_file: Path):
    """
    Update or insert records from the CSV file into the database table.
    """
    # adjust the number of rows to be inserted in each iteration
    # to meet your memory and performance requirements
    batch_size = 10000

    sql = config.canvas_tables.get(csv_file.stem).get("db_query")
    num_columns = len(config.canvas_tables.get(csv_file.stem).get("fields"))

    with oracledb.connect(
        user=config.oracle_username,
        password=config.oracle_password,
        host=config.db_host,
        port=config.db_port,
        service_name=config.db_service
    ) as connection:

        with connection.cursor() as cursor:
            
            with open(csv_file, 'r', encoding="utf-8") as csv_stream:
                csv_reader = csv.reader(csv_stream, delimiter=',')

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
                    if len(data) % batch_size == 0:
                        cursor.executemany(sql, data, batcherrors=True, arraydmlrowcounts=True)
                        data = []
                        records_affected += sum(cursor.getarraydmlrowcounts())
                if data:
                    cursor.executemany(sql, data, batcherrors=True, arraydmlrowcounts=True)
                    records_affected += sum(cursor.getarraydmlrowcounts())

            connection.commit()
            logger.info(f"Table [canvas_{csv_file.stem}] had [{records_affected}] rows updated or inserted.")

            for error in cursor.getbatcherrors():
                logger.error(f"Error {error.message} at row offset {error.offset}")


def main(config: dict) -> None:
    """
    Main function to process CSV files and update the database.
    """
    if not config.final_path.is_dir():
        logger.error(f"The path {config.final_path} is not a valid directory.")
        raise ValueError(f"The path {config.final_path} is not a valid directory.")
    
    csv_files = [file for file in config.final_path.glob("*.csv")]

    for csv_file in csv_files:
        try:
            if csv_file.stem in config.canvas_tables.keys():
                update_table_with_csv(config, csv_file)
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e


if __name__ == "__main__":
    user_config = config.get_config()
    main(user_config)