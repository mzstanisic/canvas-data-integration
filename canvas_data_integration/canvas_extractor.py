"""
Retrieves data files from DAP in JSONL format and outputs them to the data/temp folder.
"""

import datetime
import shutil
import asyncio
import logging
from pathlib import Path
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.dap_types import Format, Mode, SnapshotQuery, IncrementalQuery
import utils
import config

logger = logging.getLogger(__name__)


async def get_canvas_data(
    table: str,
    output_directory: Path,
    last_seen: datetime,
    data_format: Format = Format.JSONL,
    mode: Mode = None,
    query_type: str = "incremental",
) -> None:
    """
    Retrieves data files from Canvas for the specified Canvas table.

    :param table: A Canvas table:
    https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#datasets
    :param output_directory: The output directory for the generated data files.
    :param format: The desired format for the data files: `CSV`, `JSONL`, `TSV`, or `Parquet`
    :param query_type: The desired query type: `incremental` or `snapshot`
    """

    output_directory = output_directory / data_format.name.lower()

    # ensure output directory exists
    output_directory.mkdir(parents=True, exist_ok=True)

    async with DAPClient() as session:
        if query_type == "snapshot":
            query = SnapshotQuery(format=data_format, mode=mode)
        elif query_type == "incremental":
            query = IncrementalQuery(
                format=data_format, mode=mode, since=last_seen, until=None
            )
        else:
            logger.error("Invalid query_type: %s. Must be 'incremental' or 'snapshot'.", query_type)
            raise ValueError(f"Invalid query_type: {query_type}. Must be 'incremental' or 'snapshot'.")

        # fetch table data into web server
        query_object = await session.get_table_data("canvas", table, query)

        filenames = []
        for i_object in query_object.objects:
            filename = await session.download_object(
                i_object, output_directory, decompress=True
            )  # outputs in UTF-8 encoding
            filenames.append(filename)

        p = Path(filenames[0])
        final_file = p.with_stem(table)

        # TODO: This also merges headers for CSV and TSV files,
        # figure out how to resolve. Works fine for JSON

        if len(filenames) > 1:
            # merge files if more than one
            with open(final_file, "wb") as wfd:
                for file in filenames:
                    with open(file, "rb") as fd:
                        await asyncio.to_thread(shutil.copyfileobj, fd, wfd)
                        logger.info("Merged file: %s", final_file)

            # delete original files
            for file in filenames:
                file_path = Path(file)
                if file_path.is_file():
                    file_path.unlink()
                    logger.info("Deleted file: %s", file_path)

        else:
            # rename the single file
            p.rename(p.with_stem(table))
            logger.info("Created file: %s", final_file)


async def update_all(work_queue: asyncio.Queue, user_config: dict) -> None:
    """
    Processes tasks from the work queue to update data for the specified table.

    This function retrieves tasks from the `work_queue`, performs data retrieval
    for each table using `get_canvas_data`, and handles any exceptions that occur
    during the process. It ensures that each task is marked as done in the queue
    after processing.

    :param table_id: A unique identifier for the task. This is used for logging purposes.
    :param work_queue: An asyncio.Queue instance containing the list of tables to be processed.
    :return: None
    """

    while not work_queue.empty():
        table = await work_queue.get()

        try:
            logger.info(
                "Task [%s] beginning Canvas data pull for table: %s.", table, table
            )
            await get_canvas_data(
                table,
                user_config.temp_path,
                user_config.last_seen,
                user_config.canvas_format,
                user_config.canvas_mode,
                user_config.canvas_tables.get(table).get("query_type"),
            )
            logger.info(
                "Task [%s] completed Canvas data pull for table: %s.", table, table
            )
        except Exception as e:
            logger.error("Task [%s] failed for table: %s. Error: %s", table, table, e)
            raise RuntimeError(f"Task [{table}] failed for table: {table}") from e
        finally:
            work_queue.task_done()  # mark the task as done in the queue


async def main(user_config: dict) -> None:
    """
    Main function that sets up the work queue, creates tasks for updating tables,
    and handles exceptions.

    This function initializes an asyncio.Queue with a list of tables. It creates
    tasks to process each table concurrently using the `update_all` function. It
    collects results from all tasks and logs any exceptions encountered.

    :param1 user_config (dict): The user config.
    :return: None
    """

    # empty temp folders
    utils.empty_temp(user_config.temp_path)

    # create DAP credentials
    Credentials.create(
        client_id=user_config.dap_client_id, client_secret=user_config.dap_client_secret
    )

    # intialize work queue
    work_queue = asyncio.Queue()

    # get tables defined in the config
    tables = user_config.canvas_tables.keys()

    # add tables to the queue
    for table in tables:
        await work_queue.put(table)

    # create and gather tasks for updating all tables
    tasks = [
        asyncio.create_task(update_all(work_queue, user_config)) for table in tables
    ]

    # optionally handle exceptions for individual tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # handle exceptions if needed
    for result in results:
        if isinstance(result, Exception):
            logger.error("An error occurred: %s", result)
            raise result


if __name__ == "__main__":
    run_config = config.get_config()
    asyncio.run(main(run_config))
