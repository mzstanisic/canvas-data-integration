"""
"""
import config
import shutil
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.dap_types import Format, Mode, SnapshotQuery, IncrementalQuery

logger = logging.getLogger(__name__)


async def get_canvas_data(table: str,
                          output_directory: str,
                          last_seen: datetime,
                          format: Format = Format.JSONL,
                          query_type: str = "incremental") -> None:
    """
    Retrieves data files from Canvas for the specified Canvas table.

    :param table: A Canvas table: https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#datasets
    :param output_directory: The output directory for the generated data files.
    :param format: The desired format for the data files: `CSV`, `JSONL`, `TSV`, or `Parquet`
    :param query_type: The desired query type: `incremental` or `snapshot`
    """
    mode = Mode.expanded # lays out nested fixed-cardinality fields into several columns

    # adjust output directory and mode based on the format
    match format:
        case Format.CSV:
            output_directory = Path(output_directory) / "csv"
        case Format.JSONL:
            output_directory = Path(output_directory) / "json"
            mode = None # JSON doesn't accept a mode parameter
        case Format.TSV:
            output_directory = Path(output_directory) / "tsv"
        case Format.Parquet:
            output_directory = Path(output_directory) / "parquet"

    # ensure output directory exists
    output_directory.mkdir(parents=True, exist_ok=True)

    async with DAPClient() as session:
        if query_type == "snapshot":
            query = SnapshotQuery(format=format, mode=mode)
        elif query_type == "incremental":
            query = IncrementalQuery(format=format, mode=mode, since=last_seen, until=None) 
        else:
            raise ValueError("Invalid query_type. Must be 'incremental' or 'snapshot'.")

        # fetch table data into web server
        query_object = await session.get_table_data("canvas", table, query)
        
        filenames = []
        for object in query_object.objects:
            filename = await session.download_object(object, output_directory, decompress=True)
            filenames.append(filename)

        p = Path(filenames[0])
        final_file = p.with_stem(table)

        if len(filenames) > 1:
            # merge files if more than one
            with open(final_file, 'wb') as wfd:
                for file in filenames:
                    with open(file, 'rb') as fd:
                        await asyncio.to_thread(shutil.copyfileobj, fd, wfd)
                        logger.info(f"Merged file: {final_file}") #TODO: This merges headers for CSV and TSV files, figure out how to resolve. Works fine for JSON
            
            # delete original files
            for file in filenames:
                file_path = Path(file)
                if file_path.is_file():
                    file_path.unlink()
                    logger.info(f"Deleted file: {file_path}")

        else:
            # rename the single file
            p.rename(p.with_stem(table))
            logger.info(f"Created file: {final_file}")


async def update_all(table_id: str, work_queue: asyncio.Queue, config: dict) -> None:
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
            logger.info(f"Task [{table_id}] beginning Canvas data pull for table: {table}.")
            await get_canvas_data(table, config.temp_path, last_seen=, config.canvas_format, query_type=) #TODO: fix after fixing function first
            logger.info(f"Task [{table_id}] completed Canvas data pull for table: {table}.")
        except Exception as e:
            logger.error(f"Task [{table_id}] failed for table: {table}. Error: {e}")
        finally:
            work_queue.task_done()  # mark the task as done in the queue


async def main(config: dict) -> None:
    """
    Main function that sets up the work queue, creates tasks for updating tables, 
    and handles exceptions.

    This function initializes an asyncio.Queue with a list of tables. It creates 
    tasks to process each table concurrently using the `update_all` function. It 
    collects results from all tasks and logs any exceptions encountered.

    :return: None
    """

    # utils.empty_temp(config.temp_path) #TODO: might not be necessary, if files get overwritten properly
    cfg_query_type = "incremental" # TODO: establish this in the config
    last_seen = datetime.now(timezone.utc) - timedelta(days=1) #TODO: establish in config as well?

    Credentials.create(client_id=config.dap_client_id, client_secret=config.dap_client_secret)

    work_queue = asyncio.Queue()

    # get tables defined in the config
    tables = config.canvas_tables.keys()

    # add tables to the queue
    for table in tables:
        await work_queue.put(table)

    # create and gather tasks for updating all tables
    tasks = [asyncio.create_task(update_all(table, work_queue, config)) for table in tables]
    
    # optionally handle exceptions for individual tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # handle exceptions if needed
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"An error occurred: {result}")


if __name__ == "__main__":
    user_config = config.get_config()
    asyncio.run(main(user_config))






# CONFIG_DIR = os.path.dirname(__file__) + '/../config.yml'
# config = yaml.safe_load(open(CONFIG_DIR))

# # TODO: local environment variables, remove when deployed
# load_dotenv(os.path.dirname(__file__) + '/../resources/.env')
# base_url: str = os.environ.get("DAP_API_URL")
# client_id: str = os.environ.get("DAP_CLIENT_ID")
# client_secret: str = os.environ.get("DAP_CLIENT_SECRET")

# TODO: on-machine environment variables, uncomment when deployed
# base_url: str = os.environ["DAP_API_URL"]
# client_id: str = os.environ["DAP_CLIENT_ID"]
# client_secret: str = os.environ["DAP_CLIENT_SECRET"]
# credentials = Credentials.create(client_id=client_id, client_secret=client_secret)

# prefix_path: str = os.path.dirname(__file__) + config.get('prefix_path') # TODO: rename all config variables
# final_path: str = os.path.dirname(__file__) + config.get('final_path') # TODO: rename all config variables
# output_format = utils.get_format(config) # TODO: rename all config variables
# cfg_query_type: str = "incremental" # TODO: establish this in the config
# prefix_path = "V:/BI Project/Temp Files"
# final_path = "V:/BI Project/New Files for Devin"


# timestamp returned by last snapshot or incremental query
# last_seen = datetime.now(timezone.utc) - timedelta(days=1)
# last_seen = datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc)



# def flatten_and_select_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
#     """
#     Flattens nested JSON data in a DataFrame and selects only specified columns.

#     :param df: The DataFrame containing JSON data.
#     :param columns: The list of columns to retain after flattening.
#     :return: A new DataFrame with flattened JSON data and selected columns.
#     """
#     flat_df = pd.json_normalize(df.to_dict(orient='records'))
#     print(f"Flattened DataFrame columns: {flat_df.columns.tolist()}")  # Print columns for debugging

#     selected_columns = [col for col in columns if col in flat_df.columns]
#     print(f"Columns to keep: {selected_columns}")  # Print columns to keep for debugging

#     filtered_df = flat_df[selected_columns]

#     return filtered_df

# async def read_json_to_dataframe(file_path: Path, encoding='utf-8') -> pd.DataFrame:
#     """
#     Reads a JSON file into a pandas DataFrame.

#     Assumes the JSON file is a JSON array.

#     :param1 file_path (Path): The path to the JSON file.
#     :param2 encoding (str): The encoding to use when reading the file.
#     :return: A pandas DataFrame containing the JSON data.
#     """
#     async with aiofiles.open(file_path, mode='r', encoding=encoding) as f:
#         content = await f.read()
#         try:
#             data = json.loads(content)
#             if not isinstance(data, list):
#                 logger.error(f"Expected a JSON array in file {file_path}, but found {type(data).__name__}.")
#                 return pd.DataFrame()
#             return pd.DataFrame(data)
#         except json.JSONDecodeError as e:
#             logger.error(f"Failed to decode JSON file {file_path}. Error: {e}")
#             return pd.DataFrame()

# async def process_file(json_file: Path, dataframes: dict, columns_to_keep: list) -> None:
#     """
#     Helper function to process a single JSON file and store the DataFrame in the dictionary.

#     :param1 json_file (Path): The path to the JSON file.
#     :param2 dataframes (dict): The dictionary to store DataFrames with file stems as keys.
#     :param3 columns_to_keep (list): The columns to keep for each table.
#     """
#     try:
#         df = await read_json_to_dataframe(json_file)
#         if not df.empty:
#             stem = json_file.stem
#             df = flatten_and_select_columns(df, columns_to_keep)
#             dataframes[stem] = df # dataframes might now be named enrollments.array, etc.
#             logger.info(f"Loaded JSON file {json_file} into DataFrame with key: {stem}.")
#         else:
#             logger.warning(f"No data loaded from {json_file}.")
#     except Exception as e:
#         logger.error(f"Failed to process file {json_file}. Error: {e}")


# async def load_and_process_json_files(directory: str, columns_mapping: Dict[str, List[str]]) -> dict:
#     """
#     Reads all JSON files in the specified directory into DataFrames, flattens them,
#     and selects only specified columns.

#     :param1 directory (str): The path to the directory containing JSON files.
#     :param2 columns_mapping (dict): A dictionary where keys are JSON file name stems and values are lists of columns to keep.
#     :return dataframes: A dictionary where keys are the JSON file name stems and values are filtered DataFrames.
#     """
#     path = Path(directory)
#     if not path.is_dir():
#         raise ValueError(f"The path {directory} is not a valid directory.")
    
#     json_files = [file for file in path.glob("*.array.json")]

#     dataframes = {}
#     tasks = []

#     for json_file in json_files:
#         stem = json_file.stem
#         columns_to_keep = columns_mapping.get(stem, []) # stem now has .array attached because of previous processing

#         tasks.append(asyncio.create_task(process_file(json_file, dataframes, columns_to_keep)))

#     await asyncio.gather(*tasks)

#     return dataframes


# async def wrap_json_in_array(file_path: Path, encoding='utf-8') -> None:
#     """
#     Wraps all JSON objects in a file into a JSON array.

#     Reads a file where each line is a JSON object, and writes a new file where all
#     JSON objects are enclosed in a JSON array.

#     :param1 file_path (Path): The path to the input JSON file.
#     :param2 encoding (str): The encoding to use when reading the file.
#     """
#     output_file_path = file_path.with_suffix('.array.json')

#     json_objects = []

#     async with aiofiles.open(file_path, mode='r', encoding=encoding) as infile:
#         async for line in infile:
#             line = line.strip()
#             if not line:
#                 continue  # skip empty lines

#             try:
#                 json_objects.append(json.loads(line))
#             except json.JSONDecodeError as e:
#                 logger.error(f"Failed to decode JSON line in file {file_path}. Error: {e}. Line content: {line}")

#     if json_objects:
#         async with aiofiles.open(output_file_path, mode='w', encoding=encoding) as outfile:
#             await outfile.write(json.dumps(json_objects, indent=2))
#         logger.info(f"Wrapped JSON objects into array and saved to {output_file_path}")
#     else:
#         logger.warning(f"No valid JSON objects found in {file_path}. No output file created.")


# async def wrap_all_json_files(directory: str, encoding='utf-8') -> None:
#     """
#     Wraps all JSON files in the specified directory into JSON arrays.

#     :param1 directory (str): The path to the directory containing JSON files.
#     :param2 encoding (str): The encoding to use when reading and writing files.
#     """
#     path = Path(directory)
#     if not path.is_dir():
#         raise ValueError(f"The path {directory} is not a valid directory.")

#     json_files = [file for file in path.glob("*.json")]
#     tasks = [wrap_json_in_array(file, encoding) for file in json_files]
    
#     await asyncio.gather(*tasks)


# async def main_df():
#     directory = r"C:\Users\stanisim\Desktop\canvas-data-integration\data\temp\json"
    
#     # define column mappings for each JSON file
#     columns_mapping = {
#         'enrollment_terms': ['key.id', 'value.sis_source_id', 'value.workflow_state', 'meta.ts'],
#         'courses': ['key.id', 'value.sis_source_id', 'value.name', 'value.enrollment_term_id', 'value.workflow_state', 'value.is_public', 'meta.ts'],
#         'course_sections': ['key.id', 'value.name', 'value.course_id', 'value.workflow_state', 'meta.ts'],
#         'enrollments': ['key.id', 'value.last_activity_at', 'value.total_activity_time', 'value.course_section_id', 'value.course_id', 'value.role_id', 'value.user_id', 'value.sis_pseudonym_id', 'value.workflow_state', 'value.type', 'meta.ts'],
#         'users': ['key.id', 'value.workflow_state', 'value.name', 'meta.ts'],
#         'pseudonyms': ['key.id', 'value.sis_user_id', 'value.unique_id', 'value.workflow_state', 'meta.ts'],
#         'scores': ['key.id', 'value.current_score', 'value.enrollment_id', 'value.workflow_state', 'value.course_score', 'meta.ts']
#     }

#     # wrap JSON files into arrays, since they don't come out of Canvas that way
#     await wrap_all_json_files(directory)

#     # load and process JSON files into DataFrames
#     dataframes = await load_and_process_json_files(directory, columns_mapping)
    
#     for key, df in dataframes.items():
#         print(f"DataFrame for {key}:\n{df.head()}")





# async def update_courses():
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             format=Format.CSV,
#             filter=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "courses", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources:
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "Courses", os.path.basename(components.path)
#             )
#             async with session.stream_resource(resource) as stream:
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         # Remove empty gzip
#         fileList = glob.glob(os.path.join(prefix_path, "Courses", "*.gz"))
#         for filename in fileList:
#             if os.stat(filename).st_size < 1 * 1024:
#                 os.remove(filename)
#         # Refresh file list, then extract from gzip and rename csv
#         new_file_list = glob.glob(os.path.join(prefix_path, "Courses", "*.gz"))
#         for new_file_name in new_file_list:
#             formatted_file_name = new_file_name.replace("\\", "/")
#         with gzip.open(formatted_file_name, "rb") as f_in:
#             with open(os.path.join(prefix_path, "temp-courses.csv"), "wb") as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # Merge existing file with new extract and remove duplicates
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-courses.csv"))
#         df2 = pd.read_csv(
#             os.path.join(prefix_path, "temp-courses.csv"),
#             dtype={
#                 "value.tab_configuration": object,
#                 "value.turnitin_comments": object,
#             },
#         )
#         df2.drop(
#             [
#                 "value.syllabus_body",
#                 "value.storage_quota",
#                 "value.integration_id",
#                 "value.lti_context_id",
#                 "value.sis_batch_id",
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.account_id",
#                 "value.grading_standard_id",
#                 "value.start_at",
#                 "value.sis_source_id",
#                 "value.group_weighting_scheme",
#                 "value.conclude_at",
#                 "value.is_public",
#                 "value.allow_student_wiki_edits",
#                 "value.default_wiki_editing_roles",
#                 "value.wiki_id",
#                 "value.allow_student_organized_groups",
#                 "value.course_code",
#                 "value.default_view",
#                 "value.abstract_course_id",
#                 "value.open_enrollment",
#                 "value.tab_configuration",
#                 "value.turnitin_comments",
#                 "value.self_enrollment",
#                 "value.license",
#                 "value.indexed",
#                 "value.restrict_enrollments_to_course_dates",
#                 "value.template_course_id",
#                 "value.replacement_course_id",
#                 "value.public_description",
#                 "value.self_enrollment_code",
#                 "value.self_enrollment_limit",
#                 "value.turnitin_id",
#                 "value.show_announcements_on_home_page",
#                 "value.home_page_announcement_limit",
#                 "value.latest_outcome_import_id",
#                 "value.grade_passback_setting",
#                 "value.template",
#                 "value.homeroom_course",
#                 "value.sync_enrollments_from_homeroom",
#                 "value.homeroom_course_id",
#                 "value.locale",
#                 "value.time_zone",
#                 "value.uuid",
#                 "value.settings.allow_student_discussion_editing",
#                 "value.settings.allow_student_discussion_topics",
#                 "value.settings.course_format",
#                 "value.settings.filter_speed_grader_by_student_group",
#                 "value.settings.hide_distribution_graphs",
#                 "value.settings.hide_final_grade",
#                 "value.settings.is_public_to_auth_users",
#                 "value.settings.lock_all_announcements",
#                 "value.settings.public_syllabus",
#                 "value.settings.public_syllabus_to_auth",
#                 "value.settings.restrict_student_future_view",
#                 "value.settings.restrict_student_past_view",
#                 "value.settings.syllabus_updated_at",
#                 "value.settings.usage_rights_required",
#                 "value.settings.allow_student_forum_attachments",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(
#             os.path.join(final_path, "canvas-courses.csv"), index=False
#         )


# async def update_course_sections():
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             format=Format.CSV,
#             filter=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "course_sections", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources:
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "Course Sections", os.path.basename(components.path)
#             )
#             async with session.stream_resource(resource) as stream:
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         with gzip.open(file_path, "rb") as f_in:
#             with open(
#                 os.path.join(prefix_path, "temp-course_sections.csv"), "wb"
#             ) as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # merge existing file with new extract
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-course_sections.csv"))
#         df2 = pd.read_csv(os.path.join(prefix_path, "temp-course_sections.csv"))
#         df2.drop(
#             [
#                 "value.integration_id",
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.sis_batch_id",
#                 "value.start_at",
#                 "value.end_at",
#                 "value.sis_source_id",
#                 "value.default_section",
#                 "value.accepting_enrollments",
#                 "value.restrict_enrollments_to_section_dates",
#                 "value.nonxlist_course_id",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(
#             os.path.join(final_path, "canvas-course_sections.csv"), index=False
#         )


# async def update_enrollment_terms(output_format=Format.CSV):
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             # format=Format.CSV,
#             format=output_format,
#             # filter=None,
#             mode=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "enrollment_terms", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources.values():
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "Enrollment Terms", os.path.basename(components.path)
#             )
#             # async with session.stream_resource(resource) as stream:
#             async for stream in session.stream_resource(resource):
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         with gzip.open(file_path, "rb") as f_in:
#             with open(
#                 os.path.join(prefix_path, "temp-enrollment_terms.csv"), "wb"
#             ) as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # merge existing file with new extract
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-enrollment_terms.csv"))
#         df2 = pd.read_csv(os.path.join(prefix_path, "temp-enrollment_terms.csv"))
#         df2.drop(
#             [
#                 # "value.integration_id",
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.sis_batch_id",
#                 "value.start_at",
#                 "value.end_at",
#                 # "value.sis_source_id",
#                 "value.grading_period_group_id",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(
#             os.path.join(final_path, "canvas-enrollment_terms.csv"), index=False
#         )


# async def update_enrollments():
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             format=Format.CSV,
#             filter=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "enrollments", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources:
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "enrollments", os.path.basename(components.path)
#             )
#             async with session.stream_resource(resource) as stream:
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         # Remove empty gzip
#         fileList = glob.glob(os.path.join(prefix_path, "Enrollments", "*.gz"))
#         for filename in fileList:
#             if os.stat(filename).st_size < 1 * 1024:
#                 os.remove(filename)
#         # Refresh file list, then extract from gzip and rename csv
#         new_file_list = glob.glob(os.path.join(prefix_path, "Enrollments", "*.gz"))
#         for new_file_name in new_file_list:
#             formatted_file_name = new_file_name.replace("\\", "/")
#         with gzip.open(formatted_file_name, "rb") as f_in:
#             with open(os.path.join(prefix_path, "temp-enrollments.csv"), "wb") as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # merge existing file with new extract
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-enrollments.csv"))
#         df2 = pd.read_csv(os.path.join(prefix_path, "temp-enrollments.csv"))
#         # df1.drop(['value.start_at','value.end_at','value.completed_at','value.grade_publishing_status','value.associated_user_id','value.self_enrolled','value.limit_privileges_to_course_section','value.total_activity_time','value.last_attended_at'],axis=1,inplace=True)
#         df2.drop(
#             [
#                 "value.sis_batch_id",
#                 "value.user_id",
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.role_id",
#                 "value.start_at",
#                 "value.end_at",
#                 "value.completed_at",
#                 "value.grade_publishing_status",
#                 "value.associated_user_id",
#                 "value.self_enrolled",
#                 "value.limit_privileges_to_course_section",
#                 "value.total_activity_time",
#                 "value.last_attended_at",
#                 "value.type",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(
#             os.path.join(final_path, "canvas-enrollments.csv"), index=False
#         )


# async def update_pseudonyms():
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             format=Format.CSV,
#             filter=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "pseudonyms", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources:
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "Pseudonyms", os.path.basename(components.path)
#             )
#             async with session.stream_resource(resource) as stream:
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         with gzip.open(file_path, "rb") as f_in:
#             with open(os.path.join(prefix_path, "temp-pseudonyms.csv"), "wb") as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # merge existing file with new extract
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-pseudonyms.csv"))
#         df2 = pd.read_csv(os.path.join(prefix_path, "temp-pseudonyms.csv"))
#         df2.drop(
#             [
#                 "value.deleted_at",
#                 "value.integration_id",
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.account_id",
#                 "value.sis_batch_id",
#                 "value.unique_id",
#                 "value.login_count",
#                 "value.failed_login_count",
#                 "value.last_request_at",
#                 "value.last_login_at",
#                 "value.current_login_at",
#                 "value.last_login_ip",
#                 "value.current_login_ip",
#                 "value.authentication_provider_id",
#                 "value.position",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(
#             os.path.join(final_path, "canvas-pseudonyms.csv"), index=False
#         )


# async def update_scores():
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             format=Format.CSV,
#             filter=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "scores", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources:
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "scores", os.path.basename(components.path)
#             )
#             async with session.stream_resource(resource) as stream:
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         # Remove empty gzip
#         fileList = glob.glob(os.path.join(prefix_path, "Scores", "*.gz"))
#         for filename in fileList:
#             if os.stat(filename).st_size < 1 * 1024:
#                 os.remove(filename)
#         # Refresh file list, then extract from gzip and rename csv
#         new_file_list = glob.glob(os.path.join(prefix_path, "Scores", "*.gz"))
#         for new_file_name in new_file_list:
#             formatted_file_name = new_file_name.replace("\\", "/")
#         with gzip.open(formatted_file_name, "rb") as f_in:
#             with open(os.path.join(prefix_path, "temp-scores.csv"), "wb") as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # merge existing file with new extract
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-scores2.csv"))
#         df2 = pd.read_csv(
#             os.path.join(prefix_path, "temp-scores.csv"), dtype={"meta.ts": str}
#         )
#         df2.drop(
#             [
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.assignment_group_id",
#                 "value.grading_period_id",
#                 "value.final_score",
#                 "value.unposted_current_score",
#                 "value.unposted_final_score",
#                 "value.current_points",
#                 "value.unposted_current_points",
#                 "value.final_points",
#                 "value.unposted_final_points",
#                 "value.override_score",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(
#             os.path.join(final_path, "canvas-scores2.csv"), index=False
#         )


# async def update_users():
#     async with DAPClient() as session:
#         query = IncrementalQuery(
#             format=Format.CSV,
#             filter=None,
#             since=last_seen,
#             until=None,
#         )
#         result = await session.get_table_data("canvas", "users", query)
#         resources = await session.get_resources(result.objects)
#         for resource in resources:
#             components: ParseResult = urlparse(str(resource.url))
#             file_path = os.path.join(
#                 prefix_path, "Users", os.path.basename(components.path)
#             )
#             async with session.stream_resource(resource) as stream:
#                 async with aiofiles.open(file_path, "wb") as file:
#                     # save gzip data to file without decompressing
#                     async for chunk in stream.iter_chunked(64 * 1024):
#                         await file.write(chunk)
#         with gzip.open(file_path, "rb") as f_in:
#             with open(os.path.join(prefix_path, "temp-users.csv"), "wb") as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         # merge existing file with new extract
#         df1 = pd.read_csv(os.path.join(final_path, "canvas-users.csv"))
#         df2 = pd.read_csv(os.path.join(prefix_path, "temp-users.csv"))
#         df2.drop(
#             [
#                 "value.deleted_at",
#                 "value.storage_quota",
#                 "value.lti_context_id",
#                 "value.created_at",
#                 "value.updated_at",
#                 "value.workflow_state",
#                 "value.sortable_name",
#                 "value.avatar_image_url",
#                 "value.avatar_image_source",
#                 "value.avatar_image_updated_at",
#                 "value.short_name",
#                 "value.last_logged_out",
#                 "value.pronouns",
#                 "value.merged_into_user_id",
#                 "value.locale",
#                 "value.time_zone",
#                 "value.uuid",
#                 "value.school_name",
#                 "value.school_position",
#                 "value.public",
#             ],
#             axis=1,
#             inplace=True,
#         )
#         df_merged = pd.concat([df1, df2])
#         dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
#         dupes_removed.to_csv(os.path.join(final_path, "canvas-users.csv"), index=False)
