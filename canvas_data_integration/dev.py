# file needs to start at current term
import os
import asyncio
import gzip
import shutil
import glob
from datetime import datetime, timedelta, timezone
from urllib.parse import ParseResult, urlparse
import pandas as pd
import canvas_data_integration.utils as utils
import aiofiles
import yaml
from dotenv import load_dotenv, find_dotenv
from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.dap_types import Format, IncrementalQuery, SnapshotQuery
from pathlib import Path


import codecs

def try_encodings(file_path):
    encodings = ['utf-8', 'utf-16', 'iso-8859-1', 'cp1252']
    for encoding in encodings:
        try:
            with codecs.open(file_path, 'r', encoding) as f:
                f.read()
            print(f"Encoding {encoding} works for {file_path}")
            return encoding
        except UnicodeDecodeError:
            print(f"Encoding {encoding} failed for {file_path}")
    print("No suitable encoding found")
    return None

# Example usage
file_path = r"C:\Users\stanisim\Desktop\canvas-targetx-data-integration\data\temp\json\courses.json"
encoding = try_encodings(file_path)


quit()

# print(Path(__file__).parent)
# print((__file__).parent) Path().pure
# CONFIG_DIR = Path(__file__).parent / "../config.yml"
# config = yaml.safe_load(open(CONFIG_DIR))
# print(os.path.dirname(__file__) / "../config.yml")

# CONFIG_DIR = os.path.dirname(__file__) + '/../config.yml'
# config = yaml.safe_load(open(CONFIG_DIR))
# # TODO: local environment variables, remove when deployed
# load_dotenv(os.path.dirname(__file__) + '/../resources/.env')
# base_url: str = os.environ.get("DAP_API_URL")
# client_id: str = os.environ.get("DAP_CLIENT_ID")
# client_secret: str = os.environ.get("DAP_CLIENT_SECRET")

# # TODO: on-machine environment variables, uncomment when deployed
# # base_url: str = os.environ["DAP_API_URL"]
# # client_id: str = os.environ["DAP_CLIENT_ID"]
# # client_secret: str = os.environ["DAP_CLIENT_SECRET"]
# credentials = Credentials.create(client_id=client_id, client_secret=client_secret)

# prefix_path: str = os.path.dirname(__file__) + config.get('prefix_path')
# final_path: str = os.path.dirname(__file__) + config.get('final_path')
# output_format = helper.get_format(config)
# # prefix_path = "V:/BI Project/Temp Files"
# # final_path = "V:/BI Project/New Files for Devin"


# # timestamp returned by last snapshot or incremental query
# last_seen = datetime.now(timezone.utc) - timedelta(days=1)
# # last_seen = datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc)

# # obtaining the latest schema
# output_directory: str = os.getcwd() + "/test_data"
# async def obtain_enrollment_schema(output_format=Format.CSV):
#     async with DAPClient() as session:
#         tables = await session.get_tables("canvas")
#         for table in tables:
#             await session.download_table_schema("canvas", table, output_directory)

# async def get_canvas_data(tables=["enrollment_terms","courses"]):
#     async with DAPClient() as session:
#         query = SnapshotQuery(format=Format.CSV, mode=None)

#         object_list = []

#         for table in tables:
#             query_object = await session.get_table_data(
#                 "canvas", table, query
#             )
#             object_list.extend(query_object.objects)


#         # query_resource_dict = await session.get_resources(query_object.objects) # returns a dict
#         # query_resource_list = query_resource_dict.values()
#         # result = await session.download_resources(query_resource_list, output_directory, decompress=True)
#         result = await session.download_objects(object_list, output_directory, decompress=True)
#         return result
#         # print(query_object.objects[0].id) #gets object ID

#         query_resource = await session.get_resources(query_object.objects) # returns a dict
#         # query_resource = query_resource.values()
#         for resource in query_resource.values():
#             async_generator = session.stream_resource(resource)
        
#             async for stream_reader in async_generator:
#                 # print(stream_reader)
#                 while not stream_reader.at_eof():
#                     # Read data from StreamReader
#                     data = await stream_reader.read(1024)  # Read up to 1024 bytes
#                     if data:
#                         # Process the data
#                         print(data.decode('latin-1'))  # If it's text data
#                     else:
#                         break
#             # result = await session.download_resource(resource, output_directory, True)
#             # print(type(result))

#         # async for line in result:
#         #     yield line #json.loads(line)
#         # print(await result.asend(None))






# # output_directory = os.getcwd()
# async def update_enrollment_terms(output_format=Format.CSV):
#     async with DAPClient() as session:
#         query = SnapshotQuery(format=Format.CSV, mode=None)
#         await session.download_table_data(
#             "canvas", "enrollment_terms", query, output_directory, decompress=True
#         )


# async def update_all():
#     # await update_courses()
#     # await update_course_sections()
#     # result = await update_enrollment_terms()
#     result = await get_canvas_data()
#     # await fetch_data(result)
#     print(result)
#     # await obtain_enrollment_schema()
#     # await update_enrollments()
#     # await update_pseudonyms()
#     # await update_scores()
#     # await update_users()

# async def main():
#     task = asyncio.create_task(update_all())
#     await task

# asyncio.run(main())





















# # import yaml
# # import os
# # import glob
# # import helper

# # # test out yaml
# # CONFIG_DIR = os.path.dirname(__file__) + '/../config.yml'
# # # CONFIG_DIR = '/../config.yml'
# # config = yaml.safe_load(open(CONFIG_DIR))
# # config = {1:1}

# # print(helper.get_format(config))
# # # print(config)


# # print(isinstance(config, dict))

# # prefix_path: str = os.path.dirname(__file__) + config['prefix_path'] 
# # final_path: str = os.path.dirname(__file__) + config['final_path']
# # output_format: str = (if config.get("output_format") in ("csv","json"): config.get("output_format"))  or "csv" 

# # print(output_format)
# # print(prefix_path)
# # print(final_path)

# # Format.CSV
# # Format.JSONL
# # Format.Parquet
# # Format.TSV

# # print(os.listdir(prefix_path))
# # print(os.listdir(final_path))

# # # clear temp files from subfolders
# # for temps in glob.glob(os.path.join(prefix_path, "**"), recursive=True):
# #     if temps.endswith((".csv", ".gz")):
# #         os.remove(temps)

# # check local machine doesn't have environment variables
# # base_url: str = os.environ["DAP_API_URL"]
# # client_id: str = os.environ["DAP_CLIENT_ID"]
# # client_secret: str = os.environ["DAP_CLIENT_SECRET"]

# # base_url
# # client_id
# # client_secret


# # test out dotenv library for local development
# # from dotenv import load_dotenv, find_dotenv

# # # load_dotenv() # should search parent directory by default
# # # load_dotenv(find_dotenv()) # try this one if it doesn't
# # load_dotenv(os.path.dirname(__file__) + '/../resources/.env')

# # dap_api_url: str = os.environ.get("DAP_API_URL")
# # dap_client_id: str = os.environ.get("DAP_CLIENT_ID")
# # dap_client_secret: str = os.environ.get("DAP_CLIENT_SECRET")

# # print(dap_api_url)
# # print(dap_client_id)
# # print(dap_client_secret)