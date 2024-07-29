import yaml
import os
import glob
import helper

# test out yaml
CONFIG_DIR = os.path.dirname(__file__) + '/../config.yml'
# CONFIG_DIR = '/../config.yml'
config = yaml.safe_load(open(CONFIG_DIR))
config = {1:1}

print(helper.get_format(config))
# print(config)









# print(isinstance(config, dict))

# prefix_path: str = os.path.dirname(__file__) + config['prefix_path'] 
# final_path: str = os.path.dirname(__file__) + config['final_path']
# output_format: str = (if config.get("output_format") in ("csv","json"): config.get("output_format"))  or "csv" 

# print(output_format)
# print(prefix_path)
# print(final_path)

# Format.CSV
# Format.JSONL
# Format.Parquet
# Format.TSV

# print(os.listdir(prefix_path))
# print(os.listdir(final_path))

# # clear temp files from subfolders
# for temps in glob.glob(os.path.join(prefix_path, "**"), recursive=True):
#     if temps.endswith((".csv", ".gz")):
#         os.remove(temps)

# check local machine doesn't have environment variables
# base_url: str = os.environ["DAP_API_URL"]
# client_id: str = os.environ["DAP_CLIENT_ID"]
# client_secret: str = os.environ["DAP_CLIENT_SECRET"]

# base_url
# client_id
# client_secret


# test out dotenv library for local development
# from dotenv import load_dotenv, find_dotenv

# # load_dotenv() # should search parent directory by default
# # load_dotenv(find_dotenv()) # try this one if it doesn't
# load_dotenv(os.path.dirname(__file__) + '/../resources/.env')

# dap_api_url: str = os.environ.get("DAP_API_URL")
# dap_client_id: str = os.environ.get("DAP_CLIENT_ID")
# dap_client_secret: str = os.environ.get("DAP_CLIENT_SECRET")

# print(dap_api_url)
# print(dap_client_id)
# print(dap_client_secret)