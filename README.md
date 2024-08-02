# TODO

- establish main module
- requirements.txt: <https://pip.pypa.io/en/stable/cli/pip_freeze/#pip-freeze>
- rotate logs past retention period
- check for relevant environment variables, otherwise search for file
- change defaults from CSV to JSON

## Project Structure

Try something like this (view in code view, not preview):

canvas_data_project/
│
├── canvas_data_project/
│   ├── __init__.py
│   ├── config.yaml
│   ├── config.py
│   ├── api_client.py
│   ├── data_processing.py
│   ├── data_storage.py
│   └── utils.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── tests/
│   ├── __init__.py
│   ├── test_api_client.py
│   ├── test_data_processing.py
│   └── test_data_storage.py
│
├── .env
├── requirements.txt
├── setup.py
├── README.md
└── .gitignore
