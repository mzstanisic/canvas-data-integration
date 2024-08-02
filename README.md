### TODO
- separate file types into their own modules, recieve final files from Canvas module
- establish main module
- search for environment variables first, then search for file
- establish requirements.txt

### Project Structure
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
