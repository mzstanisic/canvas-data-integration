# Navigate to the project package directory
Set-Location "C:\{path}\{to}\{project}\canvas-data-integration"

# Activate the virtual environment
& ".venv\Scripts\Activate.ps1"

# Run the main Python script
python "canvas-data-integration\main.py"

# Deactivate the virtual environment
deactivate