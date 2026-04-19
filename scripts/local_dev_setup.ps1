<#
Sets up local development environment for BountyGate.
#>

$ErrorActionPreference = "Stop"

Write-Host "Creating Python virtual environment..."
python -m venv .venv

Write-Host "Activating virtual environment..."
. .venv\Scripts\Activate.ps1

Write-Host "Installing shared package in editable mode..."
pip install -e ./app/shared/python

Write-Host "Install Streamlit app dependencies..."
pip install -r ./app/requirements.txt

Write-Host "Install Airflow dependencies (optional for local orchestrator)..."
pip install -r ./airflow/requirements.txt

Write-Host "Done. Use 'deactivate' to exit the virtual environment."
