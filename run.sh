#!/bin/bash

set -e

output_file="run.log"
sudo rm -f "$output_file"
exec > >(tee -a "$output_file") 2>&1

sudo rm -rf /root/.cache

sudo apt update
sudo apt install python3-pip
sudo apt install virtualenv

sudo rm -rf venv
virtualenv venv
source venv/bin/activate

pip install -r requirements.txt

python -m spacy download en_core_web_sm

# Create backup

current_date=$(date +"%Y-%m-%d")
gsutil cp "sec-edgar-results/CHART_TOP_CONCEPTS_IN_AI_PUBLIC_COMPANIES_BUSINESS_DESCRIPTIONS.xlsx" "gs://sec-edgar-backup/${current_date}_CHART_TOP_CONCEPTS_IN_AI_PUBLIC_COMPANIES_BUSINESS_DESCRIPTIONS.xlsx"
gsutil cp "sec-edgar-results/CHART_AI_PUBLIC_COMPANIES_PER_US_STATE.xlsx" "gs://sec-edgar-backup/${current_date}_CHART_AI_PUBLIC_COMPANIES_PER_US_STATE.xlsx"
gsutil cp "sec-edgar-results/CHART_EVOLUTION_OF_US_REGISTERED_AI_PUBLIC_COMPANIES_PER_COUNTRY.xlsx" "gs://sec-edgar-backup/${current_date}_CHART_EVOLUTION_OF_US_REGISTERED_AI_PUBLIC_COMPANIES_PER_COUNTRY.xlsx"

cd src

python main.py

deactivate
