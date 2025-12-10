# API-script

# CNPJá CRM Enrichment Script

Python script to enrich a CRM CSV file with company data from the **CNPJá** commercial API (Brazilian CNPJ lookup).  
It normalizes CNPJs, calls the `/office/:taxId` endpoint, extracts key fields (company name, CNAE, size, address, status, etc.) and saves the enriched dataset to a new CSV file.

> ⚠️ **Important:** Never commit your real API key to GitHub.  
> In the example code below, the `API_KEY` value is a placeholder — replace it locally with your own key or use environment variables.

---

## ✨ Main Features

- Reads a CRM CSV and normalizes CNPJ values (removing punctuation, checking for 14 digits).
- Calls **CNPJá**’s `/office/:taxId` endpoint with:
  - Retry logic and exponential backoff for network/DNS errors.
  - Handling of HTTP errors, including **429 (rate limit)** with automatic wait and retry.
  - Simple per-minute rate-limit control (`MAX_REQUESTS_PER_MINUTE`).
- Extracts and flattens main JSON fields into columns:
  - Company data (legal name, trade name, opening date, legal nature).
  - Size / porte (id, acronym, description).
  - Main CNAE (code, description).
  - Address (street, number, district, city, state, ZIP).
  - Status (situational status, status date, HQ/branch).
- Joins the API results back into the original dataframe and exports to CSV.
- Includes a **`reprocess_failed()`** function that reads the output file and re-runs only rows where `api_success != True`.

---

## 📂 File & Column Configuration

At the top of the script, you configure:

```python
# 1) Your commercial CNPJá API key
API_KEY = "YOUR_API_KEY_HERE"  # <-- replace with your real key (do not commit it)

# 2) CNPJá API base URL
BASE_URL = "https://api.cnpja.com"

# 3) Input/output CSV files
INPUT_CSV = "Brazil - Data Research (Juliane)(CRM).csv"
OUTPUT_CSV = "CRMdadosatualizados.csv"

# 4) Name of the CNPJ column in your input CSV
CNPJ_COLUMN_NAME = "cnpj_normalizadoapi"  # e.g. "CNPJ" or "cnpj_b3"

# 5) Rate limit (requests per minute)
MAX_REQUESTS_PER_MINUTE = 60

# 6) Index to restart processing from (for long runs)
START_INDEX = 0

## Expected Input CSV

- The script expects a semicolon-separated (sep=";") CSV encoded in utf-8-sig.
- It must contain a column named exactly as CNPJ_COLUMN_NAME

## Output CSV

- The output file (OUTPUT_CSV) will contain:
- All original columns from the input file.

Plus enrichment columns such as

api_success
api_http_status
api_error
razao_social
nome_fantasia
data_abertura
natureza_juridica
porte_id
porte_sigla
porte_descricao
cnae_principal_codigo
cnae_principal_descricao
endereco_logradouro
endereco_numero
endereco_complemento
endereco_bairro
endereco_cidade
endereco_uf
endereco_cep
situacao_cadastral
data_situacao_cadastral
matriz_ou_filial
cnpj_original
cnpj_normalizado

# CNPJá CRM Enrichment (Python Automation)

This repository contains a Python-based automation that enriches CRM datasets using the **commercial CNPJá API**, a Brazilian company information service that provides structured business data based on company CNPJ identifiers.  

The purpose of this project is to automatically query and extract key business attributes such as legal name, trade name, registration status, CNAE classification, operating address, legal nature, and company size. All results are appended to the original CRM dataset and saved into a new enriched CSV file.

---

## 🧭 Motivation

Large CRM databases often contain outdated or incomplete company records. CNPJ-based enrichment enables:
- validation of company identity
- confirmation of registration status
- extraction of key classification information (CNAE)
- identifying headquarter vs branch
- improving segmentation and analytics
- improving targeting accuracy
- reducing manual verification time

---

## 🏗 Project Scope

This project focuses on:
- Automated CNPJ enrichment
- Error handling and retry logic
- Normalization and data cleaning
- Structured extraction of API response fields
- Data export to enriched CSV
- Automatic retry of failed API calls

It does **not** include:
- User interface
- Dashboard/report generation
- Database storage
- CRM integration API

However, the script is written in a modular way to allow future integration into larger systems.

---

## 🔍 What this script extracts

From each CNPJ lookup the script collects:

### Company attributes
- official business name (razao social)
- trade name (alias)
- opening date
- legal nature
- registration status
- company size / porte

### Classification
- primary CNAE activity code
- CNAE activity description

### Location fields
- street, number, district
- city, state, ZIP
- additional address information

### Metadata
- HTTP status
- success flag
- CNPJ validity
- error details
- original CNPJ provided

---

## ⚙️ How the Automation Works (high-level)

The script follows the steps below:

1. Load input CSV (CRM data)
2. Normalize each CNPJ into digits-only format
3. Validate 14-digit structure
4. Call CNPJá API
5. Extract JSON fields into Python dict
6. Append results to a list of enriched objects
7. Merge enriched data into original dataset
8. Export final CSV
9. Log successes and errors
10. Retry failures if needed

---

## 🧠 Why normalization matters

CNPJ digits may appear formatted like:


