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
```

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

## 🚀 CNPJá CRM Enrichment – Python Automation

This project automates the enrichment of CRM datasets using the **commercial CNPJá API**, which provides official and structured information about Brazilian companies based on their CNPJ identification. The script processes a CRM file, normalizes each CNPJ, retrieves company data through the API, and generates a fully enriched output CSV containing identity, legal, classification, address, status, and operational fields.

---

## 🧭 Overview

This automation receives a CSV containing company CNPJs, performs proper normalization of the CNPJ string, makes authenticated API calls using your commercial CNPJá API key, extracts structured fields from the response JSON, and appends them into your original dataset. It also includes retry logic, rate limiting control, error detection, and a dedicated subroutine to reprocess only failed records.

---

## 🔑 Requirements

To use this script, the following items are required:

- Python 3.9 or above
- Commercial CNPJá API access and an active subscription
- Input CSV file containing CNPJs
- Internet connectivity to reach the CNPJá platform
- Installed Python packages: `pandas`, `requests`, `pathlib`

---

## 🔐 How to Obtain the CNPJá API Key

Before running the script, you must access the CNPJá platform and obtain your commercial API key.

Follow these steps:

1. Visit the official CNPJá website using your browser.
2. Create an account or sign into your commercial environment.
3. Locate the API section, usually under “Integrações”, “Chaves”, “Token”, or “API”.
4. Copy your personal API key string exactly as provided.
5. Paste it inside the script by replacing the placeholder value.

Important: **do not commit your real API key to GitHub**. Prefer using environment variables in production environments.

---

# 🧠 How the Script Works (Conceptual Flow)

This automation follows the sequence below:

1. Load the chosen input CSV file into memory.
2. Normalize each CNPJ value into a clean 14-digit format.
3. Validate CNPJs and automatically discard malformed ones.
4. Build headers including your commercial API KEY.
5. Configure query parameters such as cache strategy and additional data flags.
6. Execute the `/office/:taxId` request for each valid CNPJ.
7. Apply retry logic when encountering DNS or timeout errors.
8. Handle status code `429` by pausing and retrying later.
9. Extract relevant fields from the JSON response.
10. Append key attributes back into the CRM dataframe.
11. Export a new enriched CSV.
12. Optionally reprocess only the unsuccessful rows.

---

## 🧩 Data Extracted from the API

The enrichment process retrieves the following:

### Business Identity
- Corporate name
- Trade name
- Opening date
- Legal nature

### Regulatory and Status Fields
- Registration status
- Status date
- Branch or headquarters indicator

### Company Size (Porte)
- Identification code
- Acronym
- Descriptive label

### CNAE Classification
- Activity code
- Activity description

### Address Structure
- Street
- Number
- District
- City
- State
- ZIP

Additionally, each record receives technical metadata including:
- API success indicator
- HTTP status code
- Error message when applicable
- Original CNPJ value
- Normalized CNPJ value

---

## 🛠 Internal Logic and Functions

### CNPJ Normalization
A dedicated function removes dots, slashes, dashes and validates the length of the resulting numeric string. Any non-valid CNPJ is automatically logged and skipped from API calls.

### Header Assembly
A function composes the HTTP headers for every request, injecting your CNPJá key into the `Authorization` header exactly as required by the official API documentation.

### Query Parameter Configuration
A function creates default parameters for caching strategies, staleness tolerances, and optional flags related to simplified taxation formats and geolocation responses.

### API Request + Retry Handling
A separate function sends requests, captures network errors, triggers exponential backoff, detects HTTP errors, handles 429 throttling, parses JSON data, and produces a standardized dictionary representing each individual response.

### Field Extraction and Data Mapping
A specialized function translates the API JSON into a consistent set of CRM fields to be merged into your dataset, ensuring unified structure regardless of errors or missing fields.

### Final CSV Construction
The main routine performs the join operation, preserving all original CRM columns while appending every enrichment field and technical indicator.

### Reprocessing of Failed Records
An additional function reopens the output CSV, filters rows where `api_success != True`, and triggers a new set of API requests only for failed entries.

---

## ⏱ Rate Limiting and API Throttling

The script implements a logical time window that counts requests per minute. When the configured maximum value is reached, execution pauses until the next minute begins. This prevents blocking, excessive credit consumption, and repeated 429 errors on the CNPJá server side.

---

## ⚠ Error Handling

The code performs:

- Invalid CNPJ detection
- Network error handling
- DNS resolution detection
- JSON parsing fallback
- 429 multiple retry attempts
- Return of unified response structure
- Graceful error continuation

Failures do not interrupt the entire batch and are recorded for later recovery.

---

## 📂 Output

The final exported CSV includes:

- All original CRM fields
- All extracted information
- Status columns describing each request
- Metadata for troubleshooting and analysis

This process results in a transformed CRM with official enriched business data, ready for analytics, segmentation, compliance, and strategic decision-making.

---

## 🚨 Security Notes

- Never publish your API key in public repositories
- Consider using `.env` files or secret managers
- Be mindful that API requests may consume credits
- Certificates and specialized queries may consume additional credits

---

## 📈 Practical Use Cases

- CRM enrichment and validation
- Lead qualification and segmentation
- Market mapping and classification
- Risk and compliance checks
- Data quality improvement
- B2B targeting analytics

---

## 🤝 Support and Issues

If you encounter problems or believe additional fields should be included, please submit an issue describing your use case so future enhancements can be considered.


