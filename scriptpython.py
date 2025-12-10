#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCRIPT - PYTHON
"""

#%%
import time
import requests
import pandas as pd
from pathlib import Path

# =======================
# CONFIGURATION
# =======================

# 1) YOUR COMMERCIAL CNPJá API KEY
#    ⚠ Replace this placeholder with your real key locally.
#    Do NOT commit your real key to public repositories.
API_KEY = "YOUR_API_KEY_HERE"  # <-- replace here

# 2) Base URL for the CNPJá API (according to official docs/Postman)
BASE_URL = "https://api.cnpja.com"

# 3) Input/output files
#    INPUT_CSV  = CRM file you want to enrich
#    OUTPUT_CSV = final enriched file with all API fields
INPUT_CSV = "CRM.csv"
OUTPUT_CSV = "CRMdadosatualizados.csv"

# 4) Name of the CNPJ column in your CSV
#    Change if your file uses a different column name (e.g., "CNPJ", "cnpj_b3", etc.)
CNPJ_COLUMN_NAME = "cnpj_normalizadoapi"

# 5) Limit of API requests per minute (adjust based on your plan/contract)
MAX_REQUESTS_PER_MINUTE = 60

# 6) Starting index for processing
#    Use this to resume from a specific row after a crash or interruption.
START_INDEX = 0  # normally 0

#%%

# =======================
# HELPER FUNCTIONS
# =======================

def normalize_cnpj(cnpj_raw: str) -> str:
    """
    Normalize CNPJ by removing punctuation and keeping only digits.
    
    - If the value is NaN or None, returns None.
    - If the cleaned string has exactly 14 digits, returns it.
    - Otherwise, returns None (invalid CNPJ).
    """
    if pd.isna(cnpj_raw):
        return None
    digits = "".join(ch for ch in str(cnpj_raw) if ch.isdigit())
    return digits if len(digits) == 14 else None


def build_headers() -> dict:
    """
    Build HTTP headers for CNPJá API authentication.
    
    According to the documentation, the API key must be sent
    directly in the 'Authorization' header (no 'Bearer' prefix).
    """
    return {
        "Authorization": API_KEY,  # API key sent directly
        "Accept": "application/json",
    }


def build_params() -> dict:
    """
    Build default query parameters for the API call.
    
    These parameters control caching, data freshness, and optional
    flags (Simples Nacional, Suframa, geocoding, etc.).
    You can adjust them for your use case or pilot.
    """
    return {
        "strategy": "CACHE_IF_ERROR",
        "maxAge": 45,
        "maxStale": 365,
        "simples": "false",        # set "true" if you want Simples info
        "simplesHistory": "false",
        "registrations": None,     # e.g. "BR" or "PR,RS,SC"
        "suframa": "false",
        "geocoding": "false",
        "sync": "false",
        # "links": "RFB_CERTIFICATE,SIMPLES_CERTIFICATE",  # enable if you want certificates
    }


def fetch_cnpj_data(cnpj: str, max_retries: int = 5) -> dict:
    """
    Call the CNPJá API for a single CNPJ, with retry and backoff logic
    to handle network/DNS issues (e.g., NameResolutionError).
    
    Returns a normalized dictionary with:
        - success (bool)
        - error (str or None)
        - http_status (int or None)
        - data (parsed JSON or None)
    """
    url = f"{BASE_URL}/office/{cnpj}"
    last_exception_str = None

    for attempt in range(max_retries):
        try:
            # Perform the API request
            response = requests.get(
                url,
                headers=build_headers(),
                params=build_params(),
                timeout=30,
            )

            # Try to parse JSON (may fail if response is not JSON)
            try:
                json_data = response.json()
            except ValueError:
                json_data = None

            # Successful HTTP status and valid JSON
            if response.status_code == 200 and json_data is not None:
                return {
                    "success": True,
                    "error": None,
                    "http_status": 200,
                    "data": json_data,
                }
            else:
                # HTTP error or invalid JSON
                return {
                    "success": False,
                    "error": f"http_error_{response.status_code}",
                    "http_status": response.status_code,
                    "data": json_data,
                }

        except requests.exceptions.RequestException as e:
            # Any network-related error (DNS, timeout, etc.)
            last_exception_str = str(e)
            print(f"⚠️ Network/DNS error on attempt {attempt + 1} for {cnpj}: {last_exception_str}")

            # Simple exponential backoff (1s, 2s, 4s, 8s, ...)
            sleep_time = 2 ** attempt
            time.sleep(sleep_time)

    # If all retries are exhausted, return an error structure
    return {
        "success": False,
        "error": f"request_exception: {last_exception_str}",
        "http_status": None,
        "data": None,
    }


def extract_fields(api_result: dict) -> dict:
    """
    Extract key fields from the JSON returned by /office/:taxId.
    
    Expected basic structure from CNPJá:
        {
          "company": {...},
          "mainActivity": {...},
          "address": {...},
          ...
        }
    
    This function flattens the JSON into a dictionary of simple columns
    that can be merged into the CRM file.
    """
    data = api_result.get("data") or {}

    company = data.get("company") or {}
    address = data.get("address") or {}
    main_activity = data.get("mainActivity") or {}
    size = company.get("size") or {}

    # Basic fields — you can extend this after inspecting real JSON examples
    return {
        # API call status
        "api_success": api_result.get("success"),
        "api_http_status": api_result.get("http_status"),
        "api_error": api_result.get("error"),

        # Company info
        "razao_social": company.get("name"),
        "nome_fantasia": company.get("alias"),
        "data_abertura": company.get("openingDate"),
        "natureza_juridica": company.get("legalNature"),

        # Company size / porte
        "porte_id": size.get("id"),
        "porte_sigla": size.get("acronym"),
        "porte_descricao": size.get("text"),

        # Main CNAE
        "cnae_principal_codigo": main_activity.get("id"),
        "cnae_principal_descricao": main_activity.get("text"),

        # Address fields
        "endereco_logradouro": address.get("street"),
        "endereco_numero": address.get("number"),
        "endereco_complemento": address.get("complement"),
        "endereco_bairro": address.get("district"),
        "endereco_cidade": address.get("city"),
        "endereco_uf": address.get("state"),
        "endereco_cep": address.get("zip"),

        # Registration status
        "situacao_cadastral": data.get("status"),
        "data_situacao_cadastral": data.get("statusDate"),
        "matriz_ou_filial": data.get("headquarterOrBranch"),
    }


def sleep_if_necessary(request_count: int, start_window: float) -> tuple[int, float]:
    """
    Simple rate-limit control:
    
    - If the number of requests in the current minute reaches
      MAX_REQUESTS_PER_MINUTE, wait until 60 seconds have passed
      since the start of the window, then reset.
    
    Returns:
        (new_request_count, new_start_window)
    """
    now = time.time()
    elapsed = now - start_window

    if request_count >= MAX_REQUESTS_PER_MINUTE:
        if elapsed < 60:
            wait_time = 60 - elapsed
            print(f"[RATE LIMIT] Waiting ~{wait_time:.1f}s to respect the API limit...")
            time.sleep(wait_time)
        # Reset window
        return 0, time.time()

    return request_count, start_window


# =======================
# MAIN SCRIPT
# =======================

def main():
    """
    Main routine for full enrichment:
      1. Reads input CSV.
      2. Normalizes CNPJs.
      3. Calls the API for each CNPJ.
      4. Extracts fields and logs errors.
      5. Joins results into the original dataframe.
      6. Saves the enriched CSV to disk.
    """
    input_path = Path(INPUT_CSV)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path.resolve()}")

    # Read everything as string to avoid interfering with CNPJ formatting
    df = pd.read_csv(input_path, dtype=str, encoding="utf-8-sig", sep=";")

    if CNPJ_COLUMN_NAME not in df.columns:
        raise ValueError(
            f"Column '{CNPJ_COLUMN_NAME}' not found in the CSV. "
            f"Available columns: {list(df.columns)}"
        )

    # Normalize CNPJ values into a clean 14-digit string
    df["cnpj_normalizado"] = df[CNPJ_COLUMN_NAME].apply(normalize_cnpj)

    resultados = []

    request_count = 0
    window_start = time.time()

    total = len(df)
    print(f"Total rows to process: {total}")

    # Iterate over rows starting from START_INDEX (allows resuming later)
    for idx, row in df.iloc[START_INDEX:].iterrows():
        cnpj_raw = row[CNPJ_COLUMN_NAME]
        cnpj = row["cnpj_normalizado"]

        # If CNPJ is invalid or missing, record it and skip API call
        if not cnpj:
            print(f"[{idx}] Invalid or missing CNPJ: {cnpj_raw!r}")
            resultados.append({
                "index_origem": idx,
                "cnpj_original": cnpj_raw,
                "cnpj_normalizado": cnpj,
                "api_success": False,
                "api_http_status": None,
                "api_error": "cnpj_invalido",
            })
            continue

        # Rate limit control per minute
        request_count, window_start = sleep_if_necessary(request_count, window_start)

        # Attempt again in case of HTTP 429 (rate limit) — up to 3 retries
        attempts_429 = 0
        while True:
            print(f"[{idx}] Querying CNPJ {cnpj} (attempt {attempts_429 + 1})...")
            api_result = fetch_cnpj_data(cnpj)
            http_status = api_result.get("http_status")

            if http_status == 429:
                attempts_429 += 1
                if attempts_429 >= 3:
                    print(f"[{idx}] ⚠️ HTTP 429 persistent after 3 attempts, skipping this CNPJ.")
                    break
                print(f"[{idx}] ⚠️ API rate limit reached (429). Waiting 60 seconds before retrying...")
                time.sleep(60)
                continue  # retries the same CNPJ
            else:
                break  # exit loop if status != 429

        request_count += 1

        # Extract fields (even if there was an error — we keep it in the record)
        fields = extract_fields(api_result)
        fields["index_origem"] = idx
        fields["cnpj_original"] = cnpj_raw
        fields["cnpj_normalizado"] = cnpj

        resultados.append(fields)

        # Small delay between each CNPJ to avoid hammering the API
        time.sleep(0.5)  # increase to 1.0 if you still get 429

    # Convert the list of dicts into a DataFrame
    df_resultados = pd.DataFrame(resultados)

    # Join with original DF (keep all existing columns)
    df_final = df.join(
        df_resultados.set_index("index_origem"),
        how="left",
        rsuffix="api"
    )

    # Save the final enriched CSV
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\nProcess completed. File saved at: {Path(OUTPUT_CSV).resolve()}")


def reprocess_failed():
    """
    Reprocess only failed rows from the already generated OUTPUT_CSV.
    
    Steps:
      1. Load OUTPUT_CSV as df_final.
      2. Filter rows where api_success != True.
      3. For each one, call the API again.
      4. Update the same row in df_final with new data.
      5. Save the CSV again.
    """
    output_path = Path(OUTPUT_CSV)
    if not output_path.exists():
        raise FileNotFoundError(f"Output file not found: {output_path.resolve()}")

    # Read everything as string (to keep formatting)
    df_final = pd.read_csv(output_path, dtype=str, encoding="utf-8-sig")

    if "api_success" not in df_final.columns:
        raise ValueError("Column 'api_success' not found in the final CSV. Run main() first.")

    # Mask: all rows where api_success is NOT 'true' (case-insensitive)
    mask_retry = df_final["api_success"].astype(str).str.lower() != "true"

    # (Optional) Example if you want to ignore some definitive errors:
    # permanent_errors = ["cnpj_invalido", "http_error_400"]
    # mask_retry &= ~df_final["api_error"].isin(permanent_errors)

    df_retry = df_final[mask_retry].copy()

    print(f"Total rows with api_success != True: {len(df_retry)}")

    if df_retry.empty:
        print("Nothing to reprocess. ✅")
        return

    request_count = 0
    window_start = time.time()

    # Iterate over df_final indexes (idx) that need retry
    for i, idx in enumerate(df_retry.index, start=1):
        row = df_final.loc[idx]

        cnpj_raw = row.get(CNPJ_COLUMN_NAME, None)
        cnpj = row.get("cnpj_normalizado", None)

        # If CNPJ is still invalid, record and skip
        if not cnpj:
            print(f"[RETRY {i}] [{idx}] Invalid/missing CNPJ: {cnpj_raw!r}")
            df_final.at[idx, "api_success"] = False
            df_final.at[idx, "api_error"] = "cnpj_invalido_retry"
            continue

        # Rate limit control
        request_count, window_start = sleep_if_necessary(request_count, window_start)

        attempts_429 = 0
        while True:
            print(f"[RETRY {i}] [{idx}] Querying CNPJ {cnpj} (attempt {attempts_429 + 1})...")
            api_result = fetch_cnpj_data(cnpj)
            http_status = api_result.get("http_status")

            if http_status == 429:
                attempts_429 += 1
                if attempts_429 >= 3:
                    print(f"[RETRY {i}] [{idx}] ⚠️ HTTP 429 persistent, skipping this CNPJ.")
                    break
                print(f"[RETRY {i}] [{idx}] ⚠️ API rate limit (429). Waiting 60 seconds...")
                time.sleep(60)
                continue
            else:
                break

        request_count += 1

        # Extract fields and update the SAME row in df_final
        fields = extract_fields(api_result)

        for col, val in fields.items():
            df_final.at[idx, col] = val

        # Ensure consistency for CNPJ columns
        df_final.at[idx, "cnpj_original"] = cnpj_raw
        df_final.at[idx, "cnpj_normalizado"] = cnpj

        # Log final status for this retry
        status = df_final.at[idx, "api_success"]
        erro = df_final.at[idx, "api_error"]
        print(f"[RETRY {i}] [{idx}] finished with api_success={status}, api_error={erro}")

    # Save updated CSV with retried results
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\nReprocessing completed. File updated at: {output_path.resolve()}")


if __name__ == "__main__":
    # Choose which flow to execute:
    main()              # Process everything from INPUT_CSV
    #reprocess_failed()    # Reprocess only rows where api_success != True
