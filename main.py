import pandas as pd
import requests
import json
import os
from dotenv import load_dotenv
import google.generativeai as genai
from google.api_core import exceptions as api_exceptions
from pdf2image import convert_from_bytes
from PIL import Image
import io
import psycopg2
from psycopg2.extras import RealDictCursor
from openpyxl.styles import Font
from collections import OrderedDict
import signal
import time

load_dotenv()


def _log_duration(step_name: str, start_time: float):
    """Log elapsed time for a step in seconds."""
    elapsed = time.perf_counter() - start_time
    print(f"  [TIMING] {step_name}: {elapsed:.2f}s")
    return elapsed

LOAN_ID_FILE = "loan_id.txt"
FINAL_OUTPUT_FILE = "final_json_to_excel_output.xlsx"
RAW_DATA_SHEET = "raw_data"  # Internal sheet for tracking processed items
DOC_FOLDER = "doc"
BATCH_SIZE = 1 # Write to Excel every N results (reduces I/O blocking)
AUTH_TOKEN = "81e6e8dc-1c2c-4ad8-902f-ae6084e95fa9"  # Authorization token for API access

# Results buffer for batch writing
results_buffer = []

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle interrupt signals gracefully"""
    global shutdown_requested
    print("\n\n⚠️  Interrupt received! Shutting down gracefully...")
    shutdown_requested = True
    print("Waiting for current operations to complete...")
    # The finally block in main() will handle flushing safely

# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Standard JSON schemas
INSURANCE_POLICY_SCHEMA = {
  "insurer_name": "",
  "insurance_start_date": "",  #(using fields like issued on)
  "chassis_number": "",
  "engine_number": "",
  "oem": "",
  "model": "",
  "cgst_amount": "",
  "sgst_amount": "",
  "igst_amount": "",
  "total_idv": "", #(Take value from field like Total IDV)
  "hp_lender_name": "",
  "own_damage_premium": "",
  "tp_premium": "", #(Take value from field like Net Liability Premium (B))
  "addon_premium": "",
  "total_insurance_premium": "" #(use fields which have toal premium having (A+B) or (A+B+C) to fill this value)
}

VEHICLE_TAX_SCHEMA = {
  "oem": "",
  "model": "",
  "dealer_name": "",
  "tax_invoice_number": "",
  "engine_number": "",
  "chassis_number": "",
  "ex_showroom_price": "", #(Search for field like TAXABLE AMOUNT to fill this value)
  "sgst_amount": "",
  "cgst_amount": "",
  "pm_e_drive_subsidy": "", #(Search for field which consist of words like PM E DRIVE SUBSIDY, OEM SUBSIDY, FAME SUBSIDY, FAME INCENTIVE etc and use them to fill this value)
  "state_subsidy": "",
  "promotional_incentive": "",
  "festival_offer": "",
  "total_subsidy": "", #(SUM all the types of subsidies proided in document and fill this value)
  "net_ex_showroom_price": "", 
  "discount": "", #(Sum the values from fields such as PROMOTIONAL INCENTIVE, FESTIVAL OFFERS, and any other incentive-related fields that are mentioned in the document, and fill this value with the total sum)
  "handling_or_accessories_charges": "",
  "rto_charges": "",
  "hp_lender_name": ""
}

DP_RECEIPT_SCHEMA = {
  "DP_amount": "",
  "date": "",
  "issuer_name": ""
}

DETAIL_PROFORMA_INVOICE_SCHEMA = {
  "oem": "",
  "model": "",
  "dealer_name": "",
  "city": "",
  "state": "",
  "ex_showroom_price": "",
  "subsidy": "",
  "net_ex_showroom_price": "",
  "insurance_amount": "",
  "rto_charges_registration": "",
  "accessories_charges": "",
  "amc_rsa_charges": "", 
  "fabrication_container_charges": "", 
  "earthing": "",
  "handling_charges": "",
  "buyback_other_charges": "", 
  "dealer_discount": "",
  "on_road_price": "",
  "total_amount": ""
}

ELECTRICITY_BILL_SCHEMA = {
  "consumer_name": "",
  "bill_no": "",
  "consumer_no": "",
  "address": "",
  "due_date": "", #(Extract due date in DD MMM YYYY format)
  "current_bill_amount": "",
  "principal_arrear": "",
  "total_amount_payable": "",
  "last_payment_date": "", #(Extract last payment date in DD MMM YYYY format)
  "last_payment_amount": "",
  "units_consumed": "",
  "units_consumed_last_month": ""
}

RC_RC_B_EXTRACT_SCHEMA = {
  "hypothecation_company": "",
  "registration_number": "",
  "customer_address": "",
  "date_of_registration": "",
  "son_daughter_wife_of": "",
  "body_type": "",
  "month_of_manufacture": "",
  "horsepower": "",
  "wheel_base": "",
  "seating_capacity": "",
  "unladen_weight": "",
  "colour_of_body": "",
  "gross_weight": "",
  "Registering_Authority": ""
}

def get_api_key():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise Exception("GEMINI_API_KEY not found in .env file")
    return api_key.strip()

def initialize_model():
    api_key = get_api_key()
    genai.configure(api_key=api_key)
    return genai.GenerativeModel("gemini-3-flash-preview")

def get_db_connection():
    host = os.getenv("DB_HOST", "")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    
    if not all([host, database, user, password]):
        raise Exception("Database credentials not found in .env file")
    
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

def read_loan_ids():
    if not os.path.exists(LOAN_ID_FILE):
        raise Exception(f"{LOAN_ID_FILE} not found")
    
    with open(LOAN_ID_FILE, 'r') as f:
        loan_ids = [line.strip() for line in f if line.strip()]
    
    return loan_ids

def query_database(loan_ids):
    conn = get_db_connection()
    try:
        loan_ids_str = "', '".join(loan_ids)
        query = f"""
        with cte as (
            select lslsdd.loan_id, lslsdd.media_ref, lslsdd.doc_type, 
                   row_number() over(partition by lslsdd.loan_id, doc_type order by created_at desc) as rn 
            from public.leads_service_leads_service_document_details lslsdd
            where lslsdd.loan_id in ('{loan_ids_str}')
        and lslsdd.doc_type in ('RC_RC_B_EXTRACT')
        )
        select * from cte where rn=1
        """
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        return [dict(row) for row in results]
    finally:
        conn.close()

def query_approval_dates(loan_ids):
    """Fetch approval dates for loan IDs to cross-check RC registration dates."""
    conn = get_db_connection()
    try:
        loan_ids_str = "', '".join(loan_ids)
        query = f"""
        WITH Approved_Log AS (
            SELECT
                loan_id,
                MAX(created_date) AS approved_date
            FROM analytics.log_loan_history_all
            WHERE loan_status = 'APPROVED'
            GROUP BY 1
        ),
        approval_log2 AS (
            SELECT
                loan_id,
                MAX(created_date) AS approved_date
            FROM analytics.log_loan_history_all
            WHERE prev_stage = 'UNDERWRITING' AND new_stage = 'APPROVED_DP_DUE'
            GROUP BY 1
        )
        SELECT app.id AS loan_id,
               coalesce(al.approved_date, al2.approved_date) AS approval_date
        FROM analytics.application app
        LEFT JOIN Approved_Log al ON al.loan_id = app.id
        LEFT JOIN approval_log2 al2 ON al2.loan_id = app.id
        WHERE coalesce(al.approved_date, al2.approved_date) IS NOT NULL
          AND app.id IN ('{loan_ids_str}')
        """
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        approval_map = {}
        for row in results:
            approval_map[row['loan_id']] = str(row['approval_date'])
        return approval_map
    except Exception as e:
        print(f"Warning: Could not fetch approval dates: {e}")
        return {}
    finally:
        conn.close()

def get_downloadable_link(media_ref):
    try:
        url = f"https://api.turnoclub.com/media/{media_ref}"
        cookies = {
            "auth_token": AUTH_TOKEN
        }
        response = requests.get(url, cookies=cookies, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success" and "payload" in data:
            return data["payload"]
        return None
    except Exception as e:
        return None

def download_file(downloadable_link):
    try:
        cookies = {
            "auth_token": AUTH_TOKEN
        }
        response = requests.get(downloadable_link, cookies=cookies, timeout=60)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '').split(';')[0]
        return response.content, content_type
    except Exception as e:
        return None, None

def bytes_to_images(file_bytes, content_type):
    images = []
    is_pdf = content_type == "application/pdf" or (len(file_bytes) >= 4 and file_bytes[:4] == b'%PDF')
    
    if is_pdf:
        try:
            images = convert_from_bytes(file_bytes)
        except Exception:
            try:
                images = [Image.open(io.BytesIO(file_bytes))]
            except:
                raise Exception("Unable to process PDF file")
    else:
        images = [Image.open(io.BytesIO(file_bytes))]
    
    if not images:
        raise Exception("No images could be extracted from the file")
    
    return images

def process_image_to_standard_json(model, images, doc_type, approval_date=None):
    if doc_type == "INSURANCE_POLICY_COPY":
        schema = INSURANCE_POLICY_SCHEMA
        schema_name = "insurance policy"
    elif doc_type == "VEHICLE_TAX_INVOICE":
        schema = VEHICLE_TAX_SCHEMA
        schema_name = "vehicle tax invoice"
    elif doc_type == "DP_RECEIPT":
        schema = DP_RECEIPT_SCHEMA
        schema_name = "DP receipt"
    elif doc_type == "DETAIL_PROFORMA_INVOICE":
        schema = DETAIL_PROFORMA_INVOICE_SCHEMA
        schema_name = "detail proforma invoice"
    elif doc_type == "ELECTRICITY_BILL":
        schema = ELECTRICITY_BILL_SCHEMA
        schema_name = "electricity bill"
    elif doc_type == "RC_RC_B_EXTRACT":
        schema = RC_RC_B_EXTRACT_SCHEMA
        schema_name = "RC RC B extract (vehicle registration certificate)"
    else:
        raise Exception(f"Unknown doc_type: {doc_type}")
    
    # Base prompt for all document types
    base_prompt = f"""
Standard JSON Schema for {schema_name}:
{json.dumps(schema, indent=2)}

Task: Parse the given images and extract information and fill required fields in the standard JSON schema above. Map the extracted information directly to the standard JSON schema above. Fill in all fields that match from the images. Read comments given in json template and extract information according to them. Do not fill any random value in fields if document is empty or information is not present in document do not hallucinate any value just keep them empty.Keep empty strings for fields that don't have matching data - don't fill NA, just keep them empty. Return ONLY valid JSON without any markdown code blocks, backticks, or formatting. Start directly with the JSON object."""
    
    # Add specific instructions for each document type
    if doc_type == "VEHICLE_TAX_INVOICE":
        prompt = base_prompt + "Do not merge discount and subsidy values to get total subsidy value, both fields are different use SUM of festival offers and incentive fields to get Discount value. Documents with FAME INCENTIVE field should be treated as PM E DRIVE SUBSIDY field and should be added to total subsidy value. Engine number will be present in document near chassis number write it properly in its format sometimes it will be named as MOTOR NUMBE. In Discount field also add values whoes field names are like XMART SPL or SAC 996421. always take price of vehicle excluding any taxes for ex showroom price filed.\n"
    elif doc_type == "DP_RECEIPT":
        prompt = base_prompt + "Extract the DP amount, date in DD MMM YYYY format, and issuer name from the receipt document.\n"
    elif doc_type == "DETAIL_PROFORMA_INVOICE":
        prompt = base_prompt + "from this document EARTHING KIT or EARTHING fields value will be assigned in earting key not in accessories_charges. Properly check fields which are striked through and assigned any other field in handwritten format so assign value corrosponding to handwritten field from json not the striked one. AMC_RSA_CHARGES field is yearly are anually mantainance charge so check any yearly mantainance charge then give value in this field. In BUYBACK_OTHER_CHARGES field check if we have any hand written or typed battery buyback charge mentioned. RTO_CHARGES_REGISTRATION field  write registration charge with sum of any HPA CHARGE field or PERMIT any feild if given seperately. if EX_SHOWROOM_PRICE is not present in document then give the value present in net ex showroom field."
    elif doc_type == "ELECTRICITY_BILL":
        prompt = base_prompt + "Dates should be in DD MMM YYYY format.Extract curren t bill amount, principal arrear if any, and total amount payable. Also extract last payment date and last payment amount. Extract units consumed in current month and units consumed in last month. Ensure all numerical values are extracted without currency symbols or extra formatting. if any value id not present just keep it empty dont add any random value or any null value\n"
    elif doc_type == "RC_RC_B_EXTRACT":
        rc_extra = "Extract vehicle registration certificate (RC) details: hypothecation company, registration number, customers address, date of registration (use DD MMM YYYY format if applicable), son/daughter/wife of (owner details), body type, month of manufacture, horsepower, wheel base, seating capacity, unladen weight, colour of body, gross weight. Leave fields empty if not present in the document. Read Registration date Properly from narrow or small images.\n"
        if approval_date:
            rc_extra += f"The loan approval date is {approval_date} — if the extracted date_of_registration has a large delta from this date, re-read it carefully from the document as a sanity check (do not alter it to match).\n"
        prompt = base_prompt + rc_extra
    else:
        prompt = base_prompt + "Insurance start date should be in this type eg. DD MMM YYYY. Take value from field like Net Own Damage Premium (A) or TOTAL OWN DAMAGE PREMIUM(A). For tp_premium take value from fields like NET LIABILITY PREMIUM (B). For ADDON PREMIUM take value from fields like ADDON PREMIUM, if not found search for which resembles ADDON PREMIUM and sum all those values to fill this value and if not found any add on leave it empty. Whenever we are fetching Insurance premium we should always fetch Final premium including Taxes and all other charges."
     
    try:
        generation_config = {
            "temperature": 0.0,  # This overrides the 1.0 default
            }
        # response = model.generate_content([prompt] + images)
        response = model.generate_content([prompt] + images, generation_config = generation_config)
        result_text = response.text.strip()
        
        if result_text.startswith("```json"):
            result_text = result_text[7:]
        elif result_text.startswith("```"):
            result_text = result_text[3:]
        if result_text.endswith("```"):
            result_text = result_text[:-3]
        result_text = result_text.strip()
        
        try:
            return json.loads(result_text)
        except json.JSONDecodeError:
            return result_text
    except api_exceptions.ResourceExhausted as e:
        raise Exception(f"API_QUOTA_EXHAUSTED: {str(e)}")
    except Exception as e:
        raise Exception(f"API Error: {str(e)}")

def process_single_item(item_data, model=None, approval_date=None):
    loan_id = item_data['loan_id']
    doc_type = item_data['doc_type']
    media_ref = item_data['media_ref']
    downloadable_link = item_data['downloadable_link']
    
    try:
        # Download file and keep in RAM only (no disk write)
        file_bytes, content_type = download_file(downloadable_link)
        if file_bytes is None:
            return {
                'success': False,
                'loan_id': loan_id,
                'doc_type': doc_type,
                'media_ref': media_ref,
                'error': 'Download failed',
                'downloadable_link': downloadable_link
            }
        
        # Process directly from memory
        images = bytes_to_images(file_bytes, content_type)
        
        # Reuse model if provided, otherwise create new one
        if model is None:
            model = initialize_model()
        json_data = process_image_to_standard_json(model, images, doc_type, approval_date=approval_date)
        
        # Clear memory immediately after processing
        del file_bytes
        del images
        
        return {
            'success': True,
            'loan_id': loan_id,
            'doc_type': doc_type,
            'media_ref': media_ref,
            'json_data': json_data,
            'downloadable_link': downloadable_link
        }
    except api_exceptions.ResourceExhausted as e:
        return {
            'success': False,
            'loan_id': loan_id,
            'doc_type': doc_type,
            'media_ref': media_ref,
            'error': f"API_QUOTA_EXHAUSTED: {str(e)}",
            'quota_exhausted': True,
            'downloadable_link': downloadable_link
        }
    except Exception as e:
        error_msg = str(e)
        is_quota_error = ("API_QUOTA_EXHAUSTED" in error_msg or 
                         "quota" in error_msg.lower() or 
                         "ResourceExhausted" in error_msg)
        return {
            'success': False,
            'loan_id': loan_id,
            'doc_type': doc_type,
            'media_ref': media_ref,
            'error': error_msg,
            'quota_exhausted': is_quota_error,
            'downloadable_link': downloadable_link
        }

def get_processed_combinations():
    if not os.path.exists(FINAL_OUTPUT_FILE):
        return set()
    
    try:
        # Read from raw_data sheet
        df = pd.read_excel(FINAL_OUTPUT_FILE, sheet_name=RAW_DATA_SHEET)
        if 'loan_id' in df.columns and 'doc_type' in df.columns:
            processed = set()
            for _, row in df.iterrows():
                loan_id = str(row.get('loan_id', '')).strip()
                doc_type = str(row.get('doc_type', '')).strip()
                standard_json = str(row.get('standard_json', '')).strip()
                if loan_id and doc_type and standard_json and not standard_json.startswith('ERROR:'):
                    processed.add((loan_id, doc_type))
            return processed
    except:
        # If raw_data sheet doesn't exist, try reading the file directly (for backward compatibility)
        try:
            df = pd.read_excel(FINAL_OUTPUT_FILE)
            if 'loan_id' in df.columns and 'doc_type' in df.columns:
                processed = set()
                for _, row in df.iterrows():
                    loan_id = str(row.get('loan_id', '')).strip()
                    doc_type = str(row.get('doc_type', '')).strip()
                    standard_json = str(row.get('standard_json', '')).strip()
                    if loan_id and doc_type and standard_json and not standard_json.startswith('ERROR:'):
                        processed.add((loan_id, doc_type))
                return processed
        except:
            pass
    return set()

def save_result_to_buffer(result):
    """Add result to buffer for batch writing"""
    global results_buffer
    
    results_buffer.append(result)
    # Check if we need to flush
    if len(results_buffer) >= BATCH_SIZE:
        flush_results_buffer()

def flush_results_buffer():
    """Write all buffered results to Excel at once"""
    global results_buffer
    
    if not results_buffer:
        return
    
    # Get copy of buffer and clear it
    results_to_write = results_buffer.copy()
    results_buffer = []
    
    # Read existing data once
    if os.path.exists(FINAL_OUTPUT_FILE):
        try:
            df = pd.read_excel(FINAL_OUTPUT_FILE, sheet_name=RAW_DATA_SHEET)
            if 'downloadable_link' not in df.columns:
                df['downloadable_link'] = None
        except:
            df = pd.DataFrame(columns=['loan_id', 'doc_type', 'media_ref', 'standard_json', 'status', 'downloadable_link'])
    else:
        df = pd.DataFrame(columns=['loan_id', 'doc_type', 'media_ref', 'standard_json', 'status', 'downloadable_link'])
    
    # Process all buffered results at once
    new_rows = []
    for result in results_to_write:
        if result['success']:
            json_str = json.dumps(result['json_data']) if isinstance(result['json_data'], (dict, list)) else str(result['json_data'])
            new_rows.append({
                'loan_id': result['loan_id'],
                'doc_type': result['doc_type'],
                'media_ref': result.get('media_ref', ''),
                'standard_json': json_str,
                'status': 'success',
                'downloadable_link': result.get('downloadable_link', '')
            })
        else:
            new_rows.append({
                'loan_id': result['loan_id'],
                'doc_type': result['doc_type'],
                'media_ref': result.get('media_ref', ''),
                'standard_json': f"ERROR: {result['error']}",
                'status': 'error',
                'downloadable_link': result.get('downloadable_link', '')
            })
    
    # Add all rows at once
    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    
    # Read existing doc_type sheets once
    existing_doc_sheets = {}
    if os.path.exists(FINAL_OUTPUT_FILE):
        try:
            excel_file = pd.ExcelFile(FINAL_OUTPUT_FILE)
            for sheet_name in excel_file.sheet_names:
                if sheet_name != RAW_DATA_SHEET:
                    existing_doc_sheets[sheet_name] = pd.read_excel(FINAL_OUTPUT_FILE, sheet_name=sheet_name)
        except:
            pass
    
    # Write once for the entire batch
    with pd.ExcelWriter(FINAL_OUTPUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=RAW_DATA_SHEET, index=False)
        for sheet_name, sheet_df in existing_doc_sheets.items():
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

def parse_json_field(json_str):
    """Parse JSON string, handling various formats."""
    if pd.isna(json_str) or not str(json_str).strip():
        return None
    
    json_str = str(json_str).strip()
    
    # Remove markdown code blocks if present
    if json_str.startswith("```json"):
        json_str = json_str[7:]
    if json_str.startswith("```"):
        json_str = json_str[3:]
    if json_str.endswith("```"):
        json_str = json_str[:-3]
    json_str = json_str.strip()
    
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        return None

def flatten_json(json_obj, parent_key='', sep='_'):
    """
    Flatten a nested JSON object into a single level dictionary.
    """
    items = []
    if isinstance(json_obj, dict):
        for k, v in json_obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # For lists, convert to string or take first element if it's a dict
                if len(v) > 0 and isinstance(v[0], dict):
                    items.extend(flatten_json(v[0], new_key, sep=sep).items())
                else:
                    items.append((new_key, json.dumps(v) if v else ""))
            else:
                items.append((new_key, v if v is not None else ""))
    else:
        items.append((parent_key, json_obj))
    return dict(items)

def get_all_json_keys(df, doc_type):
    """
    Collect all unique keys from JSON objects for a specific doc_type.
    This ensures all columns are present even if some rows don't have all fields.
    """
    all_keys = set()
    for idx, row in df.iterrows():
        if str(row.get('doc_type', '')).strip() == doc_type:
            json_data = parse_json_field(row.get('standard_json'))
            if json_data and isinstance(json_data, dict):
                flattened = flatten_json(json_data)
                all_keys.update(flattened.keys())
    return sorted(list(all_keys))

def process_data_for_doc_type(df, doc_type):
    """
    Process data for a specific doc_type and return a list of dictionaries.
    Each dictionary represents a row with loan_id, doc_type, media_ref, document_link, and all JSON fields.
    """
    rows_data = []
    
    # Get all possible keys for this doc_type
    all_keys = get_all_json_keys(df, doc_type)
    
    for idx, row in df.iterrows():
        row_doc_type = str(row.get('doc_type', '')).strip()
        
        # Only process rows matching the current doc_type and with valid JSON
        if row_doc_type != doc_type:
            continue
        
        # Get base fields
        loan_id = row.get('loan_id', '')
        media_ref = row.get('media_ref', '')
        
        # Get downloadable_link from saved data, or fetch it if not available
        document_link = row.get('downloadable_link', '')
        if not document_link or pd.isna(document_link) or str(document_link).strip() == '':
            # If not saved, fetch it from API
            document_link = get_downloadable_link(media_ref) or ""
        
        # Parse JSON
        json_data = parse_json_field(row.get('standard_json'))
        
        # Skip rows with invalid JSON or error status
        if json_data is None or not isinstance(json_data, dict):
            continue
        
        # Initialize row data with base fields
        row_data = OrderedDict([
            ('loan_id', loan_id),
            ('doc_type', doc_type),
            ('media_ref', media_ref),
            ('document_link', document_link)
        ])
        
        # Flatten JSON and add to row_data
        flattened = flatten_json(json_data)
        # Add all keys, using empty string if key doesn't exist in this row
        for key in all_keys:
            value = flattened.get(key, "")
            # Convert None to empty string, preserve other values as strings
            row_data[key] = "" if value is None or value == "" else str(value)
        
        rows_data.append(row_data)
    
    return rows_data

def create_final_excel_with_sheets():
    """
    Create final Excel file with separate sheets for each doc_type,
    including clickable document links.
    """
    print(f"\n{'='*60}")
    print("Creating final Excel with separate sheets...")
    print(f"{'='*60}")
    
    if not os.path.exists(FINAL_OUTPUT_FILE):
        print(f"Error: {FINAL_OUTPUT_FILE} not found!")
        return
    
    # Read from raw_data sheet
    try:
        df = pd.read_excel(FINAL_OUTPUT_FILE, sheet_name=RAW_DATA_SHEET)
        print(f"Total rows in input file: {len(df)}")
    except:
        # If raw_data sheet doesn't exist, try reading the file directly (for backward compatibility)
        try:
            df = pd.read_excel(FINAL_OUTPUT_FILE)
            print(f"Total rows in input file: {len(df)}")
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
            return
    
    # Verify required columns exist
    required_columns = ['loan_id', 'doc_type', 'media_ref', 'standard_json']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        return
    
    # Get all unique doc_types BEFORE filtering (to create sheets for all types)
    all_doc_types = df['doc_type'].dropna().unique()
    print(f"Found doc_types in raw_data: {list(all_doc_types)}")
    
    # Show breakdown by doc_type and status
    if 'status' in df.columns:
        print("\nBreakdown by doc_type and status:")
        for doc_type in all_doc_types:
            doc_df = df[df['doc_type'] == doc_type]
            success_count = len(doc_df[doc_df['status'] == 'success'])
            error_count = len(doc_df[doc_df['status'] == 'error'])
            print(f"  {doc_type}: {success_count} successful, {error_count} errors, {len(doc_df)} total")
    
    # Filter only successful rows
    df_success = df[df['status'] == 'success'].copy()
    print(f"\nSuccessful rows: {len(df_success)}")
    
    # Get unique doc_types from successful rows
    doc_types_with_success = df_success['doc_type'].dropna().unique()
    print(f"Doc_types with successful rows: {list(doc_types_with_success)}")
    
    # Process data for each doc_type (process all types, even if no successful rows)
    sheets_data = {}
    
    for doc_type in all_doc_types:
        doc_type_str = str(doc_type).strip()
        if not doc_type_str:
            continue
        
        print(f"\nProcessing {doc_type_str}...")
        rows_data = process_data_for_doc_type(df_success, doc_type_str)
        sheets_data[doc_type_str] = rows_data
        print(f"  Found {len(rows_data)} valid rows for {doc_type_str}")
        if rows_data:
            # Count JSON fields
            sample_row = rows_data[0]
            json_fields = [k for k in sample_row.keys() if k not in ['loan_id', 'doc_type', 'media_ref', 'document_link']]
            print(f"  Found {len(json_fields)} JSON fields")
        else:
            print(f"  No valid rows for {doc_type_str} (will create empty sheet)")
    
    # Create output Excel file with multiple sheets
    print(f"\nCreating output file: {FINAL_OUTPUT_FILE}")
    
    # Read existing raw_data sheet to preserve it (for internal tracking only)
    try:
        df_raw = pd.read_excel(FINAL_OUTPUT_FILE, sheet_name=RAW_DATA_SHEET)
    except:
        df_raw = None
    
    with pd.ExcelWriter(FINAL_OUTPUT_FILE, engine='openpyxl') as writer:
        # Write raw_data sheet first (preserved for resume functionality)
        if df_raw is not None:
            df_raw.to_excel(writer, sheet_name=RAW_DATA_SHEET, index=False)
        
        # Only write doc_type sheets
        for doc_type, rows_data in sheets_data.items():
            if rows_data:
                # Create DataFrame from rows_data
                df_output = pd.DataFrame(rows_data)
                # Replace NaN values with empty strings
                df_output = df_output.fillna("")
                # Write to Excel sheet
                sheet_name = doc_type[:31]  # Excel sheet name limit is 31 characters
                df_output.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  Created sheet '{sheet_name}' with {len(rows_data)} rows and {len(df_output.columns)} columns")
                
                # Add hyperlinks to document_link column
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                
                # Find the document_link column index
                for col_idx, col_name in enumerate(df_output.columns, start=1):
                    if col_name == 'document_link':
                        # Add hyperlinks to each cell in document_link column
                        for row_idx, url in enumerate(df_output['document_link'], start=2):  # Start from row 2 (skip header)
                            if url and str(url).strip() and str(url).startswith('http'):
                                cell = worksheet.cell(row=row_idx, column=col_idx)
                                # Use the URL directly as hyperlink and display the actual URL
                                cell.hyperlink = str(url)
                                cell.value = str(url)  # Show actual URL instead of "View Document"
                                # Style as hyperlink (blue, underlined)
                                cell.font = Font(color="0563C1", underline="single")
                        break
            else:
                # Create empty sheet with at least base columns
                df_empty = pd.DataFrame(columns=['loan_id', 'doc_type', 'media_ref', 'document_link'])
                sheet_name = doc_type[:31]
                df_empty.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  Created empty sheet '{sheet_name}'")
        
        # Keep raw_data sheet for resume functionality (don't remove it)
        # This allows the script to skip already processed items on next run
    
    print(f"\n{'='*60}")
    print(f"Final output file '{FINAL_OUTPUT_FILE}' created successfully!")
    print(f"{'='*60}")
    
    # Print summary
    print("\nSummary:")
    for doc_type, rows_data in sheets_data.items():
        if rows_data:
            sample_row = rows_data[0]
            json_fields = [k for k in sample_row.keys() if k not in ['loan_id', 'doc_type', 'media_ref', 'document_link']]
            print(f"  {doc_type}: {len(rows_data)} rows, {len(json_fields)} JSON fields")
        else:
            print(f"  {doc_type}: 0 rows (empty sheet created)")

def main():
    main_start = time.perf_counter()

    print("Reading loan IDs from loan_id.txt...")
    t0 = time.perf_counter()
    loan_ids = read_loan_ids()
    _log_duration("read_loan_ids", t0)
    print(f"Found {len(loan_ids)} loan IDs")

    print("Querying database...")
    t0 = time.perf_counter()
    db_results = query_database(loan_ids)
    _log_duration("query_database", t0)
    print(f"Found {len(db_results)} records from database")

    print("Fetching approval dates for RC registration date verification...")
    t0 = time.perf_counter()
    approval_dates = query_approval_dates(loan_ids)
    _log_duration("query_approval_dates", t0)
    print(f"Found approval dates for {len(approval_dates)} loan IDs")

    if not db_results:
        print("No records found in database")
        return

    print("Getting downloadable links sequentially...")
    t0 = time.perf_counter()
    items_to_process = []
    for row in db_results:
        try:
            downloadable_link = get_downloadable_link(row['media_ref'])
            if downloadable_link:
                items_to_process.append({
                    'loan_id': row['loan_id'],
                    'doc_type': row['doc_type'],
                    'media_ref': row['media_ref'],
                    'downloadable_link': downloadable_link
                })
        except Exception as e:
            print(f"Error getting link for {row['loan_id']}: {str(e)}")
    _log_duration("get_downloadable_links", t0)
    print(f"Got {len(items_to_process)} downloadable links")

    t0 = time.perf_counter()
    processed_combinations = get_processed_combinations()
    _log_duration("get_processed_combinations", t0)
    print(f"Found {len(processed_combinations)} already processed items in Excel file")
    
    items_before = len(items_to_process)
    items_to_process = [
        item for item in items_to_process 
        if (item['loan_id'], item['doc_type']) not in processed_combinations
    ]
    items_skipped = items_before - len(items_to_process)
    
    if items_skipped > 0:
        print(f"Skipping {items_skipped} already processed items (resume mode)")
    print(f"After resume check: {len(items_to_process)} items to process")
    
    if not items_to_process:
        print("No items to process")
        return

    # Create shared model instance
    print("Initializing AI model...")
    t0 = time.perf_counter()
    shared_model = initialize_model()
    _log_duration("initialize_model", t0)

    # Counters
    processed_count = 0
    total_processing_seconds = 0.0
    success_count = 0
    error_count = 0
    quota_exhausted = False
    
    print(f"\nStarting sequential processing...")
    
    try:
        # Process items sequentially
        for idx, item in enumerate(items_to_process, 1):
            # Check for shutdown request
            if shutdown_requested:
                print("\n⚠️  Shutdown requested. Stopping processing...")
                break
            
            # Check for quota exhaustion
            if quota_exhausted:
                break

            t_item = time.perf_counter()
            try:
                item_approval_date = approval_dates.get(item['loan_id'])
                result = process_single_item(item, model=shared_model, approval_date=item_approval_date)
                item_elapsed = time.perf_counter() - t_item
                total_processing_seconds += item_elapsed
                save_result_to_buffer(result)

                processed_count += 1

                if result['success']:
                    success_count += 1
                    print(f"  [{processed_count}/{len(items_to_process)}] {result['loan_id']} - {result['doc_type']}: ✓ Success ({item_elapsed:.2f}s)")
                else:
                    error_count += 1
                    if result.get('quota_exhausted', False):
                        quota_exhausted = True
                        flush_results_buffer()
                        print(f"\n  [{processed_count}/{len(items_to_process)}] {result['loan_id']} - {result['doc_type']}: ⚠️  Quota Exhausted")
                        print("\n" + "="*60)
                        print("⚠️  API QUOTA EXHAUSTED")
                        print("="*60)
                        print(f"Progress saved to: {FINAL_OUTPUT_FILE}")
                        print("Run the script again - it will resume from where it stopped")
                        print("="*60)
                        break
                    else:
                        print(f"  [{processed_count}/{len(items_to_process)}] {result['loan_id']} - {result['doc_type']}: ✗ Error - {result['error']} ({item_elapsed:.2f}s)")
            except Exception as e:
                item_elapsed = time.perf_counter() - t_item
                total_processing_seconds += item_elapsed
                error_count += 1
                processed_count += 1
                print(f"  [{processed_count}/{len(items_to_process)}] {item['loan_id']} - {item['doc_type']}: ✗ Unexpected error - {str(e)} ({item_elapsed:.2f}s)")
                save_result_to_buffer({
                    'success': False,
                    'loan_id': item['loan_id'],
                    'doc_type': item['doc_type'],
                    'media_ref': item.get('media_ref', ''),
                    'error': str(e),
                    'downloadable_link': item.get('downloadable_link', '')
                })
    finally:
        # Always flush buffer on exit (interrupt, quota exhaustion, or completion)
        print("\nFlushing remaining results to disk...")
        t_flush = time.perf_counter()
        flush_results_buffer()
        _log_duration("flush_results_buffer", t_flush)
        print("✓ Progress saved successfully!")
    
    # Always create final Excel sheets (even if interrupted or quota exhausted)
    # This ensures we can see what was processed
    if not quota_exhausted:
        print(f"\n{'='*60}")
        print("Processing complete!")
        print(f"Total processed: {processed_count}/{len(items_to_process)}")
        print(f"Successful: {success_count}")
        print(f"Errors: {error_count}")
        if processed_count > 0:
            print(f"Document processing time: {total_processing_seconds:.2f}s (avg {total_processing_seconds / processed_count:.2f}s per item)")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print("Processing interrupted or quota exhausted")
        print(f"Total processed: {processed_count}/{len(items_to_process)}")
        print(f"Successful: {success_count}")
        print(f"Errors: {error_count}")
        if processed_count > 0:
            print(f"Document processing time: {total_processing_seconds:.2f}s (avg {total_processing_seconds / processed_count:.2f}s per item)")
        print(f"{'='*60}")
    
    # Always create final Excel with separate sheets and document links
    # This works even if processing was interrupted
    try:
        t_excel = time.perf_counter()
        create_final_excel_with_sheets()
        _log_duration("create_final_excel_with_sheets", t_excel)
    except Exception as e:
        print(f"\n⚠️  Warning: Could not create final Excel sheets: {str(e)}")
        print("Raw data is still available in the 'raw_data' sheet")

    total_elapsed = time.perf_counter() - main_start
    print(f"\n[TIMING] Total run: {total_elapsed:.2f}s")
    if processed_count > 0:
        print(f"  [TIMING] Document processing (AI + I/O): {total_processing_seconds:.2f}s ({total_processing_seconds / processed_count:.2f}s avg per item)")
    print(f"\nOutput saved to: {FINAL_OUTPUT_FILE}")

if __name__ == "__main__":
    main()
