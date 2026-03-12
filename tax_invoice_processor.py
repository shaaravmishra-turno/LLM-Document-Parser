"""
tax_invoice_processor.py

Reads TAX Invoice Rerun.xlsx, downloads each document directly from the
file_url column, sends it to Claude for extraction, and writes an output Excel
(same column structure) with every previously-empty cell filled from the LLM
response.

Already-filled cells are NEVER overwritten.
The script is resumable — rows already in the output file are skipped.

Config is loaded from .env:
    ANTHROPIC_API_KEY, CLAUDE_MODEL, MAX_TOKENS
    TURNO_AUTH_TOKEN
    TAX_INPUT_FILE, TAX_OUTPUT_FILE
    BATCH_SIZE
    TAX_INVOICE_PROMPT
"""

import base64
import io
import json
import logging
import os
import signal
import time

import anthropic
import pandas as pd
import requests
from dotenv import load_dotenv
from pdf2image import convert_from_bytes
from PIL import Image

load_dotenv()

# ─── Logging ───────────────────────────────────────────────────────────────────
LOG_FILE = os.getenv("TAX_LOG_FILE", "tax_invoice_processor_421.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
MAX_TOKENS        = int(os.getenv("MAX_TOKENS", "4096"))

AUTH_TOKEN        = os.getenv("TURNO_AUTH_TOKEN", "")

INPUT_FILE        = os.getenv("TAX_INPUT_FILE", "TAX Invoice Rerun.xlsx")
OUTPUT_FILE       = os.getenv("TAX_OUTPUT_FILE", "TAX Invoice Filled.xlsx")

BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "5"))
PROCESS_LIMIT     = int(os.getenv("PROCESS_LIMIT", "0"))  # 0 = no limit
PROMPT            = os.getenv("TAX_INVOICE_PROMPT", "Extract all fields from this vehicle tax invoice and return valid JSON.")

# Columns that carry extracted data (in the same order as the Excel sheet)
DATA_COLUMNS = [
    "customer_name",
    "invoice_date",
    "dealer_name",
    "lender_name",
    "total_invoice_amount",
    "taxable_amount",
    "total_invoice_amount_in_words",
    "total_invoice_amount_converted",
    "subsidy",
    "tax",
    "cgst",
    "sgst",
    "oem",
    "model",
    "invoice_number",
    "engine_number",
    "chassis_number",
    "net_exshowroom_price",
    "discount",
    "handling_charges",
    "accessory_charges",
    "rto_charges",
    "insurance",
    "assumed_fields"
]

# Graceful shutdown
shutdown_requested = False

def _signal_handler(signum, frame):
    global shutdown_requested
    print("\n⚠️  Interrupt received — finishing current item then saving…")
    shutdown_requested = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ─── Download helper ──────────────────────────────────────────────────────────

def download_file(url: str) -> tuple[bytes | None, str | None]:
    """Download a document and return (bytes, content_type)."""
    try:
        resp = requests.get(
            url,
            cookies={"auth_token": AUTH_TOKEN},
            timeout=60,
        )
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "").split(";")[0].strip()
        return resp.content, ct
    except Exception as exc:
        print(f"    download error: {exc}")
        return None, None


# ─── Claude helpers ───────────────────────────────────────────────────────────

def _pil_to_b64(img: Image.Image) -> tuple[str, str]:
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return base64.standard_b64encode(buf.getvalue()).decode(), "image/jpeg"


def call_claude(client: anthropic.Anthropic, file_bytes: bytes, content_type: str) -> str:
    """Count tokens, print the count, then send the document to Claude."""
    is_pdf = content_type == "application/pdf" or file_bytes[:4] == b"%PDF"

    if is_pdf:
        pdf_b64 = base64.standard_b64encode(file_bytes).decode()
        content_blocks = [
            {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": pdf_b64,
                },
            },
            {"type": "text", "text": PROMPT},
        ]
    else:
        try:
            images = convert_from_bytes(file_bytes)
        except Exception:
            images = [Image.open(io.BytesIO(file_bytes))]

        content_blocks = []
        for img in images:
            b64, mt = _pil_to_b64(img)
            content_blocks.append(
                {"type": "image", "source": {"type": "base64", "media_type": mt, "data": b64}}
            )
        content_blocks.append({"type": "text", "text": PROMPT})

    messages = [{"role": "user", "content": content_blocks}]

    # Count tokens before sending
    try:
        token_count = client.messages.count_tokens(
            model=CLAUDE_MODEL,
            messages=messages,
        )
        print(f"tokens={token_count.input_tokens:,}", end="  ", flush=True)
    except Exception:
        pass  # Non-fatal; proceed with the actual call

    msg = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=MAX_TOKENS,
        messages=messages,
    )
    return msg.content[0].text.strip()


def parse_response(text: str) -> dict:
    """Strip markdown fences and parse JSON; return empty dict on failure."""
    for prefix in ("```json", "```"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


# ─── Excel I/O ────────────────────────────────────────────────────────────────

def load_output_df() -> pd.DataFrame:
    """Load the current output file, or return an empty frame if it doesn't exist."""
    if not os.path.exists(OUTPUT_FILE):
        return pd.DataFrame()
    try:
        return pd.read_excel(OUTPUT_FILE, dtype=str)
    except Exception:
        return pd.DataFrame()


def get_processed_uuids(out_df: pd.DataFrame) -> set[str]:
    """Return UUIDs that were already successfully processed in the output file."""
    if out_df.empty or "UUID" not in out_df.columns or "_status" not in out_df.columns:
        return set()
    done = out_df[out_df["_status"] == "done"]["UUID"].dropna()
    return set(done.astype(str).str.strip())


def save_df(df: pd.DataFrame):
    """Write the DataFrame to the output Excel file (drops internal _status column)."""
    out = df.drop(columns=["_status"], errors="ignore")
    out.to_excel(OUTPUT_FILE, index=False)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY == "your_anthropic_api_key_here":
        raise SystemExit("ERROR: Set ANTHROPIC_API_KEY in .env before running.")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # ── Load input ────────────────────────────────────────────────────────────
    print(f"Reading {INPUT_FILE}…")
    df_input = pd.read_excel(INPUT_FILE, dtype=str)
    print(f"  {len(df_input)} rows  |  columns: {[c for c in df_input.columns if not c.startswith('Unnamed')]}")

    # ── Load or initialise output ─────────────────────────────────────────────
    out_df = load_output_df()

    if out_df.empty:
        out_df = df_input.copy()
        out_df["_status"] = ""
    else:
        existing_uuids = set(out_df["UUID"].astype(str).str.strip())
        new_rows = df_input[~df_input["UUID"].astype(str).str.strip().isin(existing_uuids)].copy()
        new_rows["_status"] = ""
        out_df = pd.concat([out_df, new_rows], ignore_index=True)
        if "_status" not in out_df.columns:
            out_df["_status"] = ""

    processed_uuids = get_processed_uuids(out_df)
    print(f"  {len(processed_uuids)} already processed (resuming)")

    pending_mask = (
        (~out_df["UUID"].astype(str).str.strip().isin(processed_uuids))
        & out_df["file_url"].notna()
        & (out_df["file_url"].astype(str).str.strip() != "")
    )
    pending_idx = out_df[pending_mask].index.tolist()
    if PROCESS_LIMIT > 0:
        pending_idx = pending_idx[:PROCESS_LIMIT]
        print(f"  {len(pending_idx)} items to process (capped at PROCESS_LIMIT={PROCESS_LIMIT})\n")
    else:
        print(f"  {len(pending_idx)} items to process\n")

    if not pending_idx:
        print("Nothing to do — all rows already processed.")
        save_df(out_df)
        return

    success = error = batch_dirty = 0
    t_start = time.perf_counter()

    for pos, row_idx in enumerate(pending_idx, start=1):
        if shutdown_requested:
            print("Shutdown requested — stopping.")
            break

        row      = out_df.loc[row_idx]
        uuid_val = str(row.get("UUID", "")).strip()
        file_url = str(row.get("file_url", "")).strip()

        print(f"[{pos}/{len(pending_idx)}] {uuid_val}", end="  ", flush=True)

        # ── 1. Download directly from file_url ────────────────────────────────
        file_bytes, content_type = download_file(file_url)
        if file_bytes is None:
            print("✗ download failed")
            out_df.at[row_idx, "_status"] = "error:download_failed"
            error += 1
            batch_dirty += 1
            if batch_dirty >= BATCH_SIZE:
                save_df(out_df)
                batch_dirty = 0
            continue

        # Infer content_type from magic bytes or URL when headers are unhelpful
        if not content_type or content_type in ("application/octet-stream", "binary/octet-stream"):
            content_type = (
                "application/pdf"
                if file_bytes[:4] == b"%PDF"
                else "image/jpeg"
            )

        # ── 2. Call Claude ────────────────────────────────────────────────────
        t_item = time.perf_counter()
        try:
            raw = call_claude(client, file_bytes, content_type)
            extracted = parse_response(raw)
            elapsed = time.perf_counter() - t_item

            # ── Log raw + parsed response ─────────────────────────────────
            logger.info("UUID=%s | raw_response=%s", uuid_val, raw)
            logger.info("UUID=%s | parsed=%s", uuid_val, json.dumps(extracted))

            if not extracted:
                print(f"✗ JSON parse failed ({elapsed:.1f}s) | raw: {raw[:120]}")
                out_df.at[row_idx, "_status"] = "error:json_parse"
                error += 1
            else:
                # ── 3. Fill only empty cells ──────────────────────────────────
                filled_fields = []
                for col in DATA_COLUMNS:
                    if col not in out_df.columns:
                        continue
                    current = out_df.at[row_idx, col]
                    is_empty = (
                        current is None
                        or (isinstance(current, float) and pd.isna(current))
                        or str(current).strip() in ("", "nan", "NaN", "None")
                    )
                    if is_empty and col in extracted and str(extracted[col]).strip():
                        out_df.at[row_idx, col] = str(extracted[col]).strip()
                        filled_fields.append(col)

                out_df.at[row_idx, "_status"] = "done"
                success += 1
                print(f"✓ filled {len(filled_fields)} fields ({elapsed:.1f}s)")
                logger.info("UUID=%s | filled_fields=%s", uuid_val, filled_fields)

        except anthropic.RateLimitError as exc:
            logger.error("UUID=%s | RateLimitError: %s", uuid_val, exc)
            save_df(out_df)
            raise SystemExit(
                f"\nRate-limited after {pos} items. Re-run to resume automatically."
            ) from exc
        except Exception as exc:
            elapsed = time.perf_counter() - t_item
            logger.error("UUID=%s | error: %s (%.1fs)", uuid_val, exc, elapsed)
            print(f"✗ {exc} ({elapsed:.1f}s)")
            out_df.at[row_idx, "_status"] = f"error:{str(exc)[:80]}"
            error += 1

        # ── Batch save ────────────────────────────────────────────────────────
        batch_dirty += 1
        if batch_dirty >= BATCH_SIZE:
            save_df(out_df)
            batch_dirty = 0

    # Final save
    save_df(out_df)

    total = time.perf_counter() - t_start
    processed = success + error
    avg = total / processed if processed else 0
    print(f"\nDone — {success} succeeded, {error} failed  |  {total:.1f}s total  ({avg:.1f}s/item)")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
