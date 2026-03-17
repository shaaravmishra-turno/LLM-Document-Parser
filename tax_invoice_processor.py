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

import argparse
import base64
import io
import json
import logging
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import anthropic
import pandas as pd
import requests
from dotenv import load_dotenv
from pdf2image import convert_from_bytes
from PIL import Image

load_dotenv()

# ─── Logging ───────────────────────────────────────────────────────────────────
LOG_FILE = os.getenv("TAX_LOG_FILE", "tax_invoice_processor_ocr_failure.log")
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
MAX_WORKERS       = int(os.getenv("MAX_WORKERS", "5"))     # parallel threads
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
    "xmart_special_fee"
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

MAX_DIMENSION = 7900                          # Claude rejects anything ≥ 8000 px
MAX_RAW_BYTES = int(5 * 1024 * 1024 * 3 / 4)  # ~3.75 MB raw ≈ 5 MB base64 (b64 adds ~33%)


def _pil_to_b64(img: Image.Image) -> tuple[str, str]:
    """Convert a PIL image to a base64 JPEG string, handling:
    1. RGBA / P / LA modes → RGB  (JPEG has no alpha channel)
    2. Dimensions > 7900 px       (Claude rejects ≥ 8000 px)
    3. Encoded size > 5 MB        (Claude hard limit — raw must stay ≤ ~3.75 MB)
    """
    # ── 1. Strip alpha / palette ─────────────────────────────────────────────
    if img.mode in ("RGBA", "P", "LA"):
        img = img.convert("RGB")

    # ── 2. Down-scale if any dimension exceeds MAX_DIMENSION ─────────────────
    w, h = img.size
    if w > MAX_DIMENSION or h > MAX_DIMENSION:
        ratio = min(MAX_DIMENSION / w, MAX_DIMENSION / h)
        img = img.resize((int(w * ratio), int(h * ratio)), Image.LANCZOS)

    # ── 3. Encode, reduce quality, then resize progressively until under limit
    quality = 90
    while True:
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality)
        if buf.tell() <= MAX_RAW_BYTES:
            break

        if quality > 30:
            quality -= 10          # try lower quality first
        else:
            # Quality alone isn't enough — shrink dimensions by 20%
            w, h = img.size
            img = img.resize((int(w * 0.8), int(h * 0.8)), Image.LANCZOS)
            quality = 70           # reset quality after resize

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
    """Write the DataFrame to the output Excel file (keeps _status for resumability)."""
    df.to_excel(OUTPUT_FILE, index=False)


def export_clean(df: pd.DataFrame):
    """Write a clean copy without the internal _status column (for final delivery)."""
    clean_path = OUTPUT_FILE.replace(".xlsx", "_clean.xlsx")
    out = df.drop(columns=["_status"], errors="ignore")
    out.to_excel(clean_path, index=False)
    print(f"Clean export (no _status): {clean_path}")


# ─── Worker function (runs in thread pool) ────────────────────────────────────

def _process_one(client, row_idx, uuid_val, file_url, pos, out_df, df_lock, counters, total_pending):
    """Download + call Claude + fill cells for a single row. Thread-safe."""
    if shutdown_requested:
        return

    # ── 1. Download ───────────────────────────────────────────────────────────
    file_bytes, content_type = download_file(file_url)
    if file_bytes is None:
        with df_lock:
            out_df.at[row_idx, "_status"] = "error:download_failed"
            counters["error"] += 1
            counters["dirty"] += 1
            print(f"[{pos}/{total_pending}] {uuid_val}  ✗ download failed")
        return

    # Infer content_type from magic bytes when headers are unhelpful
    if not content_type or content_type in ("application/octet-stream", "binary/octet-stream"):
        content_type = (
            "application/pdf"
            if file_bytes[:4] == b"%PDF"
            else "image/jpeg"
        )

    # ── 2. Call Claude ────────────────────────────────────────────────────────
    t_item = time.perf_counter()
    try:
        raw = call_claude(client, file_bytes, content_type)
        extracted = parse_response(raw)
        elapsed = time.perf_counter() - t_item

        # Log raw + parsed response (logging is thread-safe)
        logger.info("UUID=%s | raw_response=%s", uuid_val, raw)
        logger.info("UUID=%s | parsed=%s", uuid_val, json.dumps(extracted))

        with df_lock:
            if not extracted:
                print(f"[{pos}/{total_pending}] {uuid_val}  ✗ JSON parse failed ({elapsed:.1f}s) | raw: {raw[:120]}")
                out_df.at[row_idx, "_status"] = "error:json_parse"
                counters["error"] += 1
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
                counters["success"] += 1
                print(f"[{pos}/{total_pending}] {uuid_val}  ✓ filled {len(filled_fields)} fields ({elapsed:.1f}s)")
                logger.info("UUID=%s | filled_fields=%s", uuid_val, filled_fields)

            counters["dirty"] += 1

    except anthropic.RateLimitError:
        with df_lock:
            logger.error("UUID=%s | RateLimitError", uuid_val)
            out_df.at[row_idx, "_status"] = "error:rate_limit"
            counters["error"] += 1
            counters["dirty"] += 1
            counters["rate_limited"] = True
        print(f"[{pos}/{total_pending}] {uuid_val}  ⚠️  rate-limited")
    except Exception as exc:
        elapsed = time.perf_counter() - t_item
        with df_lock:
            logger.error("UUID=%s | error: %s (%.1fs)", uuid_val, exc, elapsed)
            print(f"[{pos}/{total_pending}] {uuid_val}  ✗ {exc} ({elapsed:.1f}s)")
            out_df.at[row_idx, "_status"] = f"error:{str(exc)[:80]}"
            counters["error"] += 1
            counters["dirty"] += 1


# ─── CLI arguments ─────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Tax invoice processor with Claude")
    parser.add_argument(
        "--start-row", type=int, default=None,
        help="1-based starting row in the INPUT Excel to process from (e.g. --start-row 1994)"
    )
    parser.add_argument(
        "--end-row", type=int, default=None,
        help="1-based ending row (inclusive) in the INPUT Excel to process up to (e.g. --end-row 3000)"
    )
    return parser.parse_args()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY == "your_anthropic_api_key_here":
        raise SystemExit("ERROR: Set ANTHROPIC_API_KEY in .env before running.")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # ── Load input ────────────────────────────────────────────────────────────
    print(f"Reading {INPUT_FILE}…")
    df_input = pd.read_excel(INPUT_FILE, dtype=str)
    print(f"  {len(df_input)} rows  |  columns: {[c for c in df_input.columns if not c.startswith('Unnamed')]}")

    # ── Apply row range filter (1-based, inclusive) ───────────────────────────
    total_input_rows = len(df_input)
    start_idx = (args.start_row - 1) if args.start_row else 0
    end_idx   = args.end_row if args.end_row else total_input_rows

    if start_idx < 0:
        start_idx = 0
    if end_idx > total_input_rows:
        end_idx = total_input_rows

    if start_idx > 0 or end_idx < total_input_rows:
        df_input = df_input.iloc[start_idx:end_idx].reset_index(drop=True)
        print(f"  Row range: {start_idx + 1}–{end_idx} ({len(df_input)} rows selected)")

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
        print(f"  {len(pending_idx)} items to process (capped at PROCESS_LIMIT={PROCESS_LIMIT})")
    else:
        print(f"  {len(pending_idx)} items to process")

    if not pending_idx:
        print("Nothing to do — all rows already processed.")
        save_df(out_df)
        export_clean(out_df)
        return

    print(f"  Using {MAX_WORKERS} parallel workers\n")

    df_lock = threading.Lock()
    counters = {"success": 0, "error": 0, "dirty": 0, "dispatched": 0, "rate_limited": False}
    t_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for row_idx in pending_idx:
            if shutdown_requested or counters["rate_limited"]:
                break

            row      = out_df.loc[row_idx]
            uuid_val = str(row.get("UUID", "")).strip()
            file_url = str(row.get("file_url", "")).strip()

            counters["dispatched"] += 1
            pos = counters["dispatched"]

            fut = executor.submit(
                _process_one, client, row_idx, uuid_val, file_url, pos,
                out_df, df_lock, counters, len(pending_idx),
            )
            futures[fut] = uuid_val

        # Wait for all submitted tasks and do periodic batch saves
        for fut in as_completed(futures):
            try:
                fut.result()  # surfaces exceptions from the worker
            except Exception as exc:
                print(f"  Unexpected worker error: {exc}")

            # Batch save under lock
            with df_lock:
                if counters["dirty"] >= BATCH_SIZE:
                    save_df(out_df)
                    counters["dirty"] = 0

            if shutdown_requested or counters["rate_limited"]:
                # Cancel pending futures that haven't started
                for f in futures:
                    f.cancel()
                break

    # Final save (with _status for resumability)
    save_df(out_df)
    # Also produce a clean export without _status
    export_clean(out_df)

    total = time.perf_counter() - t_start
    processed = counters["success"] + counters["error"]
    avg = total / processed if processed else 0
    print(f"\nDone — {counters['success']} succeeded, {counters['error']} failed  |  {total:.1f}s total  ({avg:.1f}s/item)")
    if counters["rate_limited"]:
        print("⚠️  Stopped early due to rate limit. Re-run to resume.")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
