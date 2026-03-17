"""
tax_invoice_local.py

Reads loan reference IDs from loan_ref.txt (one per line), finds the
corresponding Vehicle Tax Invoice PDF in the local documents/ folder,
sends it to Claude for extraction, and writes an output Excel with
loan_ref as the first column followed by all parsed data fields.

Resumable — loan refs already marked "done" in the output file are skipped.

Config is loaded from .env (shared with tax_invoice_processor.py):
    ANTHROPIC_API_KEY, CLAUDE_MODEL, MAX_TOKENS
    BATCH_SIZE, MAX_WORKERS, PROCESS_LIMIT
    TAX_INVOICE_PROMPT
    TAX_LOCAL_OUTPUT_FILE, DOCUMENTS_DIR, LOAN_REF_FILE
"""

import json
import logging
import mimetypes
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import anthropic
import pandas as pd

from tax_invoice_processor import (
    ANTHROPIC_API_KEY,
    BATCH_SIZE,
    CLAUDE_MODEL,
    DATA_COLUMNS,
    MAX_WORKERS,
    PROCESS_LIMIT,
    call_claude,
    parse_response,
)

logger = logging.getLogger(__name__)

# ─── Local-specific config ─────────────────────────────────────────────────────
LOAN_REF_FILE = os.getenv("LOAN_REF_FILE", "loan_ref.txt")
DOCUMENTS_DIR = os.getenv("DOCUMENTS_DIR", "documents")
OUTPUT_FILE   = os.getenv("TAX_LOCAL_OUTPUT_FILE", "TAX Invoice Local Filled.xlsx")
DOC_TYPE_FOLDER = "Vehicle Tax Invoice"

# ─── Graceful shutdown ─────────────────────────────────────────────────────────
shutdown_requested = False

def _signal_handler(signum, frame):
    global shutdown_requested
    print("\n⚠️  Interrupt received — finishing current item then saving…")
    shutdown_requested = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ─── Local file reader ─────────────────────────────────────────────────────────

def read_loan_refs(path: str) -> list[str]:
    """Read loan_ref.txt and return a deduplicated list of non-blank IDs."""
    text = Path(path).read_text(encoding="utf-8")
    seen = set()
    refs = []
    for line in text.splitlines():
        ref = line.strip()
        if ref and ref not in seen:
            seen.add(ref)
            refs.append(ref)
    return refs


def find_local_file(loan_ref: str) -> Path | None:
    """Locate the first document inside documents/{loan_ref}/Vehicle Tax Invoice/."""
    folder = Path(DOCUMENTS_DIR) / loan_ref / DOC_TYPE_FOLDER
    if not folder.is_dir():
        return None
    for f in sorted(folder.iterdir()):
        if f.is_file() and not f.name.startswith("."):
            return f
    return None


def read_local_file(path: Path) -> tuple[bytes, str]:
    """Read file bytes and infer content type from extension / magic bytes."""
    data = path.read_bytes()
    mime, _ = mimetypes.guess_type(str(path))
    if not mime:
        mime = "application/pdf" if data[:4] == b"%PDF" else "image/jpeg"
    return data, mime


# ─── Excel I/O ────────────────────────────────────────────────────────────────

def load_output_df() -> pd.DataFrame:
    if not os.path.exists(OUTPUT_FILE):
        return pd.DataFrame()
    try:
        return pd.read_excel(OUTPUT_FILE, dtype=str)
    except Exception:
        return pd.DataFrame()


def get_processed_refs(out_df: pd.DataFrame) -> set[str]:
    if out_df.empty or "loan_ref" not in out_df.columns or "_status" not in out_df.columns:
        return set()
    done = out_df[out_df["_status"] == "done"]["loan_ref"].dropna()
    return set(done.astype(str).str.strip())


def save_df(df: pd.DataFrame):
    out = df.drop(columns=["_status"], errors="ignore")
    out.to_excel(OUTPUT_FILE, index=False)


# ─── Worker function ──────────────────────────────────────────────────────────

def _process_one(client, row_idx, loan_ref, pos, out_df, df_lock, counters, total_pending):
    """Read local file + call Claude + fill cells for a single row. Thread-safe."""
    if shutdown_requested:
        return

    file_path = find_local_file(loan_ref)
    if file_path is None:
        with df_lock:
            out_df.at[row_idx, "_status"] = "error:file_not_found"
            counters["error"] += 1
            counters["dirty"] += 1
            print(f"[{pos}/{total_pending}] {loan_ref}  ✗ no Vehicle Tax Invoice found in {DOCUMENTS_DIR}/{loan_ref}/")
        return

    try:
        file_bytes, content_type = read_local_file(file_path)
    except Exception as exc:
        with df_lock:
            out_df.at[row_idx, "_status"] = f"error:read_failed:{exc}"
            counters["error"] += 1
            counters["dirty"] += 1
            print(f"[{pos}/{total_pending}] {loan_ref}  ✗ file read error: {exc}")
        return

    t_item = time.perf_counter()
    try:
        raw = call_claude(client, file_bytes, content_type)
        extracted = parse_response(raw)
        elapsed = time.perf_counter() - t_item

        logger.info("loan_ref=%s | file=%s | raw_response=%s", loan_ref, file_path.name, raw)
        logger.info("loan_ref=%s | parsed=%s", loan_ref, json.dumps(extracted))

        with df_lock:
            if not extracted:
                print(f"[{pos}/{total_pending}] {loan_ref}  ✗ JSON parse failed ({elapsed:.1f}s) | raw: {raw[:120]}")
                out_df.at[row_idx, "_status"] = "error:json_parse"
                counters["error"] += 1
            else:
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
                print(f"[{pos}/{total_pending}] {loan_ref}  ✓ filled {len(filled_fields)} fields ({elapsed:.1f}s)")
                logger.info("loan_ref=%s | filled_fields=%s", loan_ref, filled_fields)

            counters["dirty"] += 1

    except anthropic.RateLimitError:
        with df_lock:
            logger.error("loan_ref=%s | RateLimitError", loan_ref)
            out_df.at[row_idx, "_status"] = "error:rate_limit"
            counters["error"] += 1
            counters["dirty"] += 1
            counters["rate_limited"] = True
        print(f"[{pos}/{total_pending}] {loan_ref}  ⚠️  rate-limited")
    except Exception as exc:
        elapsed = time.perf_counter() - t_item
        with df_lock:
            logger.error("loan_ref=%s | error: %s (%.1fs)", loan_ref, exc, elapsed)
            print(f"[{pos}/{total_pending}] {loan_ref}  ✗ {exc} ({elapsed:.1f}s)")
            out_df.at[row_idx, "_status"] = f"error:{str(exc)[:80]}"
            counters["error"] += 1
            counters["dirty"] += 1


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY == "your_anthropic_api_key_here":
        raise SystemExit("ERROR: Set ANTHROPIC_API_KEY in .env before running.")

    if not os.path.exists(LOAN_REF_FILE):
        raise SystemExit(f"ERROR: {LOAN_REF_FILE} not found. Create it with one loan ref per line.")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # ── Read loan refs ────────────────────────────────────────────────────────
    loan_refs = read_loan_refs(LOAN_REF_FILE)
    print(f"Read {len(loan_refs)} loan refs from {LOAN_REF_FILE}")

    # ── Load or initialise output ─────────────────────────────────────────────
    out_df = load_output_df()
    columns = ["loan_ref"] + DATA_COLUMNS

    if out_df.empty:
        out_df = pd.DataFrame(columns=columns)
        out_df["_status"] = pd.Series(dtype=str)

    existing_refs = set(out_df["loan_ref"].astype(str).str.strip()) if "loan_ref" in out_df.columns else set()
    new_refs = [r for r in loan_refs if r not in existing_refs]
    if new_refs:
        new_rows = pd.DataFrame({"loan_ref": new_refs})
        for col in DATA_COLUMNS:
            new_rows[col] = ""
        new_rows["_status"] = ""
        out_df = pd.concat([out_df, new_rows], ignore_index=True)

    if "_status" not in out_df.columns:
        out_df["_status"] = ""

    processed_refs = get_processed_refs(out_df)
    print(f"  {len(processed_refs)} already processed (resuming)")

    pending_mask = ~out_df["loan_ref"].astype(str).str.strip().isin(processed_refs)
    pending_idx = out_df[pending_mask].index.tolist()
    if PROCESS_LIMIT > 0:
        pending_idx = pending_idx[:PROCESS_LIMIT]
        print(f"  {len(pending_idx)} items to process (capped at PROCESS_LIMIT={PROCESS_LIMIT})")
    else:
        print(f"  {len(pending_idx)} items to process")

    if not pending_idx:
        print("Nothing to do — all loan refs already processed.")
        save_df(out_df)
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

            loan_ref = str(out_df.at[row_idx, "loan_ref"]).strip()
            counters["dispatched"] += 1
            pos = counters["dispatched"]

            fut = executor.submit(
                _process_one, client, row_idx, loan_ref, pos,
                out_df, df_lock, counters, len(pending_idx),
            )
            futures[fut] = loan_ref

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                print(f"  Unexpected worker error: {exc}")

            with df_lock:
                if counters["dirty"] >= BATCH_SIZE:
                    save_df(out_df)
                    counters["dirty"] = 0

            if shutdown_requested or counters["rate_limited"]:
                for f in futures:
                    f.cancel()
                break

    save_df(out_df)

    total = time.perf_counter() - t_start
    processed = counters["success"] + counters["error"]
    avg = total / processed if processed else 0
    print(f"\nDone — {counters['success']} succeeded, {counters['error']} failed  |  {total:.1f}s total  ({avg:.1f}s/item)")
    if counters["rate_limited"]:
        print("⚠️  Stopped early due to rate limit. Re-run to resume.")
    print(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
