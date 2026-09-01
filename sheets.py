"""
Google Sheets integration — pushes trade data to Trading Journal tab
Requires GOOGLE_SHEETS_ID and GOOGLE_SERVICE_ACCOUNT_JSON env vars
"""

import os
import json
import logging
import httplib2
from datetime import datetime

log = logging.getLogger(__name__)

SHEET_ID       = os.getenv("GOOGLE_SHEETS_ID", "")
CREDS_JSON     = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SHEET_TAB      = "Trading Journal"
DATA_START_ROW = 4  # first data row

# Hard timeout (seconds) on every Sheets API call. Without this, a slow or
# hung Google API response blocks the calling thread indefinitely — on a
# single-worker gunicorn setup that means a stalled Sheets call can trigger
# the worker timeout and kill the ENTIRE webhook server, not just the sync.
# This bounds the damage to a fast, loggable failure instead.
REQUEST_TIMEOUT_SECONDS = 10

# Cached service object — rebuilding this (JSON parse + OAuth credential
# construction) on every single call adds latency to the request path for
# no reason; the credentials don't change between calls.
_service_cache = None

# Column mapping (1-indexed)
COL_SR_NO        = 2   # B
COL_ENTRY_DT     = 3   # C
COL_ACCOUNT      = 4   # D
COL_EXCHANGE     = 5   # E
COL_CURRENCY     = 6   # F — static "$"
COL_START_BAL    = 7   # G
COL_SIDE         = 8   # H
COL_PAIR         = 9   # I
COL_QTY          = 10  # J
COL_ENTRY        = 11  # K
COL_SL           = 12  # L
COL_TP           = 13  # M
COL_TIMEFRAME    = 14  # N
COL_LEVERAGE     = 15  # O
COL_STRATEGY     = 16  # P
# Q onwards = auto-calculated by sheet
COL_EXIT_DT      = 28  # AB
COL_EXIT_QTY     = 29  # AC
COL_EXIT_PRICE   = 30  # AD
COL_TP1          = 31  # AE — partial exit (TP1) price, blank if no two-stage exit
COL_TP1_PCT      = 32  # AF — % of position closed at TP1
COL_OUTCOME      = 33  # AG — tp / sl / tp1_tp2 / tp1_sl

# Column used to detect the "next empty row". Must be a column that EVERY
# write path (push_trade_opened AND push_closed_trade) actually populates.
# COL_SR_NO is NOT safe for this — push_closed_trade never sets it, which
# previously caused _next_row to always return DATA_START_ROW and every
# closed trade to overwrite row 4.
ROW_DETECT_COL   = COL_PAIR  # I


def _get_service():
    """Build (or return cached) Google Sheets service, with a hard request
    timeout so a stalled Google API call can't hang the calling thread."""
    global _service_cache
    if _service_cache is not None:
        return _service_cache

    if not CREDS_JSON or not SHEET_ID:
        return None
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from google_auth_httplib2 import AuthorizedHttp

        creds_dict = json.loads(CREDS_JSON)
        scopes     = ["https://www.googleapis.com/auth/spreadsheets"]
        creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)

        # AuthorizedHttp wraps httplib2 so the timeout applies to every
        # request made through this service, including token refreshes.
        authed_http = AuthorizedHttp(
            creds, http=httplib2.Http(timeout=REQUEST_TIMEOUT_SECONDS)
        )
        service = build("sheets", "v4", http=authed_http, cache_discovery=False)
        _service_cache = service
        return service
    except Exception as e:
        log.error(f"Google Sheets service error: {e}")
        return None


def _col_letter(n: int) -> str:
    """Convert 1-indexed column number to letter (1=A, 28=AB etc)."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _next_row(service) -> int:
    """Find the next empty row by checking the Pair column (I), which every
    write path populates. Do not key this off COL_SR_NO — see comment above
    ROW_DETECT_COL."""
    try:
        col = _col_letter(ROW_DETECT_COL)
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"'{SHEET_TAB}'!{col}{DATA_START_ROW}:{col}"
        ).execute()
        values = result.get("values", [])
        # Find last non-empty row
        last_filled = 0
        for i, row in enumerate(values):
            if row and row[0]:
                last_filled = i
        return DATA_START_ROW + last_filled + (1 if last_filled > 0 or values else 0)
    except Exception as e:
        log.error(f"Error finding next row: {e}")
        return DATA_START_ROW


def _bar_seconds_to_tf(bar_seconds: int) -> str:
    """Convert barSeconds to sheet timeframe format (M15, H1 etc)."""
    mapping = {
        60:    "M1",
        180:   "M3",
        300:   "M5",
        900:   "M15",
        1800:  "M30",
        3600:  "H1",
        14400: "H4",
        86400: "D1",
    }
    return mapping.get(bar_seconds, f"M{bar_seconds // 60}" if bar_seconds < 3600 else f"H{bar_seconds // 3600}")


def push_trade_opened(symbol: str, side: str, qty: float, entry: float,
                      sl: float, tp: float, leverage: int, balance: float,
                      source: str, bar_seconds: int, order_id: str) -> int:
    """
    Push a new trade row to Google Sheets when order is placed.
    Returns the row number for later update on close.
    """
    service = _get_service()
    if not service:
        return -1

    try:
        row = _next_row(service)

        # Calculate derived values
        tf_str     = _bar_seconds_to_tf(bar_seconds)
        now        = datetime.utcnow().strftime("%d/%m/%Y %H:%M")

        # Build row data — columns A to AD (30 cols), A is empty
        row_data = [""] * 30

        row_data[COL_SR_NO      - 1] = row - DATA_START_ROW + 1
        row_data[COL_ENTRY_DT   - 1] = now
        row_data[COL_ACCOUNT    - 1] = "Trading"
        row_data[COL_EXCHANGE   - 1] = "Bybit"
        row_data[COL_CURRENCY   - 1] = "$"
        row_data[COL_START_BAL  - 1] = balance
        row_data[COL_SIDE       - 1] = "LONG" if side == "Buy" else "SHORT"
        row_data[COL_PAIR       - 1] = symbol
        row_data[COL_QTY        - 1] = qty
        row_data[COL_ENTRY      - 1] = entry
        row_data[COL_SL         - 1] = sl
        row_data[COL_TP         - 1] = tp
        row_data[COL_TIMEFRAME  - 1] = tf_str
        row_data[COL_LEVERAGE   - 1] = leverage
        row_data[COL_STRATEGY   - 1] = source.upper()
        # Calculated columns left empty for sheet formulas

        range_notation = f"'{SHEET_TAB}'!A{row}:{_col_letter(30)}{row}"
        service.spreadsheets().values().update(
            spreadsheetId  = SHEET_ID,
            range          = range_notation,
            valueInputOption = "USER_ENTERED",
            body           = {"values": [row_data]}
        ).execute()

        log.info(f"📊 Google Sheets: trade opened at row {row} — {symbol} {side}")
        return row

    except Exception as e:
        log.error(f"Google Sheets push_trade_opened error: {e}")
        return -1


def push_trade_closed(sheet_row: int, exit_price: float, qty: float):
    """
    Update exit columns when trade is closed.
    sheet_row: the row number returned by push_trade_opened
    """
    if sheet_row < DATA_START_ROW:
        return

    service = _get_service()
    if not service:
        return

    try:
        now = datetime.utcnow().strftime("%d/%m/%Y %H:%M")

        # Update exit columns AB, AC, AD
        exit_range = f"'{SHEET_TAB}'!{_col_letter(COL_EXIT_DT)}{sheet_row}:{_col_letter(COL_EXIT_PRICE)}{sheet_row}"
        service.spreadsheets().values().update(
            spreadsheetId    = SHEET_ID,
            range            = exit_range,
            valueInputOption = "USER_ENTERED",
            body             = {"values": [[now, qty, exit_price]]}
        ).execute()

        log.info(f"📊 Google Sheets: trade closed at row {sheet_row} — exit {exit_price}")

    except Exception as e:
        log.error(f"Google Sheets push_trade_closed error: {e}")


def push_closed_trade(symbol: str, side: str, qty: float, entry: float,
                      sl: float, tp: float, exit_price: float, pnl: float,
                      outcome: str, source: str, timeframe: str,
                      leverage: int, opened_at: str, closed_at: str,
                      tp1: float = None, tp1_pct: float = None) -> int:
    """Push a complete closed trade as a new row to Google Sheets.
    tp1/tp1_pct are optional — only set for trades that used the two-stage
    partial exit; left blank in the sheet otherwise."""
    service = _get_service()
    if not service:
        return -1
    try:
        row      = _next_row(service)
        row_data = [""] * COL_OUTCOME

        row_data[COL_PAIR      - 1] = symbol
        row_data[COL_SIDE      - 1] = "Long" if side == "Buy" else "Short"
        row_data[COL_STRATEGY  - 1] = source or "ob"
        row_data[COL_TIMEFRAME - 1] = timeframe or ""
        row_data[COL_ENTRY     - 1] = entry
        row_data[COL_SL        - 1] = sl
        row_data[COL_TP        - 1] = tp
        row_data[COL_QTY       - 1] = qty
        row_data[COL_LEVERAGE  - 1] = leverage
        row_data[COL_ENTRY_DT  - 1] = opened_at or ""
        row_data[COL_EXIT_DT   - 1] = closed_at or ""
        row_data[COL_EXIT_QTY  - 1] = qty
        row_data[COL_EXIT_PRICE- 1] = exit_price
        if tp1 is not None:
            row_data[COL_TP1     - 1] = tp1
            row_data[COL_TP1_PCT - 1] = tp1_pct or ""
        row_data[COL_OUTCOME   - 1] = outcome or ""

        service.spreadsheets().values().update(
            spreadsheetId    = SHEET_ID,
            range            = f"'{SHEET_TAB}'!A{row}:{_col_letter(len(row_data))}{row}",
            valueInputOption = "USER_ENTERED",
            body             = {"values": [row_data]}
        ).execute()
        log.info(f"📊 Sheets: pushed closed trade {symbol} {outcome} PnL={pnl} row {row}")
        return row
    except Exception as e:
        log.error(f"Sheets push_closed_trade error: {e}")
        return -1


def is_configured() -> bool:
    """Check if Google Sheets integration is configured."""
    return bool(SHEET_ID and CREDS_JSON)
