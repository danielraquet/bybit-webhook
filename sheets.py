"""
Google Sheets integration — pushes trade data to Trading Journal tab
Requires GOOGLE_SHEETS_ID and GOOGLE_SERVICE_ACCOUNT_JSON env vars
"""

import os
import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)

SHEET_ID       = os.getenv("GOOGLE_SHEETS_ID", "")
CREDS_JSON     = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SHEET_TAB      = "Trading Journal"
DATA_START_ROW = 4  # first data row

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


def _get_service():
    """Build Google Sheets service from service account JSON."""
    if not CREDS_JSON or not SHEET_ID:
        return None
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        creds_dict = json.loads(CREDS_JSON)
        scopes     = ["https://www.googleapis.com/auth/spreadsheets"]
        creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        service    = build("sheets", "v4", credentials=creds, cache_discovery=False)
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
    """Find the next empty row by checking Entry Date/Time column (B)."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"'{SHEET_TAB}'!B{DATA_START_ROW}:B"
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
        row_data[COL_START_BAL  - 1] = 2500
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


def is_configured() -> bool:
    """Check if Google Sheets integration is configured."""
    return bool(SHEET_ID and CREDS_JSON)
