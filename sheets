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
COL_SR_NO        = 1   # A
COL_ENTRY_DT     = 2   # B
COL_ACCOUNT      = 3   # C
COL_EXCHANGE     = 4   # D
COL_START_BAL    = 5   # E
COL_SIDE         = 7   # G
COL_PAIR         = 8   # H
COL_QTY          = 9   # I
COL_ENTRY        = 10  # J
COL_SL           = 11  # K
COL_TP           = 12  # L
COL_TIMEFRAME    = 13  # M
COL_LEVERAGE     = 14  # N
COL_STRATEGY     = 15  # O
COL_1R           = 16  # P
COL_REQ_MARGIN   = 17  # Q
COL_POS_SIZE     = 19  # S
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
    """Find the next empty row in the Trading Journal tab."""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"'{SHEET_TAB}'!A{DATA_START_ROW}:A"
        ).execute()
        values = result.get("values", [])
        return DATA_START_ROW + len(values)
    except Exception as e:
        log.error(f"Error finding next row: {e}")
        return DATA_START_ROW


def _bar_seconds_to_tf(bar_seconds: int) -> str:
    """Convert barSeconds to readable timeframe string."""
    mapping = {
        60:    "1m",
        180:   "3m",
        300:   "5m",
        900:   "15m",
        1800:  "30m",
        3600:  "1H",
        14400: "4H",
        86400: "1D",
    }
    return mapping.get(bar_seconds, f"{bar_seconds}s")


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
        direction  = 1 if side == "Buy" else -1
        one_r      = abs(entry - sl) * qty
        req_margin = round(entry * qty / leverage, 2) if leverage > 0 else 0
        pos_size   = round(entry * qty, 2)
        tf_str     = _bar_seconds_to_tf(bar_seconds)
        now        = datetime.utcnow().strftime("%d/%m/%Y %H:%M")

        # Build row data — 30 columns (A to AD)
        # Empty strings for columns we skip
        row_data = [""] * 30

        row_data[COL_SR_NO      - 1] = row - DATA_START_ROW + 1
        row_data[COL_ENTRY_DT   - 1] = now
        row_data[COL_ACCOUNT    - 1] = "Bybit Main"
        row_data[COL_EXCHANGE   - 1] = "Bybit"
        row_data[COL_START_BAL  - 1] = round(balance, 2)
        row_data[COL_SIDE       - 1] = side
        row_data[COL_PAIR       - 1] = symbol
        row_data[COL_QTY        - 1] = qty
        row_data[COL_ENTRY      - 1] = entry
        row_data[COL_SL         - 1] = sl
        row_data[COL_TP         - 1] = tp
        row_data[COL_TIMEFRAME  - 1] = tf_str
        row_data[COL_LEVERAGE   - 1] = leverage
        row_data[COL_STRATEGY   - 1] = source.upper()
        row_data[COL_1R         - 1] = round(one_r, 4)
        row_data[COL_REQ_MARGIN - 1] = req_margin
        row_data[COL_POS_SIZE   - 1] = pos_size

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
