"""
TradingView → Bybit Webhook Server
====================================
Receives alerts from TradingView and places limit orders on Bybit
with Stop Loss and Take Profit.

Requirements:
    pip install flask pybit python-dotenv

Usage:
    python server.py

Then expose port 5000 via ngrok or deploy to a VPS.
Set the webhook URL in TradingView to: http://YOUR_SERVER:5000/webhook
"""

import os
import logging
import threading
import time
from flask import Flask, request, jsonify, render_template_string
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from journal import (log_order_placed, log_order_skipped, log_trade_closed,
                     get_all_trades, get_stats, get_db)

load_dotenv()

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("webhook.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_KEY        = os.getenv("BYBIT_API_KEY",    "")
API_SECRET     = os.getenv("BYBIT_API_SECRET", "")
TESTNET        = os.getenv("TESTNET", "true").lower() == "true"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

def get_config():
    """Read config fresh from environment on every call — picks up Railway variable changes."""
    return {
        "enabled":     os.getenv("ENABLED",     "true").lower() == "true",
        "balance_pct": float(os.getenv("BALANCE_PCT", "2.0")),
        "max_trades":  int(os.getenv("MAX_TRADES",    "3")),
        "leverage":    int(os.getenv("LEVERAGE",      "5")),
        "poll_interval": int(os.getenv("POLL_INTERVAL", "300")),
    }

app = Flask(__name__)

# ─── TRADE LOCK — prevents race conditions when alerts arrive simultaneously ───
# Only one alert is processed at a time — others wait their turn
trade_lock = threading.Lock()

# ─── JOURNAL DASHBOARD HTML ───────────────────────────────────────────────────
JOURNAL_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trade Journal</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:       #0a0c10;
    --surface:  #111318;
    --border:   #1e2128;
    --text:     #c8cdd8;
    --dim:      #5a6070;
    --green:    #00c896;
    --red:      #ff4d6a;
    --yellow:   #f5a623;
    --blue:     #4d9fff;
    --white:    #eef0f5;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 14px;
    min-height: 100vh;
  }
  header {
    border-bottom: 1px solid var(--border);
    padding: 20px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  header h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 18px;
    font-weight: 600;
    color: var(--white);
    letter-spacing: 0.05em;
  }
  header span {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    color: var(--dim);
  }
  .stats {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 1px;
    background: var(--border);
    border-bottom: 1px solid var(--border);
  }
  .stat {
    background: var(--surface);
    padding: 20px 24px;
  }
  .stat-label {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--dim);
    margin-bottom: 8px;
  }
  .stat-value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 22px;
    font-weight: 600;
    color: var(--white);
  }
  .stat-value.green { color: var(--green); }
  .stat-value.red   { color: var(--red); }
  .stat-value.yellow{ color: var(--yellow); }
  .filters {
    padding: 16px 32px;
    display: flex;
    gap: 8px;
    border-bottom: 1px solid var(--border);
  }
  .filter-btn {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    padding: 6px 14px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--dim);
    cursor: pointer;
    border-radius: 3px;
    transition: all 0.15s;
  }
  .filter-btn.active, .filter-btn:hover {
    border-color: var(--blue);
    color: var(--blue);
    background: rgba(77,159,255,0.06);
  }
  .table-wrap {
    overflow-x: auto;
    padding: 0 32px 32px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 16px;
  }
  th {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--dim);
    padding: 10px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  td {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    padding: 11px 12px;
    border-bottom: 1px solid rgba(30,33,40,0.6);
    white-space: nowrap;
    color: var(--text);
  }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .badge {
    display: inline-block;
    font-size: 10px;
    padding: 2px 8px;
    border-radius: 3px;
    font-weight: 500;
    letter-spacing: 0.05em;
  }
  .badge-buy     { background: rgba(0,200,150,0.12); color: var(--green); }
  .badge-sell    { background: rgba(255,77,106,0.12); color: var(--red); }
  .badge-open    { background: rgba(77,159,255,0.12); color: var(--blue); }
  .badge-closed  { background: rgba(90,96,112,0.15); color: var(--dim); }
  .badge-skipped { background: rgba(245,166,35,0.12); color: var(--yellow); }
  .badge-tp      { background: rgba(0,200,150,0.12); color: var(--green); }
  .badge-sl      { background: rgba(255,77,106,0.12); color: var(--red); }
  .pnl-pos { color: var(--green); }
  .pnl-neg { color: var(--red); }
  .mono { font-family: 'IBM Plex Mono', monospace; }
  .dim  { color: var(--dim); }
  .refresh {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 11px;
    padding: 6px 14px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--dim);
    cursor: pointer;
    border-radius: 3px;
  }
  .refresh:hover { color: var(--white); border-color: var(--dim); }
  .empty {
    text-align: center;
    padding: 60px;
    color: var(--dim);
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
  }
</style>
</head>
<body>

<header>
  <h1>// TRADE JOURNAL</h1>
  <div style="display:flex;align-items:center;gap:16px;">
    <span id="last-update">updated just now</span>
    <button class="refresh" onclick="location.reload()">↻ refresh</button>
  </div>
</header>

<div class="stats">
  <div class="stat">
    <div class="stat-label">Total Trades</div>
    <div class="stat-value">{{ stats.total_closed }}</div>
  </div>
  <div class="stat">
    <div class="stat-label">Win Rate</div>
    <div class="stat-value {{ 'green' if stats.win_rate >= 50 else 'red' }}">
      {{ stats.win_rate }}%
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Wins / Losses</div>
    <div class="stat-value">
      <span style="color:var(--green)">{{ stats.wins }}</span>
      <span style="color:var(--dim);font-size:16px"> / </span>
      <span style="color:var(--red)">{{ stats.losses }}</span>
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Total PnL</div>
    <div class="stat-value {{ 'green' if stats.total_pnl >= 0 else 'red' }}">
      {{ '+' if stats.total_pnl >= 0 else '' }}{{ stats.total_pnl }} USDT
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg Win</div>
    <div class="stat-value green">+{{ stats.avg_win }} USDT</div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg Loss</div>
    <div class="stat-value red">{{ stats.avg_loss }} USDT</div>
  </div>
  <div class="stat">
    <div class="stat-label">Open Now</div>
    <div class="stat-value blue" style="color:var(--blue)">{{ stats.open_count }}</div>
  </div>
</div>

<div class="filters">
  <button class="filter-btn active" onclick="filterTable('all', this)">All</button>
  <button class="filter-btn" onclick="filterTable('open', this)">Open</button>
  <button class="filter-btn" onclick="filterTable('closed', this)">Closed</button>
  <button class="filter-btn" onclick="filterTable('skipped', this)">Skipped</button>
  <button class="filter-btn" onclick="filterTable('tp', this)">TP Hit</button>
  <button class="filter-btn" onclick="filterTable('sl', this)">SL Hit</button>
</div>

<div class="table-wrap">
  {% if trades %}
  <table id="journal-table">
    <thead>
      <tr>
        <th>#</th>
        <th>Symbol</th>
        <th>Side</th>
        <th>Status</th>
        <th>Qty</th>
        <th>Entry</th>
        <th>Exit</th>
        <th>Stop Loss</th>
        <th>Take Profit</th>
        <th>PnL (USDT)</th>
        <th>PnL %</th>
        <th>Outcome</th>
        <th>Source</th>
        <th>Opened</th>
        <th>Closed</th>
        <th>Notes</th>
      </tr>
    </thead>
    <tbody>
      {% for t in trades %}
      <tr data-status="{{ t.status }}" data-outcome="{{ t.outcome or '' }}">
        <td class="dim">{{ t.id }}</td>
        <td style="color:var(--white);font-weight:500">{{ t.symbol }}</td>
        <td>
          <span class="badge {{ 'badge-buy' if t.side == 'Buy' else 'badge-sell' }}">
            {{ t.side }}
          </span>
        </td>
        <td>
          <span class="badge badge-{{ t.status }}">{{ t.status }}</span>
        </td>
        <td>{{ t.qty if t.qty else '—' }}</td>
        <td>{{ t.entry }}</td>
        <td>{{ t.exit_price if t.exit_price else '—' }}</td>
        <td style="color:var(--red)">{{ t.sl }}</td>
        <td style="color:var(--green)">{{ t.tp }}</td>
        <td class="{{ 'pnl-pos' if t.pnl and t.pnl > 0 else 'pnl-neg' if t.pnl and t.pnl < 0 else '' }}">
          {% if t.pnl %}{{ '+' if t.pnl > 0 else '' }}{{ t.pnl }}{% else %}—{% endif %}
        </td>
        <td class="{{ 'pnl-pos' if t.pnl_pct and t.pnl_pct > 0 else 'pnl-neg' if t.pnl_pct and t.pnl_pct < 0 else '' }}">
          {% if t.pnl_pct %}{{ '+' if t.pnl_pct > 0 else '' }}{{ t.pnl_pct }}%{% else %}—{% endif %}
        </td>
        <td>
          {% if t.outcome %}
          <span class="badge badge-{{ t.outcome }}">{{ t.outcome.upper() }}</span>
          {% else %}—{% endif %}
        </td>
        <td class="dim">{{ t.source or '—' }}</td>
        <td class="dim">{{ t.opened_at[:16] if t.opened_at else '—' }}</td>
        <td class="dim">{{ t.closed_at[:16] if t.closed_at else '—' }}</td>
        <td class="dim" style="max-width:200px;overflow:hidden;text-overflow:ellipsis">
          {{ t.notes or '—' }}
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <div class="empty">
    no trades yet — waiting for alerts...
  </div>
  {% endif %}
</div>

<script>
function filterTable(filter, btn) {
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('#journal-table tbody tr').forEach(row => {
    const status  = row.dataset.status;
    const outcome = row.dataset.outcome;
    let show = false;
    if (filter === 'all')     show = true;
    else if (filter === 'tp') show = outcome === 'tp';
    else if (filter === 'sl') show = outcome === 'sl';
    else                      show = status === filter;
    row.style.display = show ? '' : 'none';
  });
}
</script>
</body>
</html>
"""


# ─── BYBIT SESSION ────────────────────────────────────────────────────────────
session = HTTP(
    testnet=TESTNET,
    api_key=API_KEY,
    api_secret=API_SECRET,
)

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_open_positions() -> list:
    """Return list of symbols with open positions."""
    try:
        resp = session.get_positions(category="linear", settleCoin="USDT")
        positions = resp.get("result", {}).get("list", [])
        return [p["symbol"] for p in positions if float(p.get("size", 0)) > 0]
    except Exception as e:
        log.error(f"Error fetching positions: {e}")
        return []


def get_open_orders(symbol: str) -> list:
    """Return list of open orders for a symbol."""
    try:
        resp = session.get_open_orders(category="linear", symbol=symbol)
        return resp.get("result", {}).get("list", [])
    except Exception as e:
        log.error(f"Error fetching open orders for {symbol}: {e}")
        return []


def get_available_balance() -> float:
    """Return available USDT balance."""
    try:
        resp = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        coins = resp.get("result", {}).get("list", [{}])[0].get("coin", [])
        for coin in coins:
            if coin.get("coin") == "USDT":
                return float(coin.get("availableToWithdraw", 0))
        return 0.0
    except Exception as e:
        log.error(f"Error fetching balance: {e}")
        return 0.0


def get_instrument_info(symbol: str) -> dict:
    """Get min qty, qty step, price decimals for a symbol."""
    try:
        resp = session.get_instruments_info(category="linear", symbol=symbol)
        items = resp.get("result", {}).get("list", [])
        if items:
            lot   = items[0].get("lotSizeFilter", {})
            price = items[0].get("priceFilter", {})
            return {
                "min_qty":     float(lot.get("minOrderQty",  0.001)),
                "qty_step":    float(lot.get("qtyStep",       0.001)),
                "price_scale": int(price.get("tickSize", "0.01").split(".")[1].__len__()
                                   if "." in str(price.get("tickSize", "0.01")) else 0),
            }
    except Exception as e:
        log.error(f"Error fetching instrument info for {symbol}: {e}")
    return {"min_qty": 0.001, "qty_step": 0.001, "price_scale": 2}


def round_to_step(value: float, step: float) -> float:
    """Round value to nearest step size."""
    import math
    decimals = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
    return round(round(value / step) * step, decimals)


def set_leverage(symbol: str, leverage: int):
    """Set leverage for a symbol (silently ignore if already set)."""
    try:
        session.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage=str(leverage),
            sellLeverage=str(leverage),
        )
    except Exception:
        pass  # Already set or not supported — not critical


def calculate_qty(symbol: str, entry: float, balance_pct: float, leverage: int) -> float:
    """
    Calculate order quantity based on % of available balance.
    qty = (balance * pct/100 * leverage) / entry_price
    """
    balance = get_available_balance()
    if balance <= 0:
        log.error("Zero or negative balance — cannot calculate qty")
        return 0.0

    info       = get_instrument_info(symbol)
    notional   = balance * (balance_pct / 100.0) * leverage
    raw_qty    = notional / entry
    qty        = round_to_step(raw_qty, info["qty_step"])

    if qty < info["min_qty"]:
        log.warning(f"{symbol}: calculated qty {qty} below min {info['min_qty']}")
        return 0.0

    log.info(f"{symbol}: balance={balance:.2f} USDT, notional={notional:.2f}, qty={qty}")
    return qty


def poll_closed_trades():
    """
    Background thread — runs every poll_interval seconds.
    Checks Bybit order history for any open journal entries that have
    now been filled, cancelled, or had their SL/TP triggered.
    """
    while True:
        try:
            _check_closed_trades()
        except Exception as e:
            log.error(f"Poller error: {e}")
        time.sleep(get_config()["poll_interval"])


def _check_closed_trades():
    """Find open journal entries and check if Bybit has closed them."""
    with get_db() as conn:
        open_trades = conn.execute(
            "SELECT * FROM trades WHERE status = 'open' AND order_id IS NOT NULL"
        ).fetchall()

    if not open_trades:
        return

    log.info(f"Poller: checking {len(open_trades)} open trade(s)")

    for trade in open_trades:
        order_id = trade["order_id"]
        symbol   = trade["symbol"]

        try:
            # Check order history for this symbol
            resp = session.get_order_history(
                category="linear",
                symbol=symbol,
                orderId=order_id,
            )
            orders = resp.get("result", {}).get("list", [])

            if not orders:
                # Also check closed PnL in case SL/TP was triggered
                _check_closed_pnl(trade)
                continue

            order = orders[0]
            order_status = order.get("orderStatus", "")

            if order_status == "Filled":
                # Limit order filled — entry confirmed, trade is now open on exchange
                # Don't close journal entry yet — wait for SL/TP
                avg_price = float(order.get("avgPrice", 0))
                if avg_price > 0 and trade["entry"] != avg_price:
                    # Update actual entry price if different from limit
                    with get_db() as conn:
                        conn.execute(
                            "UPDATE trades SET entry = ? WHERE order_id = ?",
                            (avg_price, order_id)
                        )
                        conn.commit()
                    log.info(f"Updated entry price for {symbol} {order_id}: {avg_price}")

                # Now check if SL/TP has been triggered
                _check_closed_pnl(trade)

            elif order_status in ("Cancelled", "Rejected", "Deactivated"):
                # Order was cancelled before fill — mark as skipped
                with get_db() as conn:
                    conn.execute(
                        "UPDATE trades SET status = 'skipped', notes = ? WHERE order_id = ?",
                        (f"Order {order_status.lower()} on exchange", order_id)
                    )
                    conn.commit()
                log.info(f"Order {order_id} {symbol} was {order_status} — updated journal")

        except Exception as e:
            log.error(f"Error checking order {order_id} for {symbol}: {e}")


def _check_closed_pnl(trade):
    """
    Check Bybit's closed PnL records to see if a trade was closed
    by SL or TP being triggered.
    """
    symbol   = trade["symbol"]
    order_id = trade["order_id"]

    try:
        resp = session.get_closed_pnl(
            category="linear",
            symbol=symbol,
            limit=50,
        )
        records = resp.get("result", {}).get("list", [])

        for record in records:
            # Match by orderId or by closeSize/closePrice proximity
            rec_order_id = record.get("orderId", "")
            exec_type    = record.get("execType", "")    # "Trade", "StopLoss", "TakeProfit"
            exit_price   = float(record.get("avgExitPrice", 0) or record.get("exitPrice", 0))
            closed_size  = float(record.get("qty", 0))

            # Match by order_id OR by symbol + size + approximate time
            is_match = (rec_order_id == order_id or
                        (abs(closed_size - trade["qty"]) < 0.0001 and
                         exec_type in ("StopLoss", "TakeProfit", "Trade")))

            if is_match and exit_price > 0:
                # Determine outcome
                if exec_type == "TakeProfit":
                    outcome = "tp"
                elif exec_type == "StopLoss":
                    outcome = "sl"
                else:
                    # Determine by comparing exit to TP/SL
                    side = trade["side"]
                    tp   = trade["tp"]
                    sl   = trade["sl"]
                    if side == "Buy":
                        outcome = "tp" if exit_price >= tp * 0.999 else "sl"
                    else:
                        outcome = "tp" if exit_price <= tp * 1.001 else "sl"

                success = log_trade_closed(order_id, exit_price, outcome)
                if success:
                    log.info(f"✅ Poller closed {symbol} {order_id} — {outcome.upper()} @ {exit_price}")
                break

    except Exception as e:
        log.error(f"Error checking closed PnL for {symbol}: {e}")


# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Expected JSON payload from TradingView alert:

    {
        "secret":  "your_webhook_secret",   // optional security check
        "symbol":  "BTCUSDT",
        "side":    "Buy",                   // "Buy" or "Sell"
        "entry":   81500.0,
        "sl":      80200.0,
        "tp":      83800.0
    }
    """
    data = request.get_json(silent=True)
    if not data:
        # Try extracting JSON from a plain text message
        # Format: "readable text | {...json...}"
        raw = request.get_data(as_text=True)
        import re
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            import json as json_lib
            try:
                data = json_lib.loads(match.group())
            except Exception:
                pass
    if not data:
        log.warning("Received non-JSON or empty payload")
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    log.info(f"Received alert: {data}")

    # ── Read config fresh — picks up any Railway variable changes ─────────────
    cfg = get_config()

    # ── Master on/off switch ──────────────────────────────────────────────────
    if not cfg["enabled"]:
        log.info("Server is DISABLED — alert received but no order placed")
        return jsonify({"status": "disabled", "message": "Server is disabled — no order placed"}), 200

    # ── Optional secret check ─────────────────────────────────────────────────
    if WEBHOOK_SECRET and data.get("secret") != WEBHOOK_SECRET:
        log.warning("Invalid webhook secret")
        return jsonify({"status": "error", "message": "Unauthorized"}), 403

    # ── Parse payload ─────────────────────────────────────────────────────────
    symbol = data.get("symbol", "").upper().replace("/", "").replace("-", "")
    side   = data.get("side",   "")    # "Buy" or "Sell"
    entry  = float(data.get("entry", 0))
    sl     = float(data.get("sl",    0))
    tp     = float(data.get("tp",    0))

    if not all([symbol, side, entry, sl, tp]):
        msg = f"Missing required fields — got: {data}"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 400

    if side not in ("Buy", "Sell"):
        return jsonify({"status": "error", "message": f"Invalid side: {side}"}), 400

    # ── Everything from here is locked — one trade at a time ─────────────────
    with trade_lock:
        # ── Check max simultaneous trades ─────────────────────────────────────
        open_positions = get_open_positions()
        log.info(f"Open positions: {open_positions}")

        if symbol not in open_positions and len(open_positions) >= cfg["max_trades"]:
            msg = f"Max trades ({cfg['max_trades']}) reached — skipping {symbol}"
            log.warning(msg)
            log_order_skipped(symbol, side, entry, sl, tp, msg)
            return jsonify({"status": "skipped", "message": msg}), 200

        if symbol in open_positions:
            msg = f"Already have open position in {symbol} — skipping"
            log.warning(msg)
            log_order_skipped(symbol, side, entry, sl, tp, msg)
            return jsonify({"status": "skipped", "message": msg}), 200

        open_orders = get_open_orders(symbol)
        if open_orders:
            msg = f"Already have open order(s) for {symbol} — skipping"
            log.warning(msg)
            log_order_skipped(symbol, side, entry, sl, tp, msg)
            return jsonify({"status": "skipped", "message": msg}), 200

        qty = calculate_qty(symbol, entry, cfg["balance_pct"], cfg["leverage"])
        if qty <= 0:
            return jsonify({"status": "error", "message": "Invalid quantity calculated"}), 400

        set_leverage(symbol, cfg["leverage"])

        # Auto-cancel any pending opposite orders on this symbol
        auto_cancel_opposite(symbol, side)

        info        = get_instrument_info(symbol)
        price_scale = info["price_scale"]
        entry_str   = f"{entry:.{price_scale}f}"
        sl_str      = f"{sl:.{price_scale}f}"
        tp_str      = f"{tp:.{price_scale}f}"
        qty_str     = str(qty)

        try:
            resp = session.place_order(
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=qty_str,
                price=entry_str,
                stopLoss=sl_str,
                takeProfit=tp_str,
                slTriggerBy="LastPrice",
                tpTriggerBy="LastPrice",
                timeInForce="GTC",
                reduceOnly=False,
                closeOnTrigger=False,
            )

            ret_code = resp.get("retCode", -1)
            if ret_code == 0:
                order_id = resp.get("result", {}).get("orderId", "?")
                log.info(f"✅ Order placed: {symbol} {side} {qty} @ {entry_str} | SL {sl_str} | TP {tp_str} | ID {order_id}")
                # Log to journal
                log_order_placed(symbol, side, qty, entry, sl, tp, order_id,
                                 source=data.get("source", "fib"))
                return jsonify({
                    "status":   "ok",
                    "symbol":   symbol,
                    "side":     side,
                    "qty":      qty_str,
                    "entry":    entry_str,
                    "sl":       sl_str,
                    "tp":       tp_str,
                    "order_id": order_id,
                }), 200
            else:
                msg = resp.get("retMsg", "Unknown error")
                log.error(f"❌ Bybit error {ret_code}: {msg}")
                return jsonify({"status": "error", "message": msg, "code": ret_code}), 400

        except Exception as e:
            log.error(f"Exception placing order: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/status", methods=["GET"])
def status():
    """Health check — shows current config, open positions and balance."""
    cfg       = get_config()
    positions = get_open_positions()
    balance   = get_available_balance()
    return jsonify({
        "status":          "running",
        "enabled":         cfg["enabled"],
        "testnet":         TESTNET,
        "open_positions":  positions,
        "position_count":  len(positions),
        "max_trades":      cfg["max_trades"],
        "balance_usdt":    balance,
        "balance_pct":     cfg["balance_pct"],
        "leverage":        cfg["leverage"],
        "poll_interval":   cfg["poll_interval"],
    })


@app.route("/poll", methods=["POST"])
def manual_poll():
    """Manually trigger a check of closed trades — useful for testing."""
    try:
        _check_closed_trades()
        return jsonify({"status": "ok", "message": "Poll complete"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/cancel/<symbol>", methods=["POST"])
def cancel_orders(symbol):
    """Cancel all pending orders for a symbol — useful when rotating assets."""
    symbol = symbol.upper()
    try:
        resp = session.cancel_all_orders(category="linear", symbol=symbol)
        ret_code = resp.get("retCode", -1)
        if ret_code == 0:
            log.info(f"Cancelled all orders for {symbol}")
            with get_db() as conn:
                conn.execute("""
                    UPDATE trades SET status = 'skipped', notes = 'Manually cancelled'
                    WHERE symbol = ? AND status = 'open'
                """, (symbol,))
                conn.commit()
            return jsonify({"status": "ok", "cancelled": symbol}), 200
        else:
            return jsonify({"status": "error", "message": resp.get("retMsg")}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def auto_cancel_opposite(symbol: str, new_side: str):
    """
    When a new setup fires, cancel any pending unfilled orders on the same
    symbol in the OPPOSITE direction.
    e.g. new Bull setup → cancel any pending Sell limit orders on that symbol.
    """
    try:
        resp  = session.get_open_orders(category="linear", symbol=symbol)
        orders = resp.get("result", {}).get("list", [])
        opposite = "Sell" if new_side == "Buy" else "Buy"

        for order in orders:
            if order.get("side") == opposite:
                order_id = order.get("orderId")
                session.cancel_order(
                    category="linear",
                    symbol=symbol,
                    orderId=order_id
                )
                log.info(f"Auto-cancelled opposite {opposite} order {order_id} for {symbol}")
                with get_db() as conn:
                    conn.execute("""
                        UPDATE trades SET status = 'skipped',
                        notes = 'Auto-cancelled — opposite setup fired'
                        WHERE order_id = ? AND status = 'open'
                    """, (order_id,))
                    conn.commit()
    except Exception as e:
        log.error(f"Error auto-cancelling opposite orders for {symbol}: {e}")


@app.route("/journal")
def journal():
    """Trading journal dashboard."""
    trades = get_all_trades(200)
    stats  = get_stats()
    return render_template_string(JOURNAL_HTML, trades=trades, stats=stats)


@app.route("/journal/data")
def journal_data():
    """JSON endpoint for journal data."""
    return jsonify({
        "trades": get_all_trades(200),
        "stats":  get_stats()
    })


if __name__ == "__main__":
    cfg = get_config()
    log.info(f"Starting webhook server — testnet={TESTNET}")
    log.info(f"Config: {cfg['balance_pct']}% per trade, max {cfg['max_trades']} trades, {cfg['leverage']}x leverage")
    log.info(f"Poller: checking closed trades every {cfg['poll_interval']}s")

    # Start background poller thread
    poller = threading.Thread(target=poll_closed_trades, daemon=True)
    poller.start()

    app.run(host="0.0.0.0", port=5000, debug=False)
