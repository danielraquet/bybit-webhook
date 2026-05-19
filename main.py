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
import re
import logging
import threading
import time
from flask import Flask, request, jsonify, render_template_string
from pybit.unified_trading import HTTP
from dotenv import load_dotenv
from journal import (log_order_placed, log_order_skipped, log_trade_closed,
                     get_all_trades, get_stats, get_db, ph, DATABASE_URL)
import sheets as gsheets

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

# ─── TRADE LOCK ───────────────────────────────────────────────────────────────
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
    <a href="/analysis" style="color:var(--blue);font-size:12px;text-decoration:none;">📊 Analysis</a>
    <span id="last-update">updated just now</span>
    <button class="refresh" onclick="location.reload()">↻ refresh</button>
  </div>
</header>

<div class="stats">
  <div class="stat">
    <div class="stat-label">Total Trades</div>
    <div class="stat-value" id="stat-total">{{ stats.total_closed }}</div>
  </div>
  <div class="stat">
    <div class="stat-label">Win Rate</div>
    <div class="stat-value" id="stat-wr" class="{{ 'green' if stats.win_rate >= 50 else 'red' }}">
      {{ stats.win_rate }}%
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Wins / Losses</div>
    <div class="stat-value">
      <span style="color:var(--green)" id="stat-wins">{{ stats.wins }}</span>
      <span style="color:var(--dim);font-size:16px"> / </span>
      <span style="color:var(--red)" id="stat-losses">{{ stats.losses }}</span>
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Total PnL</div>
    <div class="stat-value" id="stat-pnl" class="{{ 'green' if stats.total_pnl >= 0 else 'red' }}">
      {{ '+' if stats.total_pnl >= 0 else '' }}{{ stats.total_pnl }} USDT
    </div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg Win</div>
    <div class="stat-value green" id="stat-avg-win">+{{ stats.avg_win }} USDT</div>
  </div>
  <div class="stat">
    <div class="stat-label">Avg Loss</div>
    <div class="stat-value red" id="stat-avg-loss">{{ stats.avg_loss }} USDT</div>
  </div>
  <div class="stat">
    <div class="stat-label">Open Now</div>
    <div class="stat-value" style="color:var(--blue)" id="stat-open">{{ stats.open_count }}</div>
  </div>
</div>

<div class="filters">
  <button class="filter-btn active" onclick="filterTable('all', this)">All</button>
  <button class="filter-btn" onclick="filterTable('open', this)">Open</button>
  <button class="filter-btn" onclick="filterTable('closed', this)">Closed</button>
  <button class="filter-btn" onclick="filterTable('skipped', this)">Skipped</button>
  <button class="filter-btn" onclick="filterTable('tp', this)">TP Hit</button>
  <button class="filter-btn" onclick="filterTable('sl', this)">SL Hit</button>
  <span style="margin:0 8px;color:var(--dim)">|</span>
  <button class="filter-btn" onclick="filterTable('src:ob', this)">OB</button>
  <button class="filter-btn" onclick="filterTable('src:fib', this)">FIB</button>
  <button class="filter-btn" onclick="filterTable('src:fibob', this)">FIBOB</button>
  <button class="filter-btn" onclick="filterTable('src:manual', this)">Manual</button>
  <span style="margin:0 8px;color:var(--dim)">|</span>
  <button class="filter-btn" onclick="runPoll(this)" style="color:var(--blue)">🔄 Poll Now</button>
  <button class="filter-btn" onclick="runFix(this)" style="color:var(--dim)">🔧 Fix Stale</button>
</div>

<div class="table-wrap">
  {% if trades %}
  <table id="journal-table">
    <thead>
      <tr>
        <th>#</th>
        <th>Symbol</th>
        <th>Side</th>
        <th>TF</th>
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
      <tr data-status="{{ t.status }}" data-outcome="{{ t.outcome or '' }}" data-source="{{ t.source or '' }}">
        <td class="dim">{{ t.id }}</td>
        <td style="color:var(--white);font-weight:500">{{ t.symbol }}</td>
        <td>
          <span class="badge {{ 'badge-buy' if t.side == 'Buy' else 'badge-sell' }}">
            {{ t.side }}
          </span>
        </td>
        <td class="dim">{{ t.timeframe if t.timeframe else '—' }}</td>
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
function runPoll(btn) {
  const base = window.location.origin;
  btn.innerText = '⏳ Polling...';
  btn.disabled = true;
  fetch(base + '/poll', {method: 'POST'})
    .then(r => r.json())
    .then(data => {
      btn.innerText = '✅ Done';
      setTimeout(() => { btn.innerText = '🔄 Poll Now'; btn.disabled = false; location.reload(); }, 2000);
    })
    .catch(err => { btn.innerText = '❌ ' + err; btn.disabled = false; });
}

function runFix(btn) {
  const base = window.location.origin;
  btn.innerText = '⏳ Fixing...';
  btn.disabled = true;
  fetch(base + '/journal/fix', {method: 'POST'})
    .then(r => r.json())
    .then(data => {
      btn.innerText = '✅ Done';
      setTimeout(() => { btn.innerText = '🔧 Fix Stale'; btn.disabled = false; location.reload(); }, 2000);
    })
    .catch(err => { btn.innerText = '❌ ' + err; btn.disabled = false; });
}

function filterTable(filter, btn) {
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('#journal-table tbody tr').forEach(row => {
    const status  = row.dataset.status;
    const outcome = row.dataset.outcome;
    const source  = row.dataset.source;
    let show = false;
    if (filter === 'all')               show = true;
    else if (filter === 'tp')           show = outcome === 'tp';
    else if (filter === 'sl')           show = outcome === 'sl';
    else if (filter.startsWith('src:')) show = source === filter.slice(4);
    else                                show = status === filter;
    row.style.display = show ? '' : 'none';
  });
  updateStats();
}

function updateStats() {
  const rows = document.querySelectorAll('#journal-table tbody tr');
  let total = 0, wins = 0, losses = 0, totalPnl = 0;
  let winPnls = [], lossPnls = [], openCount = 0;

  rows.forEach(row => {
    if (row.style.display === 'none') return;
    const status  = row.dataset.status;
    const outcome = row.dataset.outcome;
    const cells   = row.querySelectorAll('td');
    const pnlText = cells[10] ? cells[10].innerText.trim().replace('+','') : '';
    const pnl     = parseFloat(pnlText) || 0;

    if (status === 'closed') {
      total++;
      if (outcome === 'tp') { wins++; winPnls.push(pnl); }
      if (outcome === 'sl') { losses++; lossPnls.push(pnl); }
      if (outcome === 'tp' || outcome === 'sl') totalPnl += pnl;
    }
    if (status === 'open') openCount++;
  });

  const wr     = total > 0 ? Math.round(wins / total * 100) : 0;
  const avgWin  = winPnls.length  > 0 ? (winPnls.reduce((a,b)=>a+b,0)  / winPnls.length).toFixed(2)  : 0;
  const avgLoss = lossPnls.length > 0 ? (lossPnls.reduce((a,b)=>a+b,0) / lossPnls.length).toFixed(2) : 0;

  document.getElementById('stat-total').innerText    = total;
  document.getElementById('stat-wr').innerText       = wr + '%';
  document.getElementById('stat-wr').className       = 'stat-value ' + (wr >= 50 ? 'green' : 'red');
  document.getElementById('stat-wins').innerText     = wins;
  document.getElementById('stat-losses').innerText   = losses;
  document.getElementById('stat-pnl').innerText      = (totalPnl >= 0 ? '+' : '') + totalPnl.toFixed(2) + ' USDT';
  document.getElementById('stat-pnl').className      = 'stat-value ' + (totalPnl >= 0 ? 'green' : 'red');
  document.getElementById('stat-avg-win').innerText  = '+' + avgWin + ' USDT';
  document.getElementById('stat-avg-loss').innerText = avgLoss + ' USDT';
  document.getElementById('stat-open').innerText     = openCount;
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
        result = resp.get("result", {}).get("list", [{}])[0]
        # Try account-level total equity first
        for field in ["totalAvailableBalance", "totalEquity", "totalWalletBalance"]:
            val = result.get(field, "")
            if val and val != "":
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        # Try coin-level fields
        coins = result.get("coin", [])
        for coin in coins:
            if coin.get("coin") == "USDT":
                for field in ["walletBalance", "equity", "availableToWithdraw"]:
                    val = coin.get(field, "")
                    if val and val != "":
                        try:
                            return float(val)
                        except (ValueError, TypeError):
                            continue
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
    if DATABASE_URL:
        import psycopg2.extras
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM trades WHERE status = 'open' AND order_id IS NOT NULL")
                open_trades = [dict(r) for r in cur.fetchall()]
    else:
        with get_db() as conn:
            open_trades = [dict(r) for r in conn.execute(
                "SELECT * FROM trades WHERE status = 'open' AND order_id IS NOT NULL"
            ).fetchall()]

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
                # Order not in history — check if it's still in open orders
                open_resp = session.get_open_orders(category="linear", symbol=symbol, orderId=order_id)
                open_orders = open_resp.get("result", {}).get("list", [])
                if not open_orders:
                    # Not open and not in history — must be cancelled or filled
                    # Check closed PnL first
                    _check_closed_pnl(trade)
                    # If still showing as open after PnL check, mark as cancelled
                    with get_db() as conn:
                        if DATABASE_URL:
                            import psycopg2.extras
                            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                                cur.execute("SELECT status FROM trades WHERE order_id = " + ph(), (order_id,))
                                row = cur.fetchone()
                                if row and row["status"] == "open":
                                    cur.execute("UPDATE trades SET status = 'skipped', notes = 'Order not found — likely cancelled' WHERE order_id = " + ph(), (order_id,))
                                    log.info(f"Marked {symbol} {order_id} as cancelled — not found in open orders or history")
                        else:
                            with conn.cursor() as cur:
                                cur.execute("SELECT status FROM trades WHERE order_id = " + ph(), (order_id,))
                                row = cur.fetchone()
                                if row and row[0] == "open":
                                    cur.execute("UPDATE trades SET status = 'skipped', notes = 'Order not found — likely cancelled' WHERE order_id = " + ph(), (order_id,))
                                    log.info(f"Marked {symbol} {order_id} as cancelled — not found in open orders or history")
                        conn.commit()
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
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET entry = " + ph() + " WHERE order_id = " + ph(), (avg_price, order_id))
                        conn.commit()
                    log.info(f"Updated entry price for {symbol} {order_id}: {avg_price}")

                # Now check if SL/TP has been triggered
                _check_closed_pnl(trade)

            elif order_status in ("Cancelled", "Rejected", "Deactivated"):
                # Order was cancelled before fill — mark as skipped
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE trades SET status = 'skipped', notes = " + ph() + " WHERE order_id = " + ph(), (f"Order {order_status.lower()} on exchange", order_id))
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
    qty      = float(trade["qty"] or 0)
    side     = trade["side"]

    try:
        # First check if position is still open — if so no need to check PnL yet
        pos_resp  = session.get_positions(category="linear", symbol=symbol)
        positions = pos_resp.get("result", {}).get("list", [])
        pos_open  = any(float(p.get("size", 0)) > 0 for p in positions)
        if pos_open:
            log.info(f"Position still open for {symbol} — skipping PnL check")
            return

        # Position closed — find the closing record in closed PnL
        resp    = session.get_closed_pnl(category="linear", symbol=symbol, limit=200)
        records = resp.get("result", {}).get("list", [])

        for record in records:
            rec_order_id = record.get("orderId", "")
            exec_type    = record.get("execType", "")
            exit_price   = float(record.get("avgExitPrice", 0) or record.get("exitPrice", 0))
            closed_size  = float(record.get("qty", 0))
            realised_pnl = float(record.get("closedPnl", 0) or 0)

            # Match by order ID, or by size (within 5%) + exec type
            id_match   = rec_order_id == order_id
            size_match = (qty > 0 and abs(closed_size - qty) < qty * 0.05 and
                         exec_type in ("StopLoss", "TakeProfit", "Trade"))
            is_match   = id_match or size_match

            if is_match and exit_price > 0:
                if exec_type == "TakeProfit":
                    outcome = "tp"
                elif exec_type == "StopLoss":
                    outcome = "sl"
                else:
                    tp = float(trade["tp"] or 0)
                    sl = float(trade["sl"] or 0)
                    if side == "Buy":
                        outcome = "tp" if exit_price >= tp * 0.999 else "sl"
                    else:
                        outcome = "tp" if exit_price <= tp * 1.001 else "sl"

                success = log_trade_closed(order_id, exit_price, outcome, realised_pnl=realised_pnl)
                if success:
                    log.info(f"✅ Poller closed {symbol} {order_id} — {outcome.upper()} @ {exit_price} PnL={realised_pnl}")
                    if gsheets.is_configured():
                        try:
                            with get_db() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("SELECT notes, qty FROM trades WHERE order_id = " + ph(), (order_id,))
                                    row = cur.fetchone()
                            if row and row[0] and "sheet_row:" in str(row[0]):
                                sheet_row = int(str(row[0]).split("sheet_row:")[1])
                                qty_val   = float(row[1]) if row[1] else 0
                                gsheets.push_trade_closed(sheet_row, exit_price, qty_val)
                        except Exception as e:
                            log.error(f"Google Sheets close update error: {e}")
                return

        log.warning(f"No closed PnL record found for {symbol} {order_id} — will retry next poll")

    except Exception as e:
        log.error(f"Error checking closed PnL for {symbol}: {e}")


def auto_cancel_opposite(symbol: str, new_side: str):
    """Cancel any pending orders in the opposite direction on this symbol."""
    try:
        resp     = session.get_open_orders(category="linear", symbol=symbol)
        orders   = resp.get("result", {}).get("list", [])
        opposite = "Sell" if new_side == "Buy" else "Buy"
        for order in orders:
            if order.get("side") == opposite:
                order_id = order.get("orderId")
                session.cancel_order(category="linear", symbol=symbol, orderId=order_id)
                log.info(f"Auto-cancelled opposite {opposite} order {order_id} for {symbol}")
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE trades SET status = 'skipped', notes = 'Auto-cancelled — opposite setup fired' WHERE order_id = " + ph() + " AND status = 'open'", (order_id,))
                    conn.commit()
    except Exception as e:
        log.error(f"Error auto-cancelling opposite orders for {symbol}: {e}")


# ─── POLLER STARTER — defined here so poll_closed_trades is already defined ───
def _start_poller():
    try:
        poller = threading.Thread(target=poll_closed_trades, daemon=True)
        poller.start()
        log.info("Background poller thread started")
    except Exception as e:
        log.error(f"Failed to start poller: {e}")

# Start poller immediately — all functions defined by this point
_start_poller()


# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/debug", methods=["POST", "GET"])
def debug():
    """Echo back exactly what was received — use to diagnose webhook issues."""
    raw      = request.get_data(as_text=True)
    headers  = dict(request.headers)
    json_data = request.get_json(silent=True)
    log.info(f"DEBUG endpoint hit — raw: {repr(raw[:1000])}")
    return jsonify({
        "raw_body":       raw[:1000],
        "content_type":   request.content_type,
        "parsed_json":    json_data,
        "headers":        {k: v for k, v in headers.items() if k in ["Content-Type", "User-Agent", "X-Forwarded-For"]},
    }), 200


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
    data = request.get_json(silent=True, force=True)  # force=True ignores Content-Type
    if not data:
        raw = request.get_data(as_text=True)
        log.info(f"Raw payload received: {repr(raw[:500])}")
        import json as json_lib
        # Find the last { ... } block in the message
        start = raw.rfind('{')
        end   = raw.rfind('}')
        if start != -1 and end != -1 and end > start:
            try:
                data = json_lib.loads(raw[start:end+1])
                log.info(f"Extracted JSON: {data}")
            except Exception as e:
                log.warning(f"JSON parse failed: {e} — attempted: {repr(raw[start:end+1][:200])}")
    if not data:
        raw = request.get_data(as_text=True)
        log.info(f"Non-JSON alert received (notification only, no order): {repr(raw[:200])}")
        return jsonify({"status": "ok", "message": "Notification received — no order placed"}), 200

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
    symbol        = data.get("symbol", "").upper()
    # Clean TradingView suffixes — .P (perpetual), .PS, USDT.P etc
    symbol        = re.sub(r'\.(P|PS|PERP)$', '', symbol)
    symbol        = symbol.replace("/", "").replace("-", "").replace(".", "")
    side          = data.get("side",      "")
    entry         = float(data.get("entry",  0))
    sl            = float(data.get("sl",     0))
    tp            = float(data.get("tp",     0))
    order_type    = data.get("orderType", "Limit")   # "Market" or "Limit"
    cancel_bars   = int(data.get("cancelAfterBars", 0))  # 0 = never auto-cancel
    bar_seconds   = int(data.get("barSeconds", os.getenv("BAR_SECONDS", "180")))  # from alert, fallback to env
    source        = data.get("source",    "unknown")
    test_mode     = str(data.get("testMode",     "false")).lower() == "true"
    test_bal_pct  = float(data.get("testBalancePct",  0.1))
    test_leverage = int(data.get("testLeverage", 2))
    log.info(f"Parsed: symbol={symbol} side={side} orderType={order_type} entry={entry} sl={sl} tp={tp} barSeconds={bar_seconds} testMode={test_mode}")

    if not all([symbol, side, entry, sl, tp]):
        msg = f"Missing required fields — got: {data}"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 400

    import math as _math
    if any(_math.isnan(v) or _math.isinf(v) for v in [entry, sl, tp] if isinstance(v, float)):
        msg = f"Invalid values (NaN/Inf) — entry={entry} sl={sl} tp={tp}"
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
            if source == "ob" and order_type == "Limit":
                # New OB detected — cancel existing pending limit and replace with new one
                log.info(f"New OB for {symbol} — cancelling {len(open_orders)} existing order(s) and replacing")
                try:
                    session.cancel_all_orders(category="linear", symbol=symbol)
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET status = 'skipped', notes = 'Replaced by new OB limit order' WHERE symbol = " + ph() + " AND status = 'open'", (symbol,))
                        conn.commit()
                    log.info(f"Cancelled existing orders for {symbol} — placing new OB limit order")
                except Exception as e:
                    log.error(f"Error cancelling orders for {symbol}: {e}")
                    return jsonify({"status": "error", "message": f"Failed to cancel existing orders: {e}"}), 500
            elif source == "ob" and order_type == "Market":
                # Zone entered quickly — cancel the pending limit order placed at detection
                # then place market order to ensure we get in
                log.info(f"OB zone entered for {symbol} — cancelling pending limit, placing market order")
                try:
                    session.cancel_all_orders(category="linear", symbol=symbol)
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET status = 'skipped', notes = 'Cancelled — market order placed on zone entry' WHERE symbol = " + ph() + " AND status = 'open'", (symbol,))
                        conn.commit()
                except Exception as e:
                    log.error(f"Error cancelling limit before market order for {symbol}: {e}")
                    # Continue anyway — place market order even if cancel fails
            else:
                msg = f"Already have open order(s) for {symbol} — skipping"
                log.warning(msg)
                log_order_skipped(symbol, side, entry, sl, tp, msg)
                return jsonify({"status": "skipped", "message": msg}), 200

        # Use test params if test mode — no main order, just tiny test order
        if test_mode:
            actual_bal_pct  = test_bal_pct
            actual_leverage = test_leverage
            log.info(f"🧪 Test mode ON — using {test_bal_pct}% balance × {test_leverage}x leverage")
        else:
            actual_bal_pct  = cfg["balance_pct"]
            actual_leverage = cfg["leverage"]

        trade_balance = get_available_balance()  # store for Google Sheets
        qty = calculate_qty(symbol, entry, actual_bal_pct, actual_leverage)
        if qty <= 0:
            return jsonify({"status": "error", "message": "Invalid quantity calculated"}), 400

        set_leverage(symbol, actual_leverage)

        # Auto-cancel any pending opposite orders on this symbol
        auto_cancel_opposite(symbol, side)

        info        = get_instrument_info(symbol)
        price_scale = info["price_scale"]
        entry_str   = f"{entry:.{price_scale}f}"
        sl_str      = f"{sl:.{price_scale}f}"
        tp_str      = f"{tp:.{price_scale}f}"
        qty_str     = str(qty)

        try:
            order_params = dict(
                category     = "linear",
                symbol       = symbol,
                side         = side,
                orderType    = order_type,
                qty          = qty_str,
                stopLoss     = sl_str,
                takeProfit   = tp_str,
                slTriggerBy  = "LastPrice",
                tpTriggerBy  = "LastPrice",
                timeInForce  = "IOC" if order_type == "Market" else "GTC",
                reduceOnly   = False,
                closeOnTrigger = False,
            )
            # Only include price for limit orders
            if order_type == "Limit":
                order_params["price"] = entry_str

            resp = session.place_order(**order_params)

            ret_code = resp.get("retCode", -1)
            if ret_code == 0:
                order_id = resp.get("result", {}).get("orderId", "?")
                log.info(f"✅ {order_type} order placed: {symbol} {side} {qty} @ {entry_str} | SL {sl_str} | TP {tp_str} | ID {order_id}")
                log_order_placed(symbol, side, qty, entry, sl, tp, order_id,
                                 source=source + ("_test" if test_mode else ""),
                                 timeframe=gsheets._bar_seconds_to_tf(bar_seconds),
                                 leverage=actual_leverage)
                # Push to Google Sheets if configured
                if gsheets.is_configured():
                    sheet_row = gsheets.push_trade_opened(
                        symbol=symbol, side=side, qty=qty, entry=entry,
                        sl=sl, tp=tp, leverage=actual_leverage,
                        balance=trade_balance, source=source,
                        bar_seconds=bar_seconds, order_id=order_id
                    )
                    # Store sheet row in journal for later update on close
                    if sheet_row > 0:
                        try:
                            with get_db() as conn:
                                with conn.cursor() as cur:
                                    cur.execute("UPDATE trades SET notes = " + ph() + " WHERE order_id = " + ph(), (f"sheet_row:{sheet_row}", order_id))
                                conn.commit()
                        except Exception as e:
                            log.error(f"Failed to store sheet row: {e}")
                # Schedule auto-cancel for limit orders only
                if order_type == "Limit" and cancel_bars > 0:
                    cancel_time = time.time() + cancel_bars * bar_seconds
                    threading.Thread(
                        target=_auto_cancel_after,
                        args=(symbol, order_id, cancel_time),
                        daemon=True
                    ).start()
                    log.info(f"Auto-cancel scheduled for {symbol} {order_id} after {cancel_bars} bars × {bar_seconds}s = {cancel_bars * bar_seconds}s")

                return jsonify({
                    "status":   "ok",
                    "symbol":   symbol,
                    "side":     side,
                    "qty":      qty_str,
                    "entry":    entry_str,
                    "sl":       sl_str,
                    "tp":       tp_str,
                    "order_id": order_id,
                    "test_mode": test_mode,
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


@app.route("/journal/fix", methods=["POST"])
def fix_journal():
    """
    Manually fix stale 'open' journal entries by checking Bybit.
    Use this when a trade shows as open in journal but is closed/cancelled on Bybit.
    """
    try:
        _check_closed_trades()
        return jsonify({"status": "ok", "message": "Journal fix complete — check /journal"}), 200
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
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET status = 'skipped', notes = 'Manually cancelled' WHERE symbol = " + ph() + " AND status = 'open'", (symbol,))
                conn.commit()
            return jsonify({"status": "ok", "cancelled": symbol}), 200
        else:
            return jsonify({"status": "error", "message": resp.get("retMsg")}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


def _get_bar_seconds() -> int:
    """Estimate bar duration in seconds from timeframe.period sent in alert.
    Falls back to 60s (1 min) if unknown."""
    # We don't have the TF here so use a configurable default
    return int(os.getenv("BAR_SECONDS", "180"))   # default 3 min bars


def _auto_cancel_after(symbol: str, order_id: str, cancel_at: float):
    """Wait until cancel_at timestamp then cancel the order if still unfilled."""
    wait = max(0, cancel_at - time.time())
    time.sleep(wait)
    try:
        # Check if still open
        resp = session.get_open_orders(category="linear", symbol=symbol, orderId=order_id)
        orders = resp.get("result", {}).get("list", [])
        if orders:
            session.cancel_order(category="linear", symbol=symbol, orderId=order_id)
            log.info(f"⏱ Auto-cancelled unfilled limit order {order_id} for {symbol}")
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE trades SET status = 'skipped', notes = 'Auto-cancelled — unfilled after bar timeout' WHERE order_id = " + ph() + " AND status = 'open'", (order_id,))
                conn.commit()
        else:
            log.info(f"Auto-cancel check: {symbol} {order_id} already filled or closed")
    except Exception as e:
        log.error(f"Auto-cancel error for {symbol} {order_id}: {e}")


def auto_cancel_opposite(symbol: str, new_side: str):
    """
    When a new setup fires, cancel any pending unfilled orders on the same
    symbol in the OPPOSITE direction.
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
                    with conn.cursor() as cur:
                        cur.execute("UPDATE trades SET status = 'skipped', notes = 'Auto-cancelled — opposite setup fired' WHERE order_id = " + ph() + " AND status = 'open'", (order_id,))
                    conn.commit()
    except Exception as e:
        log.error(f"Error auto-cancelling opposite orders for {symbol}: {e}")


@app.route("/journal")
def journal():
    """Trading journal dashboard."""
    try:
        trades = get_all_trades(200)
        stats  = get_stats()
        return render_template_string(JOURNAL_HTML, trades=trades, stats=stats)
    except Exception as e:
        log.error(f"Journal error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/data")
def journal_data():
    """JSON endpoint for journal data."""
    try:
        return jsonify({
            "trades": get_all_trades(200),
            "stats":  get_stats()
        })
    except Exception as e:
        log.error(f"Journal data error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


ANALYSIS_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trade Analysis</title>
<style>
  :root { --bg: #0f0f0f; --surface: #1a1a1a; --border: #2a2a2a; --text: #e8e8e8; --dim: #888; --green: #4caf50; --red: #ef5350; --blue: #42a5f5; --amber: #ffa726; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 13px; padding: 24px; }
  h1 { font-size: 18px; font-weight: 500; margin-bottom: 4px; }
  .subtitle { color: var(--dim); font-size: 12px; margin-bottom: 24px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-bottom: 24px; }
  .stat { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; }
  .stat-label { color: var(--dim); font-size: 11px; margin-bottom: 4px; }
  .stat-value { font-size: 20px; font-weight: 500; }
  .section { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  .section-title { font-size: 13px; font-weight: 500; margin-bottom: 12px; color: var(--dim); text-transform: uppercase; letter-spacing: 0.05em; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; color: var(--dim); font-weight: 400; font-size: 11px; padding: 4px 8px; border-bottom: 1px solid var(--border); }
  td { padding: 6px 8px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  .green { color: var(--green); }
  .red { color: var(--red); }
  .bar-wrap { background: var(--border); border-radius: 3px; height: 6px; width: 80px; display: inline-block; vertical-align: middle; margin-left: 6px; }
  .bar { height: 6px; border-radius: 3px; }
  .tag { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 500; }
  .tag-green { background: rgba(76,175,80,0.15); color: var(--green); }
  .tag-red { background: rgba(239,83,80,0.15); color: var(--red); }
  .tag-amber { background: rgba(255,167,38,0.15); color: var(--amber); }
  .nav { display: flex; gap: 12px; margin-bottom: 20px; }
  .nav a { color: var(--dim); text-decoration: none; font-size: 12px; }
  .nav a:hover { color: var(--text); }
  .insight { background: rgba(66,165,245,0.08); border: 1px solid rgba(66,165,245,0.2); border-radius: 6px; padding: 10px 12px; margin-bottom: 8px; font-size: 12px; line-height: 1.5; }
  .insight strong { color: var(--blue); }
</style>
</head>
<body>
<div class="nav"><a href="/journal">← Journal</a><a href="/status">Status</a></div>
<h1>// Trade Analysis</h1>
<p class="subtitle">{{ total_closed }} closed trades · {{ total_open }} open · updated just now</p>

<div class="grid">
  <div class="stat"><div class="stat-label">Win rate</div><div class="stat-value" style="color:{{ 'var(--green)' if wr >= 40 else 'var(--red)' }}">{{ wr }}%</div></div>
  <div class="stat"><div class="stat-label">Total PnL</div><div class="stat-value" style="color:{{ 'var(--green)' if total_pnl >= 0 else 'var(--red)' }}">{{ '+' if total_pnl >= 0 else '' }}{{ '%.2f'|format(total_pnl) }}</div></div>
  <div class="stat"><div class="stat-label">Avg win</div><div class="stat-value green">+{{ '%.2f'|format(avg_win) }}</div></div>
  <div class="stat"><div class="stat-label">Avg loss</div><div class="stat-value red">{{ '%.2f'|format(avg_loss) }}</div></div>
  <div class="stat"><div class="stat-label">Profit factor</div><div class="stat-value" style="color:{{ 'var(--green)' if profit_factor >= 1 else 'var(--red)' }}">{{ '%.2f'|format(profit_factor) }}</div></div>
  <div class="stat"><div class="stat-label">Expectancy</div><div class="stat-value" style="color:{{ 'var(--green)' if expectancy >= 0 else 'var(--red)' }}">{{ '+' if expectancy >= 0 else '' }}{{ '%.3f'|format(expectancy) }}</div></div>
</div>

{% if insights %}
<div class="section">
  <div class="section-title">Insights</div>
  {% for i in insights %}<div class="insight">{{ i|safe }}</div>{% endfor %}
</div>
{% endif %}

<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">

<div class="section">
  <div class="section-title">By symbol</div>
  <table>
    <tr><th>Symbol</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_symbol %}
    <tr>
      <td>{{ r.symbol }}</td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>
        {{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

<div class="section">
  <div class="section-title">By source</div>
  <table>
    <tr><th>Source</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_source %}
    <tr>
      <td><span class="tag {{ 'tag-green' if r.source == 'ob' else 'tag-amber' if r.source == 'fib' else 'tag-amber' }}">{{ r.source.upper() }}</span></td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

<div class="section">
  <div class="section-title">By side</div>
  <table>
    <tr><th>Side</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_side %}
    <tr>
      <td><span class="tag {{ 'tag-green' if r.side == 'Buy' else 'tag-red' }}">{{ 'LONG' if r.side == 'Buy' else 'SHORT' }}</span></td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

<div class="section">
  <div class="section-title">By timeframe</div>
  <table>
    <tr><th>TF</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_tf %}
    <tr>
      <td>{{ r.tf or '—' }}</td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

</div>

<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;margin-bottom:16px">

<div class="section">
  <div class="section-title">Weekend vs Weekday</div>
  <table>
    <tr><th>Period</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_weekend %}
    <tr>
      <td>{{ r.key }}</td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

<div class="section">
  <div class="section-title">By day of week</div>
  <table>
    <tr><th>Day</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_day %}
    <tr>
      <td>{{ r.key[:3] }}</td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

<div class="section">
  <div class="section-title">By session (UTC)</div>
  <table>
    <tr><th>Session</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_session %}
    <tr>
      <td style="font-size:11px">{{ r.key }}</td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>

</div>
  <div style="font-size:11px;color:var(--dim);margin-bottom:8px">SL distance from entry. Under 0.5% is very tight for crypto — normal volatility can hit it. Ideal: 1-2% depending on timeframe.</div>
  <table>
    <tr><th>Symbol</th><th>Side</th><th>Entry</th><th>SL</th><th>SL dist %</th><th>RR set</th><th>Time in trade</th></tr>
    {% for r in sl_margins %}
    <tr>
      <td>{{ r.symbol }}</td>
      <td><span class="tag {{ 'tag-green' if r.side == 'Buy' else 'tag-red' }}">{{ 'L' if r.side == 'Buy' else 'S' }}</span></td>
      <td>{{ r.entry }}</td>
      <td>{{ r.sl }}</td>
      <td><span class="tag {{ 'tag-red' if r.sl_dist_pct < 0.5 else 'tag-amber' if r.sl_dist_pct < 1.0 else 'tag-green' }}">{{ '%.3f'|format(r.sl_dist_pct) }}%</span></td>
      <td>{{ r.rr }}×</td>
      <td style="color:var(--dim)">{{ r.time_in }}</td>
    </tr>
    {% endfor %}
  </table>
  <div style="margin-top:10px;font-size:11px;display:flex;gap:12px">
    <span><span class="tag tag-red">red</span> &lt; 0.5% — very tight</span>
    <span><span class="tag tag-amber">amber</span> 0.5–1.0% — borderline</span>
    <span><span class="tag tag-green">green</span> &gt; 1.0% — healthy</span>
  </div>
</div>

</body>
</html>
"""


def _analyse_trades(trades):
    """Compute analysis metrics from trade list."""
    closed = [t for t in trades if t.get("outcome") in ("tp", "sl")]
    open_t = [t for t in trades if t.get("status") == "open"]

    wins   = [t for t in closed if t["outcome"] == "tp"]
    losses = [t for t in closed if t["outcome"] == "sl"]

    total_closed = len(closed)
    wr = round(len(wins) / total_closed * 100, 1) if total_closed > 0 else 0

    win_pnls  = [float(t["pnl"] or 0) for t in wins  if t.get("pnl")]
    loss_pnls = [float(t["pnl"] or 0) for t in losses if t.get("pnl")]

    total_pnl  = round(sum(win_pnls) + sum(loss_pnls), 2)
    avg_win    = round(sum(win_pnls)  / len(win_pnls),  2) if win_pnls  else 0
    avg_loss   = round(sum(loss_pnls) / len(loss_pnls), 2) if loss_pnls else 0
    gross_win  = sum(win_pnls)
    gross_loss = abs(sum(loss_pnls))
    profit_factor = round(gross_win / gross_loss, 2) if gross_loss > 0 else 0
    wr_dec     = len(wins) / total_closed if total_closed > 0 else 0
    expectancy = round(wr_dec * avg_win + (1 - wr_dec) * avg_loss, 3)

    def group_stats(key_fn):
        groups = {}
        for t in closed:
            k = key_fn(t)
            if k not in groups:
                groups[k] = {"wins": 0, "losses": 0, "pnl": 0.0}
            if t["outcome"] == "tp":
                groups[k]["wins"] += 1
            else:
                groups[k]["losses"] += 1
            groups[k]["pnl"] += float(t.get("pnl") or 0)
        result = []
        for k, v in sorted(groups.items(), key=lambda x: -(x[1]["wins"] + x[1]["losses"])):
            total = v["wins"] + v["losses"]
            result.append({
                "key": k, "wins": v["wins"], "losses": v["losses"],
                "wr": round(v["wins"] / total * 100, 1) if total > 0 else 0,
                "pnl": round(v["pnl"], 2)
            })
        return result

    by_symbol_raw = group_stats(lambda t: t["symbol"])
    by_source_raw = group_stats(lambda t: (t.get("source") or "").replace("_test",""))
    by_side_raw   = group_stats(lambda t: t["side"])
    by_tf_raw     = group_stats(lambda t: t.get("timeframe") or "—")

    def rename(rows, key="key"):
        for r in rows:
            r["symbol"] = r["key"]
            r["source"] = r["key"]
            r["side"]   = r["key"]
            r["tf"]     = r["key"]
        return rows

    by_symbol = [{"symbol": r["key"], **r} for r in by_symbol_raw]
    by_source = [{"source": r["key"], **r} for r in by_source_raw]
    by_side   = [{"side":   r["key"], **r} for r in by_side_raw]
    by_tf     = [{"tf":     r["key"], **r} for r in by_tf_raw]

    # SL distance analysis — how tight was the SL relative to entry?
    sl_margins = []
    for t in losses:
        entry    = float(t.get("entry")      or 0)
        sl       = float(t.get("sl")         or 0)
        tp       = float(t.get("tp")         or 0)
        exit_    = float(t.get("exit_price") or 0)
        opened   = t.get("opened_at")        or ""
        closed   = t.get("closed_at")        or ""
        if entry > 0 and sl > 0:
            sl_dist     = abs(entry - sl)
            sl_dist_pct = round(sl_dist / entry * 100, 3)
            tp_dist     = abs(tp - entry) if tp > 0 else 0
            rr          = round(tp_dist / sl_dist, 2) if sl_dist > 0 else 0
            # Time in trade
            time_str = "—"
            try:
                from datetime import datetime
                fmt  = "%Y-%m-%d %H:%M:%S"
                t_in  = datetime.strptime(opened[:19], fmt)
                t_out = datetime.strptime(closed[:19], fmt)
                mins  = int((t_out - t_in).total_seconds() / 60)
                time_str = f"{mins}m" if mins < 120 else f"{mins//60}h {mins%60}m"
            except:
                pass
            sl_margins.append({
                "symbol":       t["symbol"],
                "side":         t["side"],
                "entry":        entry,
                "sl":           sl,
                "sl_dist_pct":  sl_dist_pct,
                "rr":           rr,
                "time_in":      time_str,
            })
    sl_margins.sort(key=lambda x: x["sl_dist_pct"])

    # Auto insights
    insights = []
    if total_closed >= 5:
        long_trades  = [t for t in closed if t["side"] == "Buy"]
        short_trades = [t for t in closed if t["side"] == "Sell"]
        long_wins    = len([t for t in long_trades  if t["outcome"] == "tp"])
        short_wins   = len([t for t in short_trades if t["outcome"] == "tp"])
        long_wr  = round(long_wins  / len(long_trades)  * 100, 1) if long_trades  else 0
        short_wr = round(short_wins / len(short_trades) * 100, 1) if short_trades else 0
        if abs(long_wr - short_wr) > 20:
            better = "Long" if long_wr > short_wr else "Short"
            worse  = "Short" if long_wr > short_wr else "Long"
            worse_wr = short_wr if long_wr > short_wr else long_wr
            insights.append(f"<strong>{better} trades significantly outperform {worse}</strong> ({long_wr if better == 'Long' else short_wr}% vs {worse_wr}% win rate). Consider disabling {worse.lower()} OB alerts.")

        tight_sl = [t for t in sl_margins if t["sl_dist_pct"] < 0.5]
        if len(tight_sl) > len(sl_margins) * 0.5:
            avg_dist = round(sum(t["sl_dist_pct"] for t in sl_margins) / len(sl_margins), 3) if sl_margins else 0
            insights.append(f"<strong>{len(tight_sl)}/{len(sl_margins)} losses had SL within 0.5% of entry</strong> (avg {avg_dist}%). SL may be too tight — normal crypto volatility can trigger it. Consider widening the SL ATR multiplier.")

        if profit_factor > 0 and profit_factor < 1:
            breakeven_wr = round(1 / (1 + abs(avg_win / avg_loss)) * 100, 1) if avg_loss != 0 else 50
            insights.append(f"<strong>Profit factor {profit_factor} — unprofitable.</strong> With avg win {avg_win} and avg loss {avg_loss}, you need >{breakeven_wr}% win rate to break even.")

        worst = sorted(by_symbol, key=lambda x: x["pnl"])[:2]
        for w in worst:
            if w["pnl"] < -1 and w["losses"] >= 3:
                insights.append(f"<strong>{w['symbol']} is a consistent loser</strong> — {w['losses']} losses, {w['wins']} wins, {w['pnl']} USDT. Consider removing from watchlist.")

        if not insights:
            insights.append(f"<strong>Not enough data yet</strong> — {total_closed} closed trades. Need 50+ for reliable conclusions.")

    # Day of week and session analysis
    from datetime import datetime as _dt
    def get_day_session(opened_at):
        try:
            dt      = _dt.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
            day     = dt.strftime("%A")
            hour    = dt.hour
            if 0 <= hour < 8:    session = "Asian (00-08 UTC)"
            elif 8 <= hour < 14: session = "European (08-14 UTC)"
            elif 14 <= hour < 21:session = "US (14-21 UTC)"
            else:                session = "Late/Night (21-24 UTC)"
            is_weekend = dt.weekday() >= 5
            day_order  = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            return day, session, is_weekend, day_order.index(day) if day in day_order else 99
        except:
            return "Unknown", "Unknown", False, 99

    by_day_raw     = {}
    by_session_raw = {}
    wk_stats       = {"Weekend": {"wins":0,"losses":0,"pnl":0.0}, "Weekday": {"wins":0,"losses":0,"pnl":0.0}}

    for t in closed:
        pnl     = float(t.get("pnl") or 0)
        outcome = t["outcome"]
        day, session, is_weekend, d_order = get_day_session(t.get("opened_at") or "")

        if day not in by_day_raw:
            by_day_raw[day] = {"wins":0,"losses":0,"pnl":0.0,"order":d_order}
        by_day_raw[day]["wins" if outcome=="tp" else "losses"] += 1
        by_day_raw[day]["pnl"] += pnl

        if session not in by_session_raw:
            by_session_raw[session] = {"wins":0,"losses":0,"pnl":0.0}
        by_session_raw[session]["wins" if outcome=="tp" else "losses"] += 1
        by_session_raw[session]["pnl"] += pnl

        key = "Weekend" if is_weekend else "Weekday"
        wk_stats[key]["wins" if outcome=="tp" else "losses"] += 1
        wk_stats[key]["pnl"] += pnl

    def make_time_rows(raw, sort_key=None):
        rows = []
        for k, v in raw.items():
            total = v["wins"] + v["losses"]
            rows.append({"key":k,"wins":v["wins"],"losses":v["losses"],
                         "wr":round(v["wins"]/total*100,1) if total>0 else 0,
                         "pnl":round(v["pnl"],2),"total":total,"_order":v.get("order",0)})
        rows.sort(key=sort_key or (lambda x: -x["total"]))
        return rows

    by_day     = make_time_rows(by_day_raw,     lambda x: x["_order"])
    by_session = make_time_rows(by_session_raw)
    by_weekend = make_time_rows(wk_stats,        lambda x: x["key"])

    # Add weekend insight
    for r in by_weekend:
        if r["total"] >= 3 and r["key"] == "Weekend" and r["wr"] < 30:
            insights.append(f"<strong>Weekend performance is poor</strong> — {r['wr']}% win rate ({r['wins']}/{r['total']}). Consider pausing alerts on weekends (Sat/Sun UTC).")
        if r["total"] >= 3 and r["key"] == "Weekday" and r["wr"] < 30:
            insights.append(f"<strong>Weekday performance is poor</strong> — {r['wr']}% win rate ({r['wins']}/{r['total']}).")

    return {
        "total_closed":  total_closed,
        "total_open":    len(open_t),
        "wr":            wr,
        "total_pnl":     total_pnl,
        "avg_win":       avg_win,
        "avg_loss":      avg_loss,
        "profit_factor": profit_factor,
        "expectancy":    expectancy,
        "by_symbol":     by_symbol,
        "by_source":     by_source,
        "by_side":       by_side,
        "by_tf":         by_tf,
        "by_day":        by_day,
        "by_session":    by_session,
        "by_weekend":    by_weekend,
        "sl_margins":    sl_margins,
        "insights":      insights,
    }


@app.route("/analysis")
def analysis():
    """Trade analysis dashboard."""
    try:
        trades = get_all_trades(500)
        data   = _analyse_trades(trades)
        return render_template_string(ANALYSIS_HTML, **data)
    except Exception as e:
        log.error(f"Analysis error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/analysis/data")
def analysis_data():
    """JSON endpoint for analysis data."""
    try:
        trades = get_all_trades(500)
        return jsonify(_analyse_trades(trades))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



    cfg = get_config()
    log.info(f"Starting webhook server — testnet={TESTNET}")
    log.info(f"Config: {cfg['balance_pct']}% per trade, max {cfg['max_trades']} trades, {cfg['leverage']}x leverage")
    _start_poller()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
