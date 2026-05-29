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
import json
from datetime import datetime, timedelta
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
    def _float(key, default):
        try: return float(os.getenv(key, str(default)) or str(default))
        except: return float(default)
    def _int(key, default):
        try: return int(os.getenv(key, str(default)) or str(default))
        except: return int(default)
    def _str(key, default=""):
        return (os.getenv(key, default) or default).strip()

    return {
        "enabled":              _str("ENABLED", "true").lower() == "true",
        "balance_pct":          _float("BALANCE_PCT",   1.0),  # % of wallet to RISK per trade (SL-based)
        "max_trades":           _int("MAX_TRADES",       3),
        "leverage":             _int("LEVERAGE",         5),
        "poll_interval":        _int("POLL_INTERVAL",    360),  # 6 min default
        "filter_side":          _str("FILTER_SIDE").lower(),
        "filter_min_wr":        _float("FILTER_MIN_WR",  0),
        "filter_sources":       _str("FILTER_SOURCES").lower(),
        "filter_timeframes":    _str("FILTER_TIMEFRAMES").upper(),
        "filter_symbols_allow": _str("FILTER_SYMBOLS_ALLOW").upper(),
        "filter_symbols_block": _str("FILTER_SYMBOLS_BLOCK").upper(),
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
    <a href="/recommendations" style="color:var(--blue);font-size:12px;text-decoration:none;">🎯 Recommendations</a>
    <a href="/watchlist" style="color:var(--blue);font-size:12px;text-decoration:none;">📋 Watchlist</a>
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
  <button class="filter-btn" onclick="runImport(this)" style="color:var(--amber)">📥 Import Bybit</button>
  <button class="filter-btn" onclick="runDeleteDupes(this)" style="color:var(--red)">🗑️ Delete Dupes</button>
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
        <td class="dim">{{ trades|length - loop.index0 }}</td>
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
        <td class="dim" style="max-width:160px;overflow:hidden;text-overflow:ellipsis">
          {% set kl = t.notes and '"klLevel"' in (t.notes or '') %}
          {% if kl %}
            {% set kl_val = t.notes.split('"klLevel":')[1].split(',')[0].split('}')[0].strip() %}
            {% if kl_val != 'null' %}
            <span class="tag tag-blue" title="S/R level that triggered this alert">S/R {{ kl_val }}</span>
            {% else %}—{% endif %}
          {% else %}
            {{ t.notes or '—' }}
          {% endif %}
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
function runDeleteDupes(btn) {
  if (!confirm('Delete duplicate imported trades? This removes imports where a matching native trade already exists.')) return;
  const base = window.location.origin;
  btn.innerText = '⏳ Deleting...';
  btn.disabled = true;
  fetch(base + '/journal/delete-duplicates', {method: 'POST'})
    .then(r => r.json())
    .then(data => {
      btn.innerText = '✅ ' + (data.message || 'Done');
      setTimeout(() => { btn.innerText = '🗑️ Delete Dupes'; btn.disabled = false; location.reload(); }, 2000);
    })
    .catch(err => { btn.innerText = '❌ Error'; btn.disabled = false; });
}

function runImport(btn) {
  if (!confirm('Import missing trades from Bybit history? This will add closed trades not yet in the journal.')) return;
  const base = window.location.origin;
  btn.innerText = '⏳ Importing...';
  btn.disabled = true;
  fetch(base + '/journal/import', {method: 'POST'})
    .then(r => r.json())
    .then(data => {
      btn.innerText = '✅ ' + (data.message || 'Done');
      setTimeout(() => { btn.innerText = '📥 Import Bybit'; btn.disabled = false; location.reload(); }, 3000);
    })
    .catch(err => { btn.innerText = '❌ Error'; btn.disabled = false; });
}

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
        resp = _api_call(session.get_positions, category="linear", settleCoin="USDT")
        positions = resp.get("result", {}).get("list", [])
        return [p["symbol"] for p in positions if float(p.get("size", 0)) > 0]
    except Exception as e:
        log.error(f"Error fetching positions: {e}")
        return []


def _api_call(fn, *args, **kwargs):
    """Wrapper for Bybit API calls with retry on rate limit."""
    for attempt in range(3):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err = str(e)
            if "403" in err or "rate limit" in err.lower() or "10006" in err:
                wait = (attempt + 1) * 2
                log.warning(f"Rate limit hit — waiting {wait}s before retry {attempt+1}/3")
                time.sleep(wait)
            else:
                raise
    raise Exception(f"API call failed after 3 retries")


def get_open_orders(symbol: str) -> list:
    """Return list of open orders for a symbol."""
    try:
        resp = _api_call(session.get_open_orders, category="linear", symbol=symbol)
        return resp.get("result", {}).get("list", [])
    except Exception as e:
        log.error(f"Error fetching open orders for {symbol}: {e}")
        return []


def get_available_balance() -> float:
    """Return available USDT balance."""
    try:
        resp = _api_call(session.get_wallet_balance, accountType="UNIFIED", coin="USDT")
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


def calculate_qty(symbol: str, entry: float, sl: float, balance_pct: float, leverage: int) -> float:
    """
    Risk-based position sizing.
    Risk a fixed % of wallet per trade based on SL distance.
    qty = (balance × risk_pct / 100) / |entry - sl|
    This ensures every trade risks the same dollar amount regardless of asset price.
    """
    balance = get_available_balance()
    if balance <= 0:
        log.error("Zero or negative balance — cannot calculate qty")
        return 0.0

    sl_distance = abs(entry - sl)
    if sl_distance <= 0:
        log.error(f"{symbol}: SL distance is zero — cannot calculate qty")
        return 0.0

    info        = get_instrument_info(symbol)
    risk_amount = balance * (balance_pct / 100.0)   # $ amount to risk
    raw_qty     = risk_amount / sl_distance          # qty that risks exactly risk_amount
    qty         = round_to_step(raw_qty, info["qty_step"])

    # Check notional value meets Bybit minimums (usually $1-5)
    notional = qty * entry
    min_notional = 1.0  # Bybit minimum notional
    if notional < min_notional:
        log.warning(f"{symbol}: notional {notional:.4f} below minimum — adjusting qty")
        qty = round_to_step(min_notional / entry, info["qty_step"])

    if qty < info["min_qty"]:
        log.warning(f"{symbol}: calculated qty {qty} below min {info['min_qty']}")
        return 0.0

    actual_risk = qty * sl_distance
    margin      = qty * entry / leverage
    log.info(f"{symbol}: balance={balance:.2f} risk={risk_amount:.2f} USDT sl_dist={sl_distance:.6f} qty={qty} margin={margin:.2f} USDT actual_risk={actual_risk:.2f} USDT")
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
            # Check if order is still pending (unfilled limit)
            open_resp  = session.get_open_orders(category="linear", symbol=symbol, orderId=order_id)
            open_orders = open_resp.get("result", {}).get("list", [])

            if open_orders:
                # Still pending — check if it's been too long
                opened_at = trade.get("opened_at", "")
                try:
                    from datetime import datetime
                    opened = datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
                    mins   = (datetime.utcnow() - opened).total_seconds() / 60
                    if mins > 480:  # 8 hours — likely stuck
                        log.warning(f"{symbol} {order_id} pending for {mins:.0f}m — checking PnL anyway")
                        _check_closed_pnl(trade)
                except:
                    pass
                continue

            # Not in open orders — either filled+closed or cancelled
            # Check order history to see final status
            hist_resp = _api_call(session.get_order_history, category="linear", symbol=symbol, orderId=order_id)
            hist      = hist_resp.get("result", {}).get("list", [])
            order_status = hist[0].get("orderStatus", "") if hist else "Unknown"

            if order_status in ("Cancelled", "Rejected", "Deactivated"):
                with get_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE trades SET status = 'skipped', notes = " + ph() + " WHERE order_id = " + ph(),
                                   (f"Order {order_status.lower()}", order_id))
                    conn.commit()
                log.info(f"{symbol} {order_id} was {order_status} — marked skipped")
            else:
                # Filled or unknown — check closed PnL
                _check_closed_pnl(trade)

        except Exception as e:
            log.error(f"Error checking {symbol} {order_id}: {e}")


def _check_closed_pnl(trade):
    """Check Bybit closed PnL — matches by symbol+side+qty since SL/TP uses different orderId."""
    symbol    = trade["symbol"]
    order_id  = trade["order_id"]
    qty       = float(trade["qty"] or 0)
    side      = trade["side"]
    opened_at = trade.get("opened_at", "")

    try:
        # Check if position still open
        pos_resp  = _api_call(session.get_positions, category="linear", symbol=symbol)
        positions = pos_resp.get("result", {}).get("list", [])
        pos_open  = any(float(p.get("size", 0)) > 0 for p in positions)
        if pos_open:
            try:
                opened   = datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
                mins     = (datetime.utcnow() - opened).total_seconds() / 60
                if mins < 30:
                    log.info(f"Position open {symbol} ({mins:.0f}m) — skip")
                    return
                log.info(f"Position open {mins:.0f}m — checking PnL anyway")
            except:
                log.info(f"Position open {symbol} — skip")
                return

        # Fetch closed PnL records
        resp    = _api_call(session.get_closed_pnl, category="linear", symbol=symbol, limit=200)
        records = resp.get("result", {}).get("list", [])

        if not records:
            log.warning(f"No closed PnL records for {symbol}")
            return

        # Closing side is opposite to entry side
        closing_side = "Sell" if side == "Buy" else "Buy"

        # Find best match: order_id > qty+side match > most recent same-side close
        best = None
        for r in records:
            exit_p = float(r.get("avgExitPrice", 0) or r.get("exitPrice", 0))
            if exit_p <= 0:
                continue
            cqty   = float(r.get("qty", 0))
            cside  = r.get("side", "")
            id_ok  = r.get("orderId", "") == order_id
            qty_ok = qty > 0 and abs(cqty - qty) <= qty * 0.1 and cside == closing_side
            if id_ok:
                best = r
                break
            if qty_ok and best is None:
                best = r

        # Last resort — most recent close on this symbol same side
        if not best:
            for r in records:
                exit_p = float(r.get("avgExitPrice", 0) or r.get("exitPrice", 0))
                if exit_p > 0 and r.get("side", "") == closing_side:
                    best = r
                    log.warning(f"{symbol}: using most recent {closing_side} close as fallback")
                    break

        if not best:
            log.warning(f"No closed PnL match for {symbol} {order_id}")
            return

        exit_price   = float(best.get("avgExitPrice", 0) or best.get("exitPrice", 0))
        realised_pnl = float(best.get("closedPnl", 0) or 0)
        exec_type    = best.get("execType", "")

        # Determine outcome
        if exec_type == "TakeProfit":
            outcome = "tp"
        elif exec_type == "StopLoss":
            outcome = "sl"
        else:
            tp = float(trade.get("tp") or 0)
            if side == "Buy":
                outcome = "tp" if exit_price >= tp * 0.999 else "sl"
            else:
                outcome = "tp" if exit_price <= tp * 1.001 else "sl"

        success = log_trade_closed(order_id, exit_price, outcome, realised_pnl=realised_pnl)
        if success:
            log.info(f"✅ Closed {symbol} {order_id} — {outcome.upper()} @ {exit_price} PnL={realised_pnl:.4f}")
            if gsheets.is_configured():
                try:
                    conn = get_db()
                    with conn.cursor() as cur:
                        cur.execute("SELECT notes, qty FROM trades WHERE order_id = " + ph(), (order_id,))
                        row = cur.fetchone()
                    conn.close()
                    if row and row[0] and "sheet_row:" in str(row[0]):
                        sheet_row = int(str(row[0]).split("sheet_row:")[1].split("|")[0])
                        gsheets.push_trade_closed(sheet_row, exit_price, float(row[1] or 0))
                except Exception as e:
                    log.error(f"Sheets close error: {e}")

    except Exception as e:
        log.error(f"_check_closed_pnl {symbol}: {e}")
        import traceback; log.error(traceback.format_exc())


# ─── POLLER STARTER — defined here so poll_closed_trades is already defined ───
def _start_poller():
    try:
        poller = threading.Thread(target=poll_closed_trades, daemon=True)
        poller.start()
        log.info("Background poller thread started")
    except Exception as e:
        log.error(f"Failed to start poller: {e}")

# Start poller — deferred to avoid blocking module import
try:
    _start_poller()
except Exception as _e:
    import logging as _log
    _log.getLogger("main").error(f"Poller start failed: {_e}")


# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"status": "ok", "service": "bybit-webhook"}), 200


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


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


@app.route("/webhook/test", methods=["GET"])
def webhook_test():
    """Test the full webhook chain with a fake payload."""
    try:
        test_payload = '{"secret":"' + WEBHOOK_SECRET + '","symbol":"TESTUSDT","side":"Buy","orderType":"Limit","entry":1.0,"sl":0.9,"tp":1.3,"cancelAfterBars":20,"barSeconds":180,"testMode":true,"testBalancePct":0.1,"testLeverage":2,"source":"ob","rr":3,"slBuf":0.1,"minImpulse":1.3,"entryOffset":0.0}'
        import json as _j
        data = _j.loads(test_payload)
        return jsonify({
            "status": "ok",
            "parsed": data,
            "secret_match": data.get("secret") == WEBHOOK_SECRET,
            "message": "Parsing works. If this shows ok but real alerts fail, the issue is in TradingView sending."
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/webhook/last", methods=["GET"])
def webhook_last():
    """Show the last 5 webhook payloads received."""
    return jsonify({
        "last_payloads": _last_webhooks,
        "count": len(_last_webhooks)
    }), 200


# Store last 5 webhook payloads for debugging
_last_webhooks = []


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
    # Read raw body FIRST before any other request parsing
    raw_body = request.get_data(as_text=True)
    log.info(f"WEBHOOK received, raw_body length={len(raw_body)}, preview={repr(raw_body[:100])}")

    # Try standard JSON parse
    data = None
    import json as json_lib
    try:
        data = json_lib.loads(raw_body)
    except Exception:
        pass

    if not data:
        log.info(f"Raw payload: {repr(raw_body[:500])}")
        start = raw_body.find('{"secret"')
        if start == -1:
            start = raw_body.rfind('{')
        end = raw_body.rfind('}')
        if start != -1 and end != -1 and end > start:
            json_str = raw_body[start:end+1]
            if '\\"' in json_str:
                json_str = json_str.replace('\\"', '"')
            try:
                data = json_lib.loads(json_str)
                log.info(f"Extracted JSON: {data}")
            except Exception as e:
                log.warning(f"JSON parse failed: {e} — tried: {repr(json_str[:300])}")
        if not data and "||" in raw_body:
            try:
                part = raw_body.split("||")[-1].strip()
                if '\\"' in part:
                    part = part.replace('\\"', '"')
                data = json_lib.loads(part)
                log.info(f"Extracted JSON via || separator: {data}")
            except Exception as e:
                log.warning(f"|| parse failed: {e}")

    # Store for debugging AFTER all parsing attempts
    _last_webhooks.append({
        "time":   datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "data":   data,
        "raw":    raw_body[:1000],
        "parsed": data is not None,
    })
    if len(_last_webhooks) > 5:
        _last_webhooks.pop(0)
    if not data:
        log.warning(f"JSON extraction FAILED — raw_body={repr(raw_body[:300])}")
        return jsonify({"status": "ok", "message": "Notification received — no order placed"}), 200

    log.info(f"JSON parsed OK: symbol={data.get('symbol')} side={data.get('side')} source={data.get('source')}")

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

    # ── Server-side filters ───────────────────────────────────────────────────
    tf_label = gsheets._bar_seconds_to_tf(bar_seconds)

    # Extract WR from raw readable text — format: "WR: 28% (7/25)"
    alert_wr = 0.0
    try:
        import re as _re
        wr_match = _re.search(r'WR:\s*([\d.]+)%', raw_body)
        if wr_match:
            alert_wr = float(wr_match.group(1))
            log.info(f"Extracted WR from text: {alert_wr}%")
    except:
        pass

    # Also try from JSON payload if present
    if alert_wr == 0.0:
        try:
            wr_raw = str(data.get("wr", "") or "") if data else ""
            if wr_raw:
                alert_wr = float(wr_raw.split("%")[0].strip())
        except:
            pass

    def _filter_skip(reason):
        log.info(f"🚫 Filtered: {reason}")
        log_order_skipped(symbol, side, entry, sl, tp, f"Filtered: {reason}")
        return jsonify({"status": "filtered", "message": reason}), 200

    # Side filter: FILTER_SIDE=long or short
    if cfg["filter_side"]:
        trade_side = "long" if side == "Buy" else "short"
        if trade_side != cfg["filter_side"]:
            return _filter_skip(f"{symbol} {side} blocked — FILTER_SIDE={cfg['filter_side']}")

    # Min WR filter: FILTER_MIN_WR=49
    if cfg["filter_min_wr"] > 0:
        if alert_wr == 0.0:
            return _filter_skip(f"{symbol} WR unknown (could not extract from alert) — FILTER_MIN_WR={cfg['filter_min_wr']}% requires known WR")
        if alert_wr < cfg["filter_min_wr"]:
            return _filter_skip(f"{symbol} WR {alert_wr}% < FILTER_MIN_WR={cfg['filter_min_wr']}%")

    # Source filter: FILTER_SOURCES=ob,fibob
    if cfg["filter_sources"]:
        allowed = [s.strip() for s in cfg["filter_sources"].split(",")]
        if source.lower() not in allowed:
            return _filter_skip(f"source '{source}' not in FILTER_SOURCES={cfg['filter_sources']}")

    # Timeframe filter: FILTER_TIMEFRAMES=M15,H1
    if cfg["filter_timeframes"]:
        allowed = [t.strip() for t in cfg["filter_timeframes"].split(",")]
        if tf_label not in allowed:
            return _filter_skip(f"TF '{tf_label}' not in FILTER_TIMEFRAMES={cfg['filter_timeframes']}")

    # Symbol allow-list: FILTER_SYMBOLS_ALLOW=BTCUSDT,ETHUSDT
    if cfg["filter_symbols_allow"]:
        allowed = [s.strip() for s in cfg["filter_symbols_allow"].split(",")]
        if symbol not in allowed:
            return _filter_skip(f"{symbol} not in FILTER_SYMBOLS_ALLOW")

    # Symbol block-list: FILTER_SYMBOLS_BLOCK=POLUSDT,NEARUSDT
    if cfg["filter_symbols_block"]:
        blocked = [s.strip() for s in cfg["filter_symbols_block"].split(",")]
        if symbol in blocked:
            return _filter_skip(f"{symbol} is in FILTER_SYMBOLS_BLOCK")

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
            # Check direction of existing position vs incoming order
            try:
                pos_resp   = _api_call(session.get_positions, category="linear", symbol=symbol)
                pos_list   = pos_resp.get("result", {}).get("list", [])
                pos_side   = next((p.get("side") for p in pos_list if float(p.get("size", 0)) > 0), None)
                same_dir   = (pos_side == "Buy" and side == "Buy") or (pos_side == "Sell" and side == "Sell")
                if same_dir:
                    msg = f"Already have open {pos_side} position in {symbol} — skipping same direction"
                    log.warning(msg)
                    log_order_skipped(symbol, side, entry, sl, tp, msg)
                    return jsonify({"status": "skipped", "message": msg}), 200
                else:
                    # Opposite direction — cancel any pending limit orders and skip
                    # Let the running trade complete at TP or SL
                    open_orders = get_open_orders(symbol)
                    if open_orders:
                        session.cancel_all_orders(category="linear", symbol=symbol)
                        log.info(f"Cancelled {len(open_orders)} pending {side} limit(s) for {symbol} — opposite {pos_side} position running, letting it complete")
                    msg = f"Opposite position running for {symbol} ({pos_side}) — cancelled pending limit, skipping new {side} order"
                    log.warning(msg)
                    log_order_skipped(symbol, side, entry, sl, tp, msg)
                    return jsonify({"status": "skipped", "message": msg}), 200
            except Exception as e:
                log.error(f"Error checking position direction for {symbol}: {e}")
                msg = f"Already have open position in {symbol} — skipping"
                log.warning(msg)
                log_order_skipped(symbol, side, entry, sl, tp, msg)
                return jsonify({"status": "skipped", "message": msg}), 200

        open_orders = get_open_orders(symbol)
        if open_orders:
            # Check direction of existing pending order
            existing_side = open_orders[0].get("side", "") if open_orders else ""
            opposite_dir  = (existing_side == "Buy" and side == "Sell") or (existing_side == "Sell" and side == "Buy")

            if opposite_dir:
                # Opposite direction pending — cancel it and place new one
                log.info(f"Cancelling opposite {existing_side} pending order for {symbol} — placing new {side} order")
                try:
                    session.cancel_all_orders(category="linear", symbol=symbol)
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET status = 'skipped', notes = 'Cancelled — opposite direction signal' WHERE symbol = " + ph() + " AND status = 'open'", (symbol,))
                        conn.commit()
                except Exception as e:
                    log.error(f"Error cancelling opposite order for {symbol}: {e}")
            elif source == "ob" and order_type == "Limit":
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
        qty = calculate_qty(symbol, entry, sl, actual_bal_pct, actual_leverage)
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
                                 leverage=actual_leverage,
                                 notes=json.dumps({
                                     "rr":          data.get("rr"),
                                     "slBuf":       data.get("slBuf"),
                                     "minImpulse":  data.get("minImpulse"),
                                     "entryOffset": data.get("entryOffset"),
                                 }) if data.get("rr") else None)
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
                                    # Append sheet_row to existing notes
                                    cur.execute("SELECT notes FROM trades WHERE order_id = " + ph(), (order_id,))
                                    row = cur.fetchone()
                                    existing = str(row[0]) if row and row[0] else ""
                                    new_notes = f"{existing}|sheet_row:{sheet_row}" if existing else f"sheet_row:{sheet_row}"
                                    cur.execute("UPDATE trades SET notes = " + ph() + " WHERE order_id = " + ph(), (new_notes, order_id))
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
        "filters": {
            "side":            cfg["filter_side"]            or "both",
            "min_wr":          cfg["filter_min_wr"]          or "disabled",
            "sources":         cfg["filter_sources"]         or "all",
            "timeframes":      cfg["filter_timeframes"]      or "all",
            "symbols_allow":   cfg["filter_symbols_allow"]   or "all",
            "symbols_block":   cfg["filter_symbols_block"]   or "none",
        },
    })


@app.route("/poll", methods=["GET", "POST"])
def manual_poll():
    """Manually trigger a check of closed trades."""
    try:
        import psycopg2.extras
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, symbol, side, status, order_id, opened_at FROM trades ORDER BY opened_at DESC LIMIT 20")
            all_trades = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'")
            open_count = cur.fetchone()["cnt"]
        conn.close()

        _check_closed_trades()

        return jsonify({
            "status":        "ok",
            "open_count":    open_count,
            "recent_trades": all_trades,
            "message":       f"Poll complete — found {open_count} open trades",
        }), 200
    except Exception as e:
        import traceback
        log.error(f"Poll error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


@app.route("/journal/delete-duplicates", methods=["GET", "POST"])
def delete_duplicates():
    """Delete duplicate imported trades where an identical native trade exists."""
    try:
        conn = get_db()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Find imported trades where symbol+side+exit_price matches a non-imported trade
            cur.execute("""
                DELETE FROM trades
                WHERE source = 'bybit_import'
                AND notes = 'Imported — matched to journal entry'
                AND EXISTS (
                    SELECT 1 FROM trades t2
                    WHERE t2.id != trades.id
                    AND t2.symbol = trades.symbol
                    AND t2.side   = trades.side
                    AND t2.source != 'bybit_import'
                    AND ABS(EXTRACT(EPOCH FROM (t2.opened_at::timestamp - trades.opened_at::timestamp))) < 3600
                )
                RETURNING id
            """)
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "message": f"Deleted {deleted} duplicate imported trades"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/delete-imports", methods=["GET", "POST"])
def delete_imports():
    """Delete all bybit_import trades so they can be re-imported with better matching."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trades WHERE source = 'bybit_import'")
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "message": f"Deleted {deleted} imported trades"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/import", methods=["GET", "POST"])
def import_from_bybit():
    """Import closed trades from Bybit that are missing from the journal."""
    try:
        imported = 0
        matched  = 0
        skipped  = 0
        results  = []

        resp    = _api_call(session.get_closed_pnl, category="linear", limit=200)
        records = resp.get("result", {}).get("list", [])

        if not records:
            return jsonify({"status": "ok", "message": "No closed PnL records found"}), 200

        # Get existing trades for matching
        conn = get_db()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT order_id, symbol, side, source, timeframe, sl, tp, entry, opened_at FROM trades WHERE order_id IS NOT NULL")
            existing = [dict(r) for r in cur.fetchall()]
        conn.close()

        existing_ids  = {t["order_id"] for t in existing}
        # Index by symbol+side for quick lookup
        from collections import defaultdict
        by_sym_side = defaultdict(list)
        for t in existing:
            by_sym_side[(t["symbol"], t["side"])].append(t)

        for r in records:
            try:
                order_id   = r.get("orderId", "")
                symbol     = r.get("symbol", "")
                side_close = r.get("side", "")
                qty        = float(r.get("qty", 0))
                exit_price = float(r.get("avgExitPrice", 0) or r.get("exitPrice", 0))
                pnl        = float(r.get("closedPnl", 0) or 0)
                exec_type  = r.get("execType", "")
                created_ms = int(r.get("createdTime", 0) or 0)
                updated_ms = int(r.get("updatedTime", 0) or created_ms)

                if not order_id or not symbol or exit_price <= 0:
                    continue
                if order_id in existing_ids:
                    skipped += 1
                    continue

                entry_side = "Buy" if side_close == "Sell" else "Sell"
                outcome    = "tp" if exec_type == "TakeProfit" else "sl"
                closed_dt  = datetime.utcfromtimestamp(updated_ms/1000).strftime("%Y-%m-%d %H:%M:%S") if updated_ms else datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                opened_dt  = datetime.utcfromtimestamp(created_ms/1000).strftime("%Y-%m-%d %H:%M:%S") if created_ms else closed_dt

                # Try to find matching journal entry by symbol+side+time proximity
                source_val    = "bybit_import"
                timeframe_val = None
                sl_val        = 0
                tp_val        = 0
                entry_val     = exit_price
                notes_val     = "Imported — no indicator data"

                candidates = by_sym_side.get((symbol, entry_side), [])
                for c in candidates:
                    try:
                        c_opened = datetime.strptime(c["opened_at"][:19], "%Y-%m-%d %H:%M:%S")
                        b_opened = datetime.strptime(opened_dt[:19], "%Y-%m-%d %H:%M:%S")
                        diff_mins = abs((b_opened - c_opened).total_seconds() / 60)
                        if diff_mins < 60:  # within 1 hour
                            source_val    = c.get("source") or "bybit_import"
                            timeframe_val = c.get("timeframe")
                            sl_val        = c.get("sl") or 0
                            tp_val        = c.get("tp") or 0
                            entry_val     = c.get("entry") or exit_price
                            notes_val     = f"Imported — matched to journal entry"
                            matched      += 1
                            break
                    except:
                        pass

                # If matched to an existing journal entry — skip, don't create duplicate
                if notes_val == "Imported — matched to journal entry":
                    # Update the existing entry's PnL if it's missing
                    try:
                        conn2 = get_db()
                        with conn2.cursor() as cur2:
                            cur2.execute("""
                                UPDATE trades SET
                                    exit_price = %s,
                                    pnl        = %s,
                                    outcome    = %s,
                                    closed_at  = %s,
                                    status     = 'closed'
                                WHERE symbol = %s AND side = %s
                                AND opened_at BETWEEN %s::timestamp - interval '1 hour'
                                             AND     %s::timestamp + interval '1 hour'
                                AND status != 'closed'
                            """, (exit_price, pnl, outcome, closed_dt,
                                  symbol, entry_side, opened_dt, opened_dt))
                            updated = cur2.rowcount
                        conn2.commit()
                        conn2.close()
                        if updated:
                            matched += 1
                            results.append(f"🔄 {symbol} {entry_side} updated existing entry")
                        else:
                            skipped += 1
                    except Exception as e:
                        results.append(f"⚠️ {symbol}: match update failed: {e}")
                    continue  # don't insert duplicate

                conn = get_db()
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO trades
                            (symbol, side, status, qty, entry, sl, tp, exit_price,
                             pnl, pnl_pct, outcome, order_id, source, timeframe,
                             opened_at, closed_at, notes)
                        VALUES (%s,%s,'closed',%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (symbol, entry_side, qty, entry_val, sl_val, tp_val,
                          exit_price, pnl, outcome, order_id,
                          source_val, timeframe_val, opened_dt, closed_dt, notes_val))
                conn.commit()
                conn.close()
                imported += 1
                results.append(f"✅ {symbol} {entry_side} {outcome.upper()} src={source_val} PnL={pnl:.4f}")

            except Exception as e:
                results.append(f"❌ {symbol}: {e}")

        msg = f"Imported {imported} ({matched} matched to indicator), skipped {skipped} existing"
        log.info(f"Bybit import: {msg}")
        return jsonify({"status": "ok", "message": msg, "details": results[:30]}), 200

    except Exception as e:
        import traceback
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500
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
<p class="subtitle">{{ total_closed }} closed trades ({{ total_known }} with indicator data{% if total_imported > 0 %}, {{ total_imported }} imported{% endif %}) · {{ total_open }} open · updated just now</p>

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

<div class="section" style="margin-bottom:16px">
  <div class="section-title">🎯 Key Level Filter Impact</div>
  <p style="font-size:11px;color:var(--dim);margin-bottom:10px">Comparing trades that triggered at a key S/R level vs those without — shows whether the filter improves results</p>
  <table>
    <tr><th></th><th>Trades</th><th>WR%</th><th>Total PnL</th></tr>
    <tr>
      <td><span class="tag tag-green">With Key Level</span></td>
      <td>{{ kl_analysis.with_kl.count }}</td>
      <td>
        <span style="color:{{ 'var(--green)' if kl_analysis.with_kl.wr >= 50 else 'var(--amber)' if kl_analysis.with_kl.wr >= 35 else 'var(--red)' }};font-weight:500">{{ kl_analysis.with_kl.wr }}%</span>
        <span class="bar-wrap"><span class="bar" style="width:{{ kl_analysis.with_kl.wr }}%;background:{{ '#4caf50' if kl_analysis.with_kl.wr >= 50 else '#ffa726' if kl_analysis.with_kl.wr >= 35 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if kl_analysis.with_kl.pnl >= 0 else 'var(--red)' }}">{{ '+' if kl_analysis.with_kl.pnl >= 0 else '' }}{{ '%.2f'|format(kl_analysis.with_kl.pnl) }}</td>
    </tr>
    <tr>
      <td><span class="tag tag-amber">Without Key Level</span></td>
      <td>{{ kl_analysis.without_kl.count }}</td>
      <td>
        <span style="color:{{ 'var(--green)' if kl_analysis.without_kl.wr >= 50 else 'var(--amber)' if kl_analysis.without_kl.wr >= 35 else 'var(--red)' }};font-weight:500">{{ kl_analysis.without_kl.wr }}%</span>
        <span class="bar-wrap"><span class="bar" style="width:{{ kl_analysis.without_kl.wr }}%;background:{{ '#4caf50' if kl_analysis.without_kl.wr >= 50 else '#ffa726' if kl_analysis.without_kl.wr >= 35 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if kl_analysis.without_kl.pnl >= 0 else 'var(--red)' }}">{{ '+' if kl_analysis.without_kl.pnl >= 0 else '' }}{{ '%.2f'|format(kl_analysis.without_kl.pnl) }}</td>
    </tr>
  </table>
  {% set delta = kl_analysis.with_kl.wr - kl_analysis.without_kl.wr %}
  {% if kl_analysis.with_kl.count >= 5 and kl_analysis.without_kl.count >= 5 %}
  <p style="font-size:11px;margin-top:8px;color:{{ 'var(--green)' if delta > 3 else 'var(--red)' if delta < -3 else 'var(--dim)' }}">
    {% if delta > 3 %}✅ Key level filter improves WR by {{ '%.1f'|format(delta) }}% — keep it ON
    {% elif delta < -3 %}⚠️ Key level filter reduces WR by {{ '%.1f'|format(delta|abs) }}% — consider turning it OFF
    {% else %}📊 Key level filter has minimal impact ({{ '+' if delta >= 0 else '' }}{{ '%.1f'|format(delta) }}%) — neutral{% endif %}
  </p>
  {% else %}
  <p style="font-size:11px;margin-top:8px;color:var(--dim)">Need 5+ trades in each group for meaningful comparison</p>
  {% endif %}
</div>

<div class="section" style="margin-bottom:16px">
  <div class="section-title">Best &amp; Worst — Timeframe + Source</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
    <div>
      <div style="font-size:11px;color:var(--green);margin-bottom:6px;font-weight:500">▲ Best</div>
      <table>
        <tr><th>TF</th><th>Source</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
        {% for r in best_tf_src %}
        <tr><td>{{ r.k1 }}</td><td><span class="tag tag-green">{{ r.k2.upper() }}</span></td>
        <td class="green">{{ r.wins }}</td><td class="red">{{ r.losses }}</td>
        <td>{{ r.wr }}%<span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:#4caf50"></span></span></td>
        <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td></tr>
        {% else %}<tr><td colspan="6" style="color:var(--dim)">Need 2+ trades per combo</td></tr>
        {% endfor %}
      </table>
    </div>
    <div>
      <div style="font-size:11px;color:var(--red);margin-bottom:6px;font-weight:500">▼ Worst</div>
      <table>
        <tr><th>TF</th><th>Source</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
        {% for r in worst_tf_src %}
        <tr><td>{{ r.k1 }}</td><td><span class="tag tag-red">{{ r.k2.upper() }}</span></td>
        <td class="green">{{ r.wins }}</td><td class="red">{{ r.losses }}</td>
        <td>{{ r.wr }}%<span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:#ef5350"></span></span></td>
        <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td></tr>
        {% else %}<tr><td colspan="6" style="color:var(--dim)">Need 2+ trades per combo</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
</div>

<div class="section" style="margin-bottom:16px">
  <div class="section-title">Best &amp; Worst — Timeframe + Symbol</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
    <div>
      <div style="font-size:11px;color:var(--green);margin-bottom:6px;font-weight:500">▲ Best</div>
      <table>
        <tr><th>TF</th><th>Symbol</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
        {% for r in best_tf_sym %}
        <tr><td>{{ r.k1 }}</td><td>{{ r.k2 }}</td>
        <td class="green">{{ r.wins }}</td><td class="red">{{ r.losses }}</td>
        <td>{{ r.wr }}%<span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:#4caf50"></span></span></td>
        <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td></tr>
        {% else %}<tr><td colspan="6" style="color:var(--dim)">Need 2+ trades per combo</td></tr>
        {% endfor %}
      </table>
    </div>
    <div>
      <div style="font-size:11px;color:var(--red);margin-bottom:6px;font-weight:500">▼ Worst</div>
      <table>
        <tr><th>TF</th><th>Symbol</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
        {% for r in worst_tf_sym %}
        <tr><td>{{ r.k1 }}</td><td>{{ r.k2 }}</td>
        <td class="green">{{ r.wins }}</td><td class="red">{{ r.losses }}</td>
        <td>{{ r.wr }}%<span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:#ef5350"></span></span></td>
        <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td></tr>
        {% else %}<tr><td colspan="6" style="color:var(--dim)">Need 2+ trades per combo</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
</div>

<div class="section" style="margin-bottom:16px">
  <div class="section-title">Best &amp; Worst — Source + Symbol</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
    <div>
      <div style="font-size:11px;color:var(--green);margin-bottom:6px;font-weight:500">▲ Best</div>
      <table>
        <tr><th>Source</th><th>Symbol</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
        {% for r in best_src_sym %}
        <tr><td><span class="tag tag-green">{{ r.k1.upper() }}</span></td><td>{{ r.k2 }}</td>
        <td class="green">{{ r.wins }}</td><td class="red">{{ r.losses }}</td>
        <td>{{ r.wr }}%<span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:#4caf50"></span></span></td>
        <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td></tr>
        {% else %}<tr><td colspan="6" style="color:var(--dim)">Need 2+ trades per combo</td></tr>
        {% endfor %}
      </table>
    </div>
    <div>
      <div style="font-size:11px;color:var(--red);margin-bottom:6px;font-weight:500">▼ Worst</div>
      <table>
        <tr><th>Source</th><th>Symbol</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
        {% for r in worst_src_sym %}
        <tr><td><span class="tag tag-red">{{ r.k1.upper() }}</span></td><td>{{ r.k2 }}</td>
        <td class="green">{{ r.wins }}</td><td class="red">{{ r.losses }}</td>
        <td>{{ r.wr }}%<span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:#ef5350"></span></span></td>
        <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td></tr>
        {% else %}<tr><td colspan="6" style="color:var(--dim)">Need 2+ trades per combo</td></tr>
        {% endfor %}
      </table>
    </div>
  </div>
</div>

<div class="section">
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
    # All resolved trades for overall stats (including imports — we know TP/SL)
    closed  = [t for t in trades if t.get("outcome") in ("tp", "sl")]
    open_t  = [t for t in trades if t.get("status") == "open"]

    # For breakdowns — exclude unmatched imports (no source/timeframe data)
    def has_indicator(t):
        src = (t.get("source") or "")
        return src != "bybit_import" or bool(t.get("timeframe"))
    closed_known = [t for t in closed if has_indicator(t)]

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
        for t in closed_known:  # only trades with known indicator
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
        entry      = float(t.get("entry")      or 0)
        sl         = float(t.get("sl")         or 0)
        tp         = float(t.get("tp")         or 0)
        opened_at  = t.get("opened_at")        or ""
        closed_at  = t.get("closed_at")        or ""
        if entry > 0 and sl > 0:
            sl_dist     = abs(entry - sl)
            sl_dist_pct = round(sl_dist / entry * 100, 3)
            tp_dist     = abs(tp - entry) if tp > 0 else 0
            rr          = round(tp_dist / sl_dist, 2) if sl_dist > 0 else 0
            time_str = "—"
            try:
                from datetime import datetime
                fmt   = "%Y-%m-%d %H:%M:%S"
                t_in  = datetime.strptime(opened_at[:19], fmt)
                t_out = datetime.strptime(closed_at[:19], fmt)
                mins  = int((t_out - t_in).total_seconds() / 60)
                time_str = f"{mins}m" if mins < 120 else f"{mins//60}h {mins%60}m"
            except:
                pass
            sl_margins.append({
                "symbol":      t["symbol"],
                "side":        t["side"],
                "entry":       entry,
                "sl":          sl,
                "sl_dist_pct": sl_dist_pct,
                "rr":          rr,
                "time_in":     time_str,
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

    for t in closed_known:
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

    # ── KL Level analysis — trades with vs without key level ─────────────────
    with_kl    = [t for t in closed_known if _extract_kl_level(t.get("notes"))]
    without_kl = [t for t in closed_known if not _extract_kl_level(t.get("notes"))]

    def _wr(trades):
        if not trades: return 0
        return round(sum(1 for t in trades if t["outcome"] == "tp") / len(trades) * 100, 1)

    kl_analysis = {
        "with_kl":    {"count": len(with_kl),    "wr": _wr(with_kl),    "pnl": round(sum(float(t.get("pnl") or 0) for t in with_kl), 2)},
        "without_kl": {"count": len(without_kl), "wr": _wr(without_kl), "pnl": round(sum(float(t.get("pnl") or 0) for t in without_kl), 2)},
    }
    def cross_breakdown(key1_fn, key2_fn):
        """Group by two keys, return nested stats."""
        groups = {}
        for t in closed_known:
            k1 = key1_fn(t)
            k2 = key2_fn(t)
            key = (k1, k2)
            if key not in groups:
                groups[key] = {"wins": 0, "losses": 0, "pnl": 0.0}
            groups[key]["wins"   if t["outcome"] == "tp" else "losses"] += 1
            groups[key]["pnl"] += float(t.get("pnl") or 0)
        rows = []
        for (k1, k2), v in groups.items():
            total = v["wins"] + v["losses"]
            rows.append({
                "k1": k1, "k2": k2,
                "wins": v["wins"], "losses": v["losses"],
                "wr": round(v["wins"] / total * 100, 1) if total > 0 else 0,
                "pnl": round(v["pnl"], 2),
                "total": total
            })
        return sorted(rows, key=lambda x: -x["total"])

    src_fn = lambda t: (t.get("source") or "").replace("_test", "")
    tf_fn  = lambda t: t.get("timeframe") or "—"
    sym_fn = lambda t: t["symbol"]

    by_tf_source  = cross_breakdown(tf_fn,  src_fn)
    by_tf_symbol  = cross_breakdown(tf_fn,  sym_fn)
    by_src_symbol = cross_breakdown(src_fn, sym_fn)

    # Best and worst performers
    def best_worst(rows, min_trades=2):
        eligible = [r for r in rows if r["total"] >= min_trades]
        if not eligible:
            return [], []
        best  = sorted(eligible, key=lambda x: (-x["wr"], -x["pnl"]))[:3]
        worst = sorted(eligible, key=lambda x: (x["wr"], x["pnl"]))[:3]
        return best, worst

    best_tf_src,  worst_tf_src  = best_worst(by_tf_source)
    best_tf_sym,  worst_tf_sym  = best_worst(by_tf_symbol)
    best_src_sym, worst_src_sym = best_worst(by_src_symbol)

    return {
        "total_closed":   total_closed,
        "total_known":    len(closed_known),
        "total_imported": total_closed - len(closed_known),
        "total_open":     len(open_t),
        "wr":             wr,
        "total_pnl":      total_pnl,
        "avg_win":        avg_win,
        "avg_loss":       avg_loss,
        "profit_factor":  profit_factor,
        "expectancy":     expectancy,
        "by_symbol":      by_symbol,
        "by_source":      by_source,
        "by_side":        by_side,
        "by_tf":          by_tf,
        "by_day":         by_day,
        "by_session":     by_session,
        "by_weekend":     by_weekend,
        "sl_margins":     sl_margins,
        "by_tf_source":   by_tf_source,
        "by_tf_symbol":   by_tf_symbol,
        "by_src_symbol":  by_src_symbol,
        "best_tf_src":    best_tf_src,
        "worst_tf_src":   worst_tf_src,
        "best_tf_sym":    best_tf_sym,
        "worst_tf_sym":   worst_tf_sym,
        "best_src_sym":   best_src_sym,
        "worst_src_sym":  worst_src_sym,
        "kl_analysis":    kl_analysis,
        "insights":       insights,
    }


RECOMMENDATIONS_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Alert Recommendations</title>
<style>
  :root { --bg:#0f0f0f;--surface:#1a1a1a;--border:#2a2a2a;--text:#e8e8e8;--dim:#888;--green:#4caf50;--red:#ef5350;--blue:#42a5f5;--amber:#ffa726;--purple:#ce93d8; }
  * { box-sizing:border-box;margin:0;padding:0; }
  body { background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:13px;padding:24px; }
  h1 { font-size:18px;font-weight:500;margin-bottom:4px; }
  .subtitle { color:var(--dim);font-size:12px;margin-bottom:24px; }
  .nav { display:flex;gap:12px;margin-bottom:20px; }
  .nav a { color:var(--dim);text-decoration:none;font-size:12px; }
  .nav a:hover { color:var(--text); }
  .section { background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px; }
  .section-title { font-size:13px;font-weight:500;margin-bottom:12px;color:var(--dim);text-transform:uppercase;letter-spacing:0.05em; }
  table { width:100%;border-collapse:collapse; }
  th { text-align:left;color:var(--dim);font-weight:400;font-size:11px;padding:4px 8px;border-bottom:1px solid var(--border); }
  td { padding:7px 8px;border-bottom:1px solid var(--border); }
  tr:last-child td { border-bottom:none; }
  .green { color:var(--green); }
  .red { color:var(--red); }
  .tag { display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600; }
  .tag-green  { background:rgba(76,175,80,0.15);color:var(--green); }
  .tag-red    { background:rgba(239,83,80,0.15);color:var(--red); }
  .tag-amber  { background:rgba(255,167,38,0.15);color:var(--amber); }
  .tag-blue   { background:rgba(66,165,245,0.15);color:var(--blue); }
  .tag-purple { background:rgba(206,147,216,0.15);color:var(--purple); }
  .bar-wrap { background:var(--border);border-radius:3px;height:6px;width:60px;display:inline-block;vertical-align:middle;margin-left:6px; }
  .bar { height:6px;border-radius:3px; }
  .rec-row { display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid var(--border); }
  .rec-row:last-child { border-bottom:none; }
  .rec-num  { width:22px;height:22px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;flex-shrink:0; }
  .rec-num.active { background:rgba(76,175,80,0.2);color:var(--green); }
  .rec-num.pause  { background:rgba(239,83,80,0.15);color:var(--red); }
  .rec-body { flex:1; }
  .rec-title { font-size:13px;font-weight:500;margin-bottom:2px; }
  .rec-sub   { font-size:11px;color:var(--dim); }
  .rec-badge { margin-left:auto;text-align:right;flex-shrink:0; }
  .updated   { font-size:11px;color:var(--dim);margin-bottom:16px; }
  .empty { color:var(--dim);font-size:12px;padding:12px 0; }
  .confidence { font-size:10px;padding:2px 6px;border-radius:3px;font-weight:500; }
  .conf-high   { background:rgba(76,175,80,0.2);color:var(--green); }
  .conf-medium { background:rgba(255,167,38,0.2);color:var(--amber); }
  .conf-low    { background:rgba(239,83,80,0.15);color:var(--red); }
</style>
</head>
<body>
<div class="nav">
  <a href="/journal">← Journal</a>
  <a href="/analysis">📊 Analysis</a>
  <a href="/recommendations">🔄 Refresh</a>
  <button onclick="runBacktest(this)" style="background:rgba(66,165,245,0.15);color:var(--blue);border:1px solid rgba(66,165,245,0.3);border-radius:4px;padding:3px 10px;font-size:12px;cursor:pointer;">▶ Run Backtest</button>
</div>
<h1>// Alert Recommendations</h1>
<p class="subtitle">Based on {{ total }} closed trades · Last updated {{ updated }}</p>
<p class="updated">Minimum {{ min_trades }} trades required per combination · Sorted by win rate then PnL · Backtest: {{ bt_status }}</p>
<script>
function runBacktest(btn) {
  btn.innerText = '⏳ Running...';
  btn.disabled = true;
  fetch('/backtest/run', {method:'POST'})
    .then(r => r.json())
    .then(d => {
      btn.innerText = d.status === 'already_running' ? '⏳ Already running' : '✅ Started';
      setTimeout(() => { btn.innerText = '▶ Run Backtest'; btn.disabled = false; }, 3000);
    })
    .catch(() => { btn.innerText = '❌ Error'; btn.disabled = false; });
}
</script>

<div class="section">
  <div class="section-title">✅ Run these alerts</div>
  {% for r in active %}
  <div class="rec-row">
    <div class="rec-num active">{{ loop.index }}</div>
    <div class="rec-body">
      <div class="rec-title">
        {{ r.symbol }}
        <span class="tag tag-blue" style="margin-left:4px">{{ r.tf }}</span>
        <span class="tag {{ 'tag-green' if r.source == 'ob' else 'tag-amber' if r.source == 'fib' else 'tag-purple' }}" style="margin-left:2px">{{ r.source.upper() }}</span>
        <span class="confidence {{ r.conf_class }}" style="margin-left:4px">{{ r.confidence }}</span>
      </div>
      <div class="rec-sub">{{ r.wins }}W / {{ r.losses }}L · Avg win {{ r.avg_win }} · Avg loss {{ r.avg_loss }} · PnL {{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }} USDT</div>
    </div>
    <div class="rec-badge">
      <div style="font-size:18px;font-weight:500;color:var(--green)">{{ r.wr }}%</div>
      <div style="font-size:10px;color:var(--dim)">{{ r.total }} trades</div>
    </div>
  </div>
  {% else %}
    <p class="empty">No combinations with enough data yet — need {{ min_trades }}+ trades per combo.</p>
  {% endfor %}
</div>

<div class="section">
  <div class="section-title">⏸️ Pause or review these</div>
  {% for r in pause %}
  <div class="rec-row">
    <div class="rec-num pause">{{ loop.index }}</div>
    <div class="rec-body">
      <div class="rec-title">
        {{ r.symbol }}
        <span class="tag tag-blue" style="margin-left:4px">{{ r.tf }}</span>
        <span class="tag tag-red" style="margin-left:2px">{{ r.source.upper() }}</span>
      </div>
      <div class="rec-sub">{{ r.wins }}W / {{ r.losses }}L · PnL {{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }} USDT · Reason: {{ r.reason }}</div>
    </div>
    <div class="rec-badge">
      <div style="font-size:18px;font-weight:500;color:var(--red)">{{ r.wr }}%</div>
      <div style="font-size:10px;color:var(--dim)">{{ r.total }} trades</div>
    </div>
  </div>
  {% else %}
    <p class="empty">No underperforming combinations found.</p>
  {% endfor %}
</div>

<div class="section">
  <div class="section-title">📋 All combinations ranked</div>
  <table>
    <tr><th>Symbol</th><th>TF</th><th>Source</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th><th>Confidence</th></tr>
    {% for r in all_combos %}
    <tr>
      <td>{{ r.symbol }}</td>
      <td><span class="tag tag-blue">{{ r.tf }}</span></td>
      <td><span class="tag {{ 'tag-green' if r.source == 'ob' else 'tag-amber' if r.source == 'fib' else 'tag-purple' }}">{{ r.source.upper() }}</span></td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>
        {{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ffa726' if r.wr >= 35 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
      <td><span class="confidence {{ r.conf_class }}">{{ r.confidence }}</span></td>
    </tr>
    {% endfor %}
  </table>
</div>

{% if bt_available and bt_analysis and bt_analysis.tf_rows %}
<div class="section" style="margin-bottom:16px">
  <div class="section-title">📈 Timeframe × Indicator Analysis</div>
  <p style="font-size:11px;color:var(--dim);margin-bottom:12px">Aggregated across all symbols — which TF and indicator combination performs best overall</p>

  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px">

    <div>
      <div style="font-size:11px;font-weight:500;margin-bottom:8px;color:var(--dim)">TIMEFRAME RANKING</div>
      <table>
        <tr><th>#</th><th>TF</th><th>WR%</th><th>Trades</th><th>Best Indicator</th></tr>
        {% for r in bt_analysis.tf_rows %}
        <tr>
          <td style="color:var(--dim)">{{ loop.index }}</td>
          <td><span class="tag tag-blue">{{ r.key }}</span></td>
          <td>
            <span style="color:{{ 'var(--green)' if r.wr >= 50 else 'var(--amber)' if r.wr >= 35 else 'var(--red)' }};font-weight:500">{{ r.wr }}%</span>
            <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ffa726' if r.wr >= 35 else '#ef5350' }};"></span></span>
          </td>
          <td style="color:var(--dim)">{{ r.total }}</td>
          <td>
            {% if bt_analysis.best_src_per_tf[r.key] %}
            <span class="tag tag-green">{{ bt_analysis.best_src_per_tf[r.key].src.upper() }}</span>
            <span style="font-size:10px;color:var(--dim)">{{ bt_analysis.best_src_per_tf[r.key].wr }}%</span>
          </td>
            {% endif %}
        </tr>
        {% endfor %}
      </table>
    </div>

    <div>
      <div style="font-size:11px;font-weight:500;margin-bottom:8px;color:var(--dim)">INDICATOR RANKING</div>
      <table>
        <tr><th>Source</th><th>WR%</th><th>Trades</th><th>Best TF</th></tr>
        {% for r in bt_analysis.src_rows %}
        <tr>
          <td><span class="tag {{ 'tag-green' if r.key == 'ob' else 'tag-amber' if r.key == 'fib' else 'tag-purple' }}">{{ r.key.upper() }}</span></td>
          <td>
            <span style="color:{{ 'var(--green)' if r.wr >= 50 else 'var(--amber)' if r.wr >= 35 else 'var(--red)' }};font-weight:500">{{ r.wr }}%</span>
            <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ffa726' if r.wr >= 35 else '#ef5350' }};"></span></span>
          </td>
          <td style="color:var(--dim)">{{ r.total }}</td>
          <td>
            {% if bt_analysis.best_tf_per_src[r.key] %}
            <span class="tag tag-blue">{{ bt_analysis.best_tf_per_src[r.key].tf }}</span>
            <span style="font-size:10px;color:var(--dim)">{{ bt_analysis.best_tf_per_src[r.key].wr }}%</span>
          </td>
        </tr>
            {% endif %}
        {% endfor %}
      </table>
    </div>

    <div>
      <div style="font-size:11px;font-weight:500;margin-bottom:8px;color:var(--dim)">RECOMMENDATION</div>
      <div style="display:flex;flex-direction:column;gap:8px">
        {% for src, v in bt_analysis.best_tf_per_src.items() %}
        <div style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px">
          <div style="font-size:12px;font-weight:500;margin-bottom:4px">
            <span class="tag {{ 'tag-green' if src == 'ob' else 'tag-amber' if src == 'fib' else 'tag-purple' }}">{{ src.upper() }}</span>
            →
            <span class="tag tag-blue">{{ v.tf }}</span>
          </div>
          <div style="font-size:11px;color:var(--dim)">Best timeframe for {{ src.upper() }} indicator</div>
          <div style="font-size:13px;font-weight:500;color:{{ 'var(--green)' if v.wr >= 50 else 'var(--amber)' }}">{{ v.wr }}% WR · {{ v.total }} trades</div>
        </div>
        {% endfor %}
        {% for tf, v in bt_analysis.best_src_per_tf.items() %}
        <div style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px">
          <div style="font-size:12px;font-weight:500;margin-bottom:4px">
            <span class="tag tag-blue">{{ tf }}</span>
            →
            <span class="tag {{ 'tag-green' if v.src == 'ob' else 'tag-amber' }}">{{ v.src.upper() }}</span>
          </div>
          <div style="font-size:11px;color:var(--dim)">Best indicator for {{ tf }} timeframe</div>
          <div style="font-size:13px;font-weight:500;color:{{ 'var(--green)' if v.wr >= 50 else 'var(--amber)' }}">{{ v.wr }}% WR · {{ v.total }} trades</div>
        </div>
        {% endfor %}
      </div>
    </div>

  </div>
</div>
{% endif %}
{% if bt_available %}
<div class="section">
  <div class="section-title">🤖 Backtest Results — OB vs OB + Key Level Filter ({{ bt_updated }})</div>
  <p style="font-size:11px;color:var(--dim);margin-bottom:10px">{{ bt_rows|length }} combinations · Faded rows WR &lt; 30% · 🔑 = key level variant improves WR</p>
  <table>
    <tr><th>Symbol</th><th>TF</th><th>OB only WR%</th><th>OB + KL WR%</th><th>Δ</th><th>OB trades</th><th>KL trades</th><th>Recommendation</th></tr>
    {% for r in bt_comparison %}
    <tr style="{{ 'opacity:0.5' if r.ob_wr < 30 else '' }}">
      <td>{{ r.symbol }}</td>
      <td><span class="tag tag-blue">{{ r.tf }}</span></td>
      <td>
        {{ r.ob_wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.ob_wr }}%;background:{{ '#4caf50' if r.ob_wr >= 50 else '#ffa726' if r.ob_wr >= 35 else '#ef5350' }};"></span></span>
      </td>
      <td>
        {% if r.kl_wr %}
        <span style="font-weight:500;color:{{ 'var(--green)' if r.kl_wr >= 50 else 'var(--amber)' if r.kl_wr >= 35 else 'var(--red)' }}">{{ r.kl_wr }}%</span>
        <span class="bar-wrap"><span class="bar" style="width:{{ r.kl_wr }}%;background:{{ '#4caf50' if r.kl_wr >= 50 else '#ffa726' if r.kl_wr >= 35 else '#ef5350' }};"></span></span>
        {% else %}
        <span style="color:var(--dim)">—</span>
        {% endif %}
      </td>
      <td style="font-weight:500;color:{{ 'var(--green)' if r.delta and r.delta > 0 else 'var(--red)' if r.delta and r.delta < 0 else 'var(--dim)' }}">
        {% if r.delta %}{{ '+' if r.delta > 0 else '' }}{{ r.delta }}%{% else %}—{% endif %}
      </td>
      <td style="color:var(--dim)">{{ r.ob_total }}</td>
      <td style="color:var(--dim)">{{ r.kl_total or '—' }}</td>
      <td>
        {% if r.kl_wr and r.delta and r.delta >= 5 %}
        <span class="tag tag-green">🔑 Use KL filter</span>
        {% elif r.kl_wr and r.delta and r.delta < -5 %}
        <span class="tag tag-red">Skip KL filter</span>
        {% elif r.ob_wr >= 45 %}
        <span class="tag tag-green">✅ Trade</span>
        {% elif r.ob_wr < 35 %}
        <span class="tag tag-red">❌ Avoid</span>
        {% else %}
        <span class="tag tag-amber">⚠️ Caution</span>
        {% endif %}
      </td>
    </tr>
    {% endfor %}
  </table>
</div>
{% else %}
<div class="section">
  <div class="section-title">🤖 Backtest Results</div>
  <p style="font-size:12px;color:var(--dim)">No backtest data yet. Click ▶ Run Backtest to generate results.</p>
</div>
{% endif %}

</body>
</html>
"""


def _build_recommendations(trades, min_trades=3):
    from datetime import datetime as _dt

    closed = [t for t in trades if t.get("outcome") in ("tp", "sl")]

    # Group by symbol + tf + source
    groups = {}
    for t in closed:
        sym = t.get("symbol", "?")
        tf  = t.get("timeframe") or "—"
        src = (t.get("source") or "").replace("_test", "")
        key = (sym, tf, src)
        if key not in groups:
            groups[key] = {"wins": 0, "losses": 0, "pnl": 0.0,
                           "win_pnls": [], "loss_pnls": []}
        pnl = float(t.get("pnl") or 0)
        if t["outcome"] == "tp":
            groups[key]["wins"]  += 1
            groups[key]["win_pnls"].append(pnl)
        else:
            groups[key]["losses"] += 1
            groups[key]["loss_pnls"].append(pnl)
        groups[key]["pnl"] += pnl

    rows = []
    for (sym, tf, src), v in groups.items():
        total = v["wins"] + v["losses"]
        if total < min_trades:
            continue
        wr       = round(v["wins"] / total * 100, 1)
        avg_win  = round(sum(v["win_pnls"])  / len(v["win_pnls"]),  2) if v["win_pnls"]  else 0
        avg_loss = round(sum(v["loss_pnls"]) / len(v["loss_pnls"]), 2) if v["loss_pnls"] else 0
        pnl      = round(v["pnl"], 2)

        # Confidence based on sample size
        if total >= 10:
            conf_class = "conf-high";   confidence = "High confidence"
        elif total >= 5:
            conf_class = "conf-medium"; confidence = "Medium confidence"
        else:
            conf_class = "conf-low";    confidence = "Low confidence"

        rows.append({
            "symbol": sym, "tf": tf, "source": src,
            "wins": v["wins"], "losses": v["losses"],
            "total": total, "wr": wr, "pnl": pnl,
            "avg_win": f"+{avg_win}", "avg_loss": str(avg_loss),
            "conf_class": conf_class, "confidence": confidence,
        })

    # Sort by WR desc, then PnL desc
    rows.sort(key=lambda x: (-x["wr"], -x["pnl"]))

    # Split active vs pause
    # Active: WR >= 40% AND PnL >= 0
    # Pause: WR < 35% OR PnL < -1
    active, pause = [], []
    for r in rows:
        if r["wr"] >= 40 and r["pnl"] >= 0:
            active.append(r)
        elif r["wr"] < 35 or r["pnl"] < -1:
            reasons = []
            if r["wr"] < 35:
                reasons.append(f"WR only {r['wr']}%")
            if r["pnl"] < -1:
                reasons.append(f"losing {r['pnl']} USDT")
            r["reason"] = ", ".join(reasons)
            pause.append(r)

    return {
        "active":     active,
        "pause":      pause,
        "all_combos": rows,
        "total":      len(closed),
        "min_trades": min_trades,
        "updated":    _dt.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    }

def _build_bt_recommendations(bt_rows):
    """
    Analyse backtest results to find:
    1. Best TF per indicator (source)
    2. Best indicator per TF
    3. Overall TF ranking
    4. Overall indicator ranking
    """
    if not bt_rows:
        return None

    from collections import defaultdict

    # Group by TF
    by_tf = defaultdict(lambda: {"wins":0,"losses":0,"total":0,"pnl":0.0,"combos":0})
    # Group by source (indicator)
    by_src = defaultdict(lambda: {"wins":0,"losses":0,"total":0,"pnl":0.0,"combos":0})
    # Group by TF+source
    by_tf_src = defaultdict(lambda: {"wins":0,"losses":0,"total":0,"pnl":0.0,"combos":0})

    for r in bt_rows:
        tf  = r["timeframe"]
        src = r["source"]
        w, l, t = r["wins"], r["losses"], r["total"]
        pnl = r.get("total_pnl", 0) or 0

        by_tf[tf]["wins"]   += w
        by_tf[tf]["losses"] += l
        by_tf[tf]["total"]  += t
        by_tf[tf]["pnl"]    += pnl
        by_tf[tf]["combos"] += 1

        by_src[src]["wins"]   += w
        by_src[src]["losses"] += l
        by_src[src]["total"]  += t
        by_src[src]["pnl"]    += pnl
        by_src[src]["combos"] += 1

        by_tf_src[(tf,src)]["wins"]   += w
        by_tf_src[(tf,src)]["losses"] += l
        by_tf_src[(tf,src)]["total"]  += t
        by_tf_src[(tf,src)]["pnl"]    += pnl
        by_tf_src[(tf,src)]["combos"] += 1

    def make_row(key, v):
        t = v["total"]
        wr = round(v["wins"] / t * 100, 1) if t > 0 else 0
        ev = round((wr/100 * 2.0) - ((1-wr/100) * 1.0), 3)  # approx expectancy
        return {"key": key, "wins": v["wins"], "losses": v["losses"],
                "total": t, "wr": wr, "pnl": round(v["pnl"],2),
                "combos": v["combos"], "ev": ev}

    tf_rows  = sorted([make_row(k,v) for k,v in by_tf.items()],
                      key=lambda x: (-x["wr"], -x["total"]))
    src_rows = sorted([make_row(k,v) for k,v in by_src.items()],
                      key=lambda x: (-x["wr"], -x["total"]))

    # Best TF for each source
    best_tf_per_src = {}
    for (tf, src), v in by_tf_src.items():
        t  = v["total"]
        wr = round(v["wins"] / t * 100, 1) if t > 0 else 0
        if src not in best_tf_per_src or wr > best_tf_per_src[src]["wr"]:
            best_tf_per_src[src] = {"tf": tf, "wr": wr, "total": t,
                                     "wins": v["wins"], "losses": v["losses"]}

    # Best source for each TF
    best_src_per_tf = {}
    for (tf, src), v in by_tf_src.items():
        t  = v["total"]
        wr = round(v["wins"] / t * 100, 1) if t > 0 else 0
        if tf not in best_src_per_tf or wr > best_src_per_tf[tf]["wr"]:
            best_src_per_tf[tf] = {"src": src, "wr": wr, "total": t,
                                    "wins": v["wins"], "losses": v["losses"]}

    # TF order for display
    tf_order = ["M3","M5","M15","M30","H1","H4","H12","D1"]
    tf_rows.sort(key=lambda x: tf_order.index(x["key"]) if x["key"] in tf_order else 99)

    return {
        "tf_rows":         tf_rows,
        "src_rows":        src_rows,
        "best_tf_per_src": best_tf_per_src,
        "best_src_per_tf": best_src_per_tf,
    }



BT_RR_RATIO      = float(os.getenv("BT_RR_RATIO",    "2.0") or "2.0")
BT_SL_BUF_ATR    = float(os.getenv("BT_SL_BUF_ATR",  "0.5") or "0.5")
BT_ATR_LEN       = int(os.getenv("BT_ATR_LEN",        "14") or "14")
BT_MIN_IMPULSE   = float(os.getenv("BT_MIN_IMPULSE",  "1.3") or "1.3")
BT_MIN_OB_SIZE   = float(os.getenv("BT_MIN_OB_SIZE",  "0.8") or "0.8")
BT_ENTRY_OFFSET  = float(os.getenv("BT_ENTRY_OFFSET", "0.0") or "0.0")
BT_LOOKBACK_DAYS = int(os.getenv("BT_LOOKBACK_DAYS",  "90") or "90")
BT_MIN_TRADES    = int(os.getenv("BT_MIN_TRADES",     "5") or "5")
BT_SYMBOLS       = os.getenv("BT_SYMBOLS",
    "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,"
    "ADAUSDT,AVAXUSDT,DOTUSDT,LINKUSDT,NEARUSDT,"
    "ATOMUSDT,KASUSDT,ONDOUSDT,RENDERUSDT,SEIUSDT,"
    "VETUSDT,XLMUSDT,TRXUSDT,POLUSDT,BNBUSDT"
).split(",")
BT_TIMEFRAMES = {"3":"M3","5":"M5","15":"M15","30":"M30","60":"H1","240":"H4"}

_bt_running   = False
_bt_last_run  = None
_bt_status    = "Never run"


def _bt_fetch_klines(symbol, interval, days):
    end_ms   = int(time.time() * 1000)
    start_ms = end_ms - days * 24 * 3600 * 1000
    all_bars = []
    cursor   = end_ms
    while True:
        try:
            resp = session.get_kline(category="linear", symbol=symbol,
                                     interval=interval, start=start_ms,
                                     end=cursor, limit=1000)
            bars = resp.get("result", {}).get("list", [])
            if not bars:
                break
            bars = list(reversed(bars))
            all_bars = bars + all_bars
            oldest = int(bars[0][0])
            if oldest <= start_ms:
                break
            cursor = oldest - 1
            time.sleep(0.2)
        except Exception as e:
            log.error(f"BT kline {symbol}/{interval}: {e}")
            break
    return [{"ts":int(b[0]),"open":float(b[1]),"high":float(b[2]),
              "low":float(b[3]),"close":float(b[4]),"volume":float(b[5])}
             for b in all_bars]


def _bt_calc_atr(bars, length):
    atrs = [None] * len(bars)
    trs  = []
    for i in range(1, len(bars)):
        tr = max(bars[i]["high"] - bars[i]["low"],
                 abs(bars[i]["high"] - bars[i-1]["close"]),
                 abs(bars[i]["low"]  - bars[i-1]["close"]))
        trs.append(tr)
        if len(trs) >= length:
            atrs[i] = sum(trs[-length:]) / length
    return atrs


def _bt_detect_obs(bars, atrs, min_impulse=None, sl_buf=None, entry_offset=None):
    mi  = min_impulse  if min_impulse  is not None else BT_MIN_IMPULSE
    sb  = sl_buf       if sl_buf       is not None else BT_SL_BUF_ATR
    eo  = entry_offset if entry_offset is not None else BT_ENTRY_OFFSET
    obs = []
    for i in range(2, len(bars)):
        atr = atrs[i]
        if not atr or atr == 0:
            continue
        b0, b1 = bars[i], bars[i-1]
        body0 = abs(b0["close"] - b0["open"])
        body1 = abs(b1["close"] - b1["open"])
        rng1  = b1["high"] - b1["low"]
        if rng1 > 0 and (body1 / rng1 * 100) < 15:
            continue
        if body1 > 0 and (body0 / body1) < mi:
            continue
        ob_top = max(b1["open"], b1["close"])
        ob_bot = min(b1["open"], b1["close"])
        if (ob_top - ob_bot) < atr * BT_MIN_OB_SIZE:  # min OB size still from env
            continue
        ob_mid  = (ob_top + ob_bot) / 2
        is_bull = b1["close"] < b1["open"] and b0["close"] > b0["open"] and b0["close"] > b1["high"]
        is_bear = b1["close"] > b1["open"] and b0["close"] < b0["open"] and b0["close"] < b1["low"]
        if is_bull:
            sl    = min(b1["low"], b0["low"]) - atr * sb
            entry = ob_top - eo * 2.0 * (ob_top - ob_mid)
            risk  = abs(entry - sl)
            if risk <= 0: continue
            obs.append({"bar":i-1,"type":1,"top":ob_top,"bot":ob_bot,
                        "sl":sl,"entry":entry,"tp":None,"risk":risk})
        elif is_bear:
            sl    = max(b1["high"], b0["high"]) + atr * sb
            entry = ob_bot + eo * 2.0 * (ob_mid - ob_bot)
            risk  = abs(entry - sl)
            if risk <= 0: continue
            obs.append({"bar":i-1,"type":2,"top":ob_top,"bot":ob_bot,
                        "sl":sl,"entry":entry,"tp":None,"risk":risk})
    return obs


def _bt_find_key_levels(bars, lookback=300, tol_pct=0.3, min_score=6):
    """
    Pivot-bounce key level detection — mirrors the Pine Script algorithm.
    Returns list of significant price levels with their scores.
    """
    lb   = min(lookback, len(bars) - 1)
    pvlen = 5

    # Find pivot highs and lows
    pivots = []
    for i in range(pvlen, lb - pvlen):
        is_high = all(bars[i]["high"] >= bars[i-j]["high"] and
                      bars[i]["high"] >= bars[i+j]["high"] for j in range(1, pvlen+1))
        is_low  = all(bars[i]["low"]  <= bars[i-j]["low"]  and
                      bars[i]["low"]  <= bars[i+j]["low"]  for j in range(1, pvlen+1))
        if is_high:
            pivots.append({"price": bars[i]["high"], "is_high": True,  "bar": i})
        if is_low:
            pivots.append({"price": bars[i]["low"],  "is_high": False, "bar": i})

    if not pivots:
        return []

    # Cluster pivots
    levels = []  # {price, score, touches}
    for pv in pivots:
        p     = pv["price"]
        found = False
        for lv in levels:
            if lv["price"] > 0 and abs(p - lv["price"]) / lv["price"] * 100 < tol_pct:
                cnt = lv["touches"]
                lv["price"]   = (lv["price"] * cnt + p) / (cnt + 1)
                lv["touches"] += 1
                found = True
                break
        if not found:
            levels.append({"price": p, "score": 0, "touches": 1})

    # Score each level
    for lv in levels:
        lp        = lv["price"]
        score     = 0
        tol       = lp * tol_pct / 100
        zone_bars = 0
        cons_added = False
        last_above = bars[min(lb, len(bars)-1)]["close"] > lp

        for k in range(min(lb, len(bars)-2), 0, -1):
            b         = bars[k]
            body      = abs(b["close"] - b["open"])
            up_wick   = b["high"]  - max(b["close"], b["open"])
            dn_wick   = min(b["close"], b["open"]) - b["low"]
            near      = abs(b["close"] - lp) < tol

            # Consolidation
            if near:
                zone_bars += 1
                if zone_bars == 3 and not cons_added:
                    score     += 2
                    cons_added = True
            else:
                zone_bars  = 0
                cons_added = False

            # Touch (side switch)
            now_above = b["close"] > lp
            if near and now_above != last_above:
                score += 1
            last_above = now_above

            # Wick rejection
            wick_bull = b["low"]  < lp - tol and b["close"] > lp
            wick_bear = b["high"] > lp + tol and b["close"] < lp
            if (wick_bull or wick_bear) and body > 0 and (up_wick + dn_wick) >= body:
                score += 3

            # Failed breakout
            if k < len(bars) - 1:
                prev_c = bars[k+1]["close"]
                if (prev_c > lp + tol and b["close"] < lp) or \
                   (prev_c < lp - tol and b["close"] > lp):
                    score += 4

        lv["score"] = score

    # Filter and sort by score
    qualified = [lv for lv in levels if lv["score"] >= min_score]
    qualified.sort(key=lambda x: -x["score"])
    return qualified[:10]  # top 10 levels


def _bt_ob_near_key_level(ob_top, ob_bot, ob_type, atr, key_levels, proximity=1.0):
    """Check if OB zone overlaps a key level of the correct type (support for bull, resistance for bear)."""
    if not key_levels:
        return False
    slack = atr * proximity
    # Use midpoint of current bars as proxy for "close" to determine S/R
    mid = (ob_top + ob_bot) / 2
    for lv in key_levels:
        lp     = lv["price"]
        is_res = lp > mid
        dir_ok = (ob_type == 1 and not is_res) or (ob_type == 2 and is_res)
        in_zone = ob_bot - slack <= lp <= ob_top + slack
        if dir_ok and in_zone:
            return True
    return False



    rr_ratio = rr if rr is not None else BT_RR_RATIO
    results  = []
    # Start active from bar AFTER detection (ob["bar"]+1) — same as indicator
    active   = [{**ob, "created": ob["bar"] + 1, "done": False} for ob in obs]

    for i in range(len(bars)):
        b = bars[i]
        for ob in active:
            if ob["done"]: continue
            # Only start checking entry from bar after detection
            if i < ob["created"]: continue
            if i - ob["created"] > cancel_after:
                ob["done"] = True
                continue

            # Calculate TP using rr_ratio
            if ob.get("tp") is None or ob.get("risk"):
                risk = ob.get("risk", abs(ob["entry"] - ob["sl"]))
                ob["tp"] = ob["entry"] + risk * rr_ratio if ob["type"] == 1 else ob["entry"] - risk * rr_ratio

            # Entry: bull needs price to come DOWN to entry (open > entry, low <= entry)
            #        bear needs price to come UP to entry (open < entry, high >= entry)
            if ob["type"] == 1:
                entry_hit = b["open"] > ob["entry"] and b["low"] <= ob["entry"]
            else:
                entry_hit = b["open"] < ob["entry"] and b["high"] >= ob["entry"]

            if entry_hit:
                ob["done"] = True
                # Check resolution from NEXT bar (limit order fills at close of entry bar)
                for j in range(i + 1, min(i + 200, len(bars))):
                    bj = bars[j]
                    tp_hit = bj["high"] >= ob["tp"] if ob["type"] == 1 else bj["low"] <= ob["tp"]
                    sl_hit = bj["low"]  <= ob["sl"] if ob["type"] == 1 else bj["high"] >= ob["sl"]

                    if tp_hit and sl_hit:
                        # Tiebreaker: use open proximity
                        tp_dist = abs(bj["open"] - ob["tp"])
                        sl_dist = abs(bj["open"] - ob["sl"])
                        outcome = "tp" if tp_dist < sl_dist else "sl"
                    elif tp_hit:
                        outcome = "tp"
                    elif sl_hit:
                        outcome = "sl"
                    else:
                        continue

                    results.append({"outcome": outcome, "pnl_r": rr_ratio if outcome == "tp" else -1.0})
                    break
            else:
                # Invalidate if price closes beyond SL side of OB
                sl_breach = (ob["type"] == 1 and b["close"] < ob["bot"]) or \
                            (ob["type"] == 2 and b["close"] > ob["top"])
                if sl_breach:
                    ob["done"] = True

    return results


def _bt_stats(results):
    if not results or len(results) < BT_MIN_TRADES:
        return None
    wins  = [r for r in results if r["outcome"] == "tp"]
    loss  = [r for r in results if r["outcome"] == "sl"]
    total = len(results)
    wr    = round(len(wins)/total*100, 1)
    wp    = [r["pnl_r"] for r in wins]
    lp    = [abs(r["pnl_r"]) for r in loss]
    pnl_r = sum(r["pnl_r"] for r in results)
    gw    = sum(wp); gl = sum(lp)
    pf    = round(gw/gl, 2) if gl > 0 else 0
    ew    = sum(wp)/len(wp) if wp else 0
    el    = sum(lp)/len(lp) if lp else 0
    wr_d  = len(wins)/total
    ev    = round(wr_d * ew - (1-wr_d) * el, 3)
    eq    = 0; pk = 0; dd = 0
    for r in results:
        eq += r["pnl_r"]
        if eq > pk: pk = eq
        if pk - eq > dd: dd = pk - eq
    return {"wins":len(wins),"losses":len(loss),"total":total,"win_rate":wr,
            "total_pnl":round(pnl_r,2),"avg_win":round(ew,2),"avg_loss":round(el,2),
            "profit_factor":pf,"expectancy":ev,"max_dd":round(dd,2)}


def _get_indicator_settings():
    """
    Read the most recent OB trade settings from journal notes/payload.
    Falls back to env var defaults if no trades found.
    """
    try:
        conn = get_db()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT notes FROM trades
                WHERE source LIKE 'ob%' AND notes IS NOT NULL
                ORDER BY opened_at DESC LIMIT 1
            """)
            row = cur.fetchone()
        conn.close()
        if row and row.get("notes"):
            notes = row["notes"]
            # Parse settings from notes if stored as JSON
            if "rr:" in notes:
                import re
                rr  = re.search(r'"rr":([\d.]+)',          notes)
                sl  = re.search(r'"slBuf":([\d.]+)',        notes)
                imp = re.search(r'"minImpulse":([\d.]+)',   notes)
                off = re.search(r'"entryOffset":([\d.]+)',  notes)
                return {
                    "rr":           float(rr.group(1))  if rr  else BT_RR_RATIO,
                    "slBuf":        float(sl.group(1))  if sl  else BT_SL_BUF_ATR,
                    "minImpulse":   float(imp.group(1)) if imp else BT_MIN_IMPULSE,
                    "entryOffset":  float(off.group(1)) if off else BT_ENTRY_OFFSET,
                }
    except Exception as e:
        log.error(f"Settings sync error: {e}")
    # Fall back to env var defaults
    return {
        "rr":          BT_RR_RATIO,
        "slBuf":       BT_SL_BUF_ATR,
        "minImpulse":  BT_MIN_IMPULSE,
        "entryOffset": BT_ENTRY_OFFSET,
    }



    """Create backtest_results table if not exists."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id            SERIAL PRIMARY KEY,
                    symbol        TEXT NOT NULL,
                    timeframe     TEXT NOT NULL,
                    source        TEXT NOT NULL DEFAULT 'ob',
                    wins          INT  NOT NULL DEFAULT 0,
                    losses        INT  NOT NULL DEFAULT 0,
                    total         INT  NOT NULL DEFAULT 0,
                    win_rate      REAL NOT NULL DEFAULT 0,
                    total_pnl     REAL NOT NULL DEFAULT 0,
                    avg_win       REAL NOT NULL DEFAULT 0,
                    avg_loss      REAL NOT NULL DEFAULT 0,
                    profit_factor REAL NOT NULL DEFAULT 0,
                    expectancy    REAL NOT NULL DEFAULT 0,
                    max_dd        REAL NOT NULL DEFAULT 0,
                    settings      TEXT,
                    run_at        TEXT NOT NULL,
                    UNIQUE(symbol, timeframe, source)
                )
            """)
        conn.commit()
        conn.close()
        log.info("backtest_results table ready")
    except Exception as e:
        log.error(f"BT table init error: {e}")


def _bt_save(symbol, timeframe, stats, variant="ob"):
    """Save backtest result. variant: 'ob' = no KL filter, 'ob_kl' = with KL filter."""
    conn = get_db()
    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    sets = json.dumps({"rr": BT_RR_RATIO, "sl_buf": BT_SL_BUF_ATR,
                       "min_impulse": BT_MIN_IMPULSE, "lookback": BT_LOOKBACK_DAYS})
    try:
        with conn.cursor() as cur:
            # Always ensure table exists before inserting
            cur.execute("""
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id            SERIAL PRIMARY KEY,
                    symbol        TEXT NOT NULL,
                    timeframe     TEXT NOT NULL,
                    source        TEXT NOT NULL DEFAULT 'ob',
                    wins          INT  NOT NULL DEFAULT 0,
                    losses        INT  NOT NULL DEFAULT 0,
                    total         INT  NOT NULL DEFAULT 0,
                    win_rate      REAL NOT NULL DEFAULT 0,
                    total_pnl     REAL NOT NULL DEFAULT 0,
                    avg_win       REAL NOT NULL DEFAULT 0,
                    avg_loss      REAL NOT NULL DEFAULT 0,
                    profit_factor REAL NOT NULL DEFAULT 0,
                    expectancy    REAL NOT NULL DEFAULT 0,
                    max_dd        REAL NOT NULL DEFAULT 0,
                    settings      TEXT,
                    run_at        TEXT NOT NULL,
                    UNIQUE(symbol, timeframe, source)
                )
            """)
            cur.execute("""
                INSERT INTO backtest_results
                    (symbol, timeframe, source, wins, losses, total, win_rate,
                     total_pnl, avg_win, avg_loss, profit_factor, expectancy, max_dd, settings, run_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timeframe, source) DO UPDATE SET
                    wins=EXCLUDED.wins, losses=EXCLUDED.losses, total=EXCLUDED.total,
                    win_rate=EXCLUDED.win_rate, total_pnl=EXCLUDED.total_pnl,
                    avg_win=EXCLUDED.avg_win, avg_loss=EXCLUDED.avg_loss,
                    profit_factor=EXCLUDED.profit_factor, expectancy=EXCLUDED.expectancy,
                    max_dd=EXCLUDED.max_dd, settings=EXCLUDED.settings, run_at=EXCLUDED.run_at
            """, (symbol, timeframe, variant,
                  stats["wins"], stats["losses"], stats["total"], stats["win_rate"],
                  stats["total_pnl"], stats["avg_win"], stats["avg_loss"],
                  stats["profit_factor"], stats["expectancy"], stats["max_dd"], sets, now))
        conn.commit()
    except Exception as e:
        log.error(f"BT save error {symbol}/{timeframe}: {e}")
        conn.rollback()
    finally:
        conn.close()


def run_backtest_job():
    global _bt_running, _bt_last_run, _bt_status
    if _bt_running:
        log.info("Backtest already running — skipping")
        return
    _bt_running = True
    _bt_status  = "Running..."
    log.info(f"Backtest started — {len(BT_SYMBOLS)} symbols × {len(BT_TIMEFRAMES)} TFs")

    # Sync settings from latest indicator trade
    synced = _get_indicator_settings()
    rr          = synced["rr"]
    sl_buf      = synced["slBuf"]
    min_impulse = synced["minImpulse"]
    entry_offset= synced["entryOffset"]
    log.info(f"Using settings: RR={rr} SL_buf={sl_buf} impulse={min_impulse} offset={entry_offset}")
    saved = 0
    total_combos = len(BT_SYMBOLS) * len(BT_TIMEFRAMES)
    done  = 0
    try:
        for symbol in BT_SYMBOLS:
            symbol = symbol.strip()
            time.sleep(1.0)  # pause between symbols to avoid rate limits
            for tf_str, tf_label in BT_TIMEFRAMES.items():
                done += 1
                # Adaptive lookback — fewer bars for smaller TFs to reduce API calls
                tf_mins  = {"3":3,"5":5,"15":15,"30":30,"60":60,"240":240}[tf_str]
                lookback = min(BT_LOOKBACK_DAYS, 30) if tf_mins <= 5 else                            min(BT_LOOKBACK_DAYS, 60) if tf_mins <= 15 else BT_LOOKBACK_DAYS
                try:
                    bars = _bt_fetch_klines(symbol, tf_str, lookback)
                    if len(bars) < 100:
                        log.info(f"  [{done}/{total_combos}] {symbol}/{tf_label}: only {len(bars)} bars — skipping")
                        continue
                    atrs    = _bt_calc_atr(bars, BT_ATR_LEN)
                    obs     = _bt_detect_obs(bars, atrs, min_impulse, sl_buf, entry_offset)

                    # ── Variant 1: no key level filter ────────────────────────
                    results = _bt_simulate(bars, obs, rr)
                    stats   = _bt_stats(results)
                    log.info(f"  [{done}/{total_combos}] {symbol}/{tf_label}: {len(bars)} bars → {len(obs)} OBs → {len(results)} trades → {'✅' if stats else '❌'} (no KL)")
                    if stats:
                        _bt_save(symbol, tf_label, stats, variant="ob")
                        saved += 1

                    # ── Variant 2: with key level filter ─────────────────────
                    key_levels = _bt_find_key_levels(bars, lookback=min(lookback, 300))
                    if key_levels:
                        obs_kl  = [ob for ob in obs if _bt_ob_near_key_level(
                                    ob["top"], ob["bot"], ob["type"],
                                    atrs[ob["bar"]] or atrs[-1],
                                    key_levels)]
                        results_kl = _bt_simulate(bars, obs_kl, rr)
                        stats_kl   = _bt_stats(results_kl)
                        log.info(f"  [{done}/{total_combos}] {symbol}/{tf_label}: {len(obs_kl)}/{len(obs)} OBs near KL → {len(results_kl)} trades → {'✅' if stats_kl else '❌'} (with KL)")
                        if stats_kl:
                            _bt_save(symbol, tf_label, stats_kl, variant="ob_kl")
                            saved += 1
                except Exception as e:
                    log.error(f"  {symbol}/{tf_label}: {e}")
                time.sleep(0.5)
    finally:
        _bt_running  = False
        _bt_last_run = datetime.utcnow()
        _bt_status   = f"Completed {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} — {saved} results saved"
        log.info(f"Backtest complete — {saved} results saved")


def _schedule_daily_backtest():
    """Run backtest daily at 02:00 UTC."""
    while True:
        now  = datetime.utcnow()
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait = (next_run - now).total_seconds()
        log.info(f"Next backtest scheduled at {next_run.strftime('%Y-%m-%d %H:%M UTC')} ({int(wait/3600)}h from now)")
        time.sleep(wait)
        try:
            run_backtest_job()
        except Exception as e:
            log.error(f"Scheduled backtest error: {e}")


def _start_backtest_scheduler():
    t = threading.Thread(target=_schedule_daily_backtest, daemon=True)
    t.start()
    log.info("Backtest scheduler started — runs daily at 02:00 UTC")


    """Fetch backtest results from DB, sorted by win_rate desc."""
    try:
        conn = get_db()
        if DATABASE_URL:
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT symbol, timeframe, source, wins, losses, total,
                           win_rate, total_pnl, avg_win, avg_loss,
                           profit_factor, expectancy, max_dd, run_at
                    FROM backtest_results
                    ORDER BY win_rate DESC, total DESC
                """)
                rows = [dict(r) for r in cur.fetchall()]
        else:
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT symbol, timeframe, source, wins, losses, total,
                           win_rate, total_pnl, avg_win, avg_loss,
                           profit_factor, expectancy, max_dd, run_at
                    FROM backtest_results
                    ORDER BY win_rate DESC, total DESC
                """)
                rows = [dict(r) for r in cur.fetchall()]
            except Exception:
                rows = []
        conn.close()
        return rows
    except Exception as e:
        log.error(f"Backtest results fetch error: {e}")
        return []


def _extract_kl_level(notes):
    """Extract klLevel from trade notes JSON."""
    if not notes:
        return None
    try:
        import json as _j
        # notes can be JSON string or pipe-separated
        notes_str = str(notes).split("|")[0].strip()
        d = _j.loads(notes_str)
        kl = d.get("klLevel")
        return float(kl) if kl and kl != "null" else None
    except:
        return None
    """Fetch backtest results from DB sorted by win_rate desc."""
    try:
        conn = get_db()
        import psycopg2.extras
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id SERIAL PRIMARY KEY, symbol TEXT, timeframe TEXT,
                    source TEXT DEFAULT 'ob', wins INT DEFAULT 0, losses INT DEFAULT 0,
                    total INT DEFAULT 0, win_rate REAL DEFAULT 0, total_pnl REAL DEFAULT 0,
                    avg_win REAL DEFAULT 0, avg_loss REAL DEFAULT 0,
                    profit_factor REAL DEFAULT 0, expectancy REAL DEFAULT 0,
                    max_dd REAL DEFAULT 0, settings TEXT, run_at TEXT,
                    UNIQUE(symbol, timeframe, source)
                )
            """)
            conn.commit()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT symbol, timeframe, source, wins, losses, total,
                       win_rate, total_pnl, avg_win, avg_loss,
                       profit_factor, expectancy, max_dd, run_at
                FROM backtest_results
                ORDER BY win_rate DESC, total DESC
            """)
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        log.info(f"Backtest results fetched: {len(rows)} rows")
        return rows
    except Exception as e:
        log.error(f"Backtest results fetch error: {e}")
        import traceback
        log.error(traceback.format_exc())
        return []


@app.route("/backtest/results")
def backtest_results_debug():
    """Debug endpoint — raw backtest results."""
    try:
        rows = _get_backtest_results()
        return jsonify({"count": len(rows), "rows": rows[:10]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/backtest/run", methods=["POST"])
def trigger_backtest():
    """Manually trigger a backtest run."""
    if _bt_running:
        return jsonify({"status": "already_running", "message": "Backtest already in progress"}), 200
    threading.Thread(target=run_backtest_job, daemon=True).start()
    return jsonify({"status": "started", "message": f"Backtest started for {len(BT_SYMBOLS)} symbols × {len(BT_TIMEFRAMES)} timeframes"}), 200


@app.route("/backtest/status")
def backtest_status():
    return jsonify({
        "running":  _bt_running,
        "last_run": _bt_last_run.strftime("%Y-%m-%d %H:%M UTC") if _bt_last_run else None,
        "status":   _bt_status,
    })



    """Daily alert recommendation page."""
    try:
        trades = get_all_trades(500)
        data   = _build_recommendations(trades)
        bt_rows = _get_backtest_results()

        # Build OB vs OB+KL comparison
        ob_results  = {(r["symbol"], r["timeframe"]): r for r in bt_rows if r["source"] == "ob"}
        kl_results  = {(r["symbol"], r["timeframe"]): r for r in bt_rows if r["source"] == "ob_kl"}
        bt_comparison = []
        for key, ob in sorted(ob_results.items(), key=lambda x: -x[1]["win_rate"]):
            kl    = kl_results.get(key)
            delta = round(kl["win_rate"] - ob["win_rate"], 1) if kl else None
            bt_comparison.append({
                "symbol":   key[0],
                "tf":       key[1],
                "ob_wr":    ob["win_rate"],
                "ob_total": ob["total"],
                "kl_wr":    kl["win_rate"] if kl else None,
                "kl_total": kl["total"]    if kl else None,
                "delta":    delta,
            })

        data["bt_rows"]       = bt_rows
        data["bt_comparison"] = bt_comparison
        data["bt_available"]  = len(bt_rows) > 0
        data["bt_updated"]    = bt_rows[0]["run_at"] if bt_rows else None
        data["bt_status"]     = _bt_status
        data["bt_analysis"]   = _build_bt_recommendations(bt_rows)
        return render_template_string(RECOMMENDATIONS_HTML, **data)
    except Exception as e:
        import traceback
        log.error(f"Recommendations error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


WATCHLIST_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Watchlist Sync</title>
<style>
  :root { --bg:#0f0f0f;--surface:#1a1a1a;--border:#2a2a2a;--text:#e8e8e8;--dim:#888;--green:#4caf50;--red:#ef5350;--blue:#42a5f5;--amber:#ffa726;--purple:#ce93d8; }
  * { box-sizing:border-box;margin:0;padding:0; }
  body { background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:13px;padding:24px; }
  h1 { font-size:18px;font-weight:500;margin-bottom:4px; }
  .subtitle { color:var(--dim);font-size:12px;margin-bottom:24px; }
  .nav { display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap; }
  .nav a { color:var(--dim);text-decoration:none;font-size:12px; }
  .nav a:hover { color:var(--text); }
  .section { background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px; }
  .section-title { font-size:13px;font-weight:500;margin-bottom:12px;color:var(--dim);text-transform:uppercase;letter-spacing:0.05em; }
  .grid { display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:8px; }
  .asset-card { background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px;display:flex;align-items:center;gap:10px; }
  .asset-card.has-data { border-color:rgba(76,175,80,0.3); }
  .asset-card.no-data  { border-color:rgba(239,83,80,0.2);opacity:0.7; }
  .dot { width:8px;height:8px;border-radius:50%;flex-shrink:0; }
  .dot-green  { background:var(--green); }
  .dot-red    { background:var(--red); }
  .dot-amber  { background:var(--amber); }
  .asset-name { font-weight:500;font-size:13px;flex:1; }
  .asset-wr   { font-size:12px;font-weight:500; }
  .asset-meta { font-size:10px;color:var(--dim); }
  .tag { display:inline-block;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:500; }
  .tag-green  { background:rgba(76,175,80,0.15);color:var(--green); }
  .tag-red    { background:rgba(239,83,80,0.15);color:var(--red); }
  .tag-amber  { background:rgba(255,167,38,0.15);color:var(--amber); }
  .tag-blue   { background:rgba(66,165,245,0.15);color:var(--blue); }
  .legend { display:flex;gap:16px;margin-bottom:12px;font-size:11px;color:var(--dim); }
  .legend-item { display:flex;align-items:center;gap:5px; }
  .stats-row { display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px; }
  .stat { background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 14px; }
  .stat-label { font-size:11px;color:var(--dim);margin-bottom:2px; }
  .stat-value { font-size:18px;font-weight:500; }
  .copy-box { background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px 12px;font-family:monospace;font-size:11px;color:var(--dim);word-break:break-all;margin-top:10px; }
  button.copy-btn { background:rgba(66,165,245,0.15);color:var(--blue);border:1px solid rgba(66,165,245,0.3);border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer;margin-top:6px; }
</style>
</head>
<body>
<div class="nav">
  <a href="/journal">← Journal</a>
  <a href="/analysis">📊 Analysis</a>
  <a href="/recommendations">🎯 Recommendations</a>
  <a href="/watchlist">🔄 Refresh</a>
</div>
<h1>// Watchlist Sync</h1>
<p class="subtitle">{{ total_symbols }} symbols in backtest · {{ has_data }} have results · Last backtest: {{ bt_updated or 'Never' }}</p>

<div class="stats-row">
  <div class="stat"><div class="stat-label">Total symbols</div><div class="stat-value">{{ total_symbols }}</div></div>
  <div class="stat"><div class="stat-label">Has backtest data</div><div class="stat-value" style="color:var(--green)">{{ has_data }}</div></div>
  <div class="stat"><div class="stat-label">Recommended (WR≥45%)</div><div class="stat-value" style="color:var(--green)">{{ recommended }}</div></div>
  <div class="stat"><div class="stat-label">Avoid (WR<35%)</div><div class="stat-value" style="color:var(--red)">{{ avoid }}</div></div>
</div>

<div class="section">
  <div class="section-title">Asset Status — best timeframe per symbol</div>
  <div class="legend">
    <div class="legend-item"><div class="dot dot-green"></div>WR ≥ 45% — run alerts</div>
    <div class="legend-item"><div class="dot dot-amber"></div>WR 35–44% — use with caution</div>
    <div class="legend-item"><div class="dot dot-red"></div>WR < 35% — avoid or pause</div>
  </div>
  <div class="grid">
    {% for a in assets %}
    <div class="asset-card {{ 'has-data' if a.has_data else 'no-data' }}">
      <div class="dot {{ 'dot-green' if a.wr >= 45 else 'dot-amber' if a.wr >= 35 else 'dot-red' }}"></div>
      <div style="flex:1">
        <div class="asset-name">{{ a.symbol.replace('USDT','') }}<span style="color:var(--dim);font-size:10px">USDT.P</span></div>
        {% if a.has_data %}
        <div class="asset-meta" style="margin-top:3px;display:flex;flex-wrap:wrap;gap:3px">
          {% for t in a.all_tfs %}
          <span style="display:inline-flex;align-items:center;gap:2px;background:{{ 'rgba(76,175,80,0.12)' if t.wr >= 45 else 'rgba(255,167,38,0.12)' if t.wr >= 35 else 'rgba(239,83,80,0.1)' }};border:1px solid {{ 'rgba(76,175,80,0.3)' if t.wr >= 45 else 'rgba(255,167,38,0.3)' if t.wr >= 35 else 'rgba(239,83,80,0.2)' }};border-radius:3px;padding:1px 5px;font-size:10px">
            {{ t.tf }}
            <span style="color:{{ 'var(--green)' if t.wr >= 45 else 'var(--amber)' if t.wr >= 35 else 'var(--red)' }}">{{ t.wr }}%</span>
          </span>
          {% endfor %}
        </div>
        {% else %}
        <div class="asset-meta" style="color:var(--red)">No backtest data</div>
        {% endif %}
      </div>
    </div>
    {% endfor %}
  </div>
</div>

<div class="section">
  <div class="section-title">✅ Suggested alert setup — by timeframe</div>
  {% for tf, syms in by_tf_recommended.items() %}
  {% if syms %}
  <div style="margin-bottom:14px">
    <div style="font-size:12px;font-weight:500;margin-bottom:6px">
      <span class="tag tag-blue">{{ tf }}</span>
      <span style="color:var(--dim);font-size:11px;margin-left:6px">{{ syms|length }} asset{{ 's' if syms|length != 1 else '' }}</span>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:6px">
      {% for s in syms %}
      <span style="background:rgba(76,175,80,0.1);border:1px solid rgba(76,175,80,0.3);border-radius:4px;padding:3px 8px;font-size:12px">
        {{ s.symbol.replace('USDT','') }}
        <span style="color:var(--green);font-size:10px">{{ s.wr }}%</span>
      </span>
      {% endfor %}
    </div>
  </div>
  {% endif %}
  {% endfor %}
</div>

<div class="section">
  <div class="section-title">Railway BT_SYMBOLS env var</div>
  <p style="font-size:11px;color:var(--dim);margin-bottom:8px">Copy this into your Railway BT_SYMBOLS environment variable to keep backtest in sync:</p>
  <div class="copy-box" id="bt-symbols">{{ bt_symbols_str }}</div>
  <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('bt-symbols').innerText);this.innerText='✅ Copied';setTimeout(()=>this.innerText='📋 Copy',2000)">📋 Copy</button>
</div>

</body>
</html>
"""


@app.route("/watchlist")
def watchlist_sync():
    """Watchlist sync page — shows backtest status per symbol."""
    try:
        bt_rows    = _get_backtest_results()
        bt_updated = bt_rows[0]["run_at"] if bt_rows else None

        # All TFs per symbol
        sym_tfs = {}
        for r in bt_rows:
            sym = r["symbol"]
            if sym not in sym_tfs:
                sym_tfs[sym] = []
            sym_tfs[sym].append({
                "tf":    r["timeframe"],
                "wr":    r["win_rate"],
                "total": r["total"],
                "pf":    r.get("profit_factor", 0) or 0,
            })
        # Sort each symbol's TFs by WR desc
        tf_order_list = ["M3","M5","M15","M30","H1","H4"]
        for sym in sym_tfs:
            sym_tfs[sym].sort(key=lambda x: -x["wr"])

        # Build asset cards — best WR across all TFs
        assets = []
        for sym in BT_SYMBOLS:
            sym = sym.strip()
            tfs = sym_tfs.get(sym, [])
            good_tfs = [t for t in tfs if t["wr"] >= 35]  # show amber+ TFs
            best_wr  = tfs[0]["wr"] if tfs else 0
            assets.append({
                "symbol":   sym,
                "has_data": len(tfs) > 0,
                "wr":       best_wr,
                "best_tf":  tfs[0]["tf"] if tfs else "—",
                "total":    sum(t["total"] for t in tfs),
                "all_tfs":  tfs,
                "good_tfs": good_tfs,
            })

        # Sort: recommended first, then caution, then avoid
        assets.sort(key=lambda x: (-x["wr"]))

        # Group recommended assets by TF — include all good TFs per asset
        tf_order = ["M3","M5","M15","M30","H1","H4"]
        by_tf_recommended = {tf: [] for tf in tf_order}
        for a in assets:
            if not a["has_data"]:
                continue
            for t in a["good_tfs"]:
                if t["tf"] in by_tf_recommended and t["wr"] >= 45:
                    by_tf_recommended[t["tf"]].append({
                        "symbol": a["symbol"],
                        "wr":     t["wr"],
                        "total":  t["total"],
                    })

        recommended = sum(1 for a in assets if a["wr"] >= 45)
        avoid       = sum(1 for a in assets if a["has_data"] and a["wr"] < 35)

        return render_template_string(WATCHLIST_HTML,
            assets=assets,
            total_symbols=len(assets),
            has_data=sum(1 for a in assets if a["has_data"]),
            recommended=recommended,
            avoid=avoid,
            by_tf_recommended=by_tf_recommended,
            bt_updated=bt_updated,
            bt_symbols_str=",".join(a["symbol"] for a in assets),
        )
    except Exception as e:
        import traceback
        log.error(f"Watchlist error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500



    """Daily alert recommendation page."""
    try:
        trades  = get_all_trades(500)
        data    = _build_recommendations(trades)
        bt_rows = _get_backtest_results()
        data["bt_rows"]      = bt_rows
        data["bt_available"] = len(bt_rows) > 0
        data["bt_updated"]   = bt_rows[0]["run_at"] if bt_rows else None
        data["bt_status"]    = _bt_status
        data["bt_analysis"]  = _build_bt_recommendations(bt_rows)
        return render_template_string(RECOMMENDATIONS_HTML, **data)
    except Exception as e:
        import traceback
        log.error(f"Recommendations error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/recommendations")
def recommendations():
    """Daily alert recommendation page."""
    try:
        trades  = get_all_trades(500)
        data    = _build_recommendations(trades)
        bt_rows = _get_backtest_results()
        data["bt_rows"]      = bt_rows
        data["bt_available"] = len(bt_rows) > 0
        data["bt_updated"]   = bt_rows[0]["run_at"] if bt_rows else None
        data["bt_status"]    = _bt_status
        data["bt_analysis"]  = _build_bt_recommendations(bt_rows)
        return render_template_string(RECOMMENDATIONS_HTML, **data)
    except Exception as e:
        import traceback
        log.error(f"Recommendations error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/recommendations/data")
def recommendations_data():
    """JSON recommendations endpoint."""
    try:
        trades = get_all_trades(500)
        return jsonify(_build_recommendations(trades))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



    """Trade analysis dashboard."""
    try:
        trades = get_all_trades(500)
        data   = _analyse_trades(trades)
        return render_template_string(ANALYSIS_HTML, **data)
    except Exception as e:
        import traceback
        log.error(f"Analysis error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


@app.route("/analysis")
def analysis():
    """Trade analysis dashboard."""
    try:
        trades = get_all_trades(500)
        data   = _analyse_trades(trades)
        return render_template_string(ANALYSIS_HTML, **data)
    except Exception as e:
        import traceback
        log.error(f"Analysis error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500


@app.route("/analysis/data")
def analysis_data():
    """JSON endpoint for analysis data."""
    try:
        trades = get_all_trades(500)
        return jsonify(_analyse_trades(trades))
    except Exception as e:
        import traceback
        log.error(f"Analysis data error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e), "trace": traceback.format_exc()}), 500



    cfg = get_config()
    log.info(f"Starting webhook server — testnet={TESTNET}")
    log.info(f"Config: {cfg['balance_pct']}% per trade, max {cfg['max_trades']} trades, {cfg['leverage']}x leverage")
    # Delay startup tasks slightly to let gunicorn finish booting
    def _delayed_startup():
        time.sleep(3)
        try:
            _bt_init_table()
        except Exception as e:
            log.error(f"BT init error: {e}")
        try:
            _start_backtest_scheduler()
        except Exception as e:
            log.error(f"Backtest scheduler error: {e}")

    _start_poller()
    threading.Thread(target=_delayed_startup, daemon=True).start()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
