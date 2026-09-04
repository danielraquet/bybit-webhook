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
import uuid
import logging
from collections import deque
import threading
import time
import json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
from pybit.unified_trading import HTTP, WebSocket
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
        "journal_only":         os.environ.get("JOURNAL_ONLY", "false").lower() == "true",
        "balance_pct":          _float("BALANCE_PCT",   1.0),
        "max_trades":           _int("MAX_TRADES",       3),
        "leverage":             _int("LEVERAGE",         5),
        "poll_interval":        _int("POLL_INTERVAL",    600),
        "filter_side":          _str("FILTER_SIDE").lower(),
        "filter_min_wr":        _float("FILTER_MIN_WR",  0),
        "filter_sources":       _str("FILTER_SOURCES").lower(),
        "filter_timeframes":    _str("FILTER_TIMEFRAMES").upper(),
        "filter_symbols_allow": _str("FILTER_SYMBOLS_ALLOW").upper(),
        "filter_symbols_block": _str("FILTER_SYMBOLS_BLOCK").upper(),
        "cooldown_losses":      _int("COOLDOWN_LOSSES",    0),   # consecutive losses to trigger cooldown (0=off)
        "cooldown_hours":       _float("COOLDOWN_HOURS",   48.0), # hours to block that side after trigger
    }

app = Flask(__name__)

# ─── TRADE LOCK ───────────────────────────────────────────────────────────────
trade_lock = threading.Lock()

# ─── JOURNAL DASHBOARD HTML ───────────────────────────────────────────────────
JOURNAL_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trade Journal</title>
<style>
:root{--bg:#0f1117;--card:#1a1d27;--border:#2a2d3a;--text:#e2e8f0;--dim:#6b7280;--blue:#60a5fa;--green:#4ade80;--red:#f87171;--amber:#fbbf24}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,sans-serif;font-size:13px;padding:16px}
h1{font-size:18px;margin-bottom:12px}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px;align-items:center}
.btn{background:var(--card);border:1px solid var(--border);color:var(--text);padding:6px 12px;border-radius:6px;cursor:pointer;font-size:12px}
.btn:hover{border-color:var(--blue)}
.days-filter{display:flex;gap:4px;margin-left:8px}
.days-filter a{padding:4px 10px;border-radius:4px;border:1px solid var(--border);color:var(--dim);text-decoration:none;font-size:11px}
.days-filter a.active{border-color:var(--blue);color:var(--blue)}
.stats{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:16px}
.stat{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:10px 16px;min-width:100px}
.stat-label{color:var(--dim);font-size:11px;margin-bottom:4px}
.stat-value{font-size:18px;font-weight:600}
.green{color:var(--green)}.red{color:var(--red)}.amber{color:var(--amber)}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:var(--card);padding:8px 10px;text-align:left;color:var(--dim);font-weight:500;border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid var(--border);vertical-align:middle}
tr:hover td{background:rgba(255,255,255,0.02)}
.badge{padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}
.badge-tp{background:rgba(74,222,128,0.15);color:var(--green)}
.badge-sl{background:rgba(248,113,113,0.15);color:var(--red)}
.badge-open{background:rgba(96,165,250,0.15);color:var(--blue)}
.badge-skipped{background:rgba(107,114,128,0.15);color:var(--dim)}
.img-preview{position:fixed;z-index:9999;pointer-events:none;display:none;border:1px solid var(--border);border-radius:6px;overflow:hidden;box-shadow:0 8px 32px rgba(0,0,0,0.6);max-width:720px;max-height:480px;background:var(--card)}
.img-preview img{width:100%;height:auto;display:block;max-height:480px;object-fit:contain}
.pnl-pos{color:var(--green)}.pnl-neg{color:var(--red)}
.editable{cursor:pointer;border-bottom:1px dashed rgba(107,114,128,0.5)}
.editable:hover{border-bottom-color:var(--blue);color:var(--blue)}
.note-row{background:rgba(96,165,250,0.05);border-left:3px solid var(--blue);font-style:italic;color:var(--blue)}
</style>
</head>
<body>
<h1>Trade Journal</h1>
<div id="img-preview" class="img-preview"><img id="img-preview-img" src="" alt="preview"></div>
<div style="display:flex;gap:12px;margin-bottom:12px;font-size:12px">
  <a href="/journal" style="color:var(--blue);text-decoration:none">Journal</a>
  <a href="/analysis" style="color:var(--dim);text-decoration:none">Analysis</a>
  <a href="/recommendations" style="color:var(--dim);text-decoration:none">Recommendations</a>
  <a href="/watchlist" style="color:var(--dim);text-decoration:none">Watchlist</a>
  <a href="/backtest/configs" style="color:var(--dim);text-decoration:none">Backtest</a>
  <a href="/signal-explainer" style="color:var(--dim);text-decoration:none">Explainer</a>
</div>
<div class="toolbar">
  <button class="btn" id="btn-poll">Poll Now</button>
  <button class="btn" id="btn-note">Add Note</button>
  <button class="btn" id="btn-sheets">📊 Sync Sheets</button>
  <button class="btn" id="btn-del-old">Delete Older Than</button>
  <button class="btn" id="btn-reset" style="color:var(--red)">Reset All</button>
  <span style="margin-left:8px;color:var(--dim);font-size:11px">Show:</span>
  <button class="btn filter-status active" data-status="all">All</button>
  <button class="btn filter-status" data-status="closed">Closed</button>
  <button class="btn filter-status" data-status="open">Open</button>
  <span style="margin-left:8px;color:var(--dim);font-size:11px">Outcome:</span>
  <button class="btn filter-outcome" data-outcome="all" style="border-color:var(--blue);color:var(--blue)">All</button>
  <button class="btn filter-outcome" data-outcome="tp">TP</button>
  <button class="btn filter-outcome" data-outcome="sl">SL</button>
  <label style="margin-left:8px;font-size:11px;color:var(--dim);cursor:pointer">
    <input type="checkbox" id="show-skipped" style="margin-right:4px">Skipped
  </label>
  <div class="days-filter">
    <span id="day-links"></span>
  </div>
</div>
<div id="restricted-banner" style="display:none;margin:8px 0;padding:10px 14px;border-radius:6px;font-size:12px;background:rgba(239,83,80,0.15);border:1px solid rgba(239,83,80,0.4);color:#ef5350">
  🚫 <strong>Trading restricted</strong> — <span id="restricted-reason"></span>
</div>
<div id="restricted-info" style="display:none;margin:8px 0;padding:8px 14px;border-radius:6px;font-size:11px;background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);color:var(--dim)">
  Restricted windows: <span id="restricted-windows"></span>
</div>
<div class="stats">
  <div class="stat"><div class="stat-label">Total</div><div class="stat-value" id="s-total">—</div></div>
  <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value" id="s-wr">—</div></div>
  <div class="stat"><div class="stat-label">Wins</div><div class="stat-value green" id="s-wins">—</div></div>
  <div class="stat"><div class="stat-label">Losses</div><div class="stat-value red" id="s-losses">—</div></div>
  <div class="stat"><div class="stat-label">Total PnL</div><div class="stat-value" id="s-pnl">—</div></div>
  <div class="stat"><div class="stat-label">Open</div><div class="stat-value amber" id="s-open">—</div></div>
</div>

<div style="margin:10px 0;padding:10px 14px;background:var(--panel);border:0.5px solid var(--border);border-radius:6px;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
  <span style="font-size:11px;color:var(--dim);font-weight:500">RANGE STATS</span>
  <div style="display:flex;align-items:center;gap:6px">
    <span style="font-size:11px;color:var(--dim)">Trades #</span>
    <input type="number" id="range-from" placeholder="from" min="1" style="width:70px;font-size:11px">
    <span style="font-size:11px;color:var(--dim)">to</span>
    <input type="number" id="range-to" placeholder="to" min="1" style="width:70px;font-size:11px">
    <span id="range-hint" style="font-size:10px;color:var(--dim);opacity:0.6"></span>
  </div>
  <div style="display:flex;align-items:center;gap:6px">
    <span style="font-size:11px;color:var(--dim)">or dates</span>
    <input type="date" id="range-date-from" style="font-size:11px">
    <span style="font-size:11px;color:var(--dim)">to</span>
    <input type="date" id="range-date-to" style="font-size:11px">
  </div>
  <div style="display:flex;align-items:center;gap:6px">
    <span style="font-size:11px;color:var(--dim)">Variant</span>
    <select id="range-variant" style="font-size:11px;min-width:90px">
      <option value="">All</option>
    </select>
  </div>
  <button onclick="calcRange()" style="font-size:11px;padding:4px 12px">Calculate</button>
  <button onclick="clearRange()" style="font-size:11px;padding:4px 8px;opacity:0.5">Clear</button>
  <span id="range-result" style="font-size:12px;margin-left:4px"></span>
</div>
<table>
<thead><tr id="thead-row"></tr></thead>
<tbody id="tbody"></tbody>
</table>
<script>
var DAYS_PARAM   = parseInt(new URLSearchParams(location.search).get('days')) || 0;
var STATUS_FILTER  = 'all';
var OUTCOME_FILTER = 'all';
var SHOW_SKIPPED   = false;

// Render day filter links
(function(){
  var links = [{d:7,l:'7d'},{d:14,l:'14d'},{d:30,l:'30d'},{d:90,l:'90d'},{d:0,l:'All'}];
  document.getElementById('day-links').innerHTML = links.map(function(x){
    var active = (DAYS_PARAM === x.d) ? 'active' : '';
    var href = x.d ? '/journal?days='+x.d : '/journal';
    return '<a href="'+href+'" class="'+active+'">'+x.l+'</a>';
  }).join('');
})();

// Render table headers
document.getElementById('thead-row').innerHTML = [
  '#','Symbol','Side','TF','Status','Qty','Entry','Exit','SL','TP',
  'PnL','PnL%','R','Outcome','Source','Variant','OB Size','Impulse','Struct','KL','KL Dist','EMA','Opened','Closed','Notes','My Notes','Links'
].map(function(h){ return '<th>'+h+'</th>'; }).join('');

// Helpers
function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function badge(cls,txt){ return '<span class="badge badge-'+cls+'">'+txt+'</span>'; }
function pnlClass(v){ return v>0?'pnl-pos':v<0?'pnl-neg':''; }

// Load trades via JSON API
// Convert UTC timestamp string to local time for display
function utcToLocal(utcStr) {
  if (!utcStr) return '—';
  // Parse as UTC (append Z if not present)
  var s = utcStr.slice(0,16).replace(' ','T') + ':00Z';
  var d = new Date(s);
  if (isNaN(d)) return utcStr.slice(0,16);
  var pad = function(n){ return String(n).padStart(2,'0'); };
  return d.getFullYear() + '-' + pad(d.getMonth()+1) + '-' + pad(d.getDate())
       + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function tfToTVInterval(tf) {
  var map = {
    '1m':'1','3m':'3','5m':'5','15m':'15','30m':'30',
    '1h':'60','2h':'120','4h':'240','1D':'D','1W':'W',
    'M3':'3','M5':'5','M15':'15','M30':'30',
    'H1':'60','H2':'120','H4':'240','H12':'720','D1':'D'
  };
  return map[tf] || '';
}

function tvTimestamp(utcStr) {
  if (!utcStr) return '';
  var s = utcStr.slice(0,16).replace(' ','T') + ':00Z';
  var d = new Date(s);
  return isNaN(d) ? '' : Math.floor(d.getTime() / 1000);
}

function showNotes(el) {
  var full = el.dataset.full;
  if (!full || full === 'undefined') { alert('No notes'); return; }
  // Try to pretty-print JSON
  try {
    var jsonPart = full.split('|sheet_row:')[0].trim();
    var obj = JSON.parse(jsonPart);
    alert(JSON.stringify(obj, null, 2));
  } catch(e) {
    alert(full);
  }
}

function loadRestrictedStatus(){
  fetch('/journal/restricted-times').then(function(r){ return r.json(); }).then(function(d){
    var banner = document.getElementById('restricted-banner');
    var info   = document.getElementById('restricted-info');
    var reason = document.getElementById('restricted-reason');
    var wins   = document.getElementById('restricted-windows');
    if(d.is_restricted){
      banner.style.display = 'block';
      reason.textContent   = d.reason || '';
    } else {
      banner.style.display = 'none';
    }
    if(d.windows && d.windows.length){
      info.style.display = 'block';
      wins.textContent   = d.windows.join(' | ') + ' (UTC+' + d.timezone_offset + ')';
    }
  }).catch(function(){});
}

function loadTrades(){
  var params = [];
  if(DAYS_PARAM) params.push('days='+DAYS_PARAM);
  if(STATUS_FILTER !== 'all') params.push('status='+STATUS_FILTER);
  var url = '/journal/data' + (params.length ? '?'+params.join('&') : '');
  fetch(url).then(function(r){return r.json();}).then(function(d){
    _allTrades = d.trades || [];
    renderTrades(_allTrades);
    updateStats(_allTrades);
    // Update range hint
    var closedCount = _allTrades.filter(function(t){ return t.status==='closed' && t.outcome; }).length;
    var hint = document.getElementById('range-hint');
    if(hint) hint.textContent = closedCount ? '1 – '+closedCount : '';
    // Populate variant dropdown
    var variants = {};
    _allTrades.forEach(function(t){ if(t.variant) variants[t.variant] = true; });
    var sel = document.getElementById('range-variant');
    var cur = sel.value;
    sel.innerHTML = '<option value="">All</option>';
    Object.keys(variants).sort().forEach(function(v){
      var o = document.createElement('option');
      o.value = v; o.textContent = v;
      if(v === cur) o.selected = true;
      sel.appendChild(o);
    });
  }).catch(function(e){ console.error('Load failed',e); });
}

function renderTrades(trades){
  var tbody = document.getElementById('tbody');
  var rows = '';
  var visible = trades.filter(function(t){
    if(t.status === 'skipped' && !SHOW_SKIPPED) return false;
    if(OUTCOME_FILTER !== 'all' && t.status !== 'note' && t.outcome !== OUTCOME_FILTER) return false;
    return true;
  });
  for(var i=0; i<visible.length; i++){
    var t = visible[i];
    var num = visible.length - i;
    if(t.status === 'note'){
      rows += '<tr class="note-row">'
        + '<td colspan="27"> ' + num + '  ' + utcToLocal(t.opened_at||'') + ' — '
        + '<span class="note-row-text" data-id="'+t.id+'" style="cursor:pointer;border-bottom:1px dashed rgba(96,165,250,0.4)">' + esc(t.notes||'') + '</span>'
        + '<span class="note-row-del" data-id="'+t.id+'" style="margin-left:10px;color:var(--red);cursor:pointer;font-size:11px;opacity:0.5" title="Delete note">✕</span>'
        + '</td></tr>';
      continue;
    }
    var pnl = parseFloat(t.pnl)||0;
    var pnlStr = pnl ? (pnl>0?'+':'')+pnl.toFixed(4) : '—';
    var pnlPct = parseFloat(t.pnl_pct)||0;
    var pnlPctStr = pnlPct ? (pnlPct>0?'+':'')+pnlPct.toFixed(2)+'%' : '—';
    // R multiple
    var rStr = '—'; var rCol = '';
    if(t.exit_price && t.entry && t.sl){
      var risk = Math.abs(parseFloat(t.entry) - parseFloat(t.sl));
      if(risk > 0){
        var rVal = t.side==='Buy' ? (parseFloat(t.exit_price)-parseFloat(t.entry))/risk : (parseFloat(t.entry)-parseFloat(t.exit_price))/risk;
        rCol = rVal>=0 ? 'color:var(--green)' : 'color:var(--red)';
        rStr = (rVal>=0?'+':'')+rVal.toFixed(2)+'R';
      }
    }
    // Condition fields from notes JSON
    var notesObj = {};
    try{ notesObj = JSON.parse(t.notes||'{}'); }catch(e){}
    var obSizeAtr      = notesObj.obSizeAtr;
    var impulseActual  = notesObj.impulseRatioActual;
    var structureOk    = notesObj.structureOk;
    var klNear         = notesObj.klNear;
    var klDistAtr      = notesObj.klDistAtr;
    var emaOk          = notesObj.emaOk;
    var obSizeStr   = (obSizeAtr    != null && !isNaN(obSizeAtr))    ? parseFloat(obSizeAtr).toFixed(2)+'x'    : '—';
    var impulseStr  = (impulseActual!= null && !isNaN(impulseActual))? parseFloat(impulseActual).toFixed(2)+'x': '—';
    var klDistStr   = (klDistAtr    != null && !isNaN(klDistAtr))    ? parseFloat(klDistAtr).toFixed(2)+'x'    : '—';
    var structHtml  = structureOk===true ? '<span style="color:var(--green)">✓</span>' : structureOk===false ? '<span style="color:var(--red)">✗</span>' : '—';
    var klHtml      = klNear===true      ? '<span style="color:var(--green)">✓</span>' : klNear===false      ? '<span style="color:var(--red)">✗</span>' : '—';
    var emaHtml     = emaOk===true       ? '<span style="color:var(--green)">✓</span>' : emaOk===false       ? '<span style="color:var(--red)">✗</span>' : '—';
    var outcomeHtml = t.outcome ? badge(t.outcome, t.outcome.toUpperCase()) : '—';
    var notesFull  = (t.notes||'').split('|sheet_row:')[0];
    var notesShort = esc(notesFull.slice(0,35)) + (notesFull.length > 35 ? '…' : '');
    var mediaHtml = '';
    if(t.media){ t.media.split('|').forEach(function(l){ if(l.trim()) mediaHtml += '<a href="'+esc(l.trim())+'" target="_blank" data-preview="'+esc(l.trim())+'" style="color:var(--blue);display:block;font-size:11px">link</a>'; }); }
    mediaHtml += '<span class="editable" data-id="'+t.id+'" data-type="media" style="font-size:11px;color:var(--dim)">'+(t.media?'edit':'+')+' </span>';

    rows += '<tr>'
      + '<td class="dim">'+num+'</td>'
      + '<td><strong>' + (function(){
          var url = 'https://www.tradingview.com/chart/?symbol=BYBIT:' + esc(t.symbol) + '.P';
          var iv = tfToTVInterval(t.timeframe||'');
          if (iv) url += '&interval=' + iv;
          var ts = tvTimestamp(t.opened_at||'');
          if (ts) url += '&time=' + ts;
          return '<a href="'+url+'" target="_blank" style="color:var(--text);text-decoration:none;border-bottom:1px dashed rgba(96,165,250,0.5)" title="Open in TradingView at entry time">'+esc(t.symbol)+'</a>';
        })() + '</strong></td>'
      + '<td>'+esc(t.side||'—')+'</td>'
      + '<td class="dim editable" data-id="'+t.id+'" data-type="timeframe" data-val="'+esc(t.timeframe||'')+'">'+esc(t.timeframe||'—')+'</td>'
      + '<td>'+badge(t.status||'', t.status||'—')+'</td>'
      + '<td class="dim">'+esc(t.qty||'—')+'</td>'
      + '<td>'+esc(t.entry||'—')+'</td>'
      + '<td>'+esc(t.exit_price||'—')+'</td>'
      + '<td style="color:var(--red)">'+esc(t.sl||'—')+'</td>'
      + '<td style="color:var(--green)">'+esc(t.tp||'—')+'</td>'
      + '<td class="'+pnlClass(pnl)+' editable" data-id="'+t.id+'" data-type="pnl" data-val="'+(t.pnl||'')+'">'+pnlStr+'</td>'
      + '<td class="'+pnlClass(pnlPct)+'">'+pnlPctStr+'</td>'
      + '<td style="font-weight:500;'+rCol+'">'+rStr+'</td>'
      + '<td class="editable" data-id="'+t.id+'" data-type="outcome" data-val="'+(t.outcome||'')+'">'+outcomeHtml+'</td>'
      + '<td class="dim">'+esc(t.source||'—')+'</td>'
      + '<td class="dim">'+esc(t.variant||'—')+'</td>'
      + '<td class="dim">'+obSizeStr+'</td>'
      + '<td class="dim">'+impulseStr+'</td>'
      + '<td>'+structHtml+'</td>'
      + '<td>'+klHtml+'</td>'
      + '<td class="dim">'+klDistStr+'</td>'
      + '<td>'+emaHtml+'</td>'
      + '<td class="dim">'+utcToLocal(t.opened_at||'')+'</td>'
      + '<td class="dim">'+utcToLocal(t.closed_at||'')+'</td>'
      + '<td class="dim" style="cursor:pointer;max-width:140px;overflow:hidden;text-overflow:ellipsis" title="Click to view full notes" onclick="showNotes(this)" data-full="'+esc(notesFull)+'">'+notesShort+'</td>'
      + '<td data-id="'+t.id+'" data-type="user_notes" data-val="'+esc(t.user_notes||'')+'" style="max-width:180px;vertical-align:middle" class="user-notes-cell">'
      + (t.user_notes
          ? '<span class="user-note-text" style="font-size:12px;cursor:pointer;border-bottom:1px dashed rgba(107,114,128,0.5)" title="'+esc(t.user_notes)+'">'+esc(t.user_notes.slice(0,40))+(t.user_notes.length>40?'…':'')+'</span>'
          + '<span class="user-note-del" data-id="'+t.id+'" style="margin-left:6px;color:var(--red);cursor:pointer;font-size:11px;opacity:0.5" title="Delete note">✕</span>'
          : '<span class="user-note-text" style="font-size:11px;color:var(--dim);cursor:pointer">+ add note</span>')
      + '</td>'
      + '<td>'+mediaHtml+'</td>'
      + '</tr>';
  }
  tbody.innerHTML = rows;
}

function updateStats(trades){
  var total=0,wins=0,losses=0,pnlSum=0,open=0;
  trades.forEach(function(t){
    if(t.status==='note') return;
    if(t.status==='closed'){ total++; var pnl=parseFloat(t.pnl)||0; pnlSum+=pnl; if(pnl>0){wins++;} else {losses++;} }
    if(t.status==='open') open++;
  });
  var wr = total>0 ? Math.round(wins/total*100) : 0;
  document.getElementById('s-total').textContent = total;
  document.getElementById('s-wr').textContent = wr+'%';
  document.getElementById('s-wr').className = 'stat-value '+(wr>=50?'green':wr>0?'red':'');
  document.getElementById('s-wins').textContent = wins;
  document.getElementById('s-losses').textContent = losses;
  document.getElementById('s-pnl').textContent = (pnlSum>=0?'+':'')+pnlSum.toFixed(2)+' USDT';
  document.getElementById('s-pnl').className = 'stat-value '+(pnlSum>=0?'green':'red');
  document.getElementById('s-open').textContent = open;
}

var _allTrades = [];
function calcRange(){
  var fromNum  = parseInt(document.getElementById('range-from').value);
  var toNum    = parseInt(document.getElementById('range-to').value);
  var fromDate = document.getElementById('range-date-from').value;
  var toDate   = document.getElementById('range-date-to').value;
  var variant  = document.getElementById('range-variant').value;
  var result   = document.getElementById('range-result');

  // Get closed trades sorted by trade number (descending in UI = ascending by index)
  var closed = _allTrades.filter(function(t){ return t.status==='closed' && t.outcome; });
  var numbered = closed.slice().reverse();

  if(!numbered.length){ result.textContent = 'No closed trades loaded'; result.style.color='var(--dim)'; return; }

  var filtered = numbered;

  // Filter by trade number range
  if(!isNaN(fromNum) || !isNaN(toNum)){
    var f = isNaN(fromNum) ? 1 : Math.max(1, fromNum);
    var t = isNaN(toNum)   ? numbered.length : Math.min(numbered.length, toNum);
    if(f > numbered.length){ result.textContent = 'From # exceeds total trades ('+numbered.length+')'; result.style.color='var(--red)'; return; }
    filtered = numbered.slice(f-1, t);
  }

  // Filter by date range
  if(fromDate || toDate){
    filtered = filtered.filter(function(t){
      var d = (t.opened_at||'').slice(0,10);
      if(fromDate && d < fromDate) return false;
      if(toDate   && d > toDate)   return false;
      return true;
    });
  }

  // Filter by variant
  if(variant){
    filtered = filtered.filter(function(t){ return t.variant === variant; });
  }

  if(!filtered.length){ result.textContent = 'No trades in range'; result.style.color='var(--dim)'; return; }

  var wins=0, losses=0, pnl=0;
  filtered.forEach(function(t){
    var p = parseFloat(t.pnl)||0;
    pnl += p;
    if(p>0){ wins++; } else { losses++; }
  });
  var total = wins+losses;
  var wr    = total>0 ? Math.round(wins/total*100) : 0;
  var wrCol = wr>=50?'var(--green)':wr>=35?'var(--amber)':'var(--red)';
  var pnlCol = pnl>=0?'var(--green)':'var(--red)';
  result.innerHTML =
    filtered.length + ' trades &nbsp;|&nbsp; ' +
    '<span style="color:'+wrCol+';font-weight:500">WR: '+wr+'%</span>' +
    ' &nbsp;|&nbsp; ' + wins + 'W / ' + losses + 'L' +
    ' &nbsp;|&nbsp; PnL: <span style="color:'+pnlCol+';font-weight:500">'+(pnl>=0?'+':'')+pnl.toFixed(2)+' USDT</span>';
}

function clearRange(){
  document.getElementById('range-from').value = '';
  document.getElementById('range-to').value   = '';
  document.getElementById('range-date-from').value = '';
  document.getElementById('range-date-to').value   = '';
  document.getElementById('range-variant').value   = '';
  document.getElementById('range-result').textContent = '';
}

// Event delegation for editable cells
document.addEventListener('click', function(e){
  var el = e.target.closest('[data-id][data-type]');
  if(!el) return;
  var id = el.dataset.id, type = el.dataset.type, val = el.dataset.val||'';

  if(type==='pnl'){
    var nv = prompt('Enter PnL in USDT:', val);
    if(nv===null) return;
    var num = parseFloat(nv);
    if(isNaN(num)){alert('Invalid number');return;}
    fetch('/journal/set-pnl',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:parseInt(id),pnl:num})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok')loadTrades();else alert(d.message);});
  }
  if(type==='timeframe'){
    var nv = prompt('Enter timeframe (e.g. M3, M5, M15, H1, H4, D1) — blank to clear:', val);
    if(nv===null) return;
    fetch('/journal/set-timeframe',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:parseInt(id),timeframe:nv.trim()})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok')loadTrades();else alert(d.message);});
  }
  if(type==='outcome'){
    var opts=['tp','sl',''], labels={'tp':'TP','sl':'SL','':'Clear'};
    var next=opts[(opts.indexOf(val)+1)%opts.length];
    if(!confirm('Change outcome to: '+(labels[next]||'Clear')+'?')) return;
    fetch('/journal/set-outcome',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:parseInt(id),outcome:next})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok')loadTrades();else alert(d.message);});
  }
  if(type==='media'){
    var cur = prompt('Enter URL (separate multiple with |):','');
    if(cur===null) return;
    fetch('/journal/set-media',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:parseInt(id),media:cur.trim()})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok')loadTrades();else alert(d.message);});
  }
});

// Status filter buttons
document.querySelectorAll('.filter-status').forEach(function(btn){
  btn.addEventListener('click', function(){
    document.querySelectorAll('.filter-status').forEach(function(b){ b.classList.remove('active'); b.style.borderColor=''; b.style.color=''; });
    this.classList.add('active');
    this.style.borderColor = 'var(--blue)';
    this.style.color = 'var(--blue)';
    STATUS_FILTER = this.dataset.status;
    loadTrades();
  });
});

document.querySelectorAll('.filter-outcome').forEach(function(btn){
  btn.addEventListener('click', function(){
    document.querySelectorAll('.filter-outcome').forEach(function(b){ b.style.borderColor=''; b.style.color=''; });
    this.style.borderColor = 'var(--blue)';
    this.style.color = 'var(--blue)';
    OUTCOME_FILTER = this.dataset.outcome;
    loadTrades();
  });
});

document.getElementById('show-skipped').addEventListener('change', function(){
  SHOW_SKIPPED = this.checked;
  loadTrades();
});

// Toolbar buttons
document.getElementById('btn-poll').onclick = function(){
  var btn=this; btn.textContent='...'; btn.disabled=true;
  fetch('/poll',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    btn.textContent=d.ws_connected?'WS Active':'Done'; loadTrades();
    setTimeout(function(){btn.textContent='Poll Now';btn.disabled=false;},2000);
  }).catch(function(){btn.textContent='Error';btn.disabled=false;});
};
document.getElementById('btn-sheets').onclick = function(){
  var btn=this; btn.textContent='Syncing...'; btn.disabled=true;
  fetch('/journal/sync-sheets',{method:'POST'}).then(function(r){return r.json();}).then(function(d){
    btn.textContent = d.status==='ok' ? '✅ '+d.message : '❌ '+d.message;
    setTimeout(function(){btn.textContent='📊 Sync Sheets';btn.disabled=false;},3000);
  }).catch(function(){btn.textContent='❌ Error';btn.disabled=false;});
};
document.getElementById('btn-note').onclick = function(){
  var note=prompt('Enter note:'); if(!note||!note.trim()) return;
  fetch('/journal/add-note',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({note:note.trim()})})
    .then(function(r){return r.json();}).then(function(d){if(d.status==='ok')loadTrades();else alert(d.message);});
};
document.getElementById('btn-del-old').onclick = function(){
  var days=prompt('Delete trades older than how many days?','7');
  if(!days||isNaN(days)) return;
  if(!confirm('Delete trades older than '+days+' days?')) return;
  fetch('/journal/delete-older-than/'+parseInt(days),{method:'POST'})
    .then(function(r){return r.json();}).then(function(d){alert(d.message);loadTrades();});
};
document.getElementById('btn-reset').onclick = function(){
  if(!confirm('DELETE ALL TRADES?')) return;
  if(!confirm('Are you sure?')) return;
  fetch('/journal/reset',{method:'POST'}).then(function(r){return r.json();}).then(function(d){alert(d.message);loadTrades();});
};

// Note row — edit on text click, delete on ✕
document.addEventListener('click', function(e) {
  var noteText = e.target.closest('.note-row-text');
  if (noteText) {
    var id  = noteText.dataset.id;
    var cur = prompt('Edit note:', noteText.textContent);
    if (cur === null) return;
    fetch('/journal/edit-note', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: parseInt(id), note: cur.trim()})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok') loadTrades();});
    return;
  }
  var noteDel = e.target.closest('.note-row-del');
  if (noteDel) {
    if (!confirm('Delete this note?')) return;
    fetch('/journal/delete-note/' + noteDel.dataset.id, {method:'POST'})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok') loadTrades();});
  }
});
document.addEventListener('click', function(e) {
  var delBtn = e.target.closest('.user-note-del');
  if (delBtn) {
    var id = delBtn.dataset.id;
    if (!confirm('Delete this note?')) return;
    fetch('/journal/set-user-notes', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: parseInt(id), user_notes: ''})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok') loadTrades();});
    return;
  }
  var noteText = e.target.closest('.user-note-text');
  if (noteText) {
    var cell = noteText.closest('.user-notes-cell');
    var id   = cell ? cell.dataset.id : null;
    var val  = cell ? cell.dataset.val : '';
    if (!id) return;
    var cur = prompt('Edit note:', val);
    if (cur === null) return;
    fetch('/journal/set-user-notes', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({id: parseInt(id), user_notes: cur.trim()})})
      .then(function(r){return r.json();}).then(function(d){if(d.status==='ok') loadTrades();});
  }
});
(function(){
  var preview = document.getElementById('img-preview');
  var img     = document.getElementById('img-preview-img');
  var timer   = null;
  document.addEventListener('mouseover', function(e) {
    var el = e.target.closest('[data-preview]');
    if (!el) return;
    clearTimeout(timer);
    timer = setTimeout(function() {
      img.src = el.dataset.preview;
      preview.style.display = 'block';
      positionPreview(e.clientX, e.clientY);
    }, 200);
  });
  document.addEventListener('mousemove', function(e) {
    if (preview.style.display === 'none') return;
    positionPreview(e.clientX, e.clientY);
  });
  function positionPreview(cx, cy) {
    var margin = 8;
    var w = preview.offsetWidth  || 720;
    var h = preview.offsetHeight || 480;
    var x = cx + 16, y = cy + 16;
    if (x + w + margin > window.innerWidth)  x = cx - w - 16;
    if (y + h + margin > window.innerHeight) y = cy - h - 16;
    x = Math.max(margin, Math.min(x, window.innerWidth  - w - margin));
    y = Math.max(margin, Math.min(y, window.innerHeight - h - margin));
    preview.style.left = x + 'px';
    preview.style.top  = y + 'px';
  }
  document.addEventListener('mouseout', function(e) {
    if (!e.target.closest('[data-preview]')) return;
    clearTimeout(timer);
    preview.style.display = 'none';
    img.src = '';
  });
  img.addEventListener('error', function() { preview.style.display = 'none'; });
})();

loadTrades();
loadRestrictedStatus();
setInterval(loadRestrictedStatus, 60000);
</script>
</body>
</html>
"""


# ─── BYBIT SESSION ────────────────────────────────────────────────────────────
BYBIT_PROXY = os.environ.get("BYBIT_PROXY", "")

# ─── TRAIL / TP EXTENSION ────────────────────────────────────────────────────
EXTEND_BEYOND_TP    = os.getenv("EXTEND_BEYOND_TP",    "false").lower() == "true"
TP_EXTEND_TRIGGER_R = float(os.getenv("TP_EXTEND_TRIGGER_R", "2.75") or "2.75")
TRAIL_STEP_R        = float(os.getenv("TRAIL_STEP_R",        "1.0")  or "1.0")
BE_TRIGGER_R        = float(os.getenv("BE_TRIGGER_R",        "0")    or "0")

# Restricted trading times — e.g. "Fri 22:00-Mon 02:00" (local time, multiple separated by |)
RESTRICTED_TIMES  = os.getenv("RESTRICTED_TIMES",  "")
TIMEZONE_OFFSET   = int(os.getenv("TIMEZONE_OFFSET", "2") or "2")  # UTC+X
_trail_state: dict  = {}
_trail_lock         = threading.Lock()
# Prefix on every order this bot places, via orderLinkId. Lets the WS handlers
# tell bot orders apart from ones placed manually (e.g. directly in the Bybit
# app) — an order/execution with no orderLinkId starting with this tag is not
# ours.
BOT_ORDER_TAG       = "obbot_"
# Second, more reliable signal alongside the tag: orderLinkId isn't always
# present on every individual execution chunk of a multi-fill order (only
# reliably on the consolidated order-status event) — tracking orderId
# directly at placement time catches those chunks the tag alone misses.
_bot_order_ids: set = set()
_bot_order_ids_order: 'deque' = deque()
_bot_order_ids_lock = threading.Lock()
_BOT_ORDER_IDS_MAX  = 2000
# Dedup for execution events — Bybit's WS can redeliver the same fill; without
# this, a redelivered partial-exit fill would add its PnL to the trade a
# second time. Bounded with FIFO eviction (deque + set, not a wholesale
# .clear()) — a trade can stay open for hours, and clearing the whole set
# once it filled up (from ALL symbols' activity, not just this one trade)
# was wiping still-relevant entries mid-trade, which is exactly what let a
# genuine redelivery slip through and triple-count a partial exit's PnL.
_processed_exec_ids: set              = set()
_processed_exec_order: 'deque'        = deque()
_processed_exec_lock                  = threading.Lock()
_PROCESSED_EXEC_MAX                   = 5000

try:
    session = HTTP(
        testnet=TESTNET,
        api_key=API_KEY,
        api_secret=API_SECRET,
        **({
            "proxies": {
                "http":  BYBIT_PROXY,
                "https": BYBIT_PROXY,
            }
        } if BYBIT_PROXY else {})
    )
    log.info(f"Bybit session created {'with proxy' if BYBIT_PROXY else 'without proxy'}")
except Exception:
    log.warning("Session creation with proxy failed — retrying without proxy")
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


def get_live_position_size(symbol: str) -> float:
    """Return the current live position size for a symbol (0 if flat)."""
    try:
        resp = _api_call(session.get_positions, category="linear", symbol=symbol)
        for p in resp.get("result", {}).get("list", []):
            if float(p.get("size", 0)) > 0:
                return float(p["size"])
        return 0.0
    except Exception as e:
        log.warning(f"Error fetching live position size for {symbol}: {e}")
        return -1.0  # -1 = lookup failed, distinct from "confirmed flat" (0)


def _api_call(fn, *args, **kwargs):
    """Wrapper for Bybit API calls with retry on rate limit."""
    for attempt in range(3):
        try:
            result = fn(*args, **kwargs)
            # Check for API-level errors in response
            ret_code = result.get("retCode", 0) if isinstance(result, dict) else 0
            if ret_code == 10006:  # rate limit
                wait = (attempt + 1) * 3
                log.warning(f"Rate limit (retCode 10006) — waiting {wait}s before retry {attempt+1}/3")
                time.sleep(wait)
                continue
            return result
        except Exception as e:
            err = str(e)
            if "403" in err or "rate limit" in err.lower() or "10006" in err:
                wait = (attempt + 1) * 3
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


_last_known_balance = float(os.environ.get("FALLBACK_BALANCE", "1040.0"))
_last_balance_time  = 0.0

def _check_cooldown(side: str, cfg: dict, symbol: str = "") -> tuple:
    """
    Check if a symbol+side is in cooldown (per-symbol).
    Each symbol tracks its own consecutive-loss counter independently.
    Returns (blocked: bool, reason: str)
    """
    max_losses = cfg.get("cooldown_losses", 0)
    if not max_losses:
        return False, ""  # cooldown disabled

    cooldown_hours = cfg.get("cooldown_hours", 48.0)
    cooldown_secs  = cooldown_hours * 3600

    try:
        conn = get_db()
        import psycopg2.extras as _pge
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            if symbol:
                # Per-symbol: only look at this symbol's recent trades
                cur.execute("""
                    SELECT outcome, closed_at FROM trades
                    WHERE symbol = %s AND side = %s AND outcome IN ('tp','sl','tp1_tp2','tp1_sl') AND status = 'closed'
                    ORDER BY closed_at DESC LIMIT %s
                """, (symbol, side, max_losses + 1))
            else:
                # Fallback: global (used by /status endpoint summary)
                cur.execute("""
                    SELECT outcome, closed_at FROM trades
                    WHERE side = %s AND outcome IN ('tp','sl','tp1_tp2','tp1_sl') AND status = 'closed'
                    ORDER BY closed_at DESC LIMIT %s
                """, (side, max_losses + 1))
            recent = cur.fetchall()
        conn.close()

        if len(recent) < max_losses:
            return False, ""

        last_n = recent[:max_losses]
        # tp1_sl = partial secured, but the runner still got stopped out — same
        # signal a cooldown is meant to catch as a plain sl.
        if not all(r["outcome"] in ("sl", "tp1_sl") for r in last_n):
            return False, ""

        most_recent_loss_at = str(last_n[0]["closed_at"])
        try:
            loss_dt   = datetime.strptime(most_recent_loss_at[:19], "%Y-%m-%d %H:%M:%S")
            elapsed   = (datetime.utcnow() - loss_dt).total_seconds()
            remaining = cooldown_secs - elapsed
            if remaining > 0:
                hrs = remaining / 3600
                sym_txt = f"{symbol} " if symbol else ""
                return True, f"Cooldown active — {sym_txt}{max_losses} consecutive {side} losses. {hrs:.1f}h remaining ({cooldown_hours:.0f}h cooldown)"
        except:
            pass

    except Exception as e:
        log.warning(f"Cooldown check failed: {e}")

    return False, ""


def get_available_balance() -> float:
    """Return available USDT balance. Falls back to last known balance if API fails."""
    global _last_known_balance, _last_balance_time
    try:
        resp = _api_call(session.get_wallet_balance, accountType="UNIFIED", coin="USDT")
        for item in resp.get("result", {}).get("list", []):
            for coin in item.get("coin", []):
                if coin.get("coin") == "USDT":
                    bal = float(coin.get("availableToWithdraw") or coin.get("walletBalance") or 0)
                    if bal > 0:
                        _last_known_balance = bal
                        _last_balance_time  = time.time()
                        log.info(f"Balance: {bal:.2f} USDT (live)")
                        return bal
    except Exception as e:
        log.warning(f"Error fetching balance: {e}")
        if _last_known_balance > 0:
            age = (time.time() - _last_balance_time) / 60
            log.warning(f"Using cached balance: {_last_known_balance:.2f} USDT (from {age:.0f}m ago)")
            return _last_known_balance
        log.error("No cached balance available")
    return _last_known_balance





_instrument_cache = {}

def get_instrument_info(symbol: str) -> dict:
    """Get min qty, qty step, price decimals for a symbol. Cached."""
    if symbol in _instrument_cache:
        return _instrument_cache[symbol]
    try:
        resp = _api_call(session.get_instruments_info, category="linear", symbol=symbol)
        items = resp.get("result", {}).get("list", [])
        if items:
            lot   = items[0].get("lotSizeFilter", {})
            price = items[0].get("priceFilter", {})
            tick  = str(price.get("tickSize", "0.01"))
            scale = len(tick.rstrip("0").split(".")[1]) if "." in tick else 0
            info  = {
                "min_qty":     float(lot.get("minOrderQty",  0.001)),
                "qty_step":    float(lot.get("qtyStep",       0.001)),
                "price_scale": scale,
            }
            _instrument_cache[symbol] = info
            log.info(f"Instrument info cached: {symbol} tick={tick} scale={scale}")
            return info
    except Exception as e:
        log.error(f"Error fetching instrument info for {symbol}: {e}")

    # Smarter fallback — infer scale from entry price if known
    # e.g. 0.93962 → needs 5 decimal places, not 2
    return {"min_qty": 0.001, "qty_step": 0.001, "price_scale": 5}


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
    Background thread — tries WebSocket first, falls back to REST polling.
    WebSocket is preferred as it avoids IP rate limits on REST endpoints.
    """
    # WebSocket with auto-reconnect on ping/pong timeout
    ws_started = False
    while True:
        try:
            log.info("Starting Bybit WebSocket stream...")
            ws = WebSocket(
                testnet=TESTNET,
                channel_type="private",
                api_key=API_KEY,
                api_secret=API_SECRET,
                ping_interval=20,
                ping_timeout=10,
            )
            ws.order_stream(callback=_handle_order_update)
            ws.execution_stream(callback=_handle_execution_update)
            log.info("✅ WebSocket connected — listening for real-time order updates")
            _ws_connected = True
            ws_started = True
            # Keep alive — detect stale connection via heartbeat
            while True:
                time.sleep(30)
        except Exception as e:
            log.warning(f"WebSocket dropped ({e}) — reconnecting in 5s...")
            _ws_connected = False
            time.sleep(5)

    # Fallback to REST polling
    if not ws_started:
        log.info("Background REST poller thread started")
        while True:
            try:
                _check_closed_trades()
            except Exception as e:
                log.error(f"Poller error: {e}")
            time.sleep(get_config()["poll_interval"])


def _handle_order_update(msg):
    """Handle real-time order updates from Bybit WebSocket."""
    try:
        orders = msg.get("data", [])
        for o in orders:
            order_id   = o.get("orderId", "")
            symbol     = o.get("symbol", "")
            status     = o.get("orderStatus", "")
            side       = o.get("side", "")
            reduce_only = o.get("reduceOnly", False)
            log.info(f"WS order: {symbol} {order_id} status={status} reduceOnly={reduce_only}")
            if status in ("Cancelled", "Rejected", "Deactivated"):
                conn = get_db()
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE trades SET status='skipped', notes=%s WHERE order_id=%s AND status='open'",
                        (f"Order {status.lower()}", order_id)
                    )
                conn.commit()
                conn.close()
                _trail_deregister(order_id)
                log.info(f"WS: {symbol} {order_id} marked skipped")
            elif status in ("Filled", "PartiallyFilled") and not reduce_only:
                # Entry limit order filled — register for trail monitoring
                try:
                    import psycopg2.extras as _pge2
                    conn = get_db()
                    with conn.cursor(cursor_factory=_pge2.RealDictCursor) as cur:
                        cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS partial_done BOOLEAN DEFAULT FALSE")
                        cur.execute(
                            "SELECT entry, sl, tp1, tp1_pct, partial_done, realized_pnl_partial FROM trades WHERE order_id=%s LIMIT 1",
                            (order_id,)
                        )
                        row = cur.fetchone()
                    conn.commit()
                    conn.close()
                    if row:
                        log.info(f"Trail: registering {symbol} {side} on order fill (status={status})")
                        _trail_register(order_id, symbol, side,
                                        float(row["entry"] or 0),
                                        float(row["sl"] or 0),
                                        tp1=float(row["tp1"]) if row.get("tp1") is not None else None,
                                        tp1_pct=float(row["tp1_pct"] or 0),
                                        partial_done=bool(row.get("partial_done", False)),
                                        partial_pnl=float(row["realized_pnl_partial"]) if row.get("realized_pnl_partial") is not None else None)
                    else:
                        log.warning(f"Trail: no DB row found for order_id={order_id}")
                except Exception as tr_err:
                    log.warning(f"Trail register failed: {tr_err}")
    except Exception as e:
        log.error(f"WS order handler error: {e}")


def _handle_execution_update(msg):
    """Handle execution updates — catches TP/SL fills with PnL."""
    try:
        execs = msg.get("data", [])
        for e in execs:
            exec_id    = e.get("execId", "")
            if exec_id:
                with _processed_exec_lock:
                    if exec_id in _processed_exec_ids:
                        continue
                    _processed_exec_ids.add(exec_id)
                    _processed_exec_order.append(exec_id)
                    while len(_processed_exec_order) > _PROCESSED_EXEC_MAX:
                        oldest = _processed_exec_order.popleft()
                        _processed_exec_ids.discard(oldest)

            order_id   = e.get("orderId", "")
            symbol     = e.get("symbol", "")
            exec_type  = e.get("execType", "")
            exec_price = float(e.get("execPrice", 0) or 0)
            closed_pnl = float(e.get("closedPnl", 0) or 0)
            side       = e.get("side", "")

            if exec_type not in ("TakeProfit", "StopLoss", "Trade") or exec_price <= 0:
                continue

            log.info(f"WS execution: {symbol} {exec_type} @ {exec_price} PnL={closed_pnl}")

            entry_side = "Sell" if side == "Buy" else "Buy"
            conn = get_db()
            import psycopg2.extras as _pge
            with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM trades
                    WHERE symbol=%s AND side=%s AND status='open'
                    ORDER BY opened_at DESC LIMIT 1
                """, (symbol, entry_side))
                trade = cur.fetchone()

            if not trade:
                order_link_id = e.get("orderLinkId", "") or ""
                with _bot_order_ids_lock:
                    is_tracked_order_id = order_id in _bot_order_ids
                is_bot_order  = order_link_id.startswith(BOT_ORDER_TAG) or is_tracked_order_id

                if is_bot_order:
                    # Expected — e.g. the entry-fill echo, which never matches an
                    # open row by design (see entry_side flip above). Not manual.
                    log.warning(f"WS: no open {symbol} {entry_side} trade found")
                    conn.close()
                    continue

                # Untagged order — not placed by this bot. Could be a manual
                # trade's opening or closing leg. Wrapped defensively: a bug here
                # must never break processing of the bot's own trades below.
                try:
                    exec_qty_m = float(e.get("execQty", 0) or 0)
                    if exec_qty_m <= 0:
                        conn.close()
                        continue

                    # Check same-direction first — a manual order (or a large
                    # order matching in multiple chunks against the order book)
                    # can generate several separate execution events for what is
                    # really one accumulating position, not a series of new ones.
                    with conn.cursor(cursor_factory=_pge.RealDictCursor) as mcur:
                        mcur.execute("""
                            SELECT * FROM trades
                            WHERE symbol=%s AND side=%s AND status='open' AND source='manual'
                            ORDER BY opened_at DESC LIMIT 1
                        """, (symbol, side))
                        same_dir_open = mcur.fetchone()

                    if same_dir_open:
                        old_qty   = float(same_dir_open.get("qty") or 0)
                        old_entry = float(same_dir_open.get("entry") or exec_price)
                        new_qty   = old_qty + exec_qty_m
                        new_entry = ((old_qty * old_entry) + (exec_qty_m * exec_price)) / new_qty if new_qty > 0 else exec_price
                        mconn = get_db()
                        with mconn.cursor() as ucur:
                            ucur.execute("UPDATE trades SET qty=%s, entry=%s WHERE id=%s",
                                        (new_qty, new_entry, same_dir_open["id"]))
                        mconn.commit()
                        mconn.close()
                        log.info(f"WS {symbol}: manual position addition — qty {old_qty}->{new_qty}, avg entry {new_entry:.6f}")
                        conn.close()
                        continue

                    with conn.cursor(cursor_factory=_pge.RealDictCursor) as mcur:
                        mcur.execute("""
                            SELECT * FROM trades
                            WHERE symbol=%s AND side=%s AND status='open' AND source='manual'
                            ORDER BY opened_at DESC LIMIT 1
                        """, (symbol, entry_side))
                        manual_open = mcur.fetchone()

                    if manual_open:
                        # Closing leg of a manual trade — reuse the same
                        # combine/close logic as bot trades below.
                        trade = manual_open
                        log.info(f"WS {symbol}: detected manual trade close (untagged order)")
                    elif exec_type == "Trade":
                        # Genuinely new manual position. sl/tp are unknown for a
                        # manual trade unless the user set Bybit's native SL/TP
                        # when opening it — read those off the live position if
                        # present; the trades table requires non-null sl/tp, so
                        # fall back to 0 (Bybit's own "not set" convention).
                        sl_val = 0.0
                        tp_val = 0.0
                        try:
                            posresp = _api_call(session.get_positions, category="linear", symbol=symbol)
                            for p in posresp.get("result", {}).get("list", []):
                                if float(p.get("size", 0)) > 0:
                                    sl_val = float(p.get("stopLoss") or 0)
                                    tp_val = float(p.get("takeProfit") or 0)
                                    break
                        except Exception as pos_err:
                            log.warning(f"WS {symbol}: could not read live SL/TP for manual trade: {pos_err}")

                        mconn = get_db()
                        with mconn.cursor() as icur:
                            icur.execute("""
                                INSERT INTO trades (symbol, side, qty, entry, sl, tp, status, source, opened_at, order_id)
                                VALUES (%s, %s, %s, %s, %s, %s, 'open', 'manual', %s, %s)
                            """, (symbol, side, exec_qty_m, exec_price, sl_val, tp_val,
                                  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                                  order_id or f"manual_{exec_id or int(time.time())}"))
                        mconn.commit()
                        mconn.close()
                        log.info(f"WS {symbol}: detected manual trade OPEN — {side} {exec_qty_m} @ {exec_price} (sl={sl_val}, tp={tp_val}), tracking as source=manual")
                        conn.close()
                        continue
                    else:
                        # TP/SL exec type with no matching manual open — can't be
                        # an opening leg, and no open position to close. Skip.
                        log.warning(f"WS {symbol}: untagged {exec_type} execution with no open manual trade — skipping")
                        conn.close()
                        continue
                except Exception as manual_err:
                    log.warning(f"WS {symbol}: manual-trade detection failed: {manual_err}")
                    conn.close()
                    continue

            # If PnL is 0 (e.g. execType=Trade), try to get it from closed PnL API.
            # Match priority: exact orderId (most reliable) > qty+side within a
            # tolerance (Bybit doesn't always report qty at the same granularity
            # as a single execution chunk, so an exact match is too fragile —
            # this mirrors the matching already used elsewhere in this file for
            # the same reason) > most recent same-side close as last resort.
            exec_qty = float(e.get("execQty", 0) or 0)
            if closed_pnl == 0.0:
                try:
                    opened_at = str(trade.get("opened_at") or "")
                    start_ms  = int((datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S").timestamp() - 300) * 1000) if opened_at else None
                    kwargs    = dict(category="linear", symbol=symbol, limit=20)
                    if start_ms:
                        kwargs["startTime"] = start_ms
                    pnl_resp  = _api_call(session.get_closed_pnl, **kwargs)
                    records   = pnl_resp.get("result", {}).get("list", [])
                    match_qty = exec_qty if exec_qty > 0 else float(trade.get("qty") or 0)

                    best = None
                    for rec in records:
                        if rec.get("orderId", "") == order_id:
                            best = rec
                            break
                    if best is None:
                        for rec in records:
                            rqty = float(rec.get("qty", 0) or 0)
                            if match_qty > 0 and abs(rqty - match_qty) <= max(match_qty * 0.1, 0.01) and rec.get("side", "") == side:
                                best = rec
                                break
                    if best is None:
                        for rec in records:
                            if rec.get("side", "") == side:
                                best = rec
                                log.warning(f"WS {symbol}: no orderId/qty match — using most recent {side} close as fallback")
                                break

                    if best:
                        closed_pnl = float(best.get("closedPnl", 0) or 0)
                        log.info(f"WS: fetched PnL from closed PnL API: {closed_pnl} (orderId={'match' if best.get('orderId','')==order_id else 'no-match'}, qty={best.get('qty')})")
                    else:
                        log.warning(f"WS {symbol}: no closed-PnL record found at all for side={side} — leg PnL recorded as 0")
                except Exception as pnl_err:
                    log.warning(f"WS: could not fetch PnL from API: {pnl_err}")

            outcome   = "tp" if exec_type == "TakeProfit" else "sl"
            # Verify SL outcome with PnL — positive PnL on SL = likely TP
            if outcome == "sl" and closed_pnl > 0:
                tp_price = float(trade.get("tp") or 0)
                if tp_price > 0:
                    outcome = "tp"
                    log.info(f"WS {symbol}: SL execType but PnL={closed_pnl:.4f} positive — classifying as TP")

            # A position can now close in more than one leg (partial exit, then the
            # remainder). Check what's actually still open on the exchange before
            # deciding whether this execution finished the trade or was one leg of it.
            live_size    = get_live_position_size(symbol)
            trail_key    = str(trade.get("order_id", ""))

            # Prefer in-memory trail state — it's alive for the trade's whole
            # lifetime and isn't subject to the DB write failing partway through.
            # Fall back to the DB column only if trail state doesn't have this
            # trade (e.g. a server restart lost it, but an earlier write succeeded).
            with _trail_lock:
                mem_partial_pnl = _trail_state.get(trail_key, {}).get("partial_pnl")
            prior_partial_pnl = mem_partial_pnl if mem_partial_pnl is not None else float(trade.get("realized_pnl_partial") or 0)

            def _persist_partial_pnl(new_total):
                # Best-effort backup only — in-memory trail state above is the
                # real source of truth and is updated regardless of whether this
                # succeeds.
                try:
                    aconn = get_db()
                    with aconn.cursor() as acur:
                        acur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS realized_pnl_partial DOUBLE PRECISION DEFAULT 0")
                        acur.execute("UPDATE trades SET realized_pnl_partial=%s WHERE id=%s AND status='open'",
                                    (new_total, trade["id"]))
                    aconn.commit()
                    aconn.close()
                except Exception as acc_err:
                    log.warning(f"WS {symbol}: DB backup write for partial PnL failed (non-critical, in-memory value still correct): {acc_err}")

            if live_size < 0:
                # Position-size lookup failed — don't guess whether this is the
                # final leg. Update in-memory total (reliable), attempt DB backup,
                # leave the trade open to reconcile on the next execution.
                new_total = prior_partial_pnl + closed_pnl
                with _trail_lock:
                    if trail_key in _trail_state:
                        _trail_state[trail_key]["partial_pnl"] = new_total
                log.warning(f"WS {symbol}: could not confirm live position size — recording leg PnL={closed_pnl:.4f} (running total {new_total:.4f}) but leaving trade open, will reconcile on next execution")
                _persist_partial_pnl(new_total)
                conn.close()
                continue

            if live_size > 0:
                # Position still open — this was a partial exit, not the full close.
                # Accumulate its PnL so the eventual full-close record is correct;
                # do NOT mark closed, do NOT deregister from the trail watcher (the
                # remaining size still needs BE/trail management).
                new_total = prior_partial_pnl + closed_pnl
                with _trail_lock:
                    if trail_key in _trail_state:
                        _trail_state[trail_key]["partial_pnl"] = new_total
                log.info(f"WS {symbol}: partial close leg PnL={closed_pnl:.4f} (running total {new_total:.4f}) — {live_size} still open, trade stays open")
                _persist_partial_pnl(new_total)
                conn.close()
                continue

            # live_size == 0 — position is actually flat now. This execution is the
            # leg that finished the trade. Combine it with any earlier partial PnL
            # (read from in-memory trail state above) so the journal shows the
            # trade's true total result, not just this leg.
            closed_pnl = closed_pnl + prior_partial_pnl
            if prior_partial_pnl != 0:
                log.info(f"WS {symbol}: final leg — combining with {prior_partial_pnl:.4f} from earlier partial exit, total PnL={closed_pnl:.4f}")

            # If a partial exit happened earlier on this trade, label the outcome by
            # what the remainder did — tp1_tp2 (ran to the runner target) or tp1_sl
            # (reversed and hit stop). A trade with no partial keeps the plain tp/sl
            # label, same as before.
            journal_outcome = outcome
            if prior_partial_pnl != 0:
                journal_outcome = "tp1_tp2" if outcome == "tp" else "tp1_sl"

            closed_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            entry     = float(trade.get("entry") or exec_price)
            qty       = float(trade.get("qty") or 0)
            pnl_pct   = round(closed_pnl / (entry * qty) * 100, 2) if entry > 0 and qty > 0 else 0.0

            # Get actual fill time from order history so opened_at = entry fill, not OB detection
            fill_time = None
            try:
                hist_resp = _api_call(session.get_order_history, category="linear", symbol=symbol, limit=10)
                for h in hist_resp.get("result", {}).get("list", []):
                    if h.get("orderStatus") == "Filled" and h.get("side") == entry_side:
                        updated_ms = int(h.get("updatedTime", 0) or 0)
                        if updated_ms:
                            fill_time = datetime.utcfromtimestamp(updated_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
                            log.info(f"WS: fill time for {symbol} = {fill_time}")
                            break
            except Exception as ft_err:
                log.warning(f"WS: could not fetch fill time: {ft_err}")

            with conn.cursor() as cur:
                if fill_time:
                    cur.execute("""
                        UPDATE trades SET status='closed', outcome=%s, exit_price=%s,
                            pnl=%s, pnl_pct=%s, closed_at=%s, opened_at=%s
                        WHERE id=%s AND status='open'
                    """, (journal_outcome, exec_price, closed_pnl, pnl_pct, closed_at, fill_time, trade["id"]))
                else:
                    cur.execute("""
                        UPDATE trades SET status='closed', outcome=%s, exit_price=%s,
                            pnl=%s, pnl_pct=%s, closed_at=%s
                        WHERE id=%s AND status='open'
                    """, (journal_outcome, exec_price, closed_pnl, pnl_pct, closed_at, trade["id"]))
            conn.commit()
            conn.close()

            emoji = "✅" if outcome == "tp" else "🔴"
            log.info(f"{emoji} WS closed {symbol} — {journal_outcome.upper()} @ {exec_price} PnL={closed_pnl:.4f}")
            _trail_deregister(str(trade.get("order_id", "")))

            try:
                if gsheets.is_configured():
                    gsheets.push_closed_trade(
                        symbol=symbol, side=entry_side,
                        qty=float(trade.get("qty") or 0),
                        entry=float(trade.get("entry") or exec_price),
                        sl=float(trade.get("sl") or 0),
                        tp=float(trade.get("tp") or 0),
                        exit_price=exec_price, pnl=closed_pnl,
                        outcome=journal_outcome, source=trade.get("source","ob"),
                        timeframe=trade.get("timeframe",""),
                        leverage=int(trade.get("leverage") or 1),
                        opened_at=str(trade.get("opened_at") or ""),
                        closed_at=closed_at,
                        tp1=float(trade["tp1"]) if trade.get("tp1") is not None else None,
                        tp1_pct=float(trade["tp1_pct"]) if trade.get("tp1_pct") is not None else None
                    )
            except Exception as gs_err:
                log.warning(f"Sheets update failed: {gs_err}")

    except Exception as e:
        import traceback
        log.error(f"WS execution handler error: {e}\n{traceback.format_exc()}")




def _check_closed_trades():
    """Find open journal entries and check if Bybit has closed them."""
    if not _poll_lock.acquire(blocking=False):
        log.info("Poll already running — skipping")
        return

    try:
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
            time.sleep(1)  # 1s between each trade check to avoid rate limits
            order_id = trade["order_id"]
            symbol   = trade["symbol"]
            try:
                # Check if order is still pending — query all open orders for symbol
                # (orderId filter can cause issues on some Bybit endpoints)
                open_resp   = _api_call(session.get_open_orders, category="linear", symbol=symbol)
                all_open    = open_resp.get("result", {}).get("list", [])
                open_orders = [o for o in all_open if o.get("orderId") == order_id]

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
                hist_resp    = _api_call(session.get_order_history, category="linear", symbol=symbol, orderId=order_id)
                hist         = hist_resp.get("result", {}).get("list", [])
                order_status = hist[0].get("orderStatus", "") if hist else "Unknown"

                if order_status in ("Cancelled", "Rejected", "Deactivated"):
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET status = 'skipped', notes = " + ph() + " WHERE order_id = " + ph(),
                                       (f"Order {order_status.lower()}", order_id))
                        conn.commit()
                    log.info(f"{symbol} {order_id} was {order_status} — marked skipped")
                else:
                    _check_closed_pnl(trade)

            except Exception as e:
                import traceback
                log.error(f"Error checking {symbol} {order_id}: {e}\n{traceback.format_exc()}")

    finally:
        _poll_lock.release()


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

        # Only look at PnL records after the trade was opened
        # Add generous buffer (24h) to catch delayed closes
        try:
            opened_dt  = datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
            start_ms   = int((opened_dt.timestamp() - 300) * 1000)  # 5min before open
        except:
            start_ms   = None

        pnl_kwargs = dict(category="linear", symbol=symbol, limit=50)
        if start_ms:
            pnl_kwargs["startTime"] = start_ms

        resp    = _api_call(session.get_closed_pnl, **pnl_kwargs)
        records = resp.get("result", {}).get("list", [])

        if not records:
            log.warning(f"No closed PnL records for {symbol}")
            # Mark as skipped if trade is very old
            try:
                opened = datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
                mins   = (datetime.utcnow() - opened).total_seconds() / 60
                if mins > 240:
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET status='skipped', notes='No PnL record found after 4h' WHERE order_id=" + ph(), (order_id,))
                        conn.commit()
                    log.info(f"{symbol} {order_id} — no PnL after 4h, marked skipped")
            except:
                pass
            return

        # Closing side is opposite to entry side
        closing_side = "Sell" if side == "Buy" else "Buy"

        log.info(f"{symbol}: found {len(records)} closed PnL records, looking for order_id={order_id} qty={qty} side={closing_side}")

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
            log.warning(f"No closed PnL match for {symbol} {order_id} qty={qty} side={closing_side} — {len(records)} records checked")
            try:
                opened = datetime.strptime(opened_at[:19], "%Y-%m-%d %H:%M:%S")
                mins   = (datetime.utcnow() - opened).total_seconds() / 60
                if mins > 480:  # 8 hours — definitely done, not just slow TP
                    with get_db() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE trades SET status='skipped', notes='No match in closed PnL after 8h' WHERE order_id=" + ph(), (order_id,))
                        conn.commit()
                    log.info(f"{symbol} {order_id} — no PnL match after {mins:.0f}m, marked skipped")
            except:
                pass
            return

        exit_price   = float(best.get("avgExitPrice", 0) or best.get("exitPrice", 0))
        realised_pnl = float(best.get("closedPnl", 0) or 0)
        exec_type    = best.get("execType", "")

        # Get entry fill time from order history — updatedTime when order was Filled
        fill_time = None
        try:
            hist_resp = _api_call(session.get_order_history, category="linear", symbol=symbol, orderId=order_id)
            hist = hist_resp.get("result", {}).get("list", [])
            if hist and hist[0].get("orderStatus") == "Filled":
                updated_ms = int(hist[0].get("updatedTime", 0) or 0)
                if updated_ms:
                    fill_time = datetime.utcfromtimestamp(updated_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    log.info(f"{symbol}: fill time = {fill_time}")
        except Exception as ft_err:
            log.warning(f"{symbol}: could not fetch fill time: {ft_err}")

        # Determine outcome — trust execType first, verify with PnL as fallback
        if exec_type == "TakeProfit":
            outcome = "tp"
        elif exec_type == "StopLoss":
            # Verify — if PnL is clearly positive it might be a TP that fired via SL order
            tp = float(trade.get("tp") or 0)
            if realised_pnl > 0 and tp > 0:
                outcome = "tp"
                log.info(f"{symbol}: SL execType but positive PnL {realised_pnl:.4f} — classifying as TP")
            else:
                outcome = "sl"
        else:
            # Unknown execType — use PnL and TP price to determine
            tp = float(trade.get("tp") or 0)
            if side == "Buy":
                outcome = "tp" if exit_price >= tp * 0.999 else "sl"
            else:
                outcome = "tp" if exit_price <= tp * 1.001 else "sl"

        success = log_trade_closed(order_id, exit_price, outcome, realised_pnl=realised_pnl)
        # Update opened_at to actual fill time if we got it
        if success and fill_time:
            try:
                conn2 = get_db()
                with conn2.cursor() as cur:
                    cur.execute("UPDATE trades SET opened_at=%s WHERE order_id=%s", (fill_time, order_id))
                conn2.commit()
                conn2.close()
                log.info(f"{symbol}: opened_at updated to fill time {fill_time}")
            except Exception as ft_upd_err:
                log.warning(f"{symbol}: could not update opened_at: {ft_upd_err}")
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

# Deduplication cache: key = (symbol, side, entry) -> timestamp of last attempt
# Blocks identical signals within DEDUP_WINDOW seconds to prevent TradingView multi-fire
_dedup_cache = {}
DEDUP_WINDOW = 90  # seconds


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
        # Zone entry alerts are notification-only (no JSON) — silently ignore
        if "zone entered" in raw_body or "zone entered" in raw_body.lower():
            log.info(f"Zone entry notification received (no JSON expected): {raw_body[:80]}")
            return jsonify({"status": "ok", "message": "Zone entry notification received"}), 200

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
    variant       = data.get("variant",   "")
    test_mode     = str(data.get("testMode",     "false")).lower() == "true"
    test_bal_pct  = float(data.get("testBalancePct",  0.1))
    test_leverage = int(data.get("testLeverage", 2))
    log.info(f"Parsed: symbol={symbol} side={side} orderType={order_type} entry={entry} sl={sl} tp={tp} barSeconds={bar_seconds} testMode={test_mode}")

    # ── Deduplication guard — block identical signal within DEDUP_WINDOW seconds ─
    _dedup_key = (symbol, side, round(entry, 6))
    _now = time.time()
    _last_seen = _dedup_cache.get(_dedup_key)
    if _last_seen and (_now - _last_seen) < DEDUP_WINDOW:
        _elapsed = int(_now - _last_seen)
        log.warning(f"Duplicate signal ignored: {symbol} {side} @ {entry} — same signal {_elapsed}s ago (window={DEDUP_WINDOW}s)")
        return jsonify({"status": "skipped", "message": f"Duplicate signal — same entry seen {_elapsed}s ago"}), 200
    _dedup_cache[_dedup_key] = _now
    # Prune stale entries to avoid unbounded growth
    for k in [k for k, v in _dedup_cache.items() if _now - v > DEDUP_WINDOW * 2]:
        del _dedup_cache[k]

    if not all([symbol, side, entry, sl, tp]):
        msg = f"Missing required fields — got: {data}"
        log.error(msg)
        return jsonify({"status": "error", "message": msg}), 400

    # ── Server-side filters ───────────────────────────────────────────────────
    tf_label = gsheets._bar_seconds_to_tf(bar_seconds)

    def _log_skip(reason):
        """Log a skipped trade with full metadata then patch all available fields."""
        log_order_skipped(symbol, side, entry, sl, tp, reason)
        try:
            notes_json = json.dumps({
                "rr":                data.get("rr"),
                "slBuf":             data.get("slBuf"),
                "minImpulse":        data.get("minImpulse"),
                "entryOffset":       data.get("entryOffset"),
                "obSizeAtr":         data.get("obSizeAtr"),
                "impulseRatioActual":data.get("impulseRatioActual"),
                "structureOk":       data.get("structureOk"),
                "klNear":            data.get("klNear"),
                "klDistAtr":         data.get("klDistAtr"),
                "emaOk":             data.get("emaOk"),
            })
            conn = get_db()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE trades SET timeframe=%s, source=%s, variant=%s, leverage=%s, notes=%s "
                    "WHERE id = (SELECT id FROM trades WHERE status='skipped' AND symbol=%s AND side=%s "
                    "AND timeframe IS NULL ORDER BY opened_at DESC LIMIT 1)",
                    (tf_label, source, variant, cfg.get("leverage"), notes_json, symbol, side)
                )
                updated = cur.rowcount
            conn.commit()
            conn.close()
            log.info(f"Patched skipped trade: {symbol} {side} source={source} tf={tf_label} rows={updated}")
        except Exception as _e:
            log.warning(f"Could not patch skipped trade metadata: {_e}")

    # ── Restricted time window check ──────────────────────────────────────────
    is_restricted, restrict_reason = is_restricted_time(RESTRICTED_TIMES, TIMEZONE_OFFSET)
    if is_restricted:
        log.info(f"{symbol} {side} skipped — {restrict_reason}")
        _log_skip(restrict_reason)
        return jsonify({"status": "skipped", "message": restrict_reason}), 200

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
        _log_skip(f"Filtered: {reason}")
        return jsonify({"status": "filtered", "message": reason}), 200

    # Side filter: FILTER_SIDE=long or short
    if cfg["filter_side"]:
        trade_side = "long" if side == "Buy" else "short"
        if trade_side != cfg["filter_side"]:
            return _filter_skip(f"{symbol} {side} blocked — FILTER_SIDE={cfg['filter_side']}")

    # Cooldown filter: COOLDOWN_LOSSES=3 COOLDOWN_HOURS=24 (per-symbol)
    if cfg["cooldown_losses"] > 0:
        blocked, reason = _check_cooldown(side, cfg, symbol)
        if blocked:
            return _filter_skip(f"{symbol} {side} — {reason}")

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

    # ── JOURNAL ONLY MODE ─────────────────────────────────────────────────────
    # When JOURNAL_ONLY=true, Bybit places orders natively via their webhook.
    # This server just logs the alert for journal tracking.
    if cfg.get("journal_only"):
        notes = json_lib.dumps({
            "rr":                data.get("rr"),
            "slBuf":             data.get("slBuf"),
            "minImpulse":        data.get("minImpulse"),
            "entryOffset":       data.get("entryOffset"),
            "obSizeAtr":         data.get("obSizeAtr"),
            "impulseRatioActual":data.get("impulseRatioActual"),
            "structureOk":       data.get("structureOk"),
            "klNear":            data.get("klNear"),
            "klDistAtr":         data.get("klDistAtr"),
            "emaOk":             data.get("emaOk"),
        })
        # Use a placeholder order_id — will be matched by WebSocket on fill
        placeholder_id = f"bybit_native_{symbol}_{side}_{int(datetime.utcnow().timestamp())}"
        row_id = log_order_placed(
            symbol, side, 0, entry, sl, tp, placeholder_id,
            source=source, timeframe=tf_label, leverage=cfg["leverage"], notes=notes, variant=variant
        )
        log.info(f"📝 Journal-only mode: logged {symbol} {side} @ {entry} SL={sl} TP={tp} (row {row_id})")
        return jsonify({"status": "ok", "message": f"Logged to journal — Bybit places order natively"}), 200

    # ── Everything from here is locked — one trade at a time ─────────────────
    with trade_lock:
        # ── Check max simultaneous trades ─────────────────────────────────────
        open_positions = get_open_positions()
        log.info(f"Open positions: {open_positions}")

        if symbol not in open_positions and len(open_positions) >= cfg["max_trades"]:
            msg = f"Max trades ({cfg['max_trades']}) reached — skipping {symbol}"
            log.warning(msg)
            _log_skip(msg)
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
                    _log_skip(msg)
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
                    _log_skip(msg)
                    return jsonify({"status": "skipped", "message": msg}), 200
            except Exception as e:
                log.error(f"Error checking position direction for {symbol}: {e}")
                msg = f"Already have open position in {symbol} — skipping"
                log.warning(msg)
                _log_skip(msg)
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
                _log_skip(msg)
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
        # If instrument info unavailable, infer from actual prices
        if price_scale == 5:
            for p in [entry, sl, tp]:
                if p > 0:
                    s = str(p)
                    if "." in s:
                        price_scale = max(price_scale, len(s.rstrip("0").split(".")[1]))
            price_scale = min(price_scale, 8)  # cap at 8
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
                orderLinkId  = f"{BOT_ORDER_TAG}{uuid.uuid4().hex[:20]}",
            )
            # Only include price for limit orders
            if order_type == "Limit":
                order_params["price"] = entry_str

            resp     = session.place_order(**order_params)
            ret_code = resp.get("retCode", -1)
            if ret_code == 0:
                order_id = resp.get("result", {}).get("orderId", "?")
                with _bot_order_ids_lock:
                    _bot_order_ids.add(order_id)
                    _bot_order_ids_order.append(order_id)
                    while len(_bot_order_ids_order) > _BOT_ORDER_IDS_MAX:
                        _bot_order_ids.discard(_bot_order_ids_order.popleft())
                log.info(f"✅ {order_type} order placed: {symbol} {side} {qty} @ {entry_str} | SL {sl_str} | TP {tp_str} | ID {order_id}")
                log_order_placed(symbol, side, qty, entry, sl, tp, order_id,
                                 source=source + ("_test" if test_mode else ""),
                                 timeframe=gsheets._bar_seconds_to_tf(bar_seconds),
                                 leverage=actual_leverage,
                                 variant=variant,
                                 notes=json.dumps({
                                     "rr":                data.get("rr"),
                                     "slBuf":             data.get("slBuf"),
                                     "minImpulse":        data.get("minImpulse"),
                                     "entryOffset":       data.get("entryOffset"),
                                     "obSizeAtr":         data.get("obSizeAtr"),
                                     "impulseRatioActual":data.get("impulseRatioActual"),
                                     "structureOk":       data.get("structureOk"),
                                     "klNear":            data.get("klNear"),
                                     "klDistAtr":         data.get("klDistAtr"),
                                     "emaOk":             data.get("emaOk"),
                                 }))
                # Persist tp1/tp1Pct (partial-exit level) if this alert included one —
                # read back at fill time so the trail watcher can register the partial
                # exit, and survives server restarts via the recovery query below.
                tp1_raw     = data.get("tp1")
                tp1_pct_raw = data.get("tp1Pct")
                if tp1_raw is not None:
                    try:
                        tconn = get_db()
                        with tconn.cursor() as tcur:
                            tcur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp1 DOUBLE PRECISION")
                            tcur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp1_pct DOUBLE PRECISION")
                            tcur.execute("UPDATE trades SET tp1=%s, tp1_pct=%s WHERE order_id=%s",
                                        (float(tp1_raw), float(tp1_pct_raw or 0), order_id))
                        tconn.commit()
                        tconn.close()
                        log.info(f"Partial exit stored: {symbol} tp1={tp1_raw} ({tp1_pct_raw}%)")
                    except Exception as _tp1_err:
                        log.warning(f"Could not persist tp1/tp1Pct for {order_id}: {_tp1_err}")
                # Note: Google Sheets push happens when trade CLOSES via WebSocket
                # This avoids cluttering the sheet with trades that never fill
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
                _log_skip(f"Bybit {ret_code}: {msg}")
                return jsonify({"status": "error", "message": msg, "code": ret_code}), 400

        except Exception as e:
            log.error(f"Exception placing order: {e}")
            _log_skip(f"Exception: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/status", methods=["GET"])
def status():
    """Health check — shows current config, open positions and balance."""
    try:
        cfg       = get_config()
        positions = get_open_positions()
        balance   = get_available_balance()

        try:
            buy_cd  = _check_cooldown("Buy",  cfg)[1] or "none" if cfg["cooldown_losses"] else "disabled"
            sell_cd = _check_cooldown("Sell", cfg)[1] or "none" if cfg["cooldown_losses"] else "disabled"
        except Exception as cd_err:
            buy_cd  = f"error: {cd_err}"
            sell_cd = f"error: {cd_err}"

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
            "trail": {
                "extend_beyond_tp":    EXTEND_BEYOND_TP,
                "tp_extend_trigger_r": TP_EXTEND_TRIGGER_R,
                "trail_step_r":        TRAIL_STEP_R,
                "be_trigger_r":        BE_TRIGGER_R,
                "tracked_trades":      len(_trail_state),
            },
            "filters": {
                "side":            cfg["filter_side"]          or "both",
                "min_wr":          cfg["filter_min_wr"]        or "disabled",
                "sources":         cfg["filter_sources"]       or "all",
                "timeframes":      cfg["filter_timeframes"]    or "all",
                "symbols_allow":   cfg["filter_symbols_allow"] or "all",
                "symbols_block":   cfg["filter_symbols_block"] or "none",
                "cooldown_losses": cfg["cooldown_losses"],
                "cooldown_hours":  cfg["cooldown_hours"],
                "buy_cooldown":    buy_cd,
                "sell_cooldown":   sell_cd,
            },
        })
    except Exception as e:
        import traceback
        log.error(f"Status error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/poll", methods=["GET", "POST"])
def manual_poll():
    """Show journal state. Skips REST calls if WebSocket is active."""
    try:
        import psycopg2.extras
        conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, symbol, side, status, order_id, opened_at FROM trades ORDER BY opened_at DESC LIMIT 20")
            all_trades = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT COUNT(*) as cnt FROM trades WHERE status = 'open'")
            open_count = cur.fetchone()["cnt"]
        conn.close()

        if _ws_connected:
            msg = f"WebSocket active — {open_count} open trades updating in real-time"
        else:
            # WebSocket not connected — try REST
            if _poll_lock.acquire(blocking=False):
                try:
                    _check_closed_trades()
                finally:
                    _poll_lock.release()
            msg = f"Poll complete — found {open_count} open trades"

        return jsonify({
            "status":       "ok",
            "open_count":   open_count,
            "recent_trades": all_trades,
            "message":      msg,
            "ws_connected": _ws_connected,
        }), 200
    except Exception as e:
        import traceback
        log.error(f"Poll error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/debug/auth", methods=["GET"])
def debug_auth():
    """Test Bybit auth with a simple authenticated call."""
    try:
        resp = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        bal  = resp.get("result", {}).get("list", [{}])[0].get("totalWalletBalance", "?")
        resp2 = session.get_positions(category="linear", settleCoin="USDT", limit=1)
        pos_ok = resp2.get("retCode", -1) == 0
        resp3 = session.get_open_orders(category="linear", symbol="BTCUSDT")
        orders_ok = resp3.get("retCode", -1) == 0
        return jsonify({
            "wallet_balance": bal,
            "positions_ok":   pos_ok,
            "open_orders_ok": orders_ok,
            "open_orders_ret": resp3.get("retCode"),
            "open_orders_msg": resp3.get("retMsg"),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/ip", methods=["GET"])
def check_ip():
    """Check what IP Railway is using."""
    try:
        import urllib.request
        ip = urllib.request.urlopen("https://api.ipify.org").read().decode()
        return jsonify({"ip": ip, "message": "This is the IP Bybit sees"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/journal/restricted-times")
def journal_restricted_times():
    """Returns current restricted time status and parsed windows."""
    is_restricted, reason = is_restricted_time(RESTRICTED_TIMES, TIMEZONE_OFFSET)
    windows = []
    for w in RESTRICTED_TIMES.split("|"):
        w = w.strip()
        if w:
            windows.append(w)
    return jsonify({
        "is_restricted": is_restricted,
        "reason": reason,
        "windows": windows,
        "timezone_offset": TIMEZONE_OFFSET,
        "raw": RESTRICTED_TIMES,
    })


@app.route("/debug/trail")
def debug_trail():
    """Show current trail watcher state."""
    with _trail_lock:
        state = dict(_trail_state)
    # Get current prices for tracked trades
    result = []
    for order_id, s in state.items():
        try:
            resp = _api_call(session.get_tickers, category="linear", symbol=s["symbol"])
            mark = float(resp.get("result", {}).get("list", [{}])[0].get("markPrice", 0))
            risk = s["risk"]
            current_r = (mark - s["entry"]) / risk if s["side"] == "Buy" else (s["entry"] - mark) / risk
        except:
            mark = 0
            current_r = 0
        result.append({
            "order_id":    order_id,
            "symbol":      s["symbol"],
            "side":        s["side"],
            "entry":       s["entry"],
            "sl":          s["sl"],
            "trail_sl":    s["trail_sl"],
            "risk":        s["risk"],
            "tp_removed":  s["tp_removed"],
            "be_done":     s["be_done"],
            "mark_price":  mark,
            "current_r":   round(current_r, 3),
        })
    return jsonify({
        "extend_beyond_tp":    EXTEND_BEYOND_TP,
        "tp_extend_trigger_r": TP_EXTEND_TRIGGER_R,
        "trail_step_r":        TRAIL_STEP_R,
        "be_trigger_r":        BE_TRIGGER_R,
        "tracked_trades":      len(state),
        "trades":              result,
    })


@app.route("/journal/debug-skipped")
def debug_skipped():
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM trades GROUP BY status ORDER BY COUNT(*) DESC")
            counts = [{"status": r[0], "count": r[1]} for r in cur.fetchall()]
            cur.execute("SELECT * FROM trades WHERE status='skipped' ORDER BY opened_at DESC LIMIT 3")
            cols   = [d[0] for d in cur.description]
            sample = [dict(zip(cols, row)) for row in cur.fetchall()]
        conn.close()
        import json as _j
        return jsonify({"status_counts": counts, "sample_skipped": _j.loads(_j.dumps(sample, default=str))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/analyse-trade", methods=["POST", "OPTIONS"])
def analyse_trade():
    cors = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type"}
    if request.method == "OPTIONS":
        r = jsonify({}); [r.headers.__setitem__(k, v) for k, v in cors.items()]; return r
    try:
        import urllib.request as _ur, json as _j, os as _os
        body    = request.get_json(force=True)
        api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return jsonify({"error": "ANTHROPIC_API_KEY not set"}), 500, cors
        payload = _j.dumps({"model": "claude-sonnet-4-6", "max_tokens": 500,
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"type": "url", "url": body.get("image_url", "")}},
                {"type": "text", "text": body.get("prompt", "")}]}]}).encode()
        req = _ur.Request("https://api.anthropic.com/v1/messages", data=payload,
            headers={"Content-Type": "application/json", "x-api-key": api_key, "anthropic-version": "2023-06-01"})
        with _ur.urlopen(req, timeout=30) as r:
            data = _j.loads(r.read())
        resp = jsonify({"text": data.get("content", [{}])[0].get("text", "")})
        [resp.headers.__setitem__(k, v) for k, v in cors.items()]
        return resp
    except Exception as e:
        resp = jsonify({"error": str(e)}); [resp.headers.__setitem__(k, v) for k, v in cors.items()]; return resp, 500


@app.route("/journal/sync-sheets", methods=["POST"])
def sync_sheets():
    """Push all closed trades to Google Sheets."""
    if not gsheets.is_configured():
        return jsonify({"status": "error", "message": "Google Sheets not configured"}), 400
    try:
        import psycopg2.extras as _pge
        conn = get_db()
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            cur.execute("SELECT * FROM trades WHERE status='closed' AND outcome IN ('tp','sl','tp1_tp2','tp1_sl') AND source != 'bybit_import' ORDER BY opened_at ASC LIMIT 200")
            trades = [dict(r) for r in cur.fetchall()]
        conn.close()
        synced = 0
        for t in trades:
            try:
                notes_str = str(t.get("notes") or "")
                if "sheet_row:" in notes_str:
                    continue  # already synced
                gsheets.push_closed_trade(
                    symbol=t["symbol"], side=t.get("side","Buy"),
                    qty=float(t.get("qty") or 0),
                    entry=float(t.get("entry") or 0),
                    sl=float(t.get("sl") or 0),
                    tp=float(t.get("tp") or 0),
                    exit_price=float(t.get("exit_price") or 0),
                    pnl=float(t.get("pnl") or 0),
                    outcome=t.get("outcome",""),
                    source=t.get("source","ob"),
                    timeframe=t.get("timeframe",""),
                    leverage=int(t.get("leverage") or 1),
                    opened_at=str(t.get("opened_at") or ""),
                    closed_at=str(t.get("closed_at") or ""),
                    tp1=float(t["tp1"]) if t.get("tp1") is not None else None,
                    tp1_pct=float(t["tp1_pct"]) if t.get("tp1_pct") is not None else None
                )
                synced += 1
            except Exception as te:
                log.warning(f"Sheets sync failed for trade {t.get('id')}: {te}")
        return jsonify({"status": "ok", "message": f"Synced {synced} trades to Sheets"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/edit-note", methods=["POST"])
def edit_note():
    """Edit the text of a note row."""
    try:
        body = request.get_json(force=True)
        note_id = int(body.get("id", 0))
        note    = body.get("note", "").strip()
        if not note:
            return jsonify({"status": "error", "message": "Note cannot be empty"}), 400
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("UPDATE trades SET notes=%s WHERE id=%s AND status='note'", (note, note_id))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/delete-note/<int:note_id>", methods=["POST"])
def delete_note(note_id):
    """Delete a note row."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trades WHERE id=%s AND status='note'", (note_id,))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/add-note", methods=["POST"])
def add_note():
    """Add a manual note/milestone row to the journal."""
    try:
        body  = request.get_json(force=True)
        note  = body.get("note", "").strip()
        if not note:
            return jsonify({"status": "error", "message": "Note cannot be empty"}), 400
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades
                    (symbol, side, status, qty, entry, sl, tp, source, opened_at, notes)
                VALUES (%s, %s, 'note', 0, 0, 0, 0, 'manual', %s, %s)
            """, ('—', '—', datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), note))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/get-media", methods=["GET"])
def get_media():
    """Get media links for a trade."""
    try:
        trade_id = int(request.args.get("id", 0))
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT media FROM trades WHERE id=%s", (trade_id,))
            row = cur.fetchone()
        conn.close()
        return jsonify({"status": "ok", "media": row[0] if row and row[0] else ""}), 200
    except Exception as e:
        return jsonify({"status": "ok", "media": ""}), 200


@app.route("/journal/set-user-notes", methods=["POST"])
def set_user_notes():
    """Set manual user notes for a trade."""
    try:
        body     = request.get_json(force=True)
        trade_id = int(body.get("id", 0))
        notes    = body.get("user_notes", "").strip()
        conn = get_db()
        with conn.cursor() as cur:
            # Add column if it doesn't exist yet
            cur.execute("""
                ALTER TABLE trades ADD COLUMN IF NOT EXISTS user_notes TEXT
            """)
            cur.execute("UPDATE trades SET user_notes=%s WHERE id=%s", (notes or None, trade_id))
        conn.commit()
        conn.close()
        log.info(f"User notes set for trade {trade_id}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/set-media", methods=["POST"])
def set_media():
    """Set media links for a trade — also syncs to Google Sheets column BE."""
    try:
        body     = request.get_json(force=True)
        trade_id = int(body.get("id", 0))
        media    = body.get("media", "").strip()
        conn = get_db()
        import psycopg2.extras as _pge
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            cur.execute("UPDATE trades SET media=%s WHERE id=%s", (media or None, trade_id))
            cur.execute("SELECT notes FROM trades WHERE id=%s", (trade_id,))
            row = cur.fetchone()
        conn.commit()
        conn.close()

        # Sync to Google Sheets column BE if this trade has a sheet row
        if media and gsheets.is_configured() and row and row.get("notes"):
            notes_str = str(row["notes"])
            if "sheet_row:" in notes_str:
                try:
                    sheet_row = int(notes_str.split("sheet_row:")[1].split("|")[0])
                    gsheets.push_media_link(sheet_row, media)
                except Exception as gs_err:
                    log.warning(f"Sheets media sync failed: {gs_err}")

        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/set-pnl", methods=["POST"])
def set_pnl():
    """Manually set PnL for a trade."""
    try:
        body     = request.get_json(force=True)
        trade_id = int(body.get("id", 0))
        pnl      = float(body.get("pnl", 0))
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("UPDATE trades SET pnl=%s WHERE id=%s", (pnl, trade_id))
        conn.commit()
        conn.close()
        log.info(f"Manual PnL set: trade {trade_id} → {pnl}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/set-outcome", methods=["POST"])
def set_outcome():
    """Manually set TP/SL outcome for a trade."""
    try:
        body    = request.get_json(force=True)
        trade_id = int(body.get("id", 0))
        outcome  = body.get("outcome", "").strip().lower()
        if outcome not in ("tp", "sl", "tp1_tp2", "tp1_sl", ""):
            return jsonify({"status": "error", "message": "outcome must be tp, sl, tp1_tp2, tp1_sl or empty"}), 400
        conn = get_db()
        with conn.cursor() as cur:
            if outcome:
                cur.execute("UPDATE trades SET outcome=%s WHERE id=%s", (outcome, trade_id))
            else:
                cur.execute("UPDATE trades SET outcome=NULL WHERE id=%s", (trade_id,))
        conn.commit()
        conn.close()
        log.info(f"Manual outcome set: trade {trade_id} → {outcome or 'NULL'}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/set-timeframe", methods=["POST"])
def set_timeframe():
    """Manually set the timeframe for a trade — mainly for manual trades, which
    have no bar-based source to infer one from."""
    try:
        body      = request.get_json(force=True)
        trade_id  = int(body.get("id", 0))
        timeframe = body.get("timeframe", "").strip()
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("UPDATE trades SET timeframe=%s WHERE id=%s", (timeframe or None, trade_id))
        conn.commit()
        conn.close()
        log.info(f"Manual timeframe set: trade {trade_id} → {timeframe or 'NULL'}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/delete-older-than/<int:days>", methods=["GET", "POST"])
def delete_older_than(days):
    """Delete all trades older than X days."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM trades WHERE opened_at::timestamp < NOW() - INTERVAL '1 day' * %s",
                (days,)
            )
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "message": f"Deleted {deleted} trades older than {days} days"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/reset", methods=["GET", "POST"])
def journal_reset():
    """Delete ALL trades from the journal — start fresh."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trades")
            deleted = cur.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "message": f"Deleted {deleted} trades — journal is now empty"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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

        # Only import last 7 days
        start_ms = int((datetime.utcnow().timestamp() - 7 * 86400) * 1000)
        resp    = _api_call(session.get_closed_pnl, category="linear", limit=200, startTime=start_ms)
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


def _explain_ob_detection(bars, atrs, cfg, target_bar_index=None):
    """
    Full pipeline analysis — walks through every gate from OB detection
    to server-side filters, explaining exactly where each bar was blocked.
    """
    mi             = cfg.get("impulseRatio",    1.3)
    cob_mult       = cfg.get("cobMinAtrMult",   0.8)
    doji_pct       = cfg.get("dojiPct",         15.0)
    sl_buf         = cfg.get("slAtrMult",        0.1)
    eo             = cfg.get("entryOffset",      0.0)
    rr             = cfg.get("rrRatio",          3.0)
    kl_prox        = cfg.get("klProximity",      1.0)
    kl_min_score   = cfg.get("klMinScore",       6)
    kl_bonus       = cfg.get("klSignificanceBonus", 0.0)
    approach_atr   = cfg.get("approachMaxAtr",   1.5)
    use_kl_filter  = cfg.get("useKeyLevelFilter", False)
    range_lb       = cfg.get("rangeLookback",    40)
    range_tol      = cfg.get("rangeTolPct",      15.0)
    range_min_alt  = cfg.get("rangeMinTouches",  2)
    use_range      = cfg.get("useRangeFilter",   False)

    # Pre-compute key levels for KL filter
    kl_levels = _bt_find_key_levels(bars, lookback=min(range_lb * 5, 500),
                                     min_score=kl_min_score) if use_kl_filter else []

    results = []
    start = len(bars) - 1
    end   = max(2, len(bars) - 50)
    if target_bar_index is not None:
        idx   = len(bars) - 1 - target_bar_index
        start = min(idx + 10, len(bars) - 1)
        end   = max(2, idx - 10)

    for i in range(start, end, -1):
        if i < 2 or i >= len(bars):
            continue
        atr = atrs[i]
        if not atr or atr == 0:
            continue

        b0 = bars[i]
        b1 = bars[i - 1]
        b2 = bars[i - 2]

        body0 = abs(b0["close"] - b0["open"])
        body1 = abs(b1["close"] - b1["open"])
        rng1  = b1["high"] - b1["low"]
        ts    = datetime.utcfromtimestamp(b0["ts"] / 1000).strftime("%Y-%m-%d %H:%M")

        # ── Gate 1: OB Detection ──────────────────────────────────────────────
        def ob_checks(is_bull):
            checks = []
            if is_bull:
                checks.append({"name": "OB candle bearish",     "pass": b1["close"] < b1["open"],
                    "detail": f"open={b1['open']:.5f} close={b1['close']:.5f}"})
                checks.append({"name": "Impulse candle bullish", "pass": b0["close"] > b0["open"],
                    "detail": f"open={b0['open']:.5f} close={b0['close']:.5f}"})
                checks.append({"name": "Impulse closes above OB high", "pass": b0["close"] > b1["high"],
                    "detail": f"impulse_close={b0['close']:.5f} ob_high={b1['high']:.5f}"})
            else:
                checks.append({"name": "OB candle bullish",     "pass": b1["close"] > b1["open"],
                    "detail": f"open={b1['open']:.5f} close={b1['close']:.5f}"})
                checks.append({"name": "Impulse candle bearish", "pass": b0["close"] < b0["open"],
                    "detail": f"open={b0['open']:.5f} close={b0['close']:.5f}"})
                checks.append({"name": "Impulse closes below OB low", "pass": b0["close"] < b1["low"],
                    "detail": f"impulse_close={b0['close']:.5f} ob_low={b1['low']:.5f}"})
            doji = rng1 > 0 and (body1 / rng1 * 100) < doji_pct
            checks.append({"name": f"OB not a doji (>{doji_pct:.0f}% body)",
                "pass": not doji,
                "detail": f"body={body1/rng1*100:.1f}% of range" if rng1 > 0 else "zero range"})
            checks.append({"name": f"OB body >= {cob_mult}× ATR",
                "pass": body1 >= atr * cob_mult,
                "detail": f"body={body1:.5f}  need={atr*cob_mult:.5f}  ATR={atr:.5f}"})
            checks.append({"name": f"Impulse >= {mi}× OB body",
                "pass": body1 > 0 and (body0 / body1) >= mi,
                "detail": f"ratio={body0/body1:.2f}×  need={mi}×" if body1 > 0 else "OB body=0"})
            return checks

        bull_checks = ob_checks(True)
        bear_checks = ob_checks(False)
        bull_pass   = all(c["pass"] for c in bull_checks)
        bear_pass   = all(c["pass"] for c in bear_checks)
        ob_type     = "Bull OB" if bull_pass else ("Bear OB" if bear_pass else None)
        detected    = bull_pass or bear_pass

        if not detected:
            results.append({
                "bar": i, "timestamp": ts, "detected": False, "ob_type": None,
                "gate": "1. OB Detection", "gate_pass": False,
                "bull_checks": bull_checks, "bear_checks": bear_checks,
                "pipeline": [], "atr": round(atr, 6), "trade": None,
                "candles": {
                    "b0": {"o": b0["open"], "h": b0["high"], "l": b0["low"], "c": b0["close"]},
                    "b1": {"o": b1["open"], "h": b1["high"], "l": b1["low"], "c": b1["close"]},
                }
            })
            if len(results) >= 20: break
            continue

        # OB detected — compute entry/sl/tp
        ob_top = max(b1["open"], b1["close"])
        ob_bot = min(b1["open"], b1["close"])
        ob_mid = (ob_top + ob_bot) / 2
        if bull_pass:
            entry = ob_top - eo * 2.0 * (ob_top - ob_mid)
            sl    = min(b1["low"], b0["low"]) - atr * sl_buf
        else:
            entry = ob_bot + eo * 2.0 * (ob_mid - ob_bot)
            sl    = max(b1["high"], b0["high"]) + atr * sl_buf
        risk = abs(entry - sl)
        tp   = (entry + risk * rr) if bull_pass else (entry - risk * rr)
        trade = {"entry": round(entry, 6), "sl": round(sl, 6),
                 "tp": round(tp, 6), "risk_r": round(risk, 6)}

        # ── Gate 2: KL Filter ─────────────────────────────────────────────────
        pipeline = []
        kl_pass = True
        kl_detail = "KL filter disabled"
        kl_level  = None
        if use_kl_filter:
            ob_height = ob_top - ob_bot
            slack     = ob_height * kl_prox
            mid       = (ob_top + ob_bot) / 2
            for lv in kl_levels:
                lp     = lv["price"]
                is_res = lp > mid
                if bull_pass and not is_res and ob_bot - slack <= lp <= ob_bot + slack * 0.5:
                    kl_level = lp; break
                if bear_pass and is_res and ob_top - slack * 0.5 <= lp <= ob_top + slack:
                    kl_level = lp; break
            kl_pass   = kl_level is not None
            kl_detail = (f"Found KL at {kl_level:.5f} (score filter ≥{kl_min_score})"
                         if kl_pass else
                         f"No {'support' if bull_pass else 'resistance'} level within {kl_prox}× OB height ({ob_height:.5f}) of OB {'bottom' if bull_pass else 'top'}")
        pipeline.append({"gate": "2. Key Level Filter",
            "pass": kl_pass, "detail": kl_detail,
            "skipped": not use_kl_filter})

        # ── Gate 3: Range Filter ──────────────────────────────────────────────
        range_pass   = True
        range_detail = "Range filter disabled"
        if use_range and i >= range_lb:
            rh = max(b["high"] for b in bars[i - range_lb:i])
            rl = min(b["low"]  for b in bars[i - range_lb:i])
            tol = (rh - rl) * range_tol / 100
            last_side = 0; alt_count = 0
            for j in range(i - range_lb, i):
                b = bars[j]
                in_sup = b["low"]  <= rl + tol and b["low"]  >= rl  - tol and b["close"] > rl
                in_res = b["high"] >= rh - tol and b["high"] <= rh  + tol and b["close"] < rh
                if in_sup and last_side != 1:
                    if last_side == 2: alt_count += 1
                    last_side = 1
                elif in_res and last_side != 2:
                    if last_side == 1: alt_count += 1
                    last_side = 2
            is_ranging = alt_count >= range_min_alt and rl <= b0["close"] <= rh
            range_pass   = not is_ranging
            range_detail = (f"RANGING — {alt_count} alternations in {range_lb} bars (high={rh:.5f} low={rl:.5f})"
                            if is_ranging else
                            f"Not ranging — {alt_count}/{range_min_alt} alternations needed")
        pipeline.append({"gate": "3. Range Filter",
            "pass": range_pass, "detail": range_detail,
            "skipped": not use_range})

        # ── Gate 4: Approach Speed Filter (3-candle rule) ─────────────────────
        approach_pass   = True
        approach_detail = "Approach filter disabled (approachMaxAtr=0)"
        if approach_atr > 0:
            c0_body = abs(b0["close"] - b0["open"])
            c1_body = abs(bars[i-1]["close"] - bars[i-1]["open"]) if i >= 1 else 0
            c2_body = abs(bars[i-2]["close"] - bars[i-2]["open"]) if i >= 2 else 0
            thresh  = atr * approach_atr
            c0_ok   = c0_body < thresh
            c1_ok   = c1_body < thresh
            c2_ok   = c2_body < thresh
            approach_pass = c0_ok and c1_ok and c2_ok
            approach_detail = (
                f"All 3 candles calm (max body={max(c0_body,c1_body,c2_body):.5f} < {thresh:.5f})"
                if approach_pass else
                f"Candle too large — [0]={c0_body:.5f}{'✅' if c0_ok else '❌'}  [1]={c1_body:.5f}{'✅' if c1_ok else '❌'}  [2]={c2_body:.5f}{'✅' if c2_ok else '❌'}  threshold={thresh:.5f}"
            )
        pipeline.append({"gate": "4. Approach Speed (3-candle rule)",
            "pass": approach_pass, "detail": approach_detail,
            "skipped": approach_atr == 0})

        # ── Gate 5: Server-side filters ───────────────────────────────────────
        srv_cfg   = get_config()
        srv_gates = []

        # Max trades
        open_pos = get_open_positions()
        max_t    = srv_cfg["max_trades"]
        srv_gates.append({"gate": "5a. Max trades",
            "pass": len(open_pos) < max_t,
            "detail": f"{len(open_pos)}/{max_t} positions open"})

        # Symbol allow/block
        sym_allow = srv_cfg["filter_symbols_allow"]
        sym_block = srv_cfg["filter_symbols_block"]
        sym_ok    = (not sym_allow or b0.get("symbol","") in sym_allow.split(",")) and \
                    (not sym_block or b0.get("symbol","") not in sym_block.split(","))
        srv_gates.append({"gate": "5b. Symbol filter",
            "pass": True,  # can't check without symbol in bar data
            "detail": f"Allow: {sym_allow or 'all'}  Block: {sym_block or 'none'}"})

        # Cooldown
        side_str  = "Buy" if bull_pass else "Sell"
        cd_blocked, cd_reason = _check_cooldown(side_str, srv_cfg)
        srv_gates.append({"gate": "5c. Cooldown filter",
            "pass": not cd_blocked,
            "detail": cd_reason if cd_blocked else f"No cooldown active for {side_str}"})

        # Min WR
        min_wr = srv_cfg["filter_min_wr"]
        srv_gates.append({"gate": "5d. Min WR filter",
            "pass": True,
            "detail": f"Min WR: {min_wr}%" if min_wr > 0 else "Disabled"})

        all_srv_pass = all(g["pass"] for g in srv_gates)

        # ── Final verdict ─────────────────────────────────────────────────────
        all_pass = kl_pass and range_pass and approach_pass and all_srv_pass
        first_fail = None
        for g in pipeline + srv_gates:
            if not g["pass"] and not g.get("skipped"):
                first_fail = g["gate"]
                break

        results.append({
            "bar": i, "timestamp": ts,
            "detected": True, "ob_type": ob_type,
            "gate": first_fail or "All gates passed ✅",
            "gate_pass": all_pass,
            "bull_checks": bull_checks if bull_pass else [],
            "bear_checks": bear_checks if bear_pass else [],
            "pipeline": pipeline,
            "server_gates": srv_gates,
            "atr": round(atr, 6),
            "trade": trade,
            "candles": {
                "b0": {"o": b0["open"], "h": b0["high"], "l": b0["low"], "c": b0["close"]},
                "b1": {"o": b1["open"], "h": b1["high"], "l": b1["low"], "c": b1["close"]},
            }
        })
        if len(results) >= 20: break

    return results


SIGNAL_EXPLAINER_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signal Explainer</title>
<style>
:root{--bg:#0f0f0f;--surface:#1a1a1a;--border:#2a2a2a;--text:#e8e8e8;--dim:#888;--green:#4caf50;--red:#ef5350;--blue:#42a5f5;--amber:#ffa726}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,sans-serif;font-size:13px;padding:24px}
h1{font-size:18px;font-weight:500;margin-bottom:4px}
.nav{display:flex;gap:12px;margin-bottom:20px}
.nav a{color:var(--dim);text-decoration:none;font-size:12px}
.section{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px}
.section-title{font-size:12px;font-weight:500;color:var(--dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;margin-bottom:12px}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-size:11px;color:var(--dim)}
.field input,.field select{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:4px;font-size:12px;width:100%}
textarea{width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:8px;border-radius:4px;font-size:11px;font-family:monospace;resize:vertical}
textarea:focus,input:focus,select:focus{outline:none;border-color:var(--blue)}
.btn{padding:7px 16px;border-radius:6px;border:none;cursor:pointer;font-size:12px;font-weight:500;background:var(--blue);color:#000}
.btn:disabled{opacity:.4;cursor:wait}
.btn-sec{background:rgba(96,165,250,.15);color:var(--blue);border:1px solid rgba(96,165,250,.3)}
.bar-result{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px;margin-bottom:10px}
.bar-result.all-pass{border-color:rgba(76,175,80,.5)}
.bar-result.blocked{border-color:rgba(239,83,80,.3)}
.bar-result.no-ob{opacity:.6}
.bar-header{display:flex;align-items:center;gap:10px;cursor:pointer;flex-wrap:wrap}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}
.badge-bull{background:rgba(76,175,80,.15);color:var(--green)}
.badge-bear{background:rgba(239,83,80,.15);color:var(--red)}
.badge-none{background:rgba(107,114,128,.15);color:var(--dim)}
.badge-pass{background:rgba(76,175,80,.15);color:var(--green)}
.badge-fail{background:rgba(239,83,80,.15);color:var(--red)}
.pipeline{margin-top:12px;display:flex;flex-direction:column;gap:6px}
.gate{display:flex;align-items:flex-start;gap:10px;padding:8px 10px;border-radius:6px;font-size:12px}
.gate.pass{background:rgba(76,175,80,.07);border-left:3px solid var(--green)}
.gate.fail{background:rgba(239,83,80,.07);border-left:3px solid var(--red)}
.gate.skipped{background:rgba(107,114,128,.05);border-left:3px solid var(--dim);opacity:.6}
.gate-name{font-weight:500;min-width:220px;flex-shrink:0}
.gate-detail{color:var(--dim);font-size:11px}
.ob-checks{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-top:10px}
.check{padding:5px 8px;border-radius:4px;font-size:11px;background:rgba(255,255,255,.02)}
.check.pass{border-left:2px solid var(--green)}
.check.fail{border-left:2px solid var(--red)}
.check-detail{color:var(--dim);font-size:10px;margin-top:1px}
.trade-box{background:rgba(96,165,250,.07);border:1px solid rgba(96,165,250,.2);border-radius:6px;padding:10px;margin-top:10px;display:flex;gap:20px;flex-wrap:wrap;font-size:12px}
.trade-box .lbl{color:var(--dim);font-size:11px}
.trade-box .val{font-weight:500}
.candle-info{font-size:11px;color:var(--dim);margin:8px 0;font-family:monospace}
.expand-btn{margin-left:auto;font-size:11px;color:var(--dim)}
</style>
</head>
<body>
<div class="nav">
  <a href="/journal">← Journal</a>
  <a href="/backtest/configs">Backtest</a>
</div>
<h1>// Signal Explainer</h1>
<p style="color:var(--dim);font-size:12px;margin-bottom:20px">Walks through every gate in the pipeline — from OB detection to server filters — and shows exactly where a signal was blocked.</p>

<div class="section">
  <div class="section-title">Lookup</div>
  <div class="grid">
    <div class="field"><label>Symbol</label><input id="f-symbol" type="text" value="ETHUSDT"></div>
    <div class="field"><label>Timeframe</label>
      <select id="f-tf">
        <option value="3">M3</option><option value="5">M5</option>
        <option value="15">M15</option><option value="30">M30</option>
        <option value="60">H1</option><option value="240">H4</option>
      </select>
    </div>
    <div class="field"><label>Date/Time UTC (optional)</label><input id="f-time" type="datetime-local"></div>
  </div>

  <div class="section-title" style="margin-top:4px">Indicator Settings — paste status bar or fill manually</div>
  <textarea id="f-statusbar" rows="2" placeholder='Order Blocks [2-Candle Method] (benchmark, 10, ...): Any alert() function call' style="margin-bottom:8px"></textarea>
  <button class="btn btn-sec" onclick="parseStatusBar()" style="margin-bottom:14px">Parse status bar</button>

  <div class="grid">
    <div class="field"><label>ATR Length</label><input id="f-atrlen" type="number" value="17"></div>
    <div class="field"><label>Impulse Ratio</label><input id="f-impulse" type="number" step="0.1" value="1.3"></div>
    <div class="field"><label>Min OB Size (x ATR)</label><input id="f-cobmult" type="number" step="0.1" value="0.8"></div>
    <div class="field"><label>Doji % threshold</label><input id="f-doji" type="number" value="15"></div>
    <div class="field"><label>SL ATR Buffer</label><input id="f-slbuf" type="number" step="0.05" value="0.1"></div>
    <div class="field"><label>Entry Offset</label><input id="f-offset" type="number" step="0.05" value="0.0"></div>
    <div class="field"><label>RR Ratio</label><input id="f-rr" type="number" step="0.1" value="3.0"></div>
    <div class="field"><label>Approach Max ATR</label><input id="f-approach" type="number" step="0.1" value="1.5"></div>
    <div class="field"><label>KL Filter ON</label>
      <select id="f-klfilter"><option value="false">Off</option><option value="true">On</option></select>
    </div>
    <div class="field"><label>KL Proximity</label><input id="f-klprox" type="number" step="0.1" value="1.0"></div>
    <div class="field"><label>KL Min Score</label><input id="f-klscore" type="number" value="6"></div>
    <div class="field"><label>Range Filter ON</label>
      <select id="f-rangefilter"><option value="false">Off</option><option value="true">On</option></select>
    </div>
    <div class="field"><label>Range Lookback</label><input id="f-rangelb" type="number" value="40"></div>
    <div class="field"><label>Range Tol %</label><input id="f-rangetol" type="number" step="1" value="15"></div>
    <div class="field"><label>Range Min Alternations</label><input id="f-rangealt" type="number" value="2"></div>
  </div>
  <button class="btn" id="btn-explain" onclick="runExplainer()">🔍 Explain Pipeline</button>
</div>

<div id="results"></div>

<script>
var STATUS_MAP = ["alertName","maxBlocks","extendBars","htfExtendBars","atrLength",
  "impulseRatio","overlapBars","dojiPct","dirtyObRatio","cobMinAtrMult",
  "rrRatio","winRateMinSamples","winRateLookback","slAtrMult","slMinPct",
  "coolOffBars","maxAlertsPerOB","cancelAfterBars","prepareAtrMult","entryOffset",
  "alertBestWRMinSmp","alertMinWR","klProximity","klLookback2","klMinScore",
  "klTol2","klPvLen","klMaxLevels","klStrongOpacity","klWeakOpacity",
  "klBorderOpacity","klSignificanceBonus","webhookSecret","indicatorVariant",
  "testBalancePct","testLeverage","trendEmaLen","trendEmaTF","emaLookback",
  "timezone","schedStartHour","schedStartMin","schedEndHour","schedEndMin",
  "htf","trendTF","trendLookback","structureHistory"];

function parseStatusBar() {
  var raw = document.getElementById('f-statusbar').value.trim();
  if (!raw) return;
  // Extract values between first ( and last ):
  var start = raw.indexOf('(');
  var end   = raw.lastIndexOf('):');
  if (start === -1 || end === -1) { alert('Could not parse — paste the full status bar string'); return; }
  var inner = raw.substring(start + 1, end);
  var parts = inner.split(', '), s = {};
  STATUS_MAP.forEach(function(n,i){ if(i<parts.length) s[n]=parts[i]; });
  if(s.atrLength)           document.getElementById('f-atrlen').value    = s.atrLength;
  if(s.impulseRatio)        document.getElementById('f-impulse').value   = s.impulseRatio;
  if(s.cobMinAtrMult)       document.getElementById('f-cobmult').value   = s.cobMinAtrMult;
  if(s.dojiPct)             document.getElementById('f-doji').value      = s.dojiPct;
  if(s.slAtrMult)           document.getElementById('f-slbuf').value     = s.slAtrMult;
  if(s.entryOffset)         document.getElementById('f-offset').value    = s.entryOffset;
  if(s.rrRatio)             document.getElementById('f-rr').value        = s.rrRatio;
  if(s.klProximity)         document.getElementById('f-klprox').value    = s.klProximity;
  if(s.klMinScore)          document.getElementById('f-klscore').value   = s.klMinScore;
  alert('✅ Parsed ' + Object.keys(s).length + ' settings');
}

function runExplainer() {
  var btn = document.getElementById('btn-explain');
  btn.disabled = true; btn.textContent = '⏳ Fetching...';
  fetch('/signal-explainer/run', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      symbol:             document.getElementById('f-symbol').value.trim().toUpperCase(),
      tf:                 document.getElementById('f-tf').value,
      time:               document.getElementById('f-time').value,
      atrLength:          parseInt(document.getElementById('f-atrlen').value),
      impulseRatio:       parseFloat(document.getElementById('f-impulse').value),
      cobMinAtrMult:      parseFloat(document.getElementById('f-cobmult').value),
      dojiPct:            parseFloat(document.getElementById('f-doji').value),
      slAtrMult:          parseFloat(document.getElementById('f-slbuf').value),
      entryOffset:        parseFloat(document.getElementById('f-offset').value),
      rrRatio:            parseFloat(document.getElementById('f-rr').value),
      approachMaxAtr:     parseFloat(document.getElementById('f-approach').value),
      useKeyLevelFilter:  document.getElementById('f-klfilter').value,
      klProximity:        parseFloat(document.getElementById('f-klprox').value),
      klMinScore:         parseInt(document.getElementById('f-klscore').value),
      useRangeFilter:     document.getElementById('f-rangefilter').value,
      rangeLookback:      parseInt(document.getElementById('f-rangelb').value),
      rangeTolPct:        parseFloat(document.getElementById('f-rangetol').value),
      rangeMinTouches:    parseInt(document.getElementById('f-rangealt').value),
    })
  }).then(function(r){return r.json();}).then(function(d){
    btn.disabled=false; btn.textContent='🔍 Explain Pipeline';
    if(d.status==='error'){alert('Error: '+d.message);return;}
    renderResults(d.results, d.symbol, d.tf_label);
  }).catch(function(e){btn.disabled=false;btn.textContent='🔍 Explain Pipeline';alert(''+e);});
}

function toggleEl(uid){
  var el=document.getElementById(uid);
  if(el) el.style.display=el.style.display==='none'?'block':'none';
}
function countPass(checks){return checks.filter(function(c){return c.pass;}).length;}

function renderResults(results, symbol, tfLabel) {
  var el = document.getElementById('results');
  if(!results||!results.length){
    el.innerHTML='<div class="section"><p style="color:var(--dim)">No bars found.</p></div>';return;
  }
  var allPass = results.filter(function(r){return r.gate_pass;});
  var html = '<div class="section"><div class="section-title">'
    +symbol+' '+tfLabel+' — '+results.length+' bars analysed'
    +(allPass.length?' | <span style="color:var(--green)">'+allPass.length+' fully passed</span>':'')
    +'</div>';

  results.forEach(function(r){
    var uid = 'bar-'+r.bar;
    var cardCls = !r.detected ? 'no-ob' : r.gate_pass ? 'all-pass' : 'blocked';
    var obBadge = r.detected
      ? '<span class="badge '+(r.ob_type==='Bull OB'?'badge-bull':'badge-bear')+'">'+r.ob_type+'</span>'
      : '<span class="badge badge-none">No OB</span>';
    var statusBadge = !r.detected ? ''
      : r.gate_pass
        ? '<span class="badge badge-pass">✅ All gates passed</span>'
        : '<span class="badge badge-fail">❌ Blocked: '+r.gate+'</span>';

    html+='<div class="bar-result '+cardCls+'">';
    html+='<div class="bar-header" onclick="toggleEl(\''+uid+'\')"><span style="font-size:13px;font-weight:500">'+r.timestamp+'</span>';
    html+=obBadge+statusBadge;
    html+='<span style="font-size:10px;color:var(--dim)">ATR='+r.atr+'</span>';
    html+='<span class="expand-btn">▼</span></div>';

    html+='<div id="'+uid+'" style="display:none">';

    // Candle values
    html+='<div class="candle-info">Impulse[0] O='+r.candles.b0.o+' H='+r.candles.b0.h+' L='+r.candles.b0.l+' C='+r.candles.b0.c;
    html+='  |  OB[1] O='+r.candles.b1.o+' H='+r.candles.b1.h+' L='+r.candles.b1.l+' C='+r.candles.b1.c+'</div>';

    // Gate 1: OB detection checks
    var checks = r.detected
      ? (r.ob_type==='Bull OB' ? r.bull_checks : r.bear_checks)
      : (countPass(r.bull_checks||[])>=countPass(r.bear_checks||[]) ? r.bull_checks : r.bear_checks);
    var dirLabel = r.detected ? r.ob_type
      : (countPass(r.bull_checks||[])>=countPass(r.bear_checks||[]) ? 'Bull OB attempt' : 'Bear OB attempt');

    html+='<div style="font-size:11px;font-weight:500;color:var(--dim);margin:8px 0 4px">Gate 1 — OB Detection ('+dirLabel+')</div>';
    html+='<div class="ob-checks">';
    (checks||[]).forEach(function(c){
      html+='<div class="check '+(c.pass?'pass':'fail')+'">';
      html+=(c.pass?'✅ ':'❌ ')+'<strong>'+c.name+'</strong>';
      html+='<div class="check-detail">'+c.detail+'</div></div>';
    });
    html+='</div>';

    // Pipeline gates 2-4
    if(r.pipeline && r.pipeline.length) {
      html+='<div class="pipeline">';
      r.pipeline.forEach(function(g){
        var cls = g.skipped ? 'skipped' : g.pass ? 'pass' : 'fail';
        html+='<div class="gate '+cls+'">';
        html+='<span class="gate-name">'+(g.skipped?'⬜':g.pass?'✅':'❌')+' '+g.gate+'</span>';
        html+='<span class="gate-detail">'+g.detail+'</span>';
        html+='</div>';
      });
      html+='</div>';
    }

    // Server gates 5a-5d
    if(r.server_gates && r.server_gates.length) {
      html+='<div style="font-size:11px;font-weight:500;color:var(--dim);margin:10px 0 4px">Server Filters</div>';
      html+='<div class="pipeline">';
      r.server_gates.forEach(function(g){
        html+='<div class="gate '+(g.pass?'pass':'fail')+'">';
        html+='<span class="gate-name">'+(g.pass?'✅':'❌')+' '+g.gate+'</span>';
        html+='<span class="gate-detail">'+g.detail+'</span>';
        html+='</div>';
      });
      html+='</div>';
    }

    // Trade levels
    if(r.trade){
      html+='<div class="trade-box">';
      html+='<div><div class="lbl">Entry</div><div class="val">'+r.trade.entry+'</div></div>';
      html+='<div><div class="lbl">Stop Loss</div><div class="val" style="color:var(--red)">'+r.trade.sl+'</div></div>';
      html+='<div><div class="lbl">Take Profit</div><div class="val" style="color:var(--green)">'+r.trade.tp+'</div></div>';
      html+='<div><div class="lbl">Risk (price)</div><div class="val">'+r.trade.risk_r+'</div></div>';
      html+='</div>';
    }
    html+='</div></div>';
  });
  html+='</div>';
  el.innerHTML=html;

  // Auto-expand blocked OBs and fully-passed ones
  results.forEach(function(r){
    if(r.detected && (!r.gate_pass || r.gate_pass)) toggleEl('bar-'+r.bar);
  });
}
</script>
</body>
</html>"""


@app.route("/signal-explainer")
def signal_explainer_page():
    return render_template_string(SIGNAL_EXPLAINER_HTML)


@app.route("/signal-explainer/run", methods=["POST"])
def signal_explainer_run():
    try:
        body     = request.get_json(force=True)
        symbol   = body.get("symbol", "BTCUSDT").upper().strip()
        tf_str   = str(body.get("tf", "3"))
        time_str = body.get("time", "")
        cfg      = {
            "atrLength":           int(body.get("atrLength",    17)),
            "impulseRatio":        float(body.get("impulseRatio", 1.3)),
            "cobMinAtrMult":       float(body.get("cobMinAtrMult", 0.8)),
            "dojiPct":             float(body.get("dojiPct",    15.0)),
            "slAtrMult":           float(body.get("slAtrMult",  0.1)),
            "entryOffset":         float(body.get("entryOffset", 0.0)),
            "rrRatio":             float(body.get("rrRatio",    3.0)),
            "klProximity":         float(body.get("klProximity", 1.0)),
            "klMinScore":          int(body.get("klMinScore",   6)),
            "klSignificanceBonus": float(body.get("klSignificanceBonus", 0.0)),
            "useKeyLevelFilter":   str(body.get("useKeyLevelFilter","false")).lower() == "true",
            "approachMaxAtr":      float(body.get("approachMaxAtr", 1.5)),
            "rangeLookback":       int(body.get("rangeLookback", 40)),
            "rangeTolPct":         float(body.get("rangeTolPct", 15.0)),
            "rangeMinTouches":     int(body.get("rangeMinTouches", 2)),
            "useRangeFilter":      str(body.get("useRangeFilter","false")).lower() == "true",
        }
        tf_map   = {"3":"M3","5":"M5","15":"M15","30":"M30","60":"H1","240":"H4"}
        tf_label = tf_map.get(tf_str, tf_str)

        bars = _bt_fetch_klines(symbol, tf_str, 3)
        if not bars:
            return jsonify({"status": "error", "message": f"No data for {symbol}"}), 400

        atrs = _bt_calc_atr(bars, cfg["atrLength"])

        target_bar = None
        if time_str:
            try:
                target_dt  = datetime.strptime(time_str, "%Y-%m-%dT%H:%M")
                target_ts  = int(target_dt.timestamp() * 1000)
                closest    = min(range(len(bars)), key=lambda i: abs(bars[i]["ts"] - target_ts))
                target_bar = len(bars) - 1 - closest
                log.info(f"Signal explainer: target bar {closest} at {datetime.utcfromtimestamp(bars[closest]['ts']/1000)}")
            except Exception as te:
                log.warning(f"Signal explainer time parse: {te}")

        results = _explain_ob_detection(bars, atrs, cfg, target_bar)
        return jsonify({"status": "ok", "results": results, "symbol": symbol, "tf_label": tf_label})

    except Exception as e:
        import traceback
        log.error(f"Signal explainer error: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal")
def journal():
    """Trading journal dashboard."""
    try:
        days   = request.args.get("days", "30")
        try:
            days = int(days)
        except:
            days = 30
        trades = get_all_trades(500, days=days)
        stats  = get_stats()
        return render_template_string(JOURNAL_HTML, trades=trades, stats=stats, days=days)
    except Exception as e:
        log.error(f"Journal error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/journal/data", methods=["GET", "OPTIONS"])
def journal_data():
    """JSON endpoint for journal data — used by the journal page."""
    resp_headers = {"Access-Control-Allow-Origin": "*"}
    if request.method == "OPTIONS":
        r = jsonify({})
        r.headers["Access-Control-Allow-Origin"] = "*"
        r.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        r.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return r
    try:
        days = request.args.get("days")
        status_filter = request.args.get("status", "all")
        try:
            days = int(days) if days else None
        except:
            days = None
        trades = get_all_trades(500, days=days)
        # Fetch skipped trades separately (get_all_trades may exclude them)
        try:
            conn = get_db()
            with conn.cursor() as cur:
                if days:
                    cur.execute(
                        "SELECT * FROM trades WHERE status='skipped' AND opened_at::timestamp >= NOW() - INTERVAL %s ORDER BY opened_at DESC LIMIT 500",
                        (f"{days} days",)
                    )
                else:
                    cur.execute("SELECT * FROM trades WHERE status='skipped' ORDER BY opened_at DESC LIMIT 500")
                cols    = [d[0] for d in cur.description]
                skipped = [dict(zip(cols, row)) for row in cur.fetchall()]
            conn.close()
            import json as _j
            skipped = _j.loads(_j.dumps(skipped, default=str))
            trades_j = _j.loads(_j.dumps(trades, default=str))
            existing_ids = {t.get("id") for t in trades_j}
            for t in skipped:
                if t.get("id") not in existing_ids:
                    trades_j.append(t)
            trades_j.sort(key=lambda t: str(t.get("opened_at") or ""), reverse=True)
            trades = trades_j
            log.info(f"Journal data: {len(trades)} total trades incl. {len(skipped)} skipped")
        except Exception as e:
            log.warning(f"Could not fetch skipped trades separately: {e}")
        # Apply status filter
        if status_filter == "closed":
            trades = [t for t in trades if t.get("status") in ("closed", "note")]
        elif status_filter == "open":
            trades = [t for t in trades if t.get("status") in ("open", "note")]
        resp = jsonify({"trades": trades, "stats": get_stats()})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except Exception as e:
        log.error(f"Journal data error: {e}")
        resp = jsonify({"status": "error", "message": str(e)})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp, 500


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

<div class="section">
  <div class="section-title">Win rate over time</div>
  <div style="display:flex;gap:20px;margin-bottom:10px;font-size:11px;color:var(--dim)">
    <span><span style="display:inline-block;width:20px;height:2px;background:#42a5f5;vertical-align:middle;margin-right:4px"></span>Rolling 20</span>
    <span><span style="display:inline-block;width:20px;height:2px;background:rgba(255,255,255,0.25);vertical-align:middle;margin-right:4px"></span>Cumulative</span>
    <span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#4caf50;vertical-align:middle;margin-right:4px"></span>Win</span>
    <span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#ef5350;vertical-align:middle;margin-right:4px"></span>Loss</span>
  </div>
  <canvas id="wr-timeline" height="200"></canvas>
  <div style="margin-top:20px">
    <div style="font-size:11px;font-weight:500;color:var(--dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px">Cumulative PnL (USDT)</div>
    <canvas id="pnl-timeline" height="120"></canvas>
  </div>
</div>

<script>
(function() {
  var tl = {{ timeline|tojson }};
  if (!tl || !tl.length) return;

  var labels   = tl.map(function(d){ return '#' + d.i; });
  var rollWR   = tl.map(function(d){ return d.roll_wr; });
  var cumWR    = tl.map(function(d){ return d.cum_wr; });
  var cumPnl   = tl.map(function(d){ return d.cum_pnl; });
  var outcomes = tl.map(function(d){ return d.outcome; });
  var symbols  = tl.map(function(d){ return d.symbol; });

  // ── WR Timeline ──────────────────────────────────────────────────────────
  var wrCanvas = document.getElementById('wr-timeline');
  wrCanvas.style.width = '100%';
  wrCanvas.width = wrCanvas.offsetWidth || 800;
  var wCtx = wrCanvas.getContext('2d');
  var W = wrCanvas.width, H = wrCanvas.height;
  var pad = {top:10, right:10, bottom:30, left:36};
  var cw = W - pad.left - pad.right;
  var ch = H - pad.top - pad.bottom;

  function wrX(i) { return pad.left + (i / (tl.length - 1 || 1)) * cw; }
  function wrY(v) { return pad.top + (1 - v / 100) * ch; }

  // Grid lines
  wCtx.strokeStyle = 'rgba(255,255,255,0.05)';
  wCtx.lineWidth = 1;
  [0, 25, 50, 75, 100].forEach(function(v) {
    var y = wrY(v);
    wCtx.beginPath(); wCtx.moveTo(pad.left, y); wCtx.lineTo(W - pad.right, y); wCtx.stroke();
    wCtx.fillStyle = '#888'; wCtx.font = '10px sans-serif'; wCtx.textAlign = 'right';
    wCtx.fillText(v + '%', pad.left - 4, y + 3);
  });

  // Break-even line at 28%
  wCtx.strokeStyle = 'rgba(255,167,38,0.3)';
  wCtx.setLineDash([4, 4]);
  var beY = wrY(28);
  wCtx.beginPath(); wCtx.moveTo(pad.left, beY); wCtx.lineTo(W - pad.right, beY); wCtx.stroke();
  wCtx.setLineDash([]);
  wCtx.fillStyle = 'rgba(255,167,38,0.6)'; wCtx.font = '9px sans-serif'; wCtx.textAlign = 'left';
  wCtx.fillText('break-even ~28%', pad.left + 4, beY - 3);

  // Cumulative WR line
  wCtx.strokeStyle = 'rgba(255,255,255,0.2)';
  wCtx.lineWidth = 1;
  wCtx.beginPath();
  tl.forEach(function(d, i) { i === 0 ? wCtx.moveTo(wrX(i), wrY(d.cum_wr)) : wCtx.lineTo(wrX(i), wrY(d.cum_wr)); });
  wCtx.stroke();

  // Rolling WR line
  wCtx.strokeStyle = '#42a5f5';
  wCtx.lineWidth = 2;
  wCtx.beginPath();
  tl.forEach(function(d, i) { i === 0 ? wCtx.moveTo(wrX(i), wrY(d.roll_wr)) : wCtx.lineTo(wrX(i), wrY(d.roll_wr)); });
  wCtx.stroke();

  // Outcome dots
  tl.forEach(function(d, i) {
    wCtx.beginPath();
    wCtx.arc(wrX(i), wrY(d.roll_wr), 3, 0, Math.PI * 2);
    wCtx.fillStyle = d.outcome === 'tp' ? '#4caf50' : '#ef5350';
    wCtx.fill();
  });

  // X-axis labels (every ~20 trades)
  wCtx.fillStyle = '#888'; wCtx.font = '10px sans-serif'; wCtx.textAlign = 'center';
  tl.forEach(function(d, i) {
    if (i === 0 || (i + 1) % 20 === 0 || i === tl.length - 1) {
      wCtx.fillText(d.date || ('#' + d.i), wrX(i), H - 4);
    }
  });

  // Tooltip
  var tooltip = document.createElement('div');
  tooltip.style.cssText = 'position:fixed;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:6px;padding:8px 12px;font-size:11px;pointer-events:none;display:none;z-index:999;color:#e8e8e8;line-height:1.6';
  document.body.appendChild(tooltip);

  function showTooltip(canvas, e, data, xFn, yFn) {
    var rect = canvas.getBoundingClientRect();
    var mx = (e.clientX - rect.left) * (canvas.width / rect.width);
    var idx = Math.round((mx - pad.left) / cw * (tl.length - 1));
    idx = Math.max(0, Math.min(tl.length - 1, idx));
    var d = data[idx];
    if (!d) return;
    tooltip.innerHTML = '<strong>#' + d.i + ' — ' + d.date + '</strong><br>'
      + d.symbol + ' ' + d.side + ' <span style="color:' + (d.outcome==='tp'?'#4caf50':'#ef5350') + '">' + (d.outcome==='tp'?'WIN':'LOSS') + '</span><br>'
      + 'Roll WR: <strong>' + d.roll_wr + '%</strong> &nbsp; Cum WR: ' + d.cum_wr + '%<br>'
      + 'PnL: <span style="color:' + (d.pnl_r>=0?'#4caf50':'#ef5350') + '">' + (d.pnl_r>=0?'+':'') + d.pnl_r + '</span>'
      + ' &nbsp; Cum: <span style="color:' + (d.cum_pnl>=0?'#4caf50':'#ef5350') + '">' + (d.cum_pnl>=0?'+':'') + d.cum_pnl + '</span>';
    tooltip.style.display = 'block';
    tooltip.style.left = (e.clientX + 12) + 'px';
    tooltip.style.top  = (e.clientY - 10) + 'px';
  }

  wrCanvas.addEventListener('mousemove', function(e) { showTooltip(wrCanvas, e, tl, wrX, wrY); });
  wrCanvas.addEventListener('mouseleave', function() { tooltip.style.display = 'none'; });

  // ── PnL Timeline ─────────────────────────────────────────────────────────
  var pnlCanvas = document.getElementById('pnl-timeline');
  pnlCanvas.style.width = '100%';
  pnlCanvas.width = pnlCanvas.offsetWidth || 800;
  var pCtx = pnlCanvas.getContext('2d');
  var PH = pnlCanvas.height;
  var pch = PH - pad.top - pad.bottom;

  var minPnl = Math.min(0, Math.min.apply(null, cumPnl));
  var maxPnl = Math.max(0, Math.max.apply(null, cumPnl));
  var pRange = maxPnl - minPnl || 1;

  function pX(i) { return pad.left + (i / (tl.length - 1 || 1)) * cw; }
  function pY(v) { return pad.top + (1 - (v - minPnl) / pRange) * pch; }

  // Zero line
  pCtx.strokeStyle = 'rgba(255,255,255,0.15)';
  pCtx.lineWidth = 1;
  var zeroY = pY(0);
  pCtx.beginPath(); pCtx.moveTo(pad.left, zeroY); pCtx.lineTo(W - pad.right, zeroY); pCtx.stroke();
  pCtx.fillStyle = '#888'; pCtx.font = '10px sans-serif'; pCtx.textAlign = 'right';
  pCtx.fillText('0', pad.left - 4, zeroY + 3);
  [minPnl, maxPnl].forEach(function(v) {
    var y = pY(v);
    pCtx.fillText((v >= 0 ? '+' : '') + v.toFixed(1), pad.left - 4, y + 3);
  });

  // Fill area
  pCtx.beginPath();
  pCtx.moveTo(pX(0), pY(0));
  tl.forEach(function(d, i) { pCtx.lineTo(pX(i), pY(d.cum_pnl)); });
  pCtx.lineTo(pX(tl.length - 1), pY(0));
  pCtx.closePath();
  var grad = pCtx.createLinearGradient(0, pad.top, 0, PH - pad.bottom);
  grad.addColorStop(0, cumPnl[cumPnl.length-1] >= 0 ? 'rgba(76,175,80,0.2)' : 'rgba(239,83,80,0.2)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  pCtx.fillStyle = grad;
  pCtx.fill();

  // PnL line
  pCtx.strokeStyle = cumPnl[cumPnl.length-1] >= 0 ? '#4caf50' : '#ef5350';
  pCtx.lineWidth = 2;
  pCtx.beginPath();
  tl.forEach(function(d, i) { i === 0 ? pCtx.moveTo(pX(i), pY(d.cum_pnl)) : pCtx.lineTo(pX(i), pY(d.cum_pnl)); });
  pCtx.stroke();

  pnlCanvas.addEventListener('mousemove', function(e) { showTooltip(pnlCanvas, e, tl, pX, pY); });
  pnlCanvas.addEventListener('mouseleave', function() { tooltip.style.display = 'none'; });
})();
</script>

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

{% if by_variant and by_variant|length > 1 %}
<div class="section" style="margin-bottom:16px">
  <div class="section-title">By variant</div>
  <p style="font-size:11px;color:var(--dim);margin-bottom:8px">Comparing indicator settings versions — set via "Indicator variant label" in Pine Script</p>
  <table>
    <tr><th>Variant</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_variant %}
    <tr>
      <td><strong>{{ r.variant }}</strong></td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ffa726' if r.wr >= 35 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if by_source_full and by_source_full|length > 1 %}
<div class="section" style="margin-bottom:16px">
  <div class="section-title">OB vs OB Test</div>
  <table>
    <tr><th>Source</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_source_full %}
    <tr>
      <td><span class="tag tag-blue">{{ r.source.upper() }}</span></td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ffa726' if r.wr >= 35 else '#ef5350' }};"></span></span>
      </td>
      <td style="color:{{ 'var(--green)' if r.pnl >= 0 else 'var(--red)' }}">{{ '+' if r.pnl >= 0 else '' }}{{ '%.2f'|format(r.pnl) }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

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
  <div class="section-title">By symbol + side</div>
  <table>
    <tr><th>Symbol</th><th>Side</th><th>W</th><th>L</th><th>WR%</th><th>PnL</th></tr>
    {% for r in by_symbol_side %}
    <tr style="{{ 'opacity:0.5' if r.total < 3 else '' }}">
      <td>{{ r.symbol }}</td>
      <td><span class="tag {{ 'tag-green' if r.side == 'Buy' else 'tag-red' }}">{{ 'LONG' if r.side == 'Buy' else 'SHORT' }}</span></td>
      <td class="green">{{ r.wins }}</td>
      <td class="red">{{ r.losses }}</td>
      <td>{{ r.wr }}%
        <span class="bar-wrap"><span class="bar" style="width:{{ r.wr }}%;background:{{ '#4caf50' if r.wr >= 50 else '#ffa726' if r.wr >= 35 else '#ef5350' }};"></span></span>
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

{% if kl_analysis.with_kl.count > 0 %}
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
{% endif %}

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
    closed  = [t for t in trades if t.get("outcome") in ("tp", "sl", "tp1_tp2", "tp1_sl")]
    open_t  = [t for t in trades if t.get("status") == "open"]

    # For breakdowns — exclude unmatched imports (no source/timeframe data)
    def has_indicator(t):
        src = (t.get("source") or "")
        return src != "bybit_import" or bool(t.get("timeframe"))
    closed_known = [t for t in closed if has_indicator(t)]

    # Win/loss by actual PnL sign, not the outcome label — a two-stage trade
    # (tp1_sl: partial secured, runner stopped out) can still be net profitable
    # overall, and should count as a win. Using the label alone undercounts
    # wins for exactly that case.
    wins   = [t for t in closed if float(t.get("pnl") or 0) > 0]
    losses = [t for t in closed if float(t.get("pnl") or 0) <= 0]

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
            if float(t.get("pnl") or 0) > 0:
                groups[k]["wins"] += 1
            else:
                groups[k]["losses"] += 1
            groups[k]["pnl"] += float(t.get("pnl") or 0)
        result = []
        for k, v in sorted(groups.items(), key=lambda x: -(x[1]["wins"] + x[1]["losses"])):
            total = v["wins"] + v["losses"]
            result.append({
                "key": k, "wins": v["wins"], "losses": v["losses"],
                "total": total,
                "wr": round(v["wins"] / total * 100, 1) if total > 0 else 0,
                "pnl": round(v["pnl"], 2)
            })
        return result

    by_symbol_raw = group_stats(lambda t: t["symbol"])
    by_source_raw = group_stats(lambda t: (t.get("source") or "").replace("_test",""))
    by_variant_raw = group_stats(lambda t: (t.get("variant") or "default"))
    by_source_full_raw = group_stats(lambda t: (t.get("source") or "ob"))
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

    by_symbol_side_raw = group_stats(lambda t: t["symbol"] + "|" + (t.get("side") or ""))
    by_symbol_side = sorted([{
        "symbol": r["key"].split("|")[0],
        "side":   r["key"].split("|")[1] if "|" in r["key"] else "—",
        "wins":   r["wins"], "losses": r["losses"],
        "total":  r["total"], "wr": r["wr"], "pnl": r["pnl"],
    } for r in by_symbol_side_raw], key=lambda x: -x["wr"])
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
        long_wins    = len([t for t in long_trades  if float(t.get("pnl") or 0) > 0])
        short_wins   = len([t for t in short_trades if float(t.get("pnl") or 0) > 0])
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
        by_day_raw[day]["wins" if pnl > 0 else "losses"] += 1
        by_day_raw[day]["pnl"] += pnl

        if session not in by_session_raw:
            by_session_raw[session] = {"wins":0,"losses":0,"pnl":0.0}
        by_session_raw[session]["wins" if pnl > 0 else "losses"] += 1
        by_session_raw[session]["pnl"] += pnl

        key = "Weekend" if is_weekend else "Weekday"
        wk_stats[key]["wins" if pnl > 0 else "losses"] += 1
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
        return round(sum(1 for t in trades if float(t.get("pnl") or 0) > 0) / len(trades) * 100, 1)

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
            groups[key]["wins"   if float(t.get("pnl") or 0) > 0 else "losses"] += 1
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

    # ── Win rate timeline — rolling 20-trade window + cumulative ─────────────
    timeline = []
    sorted_closed = sorted(closed, key=lambda t: t.get("opened_at") or "")
    cum_wins = 0
    for idx, t in enumerate(sorted_closed):
        cum_wins += 1 if float(t.get("pnl") or 0) > 0 else 0
        cum_wr    = round(cum_wins / (idx + 1) * 100, 1)
        window    = sorted_closed[max(0, idx - 19): idx + 1]
        roll_wins = sum(1 for x in window if float(x.get("pnl") or 0) > 0)
        roll_wr   = round(roll_wins / len(window) * 100, 1)
        date_str  = (t.get("opened_at") or "")[:10]
        cum_pnl   = round(sum(float(x.get("pnl") or 0) for x in sorted_closed[:idx+1]), 2)
        timeline.append({
            "i": idx + 1, "date": date_str,
            "outcome": t["outcome"], "symbol": t["symbol"],
            "side": t.get("side", ""),
            "cum_wr": cum_wr, "roll_wr": roll_wr,
            "pnl_r": round(float(t.get("pnl") or 0), 2),
            "cum_pnl": cum_pnl,
        })

    return {
        "timeline":       timeline,
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
        "by_symbol_side": by_symbol_side,
        "by_source":      by_source,
        "by_source_full": [{**r, "source": r["key"]} for r in by_source_full_raw],
        "by_variant":     sorted([{**r, "variant": r["key"]} for r in by_variant_raw], key=lambda x: -x["wr"]),
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


def _get_backtest_results():
    """Fetch all backtest results from DB."""
    try:
        conn = get_db()
        import psycopg2.extras as _pge
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            cur.execute("SELECT * FROM backtest_results ORDER BY win_rate DESC")
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        log.warning(f"_get_backtest_results: {e}")
        return []


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
        if pnl > 0:
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
_bt_progress  = {"done": 0, "total": 0, "current": "", "saved": 0}


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
    mid = (ob_top + ob_bot) / 2
    for lv in key_levels:
        lp     = lv["price"]
        is_res = lp > mid
        dir_ok = (ob_type == 1 and not is_res) or (ob_type == 2 and is_res)
        in_zone = ob_bot - slack <= lp <= ob_top + slack
        if dir_ok and in_zone:
            return True
    return False


def _bt_ob_kl_score(ob_top, ob_bot, ob_type, atr, key_levels, proximity=1.0):
    """Return KL match price if OB is near a qualifying key level, else None."""
    if not key_levels:
        return None
    ob_height = ob_top - ob_bot
    slack     = ob_height * proximity
    mid       = (ob_top + ob_bot) / 2
    for lv in key_levels:
        lp     = lv["price"]
        is_res = lp > mid
        if ob_type == 1:  # bull OB — support near bottom
            if not is_res and ob_bot - slack <= lp <= ob_bot + slack * 0.5:
                return lp
        else:  # bear OB — resistance near top
            if is_res and ob_top - slack * 0.5 <= lp <= ob_top + slack:
                return lp
    return None


def _bt_remove_dominated(obs, key_levels, atrs, overlap_bars=5, kl_bonus=0.0, proximity=1.0):
    """
    Remove dominated OBs — mirrors Pine Script checkOverlap with KL significance scoring.
    kl_bonus=0 → pure size wins (old behaviour)
    kl_bonus=2 → KL-backed OB gets +2x its own height added to score
    """
    if not obs:
        return obs
    keep = list(range(len(obs)))
    removed = set()
    for i in range(len(obs)):
        if i in removed:
            continue
        for j in range(i + 1, len(obs)):
            if j in removed:
                continue
            oi, oj = obs[i], obs[j]
            # Only compare OBs within overlapBars of each other
            if abs(oi["bar"] - oj["bar"]) > overlap_bars:
                continue
            # Check overlap
            overlaps = oi["top"] >= oj["bot"] and oi["bot"] <= oj["top"]
            if not overlaps:
                continue
            # Score each
            atr_i = atrs[oi["bar"]] or 0.001
            atr_j = atrs[oj["bar"]] or 0.001
            size_i = oi["top"] - oi["bot"]
            size_j = oj["top"] - oj["bot"]
            kl_i = _bt_ob_kl_score(oi["top"], oi["bot"], oi["type"], atr_i, key_levels, proximity) if key_levels else None
            kl_j = _bt_ob_kl_score(oj["top"], oj["bot"], oj["type"], atr_j, key_levels, proximity) if key_levels else None
            score_i = size_i + (size_i * kl_bonus if kl_i is not None else 0.0)
            score_j = size_j + (size_j * kl_bonus if kl_j is not None else 0.0)
            if score_i >= score_j:
                removed.add(j)
            else:
                removed.add(i)
                break
    return [obs[i] for i in range(len(obs)) if i not in removed]


# ── Status bar string parser ──────────────────────────────────────────────────
# Maps the 48 non-bool/non-color input positions to setting names
_STATUS_BAR_MAP = [
    ("alertName",        str),
    ("maxBlocks",        int),
    ("extendBars",       int),
    ("htfExtendBars",    int),
    ("atrLength",        int),
    ("impulseRatio",     float),
    ("overlapBars",      int),
    ("dojiPct",          float),
    ("dirtyObRatio",     float),
    ("cobMinAtrMult",    float),
    ("rrRatio",          float),
    ("winRateMinSamples",int),
    ("winRateLookback",  int),
    ("slAtrMult",        float),
    ("slMinPct",         float),
    ("coolOffBars",      int),
    ("maxAlertsPerOB",   int),
    ("cancelAfterBars",  int),
    ("prepareAtrMult",   float),
    ("entryOffset",      float),
    ("alertBestWRMinSmp",int),
    ("alertMinWR",       int),
    ("klProximity",      float),
    ("klLookback2",      int),
    ("klMinScore",       int),
    ("klTol2",           float),
    ("klPvLen",          int),
    ("klMaxLevels",      int),
    ("klStrongOpacity",  int),
    ("klWeakOpacity",    int),
    ("klBorderOpacity",  int),
    ("klSignificanceBonus", float),  # pos 31: 0=old script (klFlipSR bool skipped), 2=new script with bonus
    ("webhookSecret",    str),
    ("indicatorVariant", str),
    ("testBalancePct",   float),
    ("testLeverage",     int),
    ("trendEmaLen",      int),
    ("trendEmaTF",       str),
    ("emaLookback",      int),
    ("timezone",         str),
    ("schedStartHour",   int),
    ("schedStartMin",    int),
    ("schedEndHour",     int),
    ("schedEndMin",      int),
    ("htf",              str),
    ("trendTF",          str),
    ("trendLookback",    int),
    ("structureHistory", int),
]

def _parse_status_bar(status_bar_str: str) -> dict:
    """
    Parse TradingView indicator status bar string into a settings dict.
    Format: "Indicator Name (val1, val2, ...): Any alert() function call"
    """
    import re
    # Extract the values inside the outer parentheses
    m = re.search(r'\((.+)\):', status_bar_str)
    if not m:
        raise ValueError("Could not find values in status bar string")
    raw = m.group(1)
    # Split on ", " but be careful with strings that contain commas
    # The first value (alertName) may contain commas — it's everything up to the first number
    # Split all values
    parts = [p.strip() for p in raw.split(", ")]
    result = {}
    for i, (name, typ) in enumerate(_STATUS_BAR_MAP):
        if i >= len(parts):
            break
        try:
            val = parts[i]
            if typ == float:
                result[name] = float(val)
            elif typ == int:
                # Handle bool-as-int: 0/1 → but also real ints
                result[name] = int(float(val))
            else:
                result[name] = val
        except (ValueError, IndexError):
            pass
    return result


def _bt_init_configs_table():
    """Create backtest_configs table if not exists."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS backtest_configs (
                    id            SERIAL PRIMARY KEY,
                    name          TEXT NOT NULL UNIQUE,
                    rr_ratio      REAL NOT NULL DEFAULT 3.0,
                    sl_buf        REAL NOT NULL DEFAULT 0.1,
                    min_impulse   REAL NOT NULL DEFAULT 1.3,
                    entry_offset  REAL NOT NULL DEFAULT 0.0,
                    overlap_bars  INT  NOT NULL DEFAULT 5,
                    atr_length    INT  NOT NULL DEFAULT 17,
                    cob_min_atr   REAL NOT NULL DEFAULT 0.8,
                    doji_pct      REAL NOT NULL DEFAULT 15.0,
                    kl_proximity  REAL NOT NULL DEFAULT 1.0,
                    kl_min_score  INT  NOT NULL DEFAULT 6,
                    kl_lookback   INT  NOT NULL DEFAULT 300,
                    kl_bonus      REAL NOT NULL DEFAULT 0.0,
                    lookback_days INT  NOT NULL DEFAULT 30,
                    raw_settings  TEXT,
                    created_at    TEXT NOT NULL,
                    is_active     BOOL NOT NULL DEFAULT true
                )
            """)
        conn.commit()
        # Migration: add lookback_days if missing (existing tables)
        try:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE backtest_configs ADD COLUMN IF NOT EXISTS lookback_days INT NOT NULL DEFAULT 30")
            conn.commit()
        except Exception:
            pass
        conn.close()
        log.info("backtest_configs table ready")
    except Exception as e:
        log.error(f"BT configs table init error: {e}")


def _get_bt_configs() -> list:
    try:
        conn = get_db()
        import psycopg2.extras as _pge
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            cur.execute("SELECT * FROM backtest_configs ORDER BY created_at DESC")
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        log.error(f"_get_bt_configs error: {e}")
        return []


def run_backtest_for_config(cfg_id: int):
    """Run backtest for a specific saved config."""
    global _bt_running, _bt_last_run, _bt_status
    if _bt_running:
        log.info("Backtest already running — skipping")
        return

    # Load config
    try:
        conn = get_db()
        import psycopg2.extras as _pge
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            cur.execute("SELECT * FROM backtest_configs WHERE id=%s", (cfg_id,))
            cfg = cur.fetchone()
        conn.close()
        if not cfg:
            log.error(f"Config {cfg_id} not found")
            return
    except Exception as e:
        log.error(f"Config load error: {e}")
        return

    _bt_running = True
    cfg_name    = cfg["name"]
    _bt_status  = f"Running config: {cfg_name}..."
    _bt_progress.update({"done": 0, "total": len(BT_SYMBOLS) * len(BT_TIMEFRAMES), "current": "", "saved": 0})
    log.info(f"Backtest started for config '{cfg_name}'")

    rr           = cfg["rr_ratio"]
    sl_buf       = cfg["sl_buf"]
    min_impulse  = cfg["min_impulse"]
    entry_offset = cfg["entry_offset"]
    overlap_bars = cfg["overlap_bars"]
    atr_len      = cfg["atr_length"]
    kl_proximity = cfg["kl_proximity"]
    kl_min_score = cfg["kl_min_score"]
    kl_lookback  = cfg["kl_lookback"]
    kl_bonus     = cfg["kl_bonus"]
    lookback_days = int(cfg.get("lookback_days") or 30)

    # Use variant name = config name for storage
    variant_no_kl = f"{cfg_name}_raw"
    variant_kl    = f"{cfg_name}_kl"

    saved = 0
    total_combos = len(BT_SYMBOLS) * len(BT_TIMEFRAMES)
    done  = 0

    try:
        for symbol in BT_SYMBOLS:
            symbol = symbol.strip()
            time.sleep(1.0)
            for tf_str, tf_label in BT_TIMEFRAMES.items():
                done += 1
                _bt_progress["done"]    = done
                _bt_progress["current"] = f"{symbol} {tf_label}"
                _bt_progress["saved"]   = saved
                tf_mins  = {"3":3,"5":5,"15":15,"30":30,"60":60,"240":240}[tf_str]
                lookback = min(lookback_days, 30) if tf_mins <= 5 else \
                           min(lookback_days, 60) if tf_mins <= 15 else lookback_days
                try:
                    bars = _bt_fetch_klines(symbol, tf_str, lookback)
                    if len(bars) < 100:
                        continue
                    atrs       = _bt_calc_atr(bars, atr_len)
                    key_levels = _bt_find_key_levels(bars, lookback=min(lookback, kl_lookback),
                                                     min_score=kl_min_score)

                    # Detect OBs
                    obs_raw = _bt_detect_obs(bars, atrs, min_impulse, sl_buf, entry_offset)

                    # ── Variant A: no overlap removal, no KL filter (pure signal) ──
                    results_raw = _bt_simulate(bars, obs_raw, rr)
                    stats_raw   = _bt_stats(results_raw)
                    if stats_raw:
                        _bt_save(symbol, tf_label, stats_raw, variant=variant_no_kl)
                        saved += 1

                    # ── Variant B: overlap removal + KL significance scoring ────
                    obs_scored = _bt_remove_dominated(obs_raw, key_levels, atrs,
                                                      overlap_bars=overlap_bars,
                                                      kl_bonus=kl_bonus,
                                                      proximity=kl_proximity)
                    if key_levels:
                        obs_kl = [ob for ob in obs_scored if _bt_ob_near_key_level(
                                    ob["top"], ob["bot"], ob["type"],
                                    atrs[ob["bar"]] or atrs[-1],
                                    key_levels, kl_proximity)]
                    else:
                        obs_kl = obs_scored

                    results_kl = _bt_simulate(bars, obs_kl, rr)
                    stats_kl   = _bt_stats(results_kl)
                    if stats_kl:
                        _bt_save(symbol, tf_label, stats_kl, variant=variant_kl)
                        saved += 1

                    log.info(f"  [{done}/{total_combos}] {symbol}/{tf_label}: "
                             f"raw={len(obs_raw)} scored={len(obs_scored)} kl={len(obs_kl) if key_levels else '—'} "
                             f"→ {saved} saved")
                except Exception as e:
                    log.error(f"  {symbol}/{tf_label} [{cfg_name}]: {e}")
                time.sleep(0.5)
    finally:
        _bt_running  = False
        _bt_last_run = datetime.utcnow()
        _bt_status   = f"Config '{cfg_name}' done {datetime.utcnow().strftime('%H:%M UTC')} — {saved} results"
        log.info(f"Backtest complete for '{cfg_name}' — {saved} results")


def _simulate(bars, obs, rr):
    """Alias used by run_backtest_for_config."""
    return _bt_simulate(bars, obs, rr)


def _bt_simulate(bars, obs, rr=None, cancel_after=20):
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


def _get_setting(key: str, default: str = "") -> str:
    """Tiny key-value settings store, created on first use."""
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")
            cur.execute("SELECT value FROM app_settings WHERE key=%s", (key,))
            row = cur.fetchone()
        conn.commit()
        conn.close()
        return row[0] if row else default
    except Exception as e:
        log.warning(f"_get_setting({key}) failed, using default: {e}")
        return default


def _set_setting(key: str, value: str):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)")
        cur.execute("""
            INSERT INTO app_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, value))
    conn.commit()
    conn.close()


def _schedule_daily_backtest():
    """Run backtest daily at 02:00 UTC — unless switched off via the backtest configs page."""
    while True:
        now  = datetime.utcnow()
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait = (next_run - now).total_seconds()
        log.info(f"Next backtest scheduled at {next_run.strftime('%Y-%m-%d %H:%M UTC')} ({int(wait/3600)}h from now)")
        time.sleep(wait)
        if _get_setting("daily_backtest_enabled", "true") != "true":
            log.info("Daily backtest is switched off (backtest configs page) — skipping this run")
            continue
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
        "progress": _bt_progress,
        "daily_backtest_enabled": _get_setting("daily_backtest_enabled", "true") == "true",
    })


@app.route("/backtest/scheduler-toggle", methods=["POST"])
def backtest_scheduler_toggle():
    data    = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled", True))
    _set_setting("daily_backtest_enabled", "true" if enabled else "false")
    log.info(f"Daily backtest scheduler {'enabled' if enabled else 'disabled'} via backtest configs page")
    return jsonify({"status": "ok", "daily_backtest_enabled": enabled})


# ─── BACKTEST CONFIGS ─────────────────────────────────────────────────────────

BACKTEST_CONFIGS_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Backtest Configs</title>
<style>
:root{--bg:#0f0f0f;--surface:#1a1a1a;--border:#2a2a2a;--text:#e8e8e8;--dim:#888;--green:#4caf50;--red:#ef5350;--blue:#42a5f5;--amber:#ffa726}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,sans-serif;font-size:13px;padding:24px}
h1{font-size:18px;font-weight:500;margin-bottom:4px}
.nav{display:flex;gap:12px;margin-bottom:20px}
.nav a{color:var(--dim);text-decoration:none;font-size:12px}
.section{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px}
.section-title{font-size:12px;font-weight:500;color:var(--dim);text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px}
textarea{width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:10px;border-radius:6px;font-size:12px;font-family:monospace;resize:vertical;min-height:60px}
textarea:focus{outline:none;border-color:var(--blue)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px;margin:12px 0}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-size:11px;color:var(--dim)}
.field input{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:6px 8px;border-radius:4px;font-size:12px;width:100%}
.field input:focus{outline:none;border-color:var(--blue)}
.btn{padding:7px 14px;border-radius:6px;border:none;cursor:pointer;font-size:12px;font-weight:500}
.btn-primary{background:var(--blue);color:#000}
.btn-primary:hover{opacity:.9}
.btn-danger{background:rgba(248,113,113,.15);color:var(--red);border:1px solid rgba(248,113,113,.3)}
.btn-run{background:rgba(76,175,80,.15);color:var(--green);border:1px solid rgba(76,175,80,.3)}
.btn-run:hover{background:rgba(76,175,80,.25)}
table{width:100%;border-collapse:collapse;font-size:12px}
th{text-align:left;color:var(--dim);font-weight:400;padding:6px 8px;border-bottom:1px solid var(--border);white-space:nowrap}
td{padding:7px 8px;border-bottom:1px solid var(--border);vertical-align:middle}
.tag{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600}
.tag-green{background:rgba(76,175,80,.15);color:var(--green)}
.tag-red{background:rgba(248,113,113,.15);color:var(--red)}
.tag-blue{background:rgba(96,165,250,.15);color:var(--blue)}
.tag-amber{background:rgba(251,191,36,.15);color:var(--amber)}
.status-bar{background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:8px;font-size:11px;color:var(--dim);margin-bottom:12px;font-family:monospace;word-break:break-all}
.parsed-preview{background:rgba(96,165,250,.05);border:1px solid rgba(96,165,250,.2);border-radius:6px;padding:10px;margin:10px 0;font-size:11px;display:none}
.result-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;margin-top:12px}
.result-card{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px}
.result-card .symbol{font-weight:500;font-size:13px}
.result-card .tf{color:var(--dim);font-size:11px}
.result-card .wr{font-size:20px;font-weight:500}
.running-badge{display:inline-block;padding:3px 10px;border-radius:4px;background:rgba(251,191,36,.15);color:var(--amber);font-size:11px;animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.bar-wrap{background:var(--border);border-radius:3px;height:5px;width:60px;display:inline-block;vertical-align:middle;margin-left:6px}
.bar{height:5px;border-radius:3px}
.toggle-row{display:flex;align-items:center;justify-content:space-between;gap:12px}
.toggle-row .label{font-size:12px}
.toggle-row .sub{font-size:11px;color:var(--dim);margin-top:2px}
.switch{position:relative;display:inline-block;width:38px;height:21px;flex-shrink:0}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;inset:0;background-color:var(--border);transition:.2s;border-radius:21px}
.slider:before{position:absolute;content:"";height:15px;width:15px;left:3px;bottom:3px;background-color:var(--dim);transition:.2s;border-radius:50%}
input:checked + .slider{background-color:rgba(76,175,80,.3)}
input:checked + .slider:before{background-color:var(--green);transform:translateX(17px)}
</style>
</head>
<body>
<div class="nav">
  <a href="/journal">← Journal</a>
  <a href="/analysis">Analysis</a>
  <a href="/recommendations">Recommendations</a>
</div>
<h1>// Backtest Configs</h1>
<p style="color:var(--dim);font-size:12px;margin-bottom:20px">Paste your TradingView indicator status bar string to import settings. Each config runs a backtest independently so you can compare variants.</p>

<div class="section">
  <div class="toggle-row">
    <div>
      <div class="label">Daily automatic backtest (02:00 UTC, all symbols × timeframes)</div>
      <div class="sub" id="scheduler-sub">Uses a large number of API calls each run — turn off if you're not actively using it.</div>
    </div>
    <label class="switch">
      <input type="checkbox" id="scheduler-toggle" onchange="toggleScheduler(this.checked)">
      <span class="slider"></span>
    </label>
  </div>
</div>

<div id="running-status" style="display:none;margin-bottom:12px">
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px 16px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span class="running-badge" id="running-text">⏳ Running...</span>
      <span id="progress-count" style="font-size:11px;color:var(--dim)"></span>
    </div>
    <div style="background:var(--border);border-radius:4px;height:6px;width:100%;margin-bottom:8px">
      <div id="progress-bar" style="background:var(--blue);height:6px;border-radius:4px;width:0%;transition:width .3s ease"></div>
    </div>
    <div id="progress-current" style="font-size:11px;color:var(--dim)"></div>
  </div>
</div>

<div class="section">
  <div class="section-title">Import from TradingView status bar</div>
  <p style="font-size:11px;color:var(--dim);margin-bottom:8px">In TradingView: right-click the indicator name in the status bar → copy → paste below.</p>
  <textarea id="status-bar-input" placeholder='Order Blocks [2-Candle Method] (benchmark, 10, 20, ...): Any alert() function call'></textarea>
  <div class="parsed-preview" id="parsed-preview"></div>
  <div style="display:flex;gap:8px;margin-top:8px">
    <button class="btn btn-primary" onclick="parseStatusBar()">Parse</button>
    <button class="btn" onclick="clearForm()" style="background:var(--surface);border:1px solid var(--border)">Clear</button>
  </div>
</div>

<div class="section" id="config-form-section" style="display:none">
  <div class="section-title">Config details — edit before saving</div>
  <div class="grid">
    <div class="field"><label>Config name</label><input id="f-name" type="text" placeholder="e.g. v3-kl-test"></div>
    <div class="field"><label>RR Ratio</label><input id="f-rr" type="number" step="0.1"></div>
    <div class="field"><label>SL ATR Buffer</label><input id="f-sl" type="number" step="0.05"></div>
    <div class="field"><label>Min Impulse</label><input id="f-impulse" type="number" step="0.1"></div>
    <div class="field"><label>Entry Offset</label><input id="f-offset" type="number" step="0.05"></div>
    <div class="field"><label>Overlap Bars</label><input id="f-overlap" type="number"></div>
    <div class="field"><label>ATR Length</label><input id="f-atr" type="number"></div>
    <div class="field"><label>COB Min ATR</label><input id="f-cobmult" type="number" step="0.1"></div>
    <div class="field"><label>KL Proximity</label><input id="f-klprox" type="number" step="0.1"></div>
    <div class="field"><label>KL Min Score</label><input id="f-klscore" type="number"></div>
    <div class="field"><label>KL Lookback</label><input id="f-kllb" type="number"></div>
    <div class="field"><label>KL Significance Bonus</label><input id="f-klbonus" type="number" step="0.5"></div>
    <div class="field"><label>Lookback Days</label><input id="f-lookback" type="number" min="7" max="90" title="More days = more data but slower. 30 recommended."></div>
  </div>
  <div style="display:flex;gap:8px;margin-top:12px">
    <button class="btn btn-primary" onclick="saveConfig()">💾 Save Config</button>
    <button class="btn" onclick="document.getElementById('config-form-section').style.display='none'" style="background:var(--surface);border:1px solid var(--border)">Cancel</button>
  </div>
</div>

<div class="section">
  <div class="section-title">Saved configs</div>
  <div id="configs-list"><p style="color:var(--dim);font-size:12px">Loading...</p></div>
</div>

<div class="section" id="results-section" style="display:none">
  <div class="section-title" id="results-title">Results</div>
  <div id="results-body"></div>
</div>

<script>
var parsedData = {};

function parseStatusBar() {
  var raw = document.getElementById('status-bar-input').value.trim();
  if (!raw) { alert('Paste a status bar string first'); return; }
  fetch('/backtest/configs/parse', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({status_bar: raw})
  }).then(r => {
    if (!r.ok) { alert('Server error ' + r.status + ' — is the latest main.py deployed?'); return null; }
    return r.json();
  }).then(d => {
    if (!d) return;
    if (d.status === 'error') { alert('Parse error: ' + d.message); return; }
    parsedData = d.settings;
    // Fill form
    document.getElementById('f-name').value    = parsedData.alertName || '';
    document.getElementById('f-rr').value      = parsedData.rrRatio || 3.0;
    document.getElementById('f-sl').value      = parsedData.slAtrMult || 0.1;
    document.getElementById('f-impulse').value = parsedData.impulseRatio || 1.3;
    document.getElementById('f-offset').value  = parsedData.entryOffset || 0.0;
    document.getElementById('f-overlap').value = parsedData.overlapBars || 5;
    document.getElementById('f-atr').value     = parsedData.atrLength || 17;
    document.getElementById('f-cobmult').value = parsedData.cobMinAtrMult || 0.8;
    document.getElementById('f-klprox').value  = parsedData.klProximity || 1.0;
    document.getElementById('f-klscore').value = parsedData.klMinScore || 6;
    document.getElementById('f-kllb').value    = parsedData.klLookback2 || 300;
    document.getElementById('f-klbonus').value = parsedData.klSignificanceBonus || 0.0;
    document.getElementById('f-lookback').value = 30;

    // Show preview
    var prev = document.getElementById('parsed-preview');
    prev.style.display = 'block';
    var keys = ['rrRatio','slAtrMult','impulseRatio','entryOffset','overlapBars','klProximity','klMinScore','klSignificanceBonus'];
    prev.innerHTML = '<strong style="color:var(--blue)">✅ Parsed ' + Object.keys(parsedData).length + ' settings</strong><br>' +
      keys.map(k => '<span style="color:var(--dim)">' + k + ':</span> ' + (parsedData[k] !== undefined ? parsedData[k] : '—')).join('  ·  ');
    document.getElementById('config-form-section').style.display = 'block';
  }).catch(e => alert('Error: ' + e));
}

function saveConfig() {
  var payload = {
    name:         document.getElementById('f-name').value.trim(),
    rr_ratio:     parseFloat(document.getElementById('f-rr').value),
    sl_buf:       parseFloat(document.getElementById('f-sl').value),
    min_impulse:  parseFloat(document.getElementById('f-impulse').value),
    entry_offset: parseFloat(document.getElementById('f-offset').value),
    overlap_bars: parseInt(document.getElementById('f-overlap').value),
    atr_length:   parseInt(document.getElementById('f-atr').value),
    cob_min_atr:  parseFloat(document.getElementById('f-cobmult').value),
    kl_proximity: parseFloat(document.getElementById('f-klprox').value),
    kl_min_score: parseInt(document.getElementById('f-klscore').value),
    kl_lookback:  parseInt(document.getElementById('f-kllb').value),
    kl_bonus:     parseFloat(document.getElementById('f-klbonus').value),
    lookback_days: parseInt(document.getElementById('f-lookback').value) || 30,
    raw_settings: JSON.stringify(parsedData),
  };
  if (!payload.name) { alert('Enter a config name'); return; }
  fetch('/backtest/configs/save', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  }).then(r => r.json()).then(d => {
    if (d.status === 'ok') {
      document.getElementById('config-form-section').style.display = 'none';
      document.getElementById('status-bar-input').value = '';
      document.getElementById('parsed-preview').style.display = 'none';
      loadConfigs();
    } else {
      alert('Save error: ' + d.message);
    }
  });
}

function clearForm() {
  document.getElementById('status-bar-input').value = '';
  document.getElementById('parsed-preview').style.display = 'none';
  document.getElementById('config-form-section').style.display = 'none';
}

function runConfig(id, name) {
  if (!confirm('Run backtest for "' + name + '"? This will take several minutes.')) return;
  fetch('/backtest/configs/' + id + '/run', {method:'POST'})
    .then(r => r.json()).then(d => {
      alert(d.message || d.status);
      pollStatus();
    });
}

function deleteConfig(id, name) {
  if (!confirm('Delete config "' + name + '"?')) return;
  fetch('/backtest/configs/' + id, {method:'DELETE'})
    .then(r => r.json()).then(() => loadConfigs());
}

function showResults(id, name) {
  fetch('/backtest/configs/' + id + '/results')
    .then(r => r.json()).then(d => {
      var sec = document.getElementById('results-section');
      sec.style.display = 'block';
      document.getElementById('results-title').textContent = 'Results — ' + name;
      var rows = d.results || [];
      if (!rows.length) {
        document.getElementById('results-body').innerHTML = '<p style="color:var(--dim);font-size:12px">No results yet — run the backtest first.</p>';
        return;
      }

      // Split into _raw and _kl variants
      var raw = rows.filter(r => r.source.endsWith('_raw'));
      var kl  = rows.filter(r => r.source.endsWith('_kl'));

      function summaryStats(vrows) {
        var w = vrows.reduce((s,r) => s+r.wins, 0);
        var l = vrows.reduce((s,r) => s+r.losses, 0);
        var t = w + l;
        var pnl = vrows.reduce((s,r) => s+(r.total_pnl||0), 0);
        return { w, l, t, pnl: pnl.toFixed(2), wr: t > 0 ? Math.round(w/t*100) : 0 };
      }

      function wrCol(wr) {
        return wr >= 50 ? 'var(--green)' : wr >= 35 ? 'var(--amber)' : 'var(--red)';
      }

      function delta(a, b, suffix) {
        var d = a - b;
        var col = d > 0 ? 'var(--green)' : d < 0 ? 'var(--red)' : 'var(--dim)';
        return '<span style="color:'+col+';font-size:11px">'+(d>0?'+':'')+d.toFixed(suffix||0)+'</span>';
      }

      var rs = summaryStats(raw);
      var ks = summaryStats(kl);

      // Summary header
      var html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">';

      // Raw summary
      html += '<div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px">';
      html += '<div style="font-size:11px;font-weight:500;color:var(--dim);text-transform:uppercase;margin-bottom:10px">Without KL filter</div>';
      html += '<div style="display:flex;gap:20px">';
      html += '<div><div style="font-size:11px;color:var(--dim)">WR</div><div style="font-size:24px;font-weight:500;color:'+wrCol(rs.wr)+'">'+rs.wr+'%</div></div>';
      html += '<div><div style="font-size:11px;color:var(--dim)">Trades</div><div style="font-size:20px;font-weight:500">'+rs.t+'</div></div>';
      html += '<div><div style="font-size:11px;color:var(--dim)">PnL (R)</div><div style="font-size:20px;font-weight:500;color:'+(rs.pnl>=0?'var(--green)':'var(--red)')+'">'+rs.pnl+'</div></div>';
      html += '</div></div>';

      // KL summary
      html += '<div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px">';
      html += '<div style="font-size:11px;font-weight:500;color:var(--dim);text-transform:uppercase;margin-bottom:10px">With KL filter</div>';
      html += '<div style="display:flex;gap:20px">';
      html += '<div><div style="font-size:11px;color:var(--dim)">WR</div><div style="font-size:24px;font-weight:500;color:'+wrCol(ks.wr)+'">'+ks.wr+'% '+delta(ks.wr,rs.wr,0)+'</div></div>';
      html += '<div><div style="font-size:11px;color:var(--dim)">Trades</div><div style="font-size:20px;font-weight:500">'+ks.t+' '+delta(ks.t,rs.t,0)+'</div></div>';
      html += '<div><div style="font-size:11px;color:var(--dim)">PnL (R)</div><div style="font-size:20px;font-weight:500;color:'+(ks.pnl>=0?'var(--green)':'var(--red)')+'">'+ks.pnl+' '+delta(parseFloat(ks.pnl),parseFloat(rs.pnl),1)+'</div></div>';
      html += '</div></div>';
      html += '</div>';

      // Verdict
      var wrDelta = ks.wr - rs.wr;
      var tradeDelta = ks.t - rs.t;
      var pnlDelta = parseFloat(ks.pnl) - parseFloat(rs.pnl);
      var verdict = '';
      if (ks.t === 0) {
        verdict = '⚠️ KL filter removed all signals — proximity too tight or min score too high';
      } else if (wrDelta >= 5 && pnlDelta >= 0) {
        verdict = '✅ KL filter improves WR by '+wrDelta+'% and reduces trades by '+Math.abs(tradeDelta)+' — keep it ON';
      } else if (wrDelta >= 3) {
        verdict = '✅ KL filter improves WR by '+wrDelta+'% but reduces signal count significantly ('+tradeDelta+' trades)';
      } else if (wrDelta < -3) {
        verdict = '❌ KL filter hurts WR by '+Math.abs(wrDelta)+'% — consider turning it OFF';
      } else if (Math.abs(tradeDelta) > rs.t * 0.5) {
        verdict = '⚠️ KL filter removes '+Math.abs(tradeDelta)+' trades ('+Math.round(Math.abs(tradeDelta)/rs.t*100)+'%) with minimal WR gain — filter may be too aggressive';
      } else {
        verdict = '📊 KL filter has minimal impact ('+wrDelta+'% WR change) — neutral';
      }
      html += '<div style="background:rgba(96,165,250,0.07);border:1px solid rgba(96,165,250,0.2);border-radius:6px;padding:10px 14px;margin-bottom:16px;font-size:12px">'+verdict+'</div>';

      // Side-by-side table
      // Build lookup maps
      var rawMap = {}, klMap = {};
      raw.forEach(r => rawMap[r.symbol+'|'+r.timeframe] = r);
      kl.forEach(r  => klMap[r.symbol+'|'+r.timeframe]  = r);
      var allKeys = [...new Set([...Object.keys(rawMap), ...Object.keys(klMap)])];
      // Sort by raw WR desc
      allKeys.sort((a,b) => {
        var ra = rawMap[a] ? rawMap[a].win_rate : 0;
        var rb = rawMap[b] ? rawMap[b].win_rate : 0;
        return rb - ra;
      });

      html += '<table>';
      html += '<tr><th>Symbol</th><th>TF</th>';
      html += '<th colspan="3" style="text-align:center;border-left:1px solid var(--border)">Without KL</th>';
      html += '<th colspan="3" style="text-align:center;border-left:1px solid var(--border)">With KL</th>';
      html += '<th style="border-left:1px solid var(--border)">WR Δ</th></tr>';
      html += '<tr style="font-size:10px;color:var(--dim)">';
      html += '<th></th><th></th>';
      html += '<th style="border-left:1px solid var(--border)">WR%</th><th>Trades</th><th>PnL-R</th>';
      html += '<th style="border-left:1px solid var(--border)">WR%</th><th>Trades</th><th>PnL-R</th>';
      html += '<th style="border-left:1px solid var(--border)"></th></tr>';

      allKeys.forEach(key => {
        var parts = key.split('|');
        var sym = parts[0], tf = parts[1];
        var r = rawMap[key], k = klMap[key];
        var rwr = r ? r.win_rate : null;
        var kwr = k ? k.win_rate : null;
        var wd  = (rwr !== null && kwr !== null) ? kwr - rwr : null;
        var wdCol = wd === null ? 'var(--dim)' : wd > 3 ? 'var(--green)' : wd < -3 ? 'var(--red)' : 'var(--dim)';

        html += '<tr>';
        html += '<td>'+sym+'</td>';
        html += '<td><span class="tag tag-blue">'+tf+'</span></td>';
        // Raw
        if (r) {
          var wc = r.win_rate >= 50 ? '#4caf50' : r.win_rate >= 35 ? '#ffa726' : '#ef5350';
          html += '<td style="border-left:1px solid var(--border)"><span style="color:'+wc+';font-weight:500">'+r.win_rate+'%</span></td>';
          html += '<td style="color:var(--dim)">'+r.total+'</td>';
          html += '<td style="color:'+(r.total_pnl>=0?'var(--green)':'var(--red)')+'">'+( r.total_pnl>=0?'+':'')+r.total_pnl.toFixed(1)+'</td>';
        } else {
          html += '<td style="border-left:1px solid var(--border);color:var(--dim)" colspan="3">—</td>';
        }
        // KL
        if (k) {
          var kc = k.win_rate >= 50 ? '#4caf50' : k.win_rate >= 35 ? '#ffa726' : '#ef5350';
          html += '<td style="border-left:1px solid var(--border)"><span style="color:'+kc+';font-weight:500">'+k.win_rate+'%</span></td>';
          html += '<td style="color:var(--dim)">'+k.total+'</td>';
          html += '<td style="color:'+(k.total_pnl>=0?'var(--green)':'var(--red)')+'">'+( k.total_pnl>=0?'+':'')+k.total_pnl.toFixed(1)+'</td>';
        } else {
          html += '<td style="border-left:1px solid var(--border);color:var(--dim)" colspan="3">—</td>';
        }
        // Delta
        html += '<td style="border-left:1px solid var(--border);font-weight:500;color:'+wdCol+'">';
        html += wd !== null ? (wd>0?'+':'')+wd.toFixed(1)+'%' : '—';
        html += '</td></tr>';
      });
      html += '</table>';

      document.getElementById('results-body').innerHTML = html;
      sec.scrollIntoView({behavior:'smooth'});
    });
}

function loadConfigs() {
  fetch('/backtest/configs/list').then(r => {
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return r.json();
  }).then(d => {
    var configs = (d && d.configs) ? d.configs : [];
    var el = document.getElementById('configs-list');
    if (!el) return;
    if (!configs.length) {
      el.innerHTML = '<p style="color:var(--dim);font-size:12px">No configs saved yet — paste a status bar string above to get started.</p>';
      return;
    }
    var html = '<table><tr><th>Name</th><th>RR</th><th>SL Buf</th><th>Impulse</th><th>Overlap</th><th>KL Bonus</th><th>Lookback</th><th>Saved</th><th>Actions</th></tr>';
    configs.forEach(c => {
      html += '<tr>';
      html += '<td><strong>'+c.name+'</strong></td>';
      html += '<td>'+c.rr_ratio+'×</td>';
      html += '<td>'+c.sl_buf+'</td>';
      html += '<td>'+c.min_impulse+'</td>';
      html += '<td>'+c.overlap_bars+'</td>';
      html += '<td>'+(c.kl_bonus > 0 ? '<span class="tag tag-green">'+c.kl_bonus+'×</span>' : '<span class="tag" style="background:rgba(107,114,128,.15);color:var(--dim)">off</span>')+'</td>';
      html += '<td style="color:var(--dim)">'+(c.lookback_days||30)+'d</td>';
      html += '<td style="color:var(--dim);font-size:11px">'+(c.created_at||'').slice(0,16)+'</td>';
      html += '<td><div style="display:flex;gap:6px">';
      html += '<button class="btn btn-run" data-id="'+c.id+'" data-name="'+c.name+'" data-action="run">▶ Run</button>';
      html += '<button class="btn btn-primary" style="background:rgba(96,165,250,.15);color:var(--blue);border:1px solid rgba(96,165,250,.3)" data-id="'+c.id+'" data-name="'+c.name+'" data-action="results">Results</button>';
      html += '<button class="btn btn-danger" data-id="'+c.id+'" data-name="'+c.name+'" data-action="delete">Delete</button>';
      html += '</div></td></tr>';
    });
    html += '</table>';
    el.innerHTML = html;
  }).catch(e => {
    // Try to init the table then retry once
    fetch('/backtest/configs/init', {method:'POST'}).then(() => {
      document.getElementById('configs-list').innerHTML = '<p style="color:var(--dim);font-size:12px">DB table initialised — no configs yet. Paste a status bar string above.</p>';
    }).catch(() => {
      document.getElementById('configs-list').innerHTML = '<p style="color:var(--red);font-size:12px">Error loading configs: ' + e.message + ' — check Railway logs.</p>';
    });
  });
}

function pollStatus() {
  fetch('/backtest/status').then(r => r.json()).then(d => {
    var el = document.getElementById('running-status');
    if (d.running) {
      el.style.display = 'block';
      var p = d.progress || {};
      var done  = p.done  || 0;
      var total = p.total || 1;
      var pct   = Math.round(done / total * 100);
      document.getElementById('running-text').textContent   = '⏳ ' + (d.status || 'Running...');
      document.getElementById('progress-bar').style.width   = pct + '%';
      document.getElementById('progress-count').textContent = done + ' / ' + total + ' (' + pct + '%)  ·  ' + (p.saved || 0) + ' results saved';
      document.getElementById('progress-current').textContent = p.current ? '→ ' + p.current : '';
      setTimeout(pollStatus, 2000);
    } else {
      el.style.display = 'none';
      if (d.status && d.status !== 'Never run') {
        loadConfigs();
      }
    }
    var toggle = document.getElementById('scheduler-toggle');
    if (toggle) toggle.checked = !!d.daily_backtest_enabled;
  });
}

function toggleScheduler(enabled) {
  fetch('/backtest/scheduler-toggle', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({enabled: enabled})
  }).then(r => r.json()).then(d => {
    document.getElementById('scheduler-sub').textContent = enabled
      ? 'Uses a large number of API calls each run — turn off if you\'re not actively using it.'
      : 'Off — the daily 02:00 UTC run will be skipped until you switch this back on.';
  }).catch(e => alert('Could not update setting: ' + e));
}

loadConfigs();
pollStatus();

// Delegated handler for dynamically created config buttons
document.addEventListener('click', function(e) {
  var btn = e.target.closest('[data-action]');
  if (!btn) return;
  var id   = btn.dataset.id;
  var name = btn.dataset.name;
  var action = btn.dataset.action;
  if (action === 'run')     runConfig(id, name);
  if (action === 'results') showResults(id, name);
  if (action === 'delete')  deleteConfig(id, name);
});
</script>
</body>
</html>
"""


@app.route("/backtest/configs/init", methods=["GET", "POST"])
def backtest_configs_init():
    """Manually create backtest_configs table — call once if page shows errors."""
    try:
        _bt_init_configs_table()
        return jsonify({"status": "ok", "message": "backtest_configs table ready"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/backtest/configs")
def backtest_configs_page():
    return render_template_string(BACKTEST_CONFIGS_HTML)


@app.route("/backtest/configs/parse", methods=["POST"])
def backtest_configs_parse():
    """Parse a TradingView status bar string into settings."""
    try:
        body = request.get_json(force=True)
        raw  = body.get("status_bar", "").strip()
        if not raw:
            return jsonify({"status": "error", "message": "Empty string"}), 400
        settings = _parse_status_bar(raw)
        return jsonify({"status": "ok", "settings": settings})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/backtest/configs/save", methods=["POST"])
def backtest_configs_save():
    """Save a named backtest config."""
    try:
        body = request.get_json(force=True)
        name = body.get("name", "").strip()
        if not name:
            return jsonify({"status": "error", "message": "Name required"}), 400
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO backtest_configs
                    (name, rr_ratio, sl_buf, min_impulse, entry_offset,
                     overlap_bars, atr_length, cob_min_atr, kl_proximity,
                     kl_min_score, kl_lookback, kl_bonus, lookback_days, raw_settings, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (name) DO UPDATE SET
                    rr_ratio=EXCLUDED.rr_ratio, sl_buf=EXCLUDED.sl_buf,
                    min_impulse=EXCLUDED.min_impulse, entry_offset=EXCLUDED.entry_offset,
                    overlap_bars=EXCLUDED.overlap_bars, atr_length=EXCLUDED.atr_length,
                    cob_min_atr=EXCLUDED.cob_min_atr, kl_proximity=EXCLUDED.kl_proximity,
                    kl_min_score=EXCLUDED.kl_min_score, kl_lookback=EXCLUDED.kl_lookback,
                    kl_bonus=EXCLUDED.kl_bonus, lookback_days=EXCLUDED.lookback_days,
                    raw_settings=EXCLUDED.raw_settings
            """, (name,
                  float(body.get("rr_ratio", 3.0)),
                  float(body.get("sl_buf", 0.1)),
                  float(body.get("min_impulse", 1.3)),
                  float(body.get("entry_offset", 0.0)),
                  int(body.get("overlap_bars", 5)),
                  int(body.get("atr_length", 17)),
                  float(body.get("cob_min_atr", 0.8)),
                  float(body.get("kl_proximity", 1.0)),
                  int(body.get("kl_min_score", 6)),
                  int(body.get("kl_lookback", 300)),
                  float(body.get("kl_bonus", 0.0)),
                  int(body.get("lookback_days", 30)),
                  body.get("raw_settings", ""),
                  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        conn.close()
        log.info(f"Backtest config saved: {name}")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/backtest/configs/list")
def backtest_configs_list():
    return jsonify({"configs": _get_bt_configs()})


@app.route("/backtest/configs/<int:cfg_id>/run", methods=["POST"])
def backtest_configs_run(cfg_id):
    if _bt_running:
        return jsonify({"status": "already_running", "message": "Backtest already in progress"}), 200
    threading.Thread(target=run_backtest_for_config, args=(cfg_id,), daemon=True).start()
    return jsonify({"status": "started", "message": f"Backtest started for config {cfg_id}"}), 200


@app.route("/backtest/configs/<int:cfg_id>/results")
def backtest_configs_results(cfg_id):
    """Get backtest results for a specific config."""
    try:
        conn = get_db()
        import psycopg2.extras as _pge
        with conn.cursor(cursor_factory=_pge.RealDictCursor) as cur:
            # Get config name first
            cur.execute("SELECT name FROM backtest_configs WHERE id=%s", (cfg_id,))
            cfg = cur.fetchone()
            if not cfg:
                return jsonify({"results": []})
            name = cfg["name"]
            # Results are stored with variant = "{name}_raw" or "{name}_kl"
            cur.execute("""
                SELECT * FROM backtest_results
                WHERE source LIKE %s OR source LIKE %s
                ORDER BY win_rate DESC
            """, (f"{name}_%", f"{name}_%"))
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"results": rows, "config_name": name})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/backtest/configs/<int:cfg_id>", methods=["DELETE"])
def backtest_configs_delete(cfg_id):
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM backtest_configs WHERE id=%s", (cfg_id,))
            row = cur.fetchone()
            if row:
                name = row[0]
                cur.execute("DELETE FROM backtest_configs WHERE id=%s", (cfg_id,))
                cur.execute("DELETE FROM backtest_results WHERE source LIKE %s OR source LIKE %s",
                           (f"{name}_%", f"{name}_%"))
        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
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

def _delayed_startup():
    time.sleep(3)
    try:
        if callable(globals().get("_bt_init_table")):
            _bt_init_table()
    except Exception as e:
        log.error(f"BT init error: {e}")
    try:
        if callable(globals().get("_bt_init_configs_table")):
            _bt_init_configs_table()
    except Exception as e:
        log.error(f"BT configs init error: {e}")
    try:
        _start_backtest_scheduler()
    except Exception as e:
        log.error(f"Backtest scheduler error: {e}")

def is_restricted_time(windows_str: str, tz_offset: int = 0) -> tuple:
    """Check if current time falls within any restricted trading window.
    windows_str format: 'Fri 22:00-Mon 02:00' or multiple separated by '|'
    Returns (is_restricted: bool, reason: str)
    """
    if not windows_str.strip():
        return False, ""
    from datetime import datetime, timezone, timedelta
    DAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    now_utc   = datetime.now(timezone.utc)
    now_local = now_utc + timedelta(hours=tz_offset)
    now_day   = now_local.weekday()
    now_mins  = now_local.hour * 60 + now_local.minute
    for window in windows_str.split("|"):
        window = window.strip()
        if not window:
            continue
        try:
            parts = window.split("-")
            if len(parts) != 2:
                continue
            start_str, end_str = parts[0].strip(), parts[1].strip()
            s_day_str, s_time  = start_str.split()
            e_day_str, e_time  = end_str.split()
            s_day = DAY_MAP.get(s_day_str.lower())
            e_day = DAY_MAP.get(e_day_str.lower())
            if s_day is None or e_day is None:
                continue
            sh, sm = map(int, s_time.split(":"))
            eh, em = map(int, e_time.split(":"))
            s_mins = sh * 60 + sm
            e_mins = eh * 60 + em
            # Convert to minutes since Monday 00:00
            s_total = s_day * 1440 + s_mins
            e_total = e_day * 1440 + e_mins
            n_total = now_day * 1440 + now_mins
            if s_total <= e_total:
                in_window = s_total <= n_total < e_total
            else:  # wraps around week boundary
                in_window = n_total >= s_total or n_total < e_total
            if in_window:
                return True, f"Restricted window: {window} (UTC+{tz_offset})"
        except Exception as e:
            log.warning(f"Restricted time parse error '{window}': {e}")
    return False, ""


def _restricted_time_watcher():
    """Background thread — cancels open limit orders when restricted window starts."""
    log.info(f"Restricted time watcher started — windows: '{RESTRICTED_TIMES}' UTC+{TIMEZONE_OFFSET}")
    was_restricted = False
    while True:
        try:
            is_restricted, reason = is_restricted_time(RESTRICTED_TIMES, TIMEZONE_OFFSET)
            if is_restricted and not was_restricted:
                log.info(f"Restricted window started: {reason} — cancelling open limit orders")
                try:
                    conn = get_db()
                    with conn.cursor() as cur:
                        cur.execute("SELECT order_id, symbol FROM trades WHERE status='open'")
                        open_trades = cur.fetchall()
                    conn.close()
                    for order_id, symbol in open_trades:
                        if not order_id:
                            continue
                        cancelled = False
                        try:
                            _api_call(session.cancel_order, category="linear",
                                      symbol=symbol, orderId=order_id)
                            log.info(f"Restricted: cancelled {symbol} {order_id}")
                            cancelled = True
                        except Exception as e:
                            # A failed cancel here almost always means the order
                            # already filled and became a live position — not a
                            # pending order to skip. Leave its status alone; the
                            # trail watcher / close-detection logic already owns
                            # that trade's lifecycle.
                            log.warning(f"Restricted: cancel failed {symbol} (likely already filled — leaving trade status untouched): {e}")
                        if cancelled:
                            conn2 = get_db()
                            with conn2.cursor() as cur2:
                                cur2.execute(
                                    "UPDATE trades SET status='skipped', notes=%s WHERE order_id=%s",
                                    (reason, order_id)
                                )
                            conn2.commit()
                            conn2.close()
                except Exception as e:
                    log.error(f"Restricted watcher cancel error: {e}")
            was_restricted = is_restricted
        except Exception as e:
            log.error(f"Restricted watcher error: {e}")
        time.sleep(60)


def _trail_watcher():
    log.info(f"Trail watcher started — BE={BE_TRIGGER_R}R trigger={TP_EXTEND_TRIGGER_R}R trail={TRAIL_STEP_R}R (partial-exit driven per-trade via tp1)")

    # On startup, register any already-open trades from DB
    try:
        import psycopg2.extras as _pge2s
        conn = get_db()
        with conn.cursor(cursor_factory=_pge2s.RealDictCursor) as cur:
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp1 DOUBLE PRECISION")
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp1_pct DOUBLE PRECISION")
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS partial_done BOOLEAN DEFAULT FALSE")
            cur.execute("SELECT order_id, symbol, side, entry, sl, tp1, tp1_pct, partial_done, realized_pnl_partial FROM trades WHERE status='open' AND order_id IS NOT NULL")
            open_trades = cur.fetchall()
        conn.commit()
        conn.close()
        for t in open_trades:
            if t["order_id"] and t["entry"] and t["sl"]:
                _trail_register(t["order_id"], t["symbol"], t["side"],
                                float(t["entry"] or 0), float(t["sl"] or 0),
                                tp1=float(t["tp1"]) if t.get("tp1") is not None else None,
                                tp1_pct=float(t["tp1_pct"] or 0),
                                partial_done=bool(t.get("partial_done", False)),
                                partial_pnl=float(t["realized_pnl_partial"]) if t.get("realized_pnl_partial") is not None else None)
        if open_trades:
            log.info(f"Trail watcher: recovered {len(open_trades)} open trades from DB")
    except Exception as e:
        log.warning(f"Trail watcher startup recovery failed: {e}")

    while True:
        try:
            with _trail_lock:
                tracked = dict(_trail_state)
            for order_id, state in tracked.items():
                try:
                    symbol  = state["symbol"]; side = state["side"]
                    entry   = state["entry"];  risk = state["risk"]
                    is_long = side == "Buy"
                    if risk <= 0: continue

                    # Only need to poll until TP is removed — after that Bybit handles trail natively
                    if state.get("tp_removed"):
                        continue

                    resp    = _api_call(session.get_tickers, category="linear", symbol=symbol)
                    tickers = resp.get("result", {}).get("list", [])
                    if not tickers: continue
                    mark    = float(tickers[0].get("markPrice", 0))
                    if mark <= 0: continue
                    current_r = (mark - entry) / risk if is_long else (entry - mark) / risk

                    # Partial exit — close tp1_pct% of the position at partial_r.
                    # SL is left completely untouched; Bybit's native full-position TP
                    # (already set to the runner target at order placement) auto-tracks
                    # the reduced position size for the remainder.
                    if state.get("tp1") and not state.get("partial_done") and state.get("partial_r") and current_r >= state["partial_r"]:
                        try:
                            live_size = get_live_position_size(symbol)
                            if live_size > 0:
                                info      = get_instrument_info(symbol)
                                close_qty = round_to_step(live_size * state["tp1_pct"] / 100.0, info["qty_step"])
                                if close_qty > 0 and close_qty < live_size:
                                    close_side = "Sell" if is_long else "Buy"
                                    presp = _api_call(session.place_order, category="linear", symbol=symbol,
                                                       side=close_side, orderType="Market", qty=str(close_qty),
                                                       reduceOnly=True, timeInForce="IOC", positionIdx=0,
                                                       orderLinkId=f"{BOT_ORDER_TAG}{uuid.uuid4().hex[:20]}")
                                    if presp.get("retCode", -1) == 0:
                                        p_order_id = presp.get("result", {}).get("orderId", "")
                                        if p_order_id:
                                            with _bot_order_ids_lock:
                                                _bot_order_ids.add(p_order_id)
                                                _bot_order_ids_order.append(p_order_id)
                                                while len(_bot_order_ids_order) > _BOT_ORDER_IDS_MAX:
                                                    _bot_order_ids.discard(_bot_order_ids_order.popleft())
                                        state["partial_done"] = True
                                        with _trail_lock: _trail_state[order_id] = state
                                        log.info(f"Trail: {symbol} partial exit — closed {close_qty} ({state['tp1_pct']}%) at {current_r:.2f}R, SL unchanged")
                                        # Small, independent write — deliberately separate from the
                                        # PnL-accumulator backup write below, which has repeatedly hit
                                        # statement timeouts. This one matters more: without it, a
                                        # server restart before the trade fully closes can't tell a
                                        # partial exit already happened, and will fire a SECOND one on
                                        # the already-reduced position.
                                        try:
                                            pdconn = get_db()
                                            with pdconn.cursor() as pdcur:
                                                pdcur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS partial_done BOOLEAN DEFAULT FALSE")
                                                pdcur.execute("UPDATE trades SET partial_done=TRUE WHERE order_id=%s", (order_id,))
                                            pdconn.commit()
                                            pdconn.close()
                                        except Exception as pd_err:
                                            log.error(f"Trail: FAILED to persist partial_done for {symbol} order_id={order_id} — a restart before this trade fully closes WILL trigger a duplicate partial exit: {pd_err}")
                                    else:
                                        log.warning(f"Trail: partial exit order failed {symbol}: {presp.get('retMsg')}")
                                else:
                                    log.warning(f"Trail: partial exit qty invalid for {symbol} — live_size={live_size} pct={state['tp1_pct']} rounded={close_qty}")
                            else:
                                log.warning(f"Trail: no live position found for {symbol} — skipping partial exit")
                        except Exception as e:
                            log.warning(f"Trail partial exit failed {symbol}: {e}")

                    # BE trigger (optional)
                    if BE_TRIGGER_R > 0 and not state.get("be_done") and current_r >= BE_TRIGGER_R:
                        try:
                            _api_call(session.set_trading_stop, category="linear",
                                      symbol=symbol, stopLoss=str(round(entry, 8)), positionIdx=0)
                            state["be_done"] = True
                            with _trail_lock: _trail_state[order_id] = state
                            log.info(f"Trail: {symbol} BE triggered at {current_r:.2f}R → SL moved to entry {entry}")
                        except Exception as e:
                            log.warning(f"Trail BE failed {symbol}: {e}")

                    # TP extension trigger — cancel TP and activate Bybit native trailing stop
                    if EXTEND_BEYOND_TP and current_r >= TP_EXTEND_TRIGGER_R:
                        # Cancel the TP order
                        try:
                            for o in get_open_orders(symbol):
                                if o.get("reduceOnly") or o.get("orderId") == state.get("tp_order_id"):
                                    _api_call(session.cancel_order, category="linear",
                                              symbol=symbol, orderId=o["orderId"])
                                    log.info(f"Trail: {symbol} TP cancelled at {current_r:.2f}R")
                                    break
                        except Exception as e:
                            log.warning(f"Trail cancel TP {symbol}: {e}")

                        # Set Bybit native trailing stop — distance = TRAIL_STEP_R x risk
                        trail_distance = round(TRAIL_STEP_R * risk, 8)
                        try:
                            _api_call(session.set_trading_stop, category="linear",
                                      symbol=symbol,
                                      trailingStop=str(trail_distance),
                                      positionIdx=0)
                            state["tp_removed"] = True
                            with _trail_lock: _trail_state[order_id] = state
                            log.info(f"Trail: {symbol} native trailing stop set — distance={trail_distance} ({TRAIL_STEP_R}R) at {current_r:.2f}R")
                        except Exception as e:
                            log.warning(f"Trail set_trading_stop failed {symbol}: {e}")

                except Exception as e:
                    log.warning(f"Trail error {order_id}: {e}")
        except Exception as e:
            log.error(f"Trail watcher: {e}")
        time.sleep(5)


def _trail_register(order_id, symbol, side, entry, sl, tp1=None, tp1_pct=0, partial_done=False, partial_pnl=None):
    if not EXTEND_BEYOND_TP and BE_TRIGGER_R <= 0 and not tp1:
        return
    risk = abs(entry - sl)
    if risk <= 0: return
    partial_r = (abs(tp1 - entry) / risk) if tp1 else None
    with _trail_lock:
        # If this trade already had a live in-memory trail state (e.g. a
        # duplicate registration call on the same fill event), don't clobber
        # a partial_done that's already True there — only widen it, never
        # reset it back to False.
        existing = _trail_state.get(order_id, {})
        already_done = existing.get("partial_done", False) or partial_done
        seeded_pnl    = existing.get("partial_pnl", None)
        if seeded_pnl is None:
            seeded_pnl = partial_pnl
        _trail_state[order_id] = {"symbol": symbol, "side": side, "entry": entry, "sl": sl,
                                   "risk": risk, "tp_removed": False, "be_done": False, "trail_sl": sl,
                                   "tp1": tp1, "tp1_pct": tp1_pct, "partial_r": partial_r,
                                   "partial_done": already_done, "partial_pnl": seeded_pnl}
    log.info(f"Trail: registered {symbol} {side} entry={entry} sl={sl}" +
             (f" partial={tp1_pct}% at {partial_r:.2f}R (tp1={tp1})" if tp1 else "") +
             (f" [partial already done, PnL so far={seeded_pnl}]" if already_done else ""))


def _trail_deregister(order_id):
    with _trail_lock: _trail_state.pop(order_id, None)


import os as _os_guard
if not _os_guard.environ.get("_MAIN_STARTED"):
    _os_guard.environ["_MAIN_STARTED"] = "1"
    _start_poller()
    threading.Thread(target=_delayed_startup, daemon=True).start()
    threading.Thread(target=_restricted_time_watcher, daemon=True).start()
    threading.Thread(target=_trail_watcher, daemon=True).start()
