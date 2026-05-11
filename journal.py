"""
Trade Journal — SQLite database for tracking all trades
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.getenv("JOURNAL_DB", "journal.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT    NOT NULL,
                side        TEXT    NOT NULL,
                status      TEXT    NOT NULL DEFAULT 'open',
                qty         REAL    NOT NULL,
                entry       REAL    NOT NULL,
                sl          REAL    NOT NULL,
                tp          REAL    NOT NULL,
                exit_price  REAL,
                pnl         REAL,
                pnl_pct     REAL,
                outcome     TEXT,
                order_id    TEXT,
                source      TEXT,
                opened_at   TEXT    NOT NULL,
                closed_at   TEXT,
                notes       TEXT
            )
        """)
        conn.commit()


def log_order_placed(symbol, side, qty, entry, sl, tp, order_id, source="fib"):
    """Log a new order when it's placed."""
    with get_db() as conn:
        cur = conn.execute("""
            INSERT INTO trades
                (symbol, side, status, qty, entry, sl, tp, order_id, source, opened_at)
            VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?, ?)
        """, (symbol, side, qty, entry, sl, tp, order_id, source,
              datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        return cur.lastrowid


def log_trade_closed(order_id, exit_price, outcome):
    """
    Update trade when it closes (TP or SL hit).
    outcome: 'tp' or 'sl'
    """
    with get_db() as conn:
        trade = conn.execute(
            "SELECT * FROM trades WHERE order_id = ? AND status = 'open'",
            (order_id,)
        ).fetchone()

        if not trade:
            return False

        entry = trade["entry"]
        qty   = trade["qty"]
        side  = trade["side"]

        # Calculate PnL
        if side == "Buy":
            pnl = (exit_price - entry) * qty
            pnl_pct = ((exit_price - entry) / entry) * 100
        else:
            pnl = (entry - exit_price) * qty
            pnl_pct = ((entry - exit_price) / entry) * 100

        conn.execute("""
            UPDATE trades SET
                status     = 'closed',
                exit_price = ?,
                pnl        = ?,
                pnl_pct    = ?,
                outcome    = ?,
                closed_at  = ?
            WHERE order_id = ?
        """, (exit_price, round(pnl, 4), round(pnl_pct, 2), outcome,
              datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), order_id))
        conn.commit()
        return True


def log_order_skipped(symbol, side, entry, sl, tp, reason, source="fib"):
    """Log skipped signals for review."""
    with get_db() as conn:
        conn.execute("""
            INSERT INTO trades
                (symbol, side, status, qty, entry, sl, tp, source, opened_at, notes)
            VALUES (?, ?, 'skipped', 0, ?, ?, ?, ?, ?, ?)
        """, (symbol, side, entry, sl, tp, source,
              datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), reason))
        conn.commit()


def get_all_trades(limit=200):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM trades ORDER BY opened_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_stats():
    with get_db() as conn:
        closed = conn.execute(
            "SELECT * FROM trades WHERE status = 'closed'"
        ).fetchall()

        total   = len(closed)
        wins    = sum(1 for t in closed if t["outcome"] == "tp")
        losses  = sum(1 for t in closed if t["outcome"] == "sl")
        total_pnl = sum(t["pnl"] or 0 for t in closed)
        win_rate  = round(wins / total * 100, 1) if total > 0 else 0
        avg_win   = sum(t["pnl"] for t in closed if t["outcome"] == "tp" and t["pnl"]) / wins if wins > 0 else 0
        avg_loss  = sum(t["pnl"] for t in closed if t["outcome"] == "sl" and t["pnl"]) / losses if losses > 0 else 0

        open_trades = conn.execute(
            "SELECT * FROM trades WHERE status = 'open'"
        ).fetchall()

        return {
            "total_closed": total,
            "wins":         wins,
            "losses":       losses,
            "win_rate":     win_rate,
            "total_pnl":    round(total_pnl, 2),
            "avg_win":      round(avg_win, 2),
            "avg_loss":     round(avg_loss, 2),
            "open_count":   len(open_trades),
        }


# Initialise on import
init_db()
