"""
Trade Journal — PostgreSQL database for tracking all trades
Falls back to SQLite if DATABASE_URL is not set (local development)
"""

import os
import logging
from datetime import datetime

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ─── DATABASE BACKEND ─────────────────────────────────────────────────────────
if DATABASE_URL:
    import psycopg2
    import psycopg2.extras

    class _DBConn:
        """Wrapper that makes psycopg2 connection properly close on context exit."""
        def __init__(self, conn):
            self._conn = conn
        def __getattr__(self, name):
            return getattr(self._conn, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            try: self._conn.close()
            except: pass

    def get_db():
        try:
            conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        except Exception:
            conn = psycopg2.connect(DATABASE_URL)
        return _DBConn(conn)

    def init_db():
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id          SERIAL PRIMARY KEY,
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
                        timeframe   TEXT,
                        leverage    INTEGER,
                        opened_at   TEXT    NOT NULL,
                        closed_at   TEXT,
                        notes       TEXT
                    )
                """)
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS timeframe TEXT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS leverage INTEGER")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS notes TEXT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS media TEXT")
            conn.commit()
            log.info("PostgreSQL journal initialised")
        finally:
            conn.close()

    def log_order_placed(symbol, side, qty, entry, sl, tp, order_id, source="fib", timeframe=None, leverage=None, notes=None):
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO trades
                            (symbol, side, status, qty, entry, sl, tp, order_id, source, timeframe, leverage, notes, opened_at)
                        VALUES (%s, %s, 'open', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (symbol, side, qty, entry, sl, tp, order_id, source, timeframe, leverage, notes,
                          datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
                    row_id = cur.fetchone()[0]
                conn.commit()
                return row_id
        except Exception as e:
            log.error(f"log_order_placed failed for {symbol} {side}: {e}")
            import traceback
            log.error(traceback.format_exc())
            return None

    def log_trade_closed(order_id, exit_price, outcome, realised_pnl=None):
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM trades WHERE order_id = %s AND status = 'open'",
                    (order_id,)
                )
                trade = cur.fetchone()
                if not trade:
                    return False

                entry    = trade["entry"]
                qty      = trade["qty"]
                side     = trade["side"]
                leverage = trade.get("leverage", 1) or 1

                # Use Bybit realised PnL if available, otherwise calculate
                if realised_pnl is not None and realised_pnl != 0:
                    pnl = realised_pnl
                elif side == "Buy":
                    pnl = (exit_price - entry) * qty
                else:
                    pnl = (entry - exit_price) * qty

                # PnL % relative to margin used
                margin  = (entry * qty / leverage) if leverage > 0 else entry * qty
                pnl_pct = (pnl / margin * 100) if margin > 0 else 0

                cur.execute("""
                    UPDATE trades SET
                        status     = 'closed',
                        exit_price = %s,
                        pnl        = %s,
                        pnl_pct    = %s,
                        outcome    = %s,
                        closed_at  = %s
                    WHERE order_id = %s
                """, (exit_price, round(pnl, 4), round(pnl_pct, 2), outcome,
                      datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), order_id))
            conn.commit()
            return True

    def log_order_skipped(symbol, side, entry, sl, tp, reason, source="fib"):
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades
                        (symbol, side, status, qty, entry, sl, tp, source, opened_at, notes)
                    VALUES (%s, %s, 'skipped', 0, %s, %s, %s, %s, %s, %s)
                """, (symbol, side, entry, sl, tp, source,
                      datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), reason))
            conn.commit()

    def get_all_trades(limit=200, days=None):
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                if days:
                    cur.execute(
                        "SELECT * FROM trades WHERE opened_at::timestamp >= NOW() - INTERVAL '1 day' * %s ORDER BY opened_at DESC, id DESC LIMIT %s",
                        (days, limit)
                    )
                else:
                    cur.execute(
                        "SELECT * FROM trades ORDER BY opened_at DESC, id DESC LIMIT %s", (limit,)
                    )
                return [dict(r) for r in cur.fetchall()]

    def get_stats():
        with get_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM trades WHERE status = 'closed'")
                closed = cur.fetchall()
                total     = len(closed)
                wins      = sum(1 for t in closed if t["outcome"] == "tp")
                losses    = sum(1 for t in closed if t["outcome"] == "sl")
                resolved  = [t for t in closed if t["outcome"] in ("tp", "sl")]
                total     = len(resolved)
                total_pnl = sum(t["pnl"] or 0 for t in resolved)
                win_rate  = round(wins / total * 100, 1) if total > 0 else 0
                avg_win   = sum(t["pnl"] for t in resolved if t["outcome"] == "tp" and t["pnl"]) / wins if wins > 0 else 0
                avg_loss  = sum(t["pnl"] for t in resolved if t["outcome"] == "sl" and t["pnl"]) / losses if losses > 0 else 0
                cur.execute("SELECT COUNT(*) as count FROM trades WHERE status = 'open'")
                open_count = cur.fetchone()["count"]
                return {
                    "total_closed": total,
                    "wins":         wins,
                    "losses":       losses,
                    "win_rate":     win_rate,
                    "total_pnl":    round(total_pnl, 2),
                    "avg_win":      round(avg_win, 2),
                    "avg_loss":     round(avg_loss, 2),
                    "open_count":   open_count,
                }

    def get_db_for_update():
        return get_db()

else:
    # ─── SQLITE FALLBACK (local dev / no DATABASE_URL) ────────────────────────
    import sqlite3

    DB_PATH = os.getenv("JOURNAL_DB", "journal.db")
    db_dir  = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    def get_db():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db():
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
                    timeframe   TEXT,
                    leverage    INTEGER,
                    opened_at   TEXT    NOT NULL,
                    closed_at   TEXT,
                    notes       TEXT
                )
            """)
            conn.commit()
        log.info("SQLite journal initialised")

    def log_order_placed(symbol, side, qty, entry, sl, tp, order_id, source="fib", timeframe=None, leverage=None, notes=None):
        with get_db() as conn:
            cur = conn.execute("""
                INSERT INTO trades
                    (symbol, side, status, qty, entry, sl, tp, order_id, source, timeframe, leverage, notes, opened_at)
                VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, side, qty, entry, sl, tp, order_id, source, timeframe, leverage, notes,
                  datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")))
            conn.commit()
            return cur.lastrowid

    def log_trade_closed(order_id, exit_price, outcome, realised_pnl=None):
        with get_db() as conn:
            trade = conn.execute(
                "SELECT * FROM trades WHERE order_id = ? AND status = 'open'",
                (order_id,)
            ).fetchone()
            if not trade:
                return False
            entry    = trade["entry"]
            qty      = trade["qty"]
            side     = trade["side"]
            leverage = trade["leverage"] if trade["leverage"] else 1

            # Use Bybit realised PnL if available, otherwise calculate
            if realised_pnl is not None and realised_pnl != 0:
                pnl = realised_pnl
            elif side == "Buy":
                pnl = (exit_price - entry) * qty
            else:
                pnl = (entry - exit_price) * qty

            # PnL % relative to margin used
            margin  = (entry * qty / leverage) if leverage > 0 else entry * qty
            pnl_pct = (pnl / margin * 100) if margin > 0 else 0

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
            total     = len(closed)
            wins      = sum(1 for t in closed if t["outcome"] == "tp")
            losses    = sum(1 for t in closed if t["outcome"] == "sl")
            resolved  = [t for t in closed if t["outcome"] in ("tp", "sl")]
            total     = len(resolved)
            total_pnl = sum(t["pnl"] or 0 for t in resolved)
            win_rate  = round(wins / total * 100, 1) if total > 0 else 0
            avg_win   = sum(t["pnl"] for t in closed if t["outcome"] == "tp" and t["pnl"]) / wins if wins > 0 else 0
            avg_loss  = sum(t["pnl"] for t in closed if t["outcome"] == "sl" and t["pnl"]) / losses if losses > 0 else 0
            open_count = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE status = 'open'"
            ).fetchone()[0]
            return {
                "total_closed": total,
                "wins":         wins,
                "losses":       losses,
                "win_rate":     win_rate,
                "total_pnl":    round(total_pnl, 2),
                "avg_win":      round(avg_win, 2),
                "avg_loss":     round(avg_loss, 2),
                "open_count":   open_count,
            }

    def get_db_for_update():
        return get_db()


# ─── INIT ON IMPORT ───────────────────────────────────────────────────────────
try:
    init_db()
except Exception as e:
    log.error(f"Failed to initialise database: {e}")


# ─── PLACEHOLDER HELPER ───────────────────────────────────────────────────────
# Returns the correct SQL placeholder for the current database backend
def ph():
    """Return SQL placeholder — %s for PostgreSQL, ? for SQLite."""
    return "%s" if DATABASE_URL else "?"
