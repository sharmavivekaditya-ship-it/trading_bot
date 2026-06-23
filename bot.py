"""
First-Orbit Trader PRO — NSE Swing Trading Bot
Pure algorithmic, no AI API required.

STRATEGY : Dual RSI Momentum + Market Cap Filter
ENTRY    : Weekly RSI(14) > 60 AND Daily RSI(14) > 60 AND MCap > Rs.20,000 Cr
STOP     : Entry - 2.0 x ATR(14)
TARGET   : Entry + 3.0 x ATR(14)
EXIT     : Daily RSI < 50  |  Weekly RSI < 55  |  Bearish divergence (min 2 days)  |  Hard stop
SIZE     : qty = Rs.800 / (entry - stop)
SCAN     : Two-pass — collect ALL setups across Nifty 500, rank by score, trade TOP 5 only
UNIVERSE : Nifty 500 (covers ~95% of NSE market cap)
"""

import time, sqlite3, os, logging, io, math
from datetime import datetime, date, timedelta
import urllib.request

# ── PATHS ─────────────────────────────────────────────────────────────────────
DB_PATH  = "/data/trades.db"   if os.path.isdir("/data") else "trades.db"
LOG_PATH = "/data/bot.log"     if os.path.isdir("/data") else "bot.log"

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()]
)
log = logging.getLogger("bot")

# ── INSTALL DEPS ──────────────────────────────────────────────────────────────
def ensure_deps():
    import importlib, subprocess, sys
    for pkg in ["yfinance", "curl_cffi", "pandas", "numpy", "flask"]:
        try:
            importlib.import_module(pkg.replace("-", "_"))
        except ImportError:
            log.info(f"Installing {pkg}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

ensure_deps()

import yfinance as yf
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string
import threading

# ── CONFIG ────────────────────────────────────────────────────────────────────
CAPITAL          = 100_000
MAX_WEEKLY_RISK  = 3_000
RISK_PER_TRADE   = 800

# ── EXECUTION MODE ────────────────────────────────────────────────────────────
# LIVE_MODE stays False until: (1) Kite app created, (2) DDPI mandate cleared,
# (3) static-IP hosting in place, (4) real signal-exit track record exists.
LIVE_MODE        = os.environ.get("FIRST_ORBIT_LIVE", "0") == "1"
KITE_API_KEY     = os.environ.get("KITE_API_KEY", "")
KITE_ACCESS_TOKEN= os.environ.get("KITE_ACCESS_TOKEN", "")
MAX_OPEN         = 5           # hard cap on concurrent positions
TOP_N            = 5           # take top N setups by score each scan
SCAN_INTERVAL    = 300         # seconds between cycles
BATCH_SIZE       = 50
BATCH_PAUSE      = 1

# Quality gates
MIN_PRICE        = 50
MAX_PRICE        = 50_000
MIN_AVG_VOL      = 200_000
MIN_SCORE        = 5.0         # minimum composite score to qualify for trading
MIN_MCAP_CR      = 20_000      # Rs. Crores

# Strategy parameters
RSI_PERIOD       = 14
WEEKLY_RSI_MIN   = 60          # entry: weekly RSI floor
WEEKLY_RSI_MAX   = 999         # entry: weekly RSI ceiling (none — trend can be strong)
DAILY_RSI_MIN    = 57          # entry: daily RSI floor
DAILY_RSI_MAX    = 67          # entry: daily RSI ceiling (not overextended)
DAILY_RSI_EXIT   = 52          # exit: daily RSI fade (tighter for fast exits)
WEEKLY_RSI_EXIT  = 52          # exit: weekly RSI drops below this
ATR_PERIOD       = 14
ATR_STOP_MULT    = 2.0         # stop  = entry - 2.0*ATR (swing, survives gaps)
ATR_TARGET_MULT  = 3.0         # target = entry + 3.0*ATR  → R:R 1.5 (swing)
DIV_LOOKBACK     = 10          # bars for divergence detection
MIN_DAYS_DIV     = 2           # min days held before divergence can trigger

NIFTY500_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
FALLBACK_SYMS = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","ITC","LT","AXISBANK","ASIANPAINT","MARUTI","SUNPHARMA","TATAMOTORS",
    "ULTRACEMCO","WIPRO","NESTLEIND","POWERGRID","NTPC","TECHM","HCLTECH","BAJFINANCE",
    "BAJAJFINSV","TITAN","ADANIPORTS","ONGC","DIVISLAB","DRREDDY","CIPLA","COALINDIA",
    "JSWSTEEL","TATASTEEL","INDUSINDBK","HINDALCO","BPCL","GRASIM","BRITANNIA",
    "EICHERMOT","HEROMOTOCO","M&M","APOLLOHOSP","TATACONSUM","DABUR","PIDILITIND",
    "BERGEPAINT","LUPIN","TORNTPHARM","MUTHOOTFIN","CHOLAFIN","SBILIFE","HDFCLIFE",
    "ICICIGI","BANDHANBNK","FEDERALBNK","PNB","CANBK","BANKBARODA","PERSISTENT",
    "LTIM","MPHASIS","COFORGE","ZOMATO","IRCTC","TATAPOWER","PFC","RECLTD",
    "BEL","HAL","BHEL","SAIL","NMDC","VEDL","POLYCAB","HAVELLS","VOLTAS",
    "ABB","SIEMENS","CUMMINSIND","DIXON","TATAELXSI","KPITTECH","APOLLOTYRE",
    "MRF","BAJAJ-AUTO","TVSMOTOR","ALKEM","AUROPHARMA","IPCA","LAURUS","DIVI",
]

# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id          TEXT PRIMARY KEY,
        sym         TEXT,
        direction   TEXT,
        entry       REAL,
        sl          REAL,
        target      REAL,
        qty         INTEGER,
        risk_amt    REAL,
        target_gain REAL,
        rr          REAL,
        score       REAL  DEFAULT 0,
        status      TEXT  DEFAULT 'open',
        pnl         REAL  DEFAULT 0,
        days_held   INTEGER DEFAULT 0,
        opened_at   TEXT,
        closed_at   TEXT,
        exit_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        pnl        REAL DEFAULT 0,
        risk_used  REAL DEFAULT 0,
        wins       INTEGER DEFAULT 0,
        losses     INTEGER DEFAULT 0,
        time_exits INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS screener_cache (
        sym          TEXT PRIMARY KEY,
        price        REAL,
        daily_rsi    REAL,
        weekly_rsi   REAL,
        atr          REAL,
        score        REAL,
        entry        REAL,
        sl           REAL,
        target       REAL,
        reject       TEXT,
        updated_at   TEXT
    );
    """)
    con.commit()
    # Before creating the unique index, collapse any pre-existing duplicate open
    # rows (from the old buggy build) — otherwise the index creation would fail.
    dupes = con.execute(
        "SELECT sym FROM trades WHERE status='open' GROUP BY sym HAVING COUNT(*) > 1"
    ).fetchall()
    for (sym,) in dupes:
        ids = [r[0] for r in con.execute(
            "SELECT id FROM trades WHERE sym=? AND status='open' ORDER BY opened_at DESC", (sym,)
        ).fetchall()]
        for tid in ids[1:]:
            con.execute(
                "UPDATE trades SET status='loss',pnl=0,closed_at=?,exit_reason='DUP_CANCELLED' WHERE id=?",
                (datetime.now().isoformat(), tid)
            )
    con.commit()
    # Now safe to enforce: at most ONE open row per symbol.
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_one_open_per_sym "
        "ON trades(sym) WHERE status='open'"
    )
    con.commit()
    return con


# ── BROKER EXECUTION LAYER ────────────────────────────────────────────────────
# All order placement and live pricing routes through Broker. In paper mode it
# simulates fills at the given price. In live mode it calls Kite Connect. The
# strategy logic above never changes — only this layer swaps paper <-> live.
# ── KITE SESSION TOKEN (set via dashboard "Authenticate" button each morning) ──
# Token is stored in the persistent DB so it survives restarts within the day.
# Expires ~6 AM IST; operator re-authenticates each morning via the button.
def _save_token(token):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("CREATE TABLE IF NOT EXISTS kite_session (id INTEGER PRIMARY KEY CHECK(id=1), token TEXT, created_at TEXT)")
        con.execute("INSERT OR REPLACE INTO kite_session (id, token, created_at) VALUES (1, ?, ?)",
                    (token, datetime.now().isoformat()))
        con.commit(); con.close()
    except Exception as e:
        log.error(f"  Token save failed: {e}")

def _get_saved_token():
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("CREATE TABLE IF NOT EXISTS kite_session (id INTEGER PRIMARY KEY CHECK(id=1), token TEXT, created_at TEXT)")
        row = con.execute("SELECT token, created_at FROM kite_session WHERE id=1").fetchone()
        con.close()
        if not row: return None
        token, created = row
        # Token invalid after 6 AM IST the day after creation
        created_dt = datetime.fromisoformat(created)
        expiry = (created_dt + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
        if datetime.now() > expiry:
            return None  # stale — needs morning re-auth
        return token
    except Exception:
        return None


class Broker:
    def __init__(self):
        self.live = LIVE_MODE
        self.kite = None
        if self.live:
            try:
                from kiteconnect import KiteConnect
                token = _get_saved_token() or KITE_ACCESS_TOKEN
                if not token:
                    raise RuntimeError("No Kite access token — authenticate via dashboard button first")
                self.kite = KiteConnect(api_key=KITE_API_KEY)
                self.kite.set_access_token(token)
                log.info("  Broker: LIVE mode — Kite Connect active")
            except Exception as e:
                log.error(f"  Broker: Kite init failed ({e}) — refusing to trade live")
                # Fail safe: if live init fails, do NOT silently fall back to paper
                # and do NOT trade. Raise so the operator notices.
                raise
        else:
            log.info("  Broker: PAPER mode — simulated fills, no real orders")

    def _refresh_token(self):
        """Pull the latest token saved by the morning Authenticate button."""
        if not self.live:
            return True
        token = _get_saved_token()
        if not token:
            return False
        try:
            self.kite.set_access_token(token)
            return True
        except Exception:
            return False

    def place_buy(self, sym, qty, price):
        """Place a BUY. Paper: returns simulated fill. Live: real CNC market order."""
        if not self.live:
            return {"status": "PAPER_FILL", "sym": sym, "qty": qty, "price": price}
        if not self._refresh_token():
            return {"status": "NO_TOKEN", "sym": sym, "note": "re-authenticate via dashboard"}
        # LIVE — real order. CNC = delivery (for swing holds).
        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange=self.kite.EXCHANGE_NSE,
            tradingsymbol=sym,
            transaction_type=self.kite.TRANSACTION_TYPE_BUY,
            quantity=qty,
            product=self.kite.PRODUCT_CNC,
            order_type=self.kite.ORDER_TYPE_MARKET,
        )
        return {"status": "LIVE_PLACED", "order_id": order_id, "sym": sym, "qty": qty}

    def place_sell(self, sym, qty, price):
        """Place a SELL. Paper: simulated. Live: real CNC market sell (needs DDPI)."""
        if not self.live:
            return {"status": "PAPER_FILL", "sym": sym, "qty": qty, "price": price}
        if not self._refresh_token():
            return {"status": "NO_TOKEN", "sym": sym, "note": "re-authenticate via dashboard"}
        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange=self.kite.EXCHANGE_NSE,
            tradingsymbol=sym,
            transaction_type=self.kite.TRANSACTION_TYPE_SELL,
            quantity=qty,
            product=self.kite.PRODUCT_CNC,
            order_type=self.kite.ORDER_TYPE_MARKET,
        )
        return {"status": "LIVE_PLACED", "order_id": order_id, "sym": sym, "qty": qty}

    def mode_label(self):
        return "LIVE" if self.live else "PAPER"

broker = Broker()

# ── MARKET HOURS ─────────────────────────────────────────────────────────────
def ist_now():
    return datetime.now(tz=__import__("datetime").timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)

def is_market_open():
    t = ist_now()
    if t.weekday() >= 5:
        return False
    o = t.replace(hour=9, minute=15, second=0, microsecond=0)
    c = t.replace(hour=15, minute=30, second=0, microsecond=0)
    return o <= t <= c

def secs_to_open():
    t = ist_now()
    nxt = t.replace(hour=9, minute=15, second=0, microsecond=0)
    if t >= nxt:
        nxt += timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return max(0, int((nxt - t).total_seconds()))

def fmt_time(s):
    h, r = divmod(int(s), 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# ── UNIVERSE ──────────────────────────────────────────────────────────────────
def fetch_universe():
    try:
        req = urllib.request.Request(
            NIFTY500_URL,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://nseindia.com"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            df = pd.read_csv(io.StringIO(r.read().decode("latin-1")))
        col = next(c for c in df.columns if "symbol" in c.lower())
        syms = [s for s in df[col].dropna().str.strip()
                if s and not s.upper().startswith("DUMMY") and len(s) <= 20]
        log.info(f"Universe: {len(syms)} Nifty 500 symbols")
        return syms
    except Exception as e:
        log.warning(f"Nifty500 fetch failed ({e}) — using {len(FALLBACK_SYMS)}-stock fallback")
        return FALLBACK_SYMS

# ── INDICATORS ────────────────────────────────────────────────────────────────
def rsi(close, period=14):
    close = np.array(close, dtype=float)
    d = np.diff(close)
    g = np.where(d > 0, d, 0.0)
    l = np.where(d < 0, -d, 0.0)
    ag, al = g[:period].mean(), l[:period].mean()
    vals = []
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
        vals.append(100.0 - 100.0 / (1.0 + ag / al) if al > 0 else 100.0)
    return vals  # most recent last

def ema(close, span):
    return float(pd.Series(close).ewm(span=span, adjust=False).mean().iloc[-1])

def atr(high, low, close, period=14):
    h, l, c = high[1:], low[1:], close[:-1]
    tr = np.maximum(h - l, np.maximum(np.abs(h - c), np.abs(l - c)))
    return float(tr[-period:].mean())

def get_mcap(sym):
    try:
        return (getattr(yf.Ticker(sym + ".NS").fast_info, "market_cap", None) or 0) / 1e7
    except Exception:
        return 0

def weekly_rsi(sym, period=14):
    try:
        df = yf.Ticker(sym + ".NS").history(period="2y", interval="1wk",
                                             timeout=12, auto_adjust=True)
        if df is None or len(df) < period + 5:
            return 0.0
        r = rsi(df["Close"].values, period)
        return float(r[-1]) if r else 0.0
    except Exception:
        return 0.0

# ── SCREENER ──────────────────────────────────────────────────────────────────
def screen(sym, cache_cutoff, con):
    """Return (setup_dict, None) on valid entry, (None, reason) otherwise."""
    # Skip stocks closed today by ANY exit (manual, RSI, stop, divergence) —
    # prevents same-day churn where a signal exit frees the slot and the stock
    # is immediately re-bought, only to exit again on the same reading.
    today = datetime.now().date().isoformat()
    closed_today = con.execute(
        "SELECT 1 FROM trades WHERE sym=? AND status IN ('win','loss') "
        "AND closed_at >= ?", (sym, today)
    ).fetchone()
    if closed_today:
        return None, "closed_today"
    row = con.execute(
        "SELECT price,daily_rsi,weekly_rsi,atr,score,entry,sl,target,reject "
        "FROM screener_cache WHERE sym=? AND updated_at>?",
        (sym, cache_cutoff)
    ).fetchone()
    if row:
        price, drsi, wrsi, atr_v, score, entry, sl, tgt, reject = row
        if entry:
            return {"sym": sym, "price": price, "daily_rsi": drsi,
                    "weekly_rsi": wrsi, "atr": atr_v, "score": score,
                    "entry": entry, "sl": sl, "target": tgt,
                    "rr": round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)}, None
        return None, f"cached:{reject}"

    now = datetime.now().isoformat()
    reject = None
    try:
        df = yf.Ticker(sym + ".NS").history(period="90d", interval="1d",
                                             timeout=12, auto_adjust=True)
        if df is None or len(df) < RSI_PERIOD + 5:
            reject = "no_data"
        else:
            c  = df["Close"].values.astype(float)
            h  = df["High"].values.astype(float)
            lo = df["Low"].values.astype(float)
            v  = df["Volume"].values.astype(float)
            price   = float(c[-1])
            avg_vol = float(v[-20:].mean())

            if   price < MIN_PRICE:     reject = f"price_low"
            elif price > MAX_PRICE:     reject = f"price_high"
            elif avg_vol < MIN_AVG_VOL: reject = f"low_vol"
            else:
                mcap = get_mcap(sym)
                if mcap < MIN_MCAP_CR:
                    reject = f"small_cap"
                else:
                    d_rsi = rsi(c, RSI_PERIOD)
                    if not d_rsi:
                        reject = "rsi_error"
                    elif float(d_rsi[-1]) < DAILY_RSI_MIN:
                        reject = f"daily_rsi_low({float(d_rsi[-1]):.0f})"
                    elif float(d_rsi[-1]) > DAILY_RSI_MAX:
                        reject = f"daily_rsi_high({float(d_rsi[-1]):.0f})"  # overbought
                    else:
                        drsi_val = float(d_rsi[-1])
                        wrsi_val = weekly_rsi(sym)
                        if wrsi_val < WEEKLY_RSI_MIN:
                            reject = f"weekly_rsi_low({wrsi_val:.0f})"
                        elif wrsi_val > WEEKLY_RSI_MAX:
                            pass  # no weekly ceiling — strong trend is fine
                        else:
                            # Quality filter 1: RSI 3-day trend check
                            # Not a hard "must be rising" — allows healthy consolidation
                            # Rejects only if RSI is in a clear 3-day downtrend within zone
                            # Compute RSI trend metrics — available throughout this block
                            rsi_prev1    = float(d_rsi[-2]) if len(d_rsi) >= 2 else drsi_val
                            rsi_prev2    = float(d_rsi[-3]) if len(d_rsi) >= 3 else rsi_prev1
                            rsi_3d_trend = drsi_val - rsi_prev2  # +ve = rising, -ve = falling
                            rsi_1d_drop  = rsi_prev1 - drsi_val  # how much fell today
                            if rsi_1d_drop > 5:
                                # Sharp 1-day drop — clear deterioration
                                reject = f"rsi_sharp_drop({drsi_val:.0f},{rsi_1d_drop:.1f}pts)"
                            elif rsi_3d_trend < -4:
                                # Falling for 3 days — trend weakening
                                reject = f"rsi_3d_falling({drsi_val:.0f},{rsi_3d_trend:.1f}pts)"
                            # Quality filter 2: price above 20-day EMA (uptrend only)
                            elif price < float(pd.Series(c).ewm(span=20, adjust=False).mean().iloc[-1]) * 0.98:
                                reject = "below_ema20"
                            # Quality filter 3: not in last 5% of ATR move (not stretched)
                            else:
                                atr_val = atr(h, lo, c, ATR_PERIOD)
                                # Check if price has already moved > 1.5x ATR from 10d low
                                low_10d = float(np.min(lo[-10:]))
                                if price > low_10d + 1.5 * atr_val:
                                    reject = "overextended"
                                else:
                                    entry   = round(price, 2)
                                    sl      = round(entry - ATR_STOP_MULT * atr_val, 2)
                                    target  = round(entry + ATR_TARGET_MULT * atr_val, 2)
                                    # Quality filter 4: minimum R:R viability
                                    if (target - entry) < (entry - sl) * 1.4:
                                        reject = "poor_rr"
                                    else:
                                        # Score rewards proximity to midpoint (62), not raw RSI height
                                        # Peak score at RSI=62, falls off toward both edges (57 and 67)
                                        # This means RSI 62 > RSI 66 — sweet spot, not ceiling chaser
                                        RSI_MID = 62.0
                                        RSI_HALF = 5.0  # half-range (57 to 67)
                                        w_quality = max(0.0, RSI_HALF - abs(wrsi_val - RSI_MID)) / RSI_HALF  # 0→1
                                        d_quality = max(0.0, RSI_HALF - abs(drsi_val - RSI_MID)) / RSI_HALF  # 0→1
                                        rsi_score = (w_quality + d_quality) * 10.0  # max 20 pts
                                        mcap_score = min(15, (mcap / 100_000) * 4)   # max 15 pts
                                        # Score bonus: 3-day RSI trend, not just 1-day
                                        # rsi_3d_trend already defined above in same scope
                                        rsi_bonus = 2.0 if rsi_3d_trend > 1 else (0.0 if rsi_3d_trend >= -1 else -1.0)
                                        score = round(rsi_score + mcap_score + rsi_bonus, 1)
                            con.execute("""
                                INSERT OR REPLACE INTO screener_cache
                                (sym,price,daily_rsi,weekly_rsi,atr,score,
                                 entry,sl,target,reject,updated_at)
                                VALUES (?,?,?,?,?,?,?,?,?,NULL,?)
                            """, (sym, price, round(drsi_val, 1), round(wrsi_val, 1),
                                  round(atr_val, 2), score, entry, sl, target, now))
                            con.commit()
                            return {"sym": sym, "price": price,
                                    "daily_rsi": round(drsi_val, 1),
                                    "weekly_rsi": round(wrsi_val, 1),
                                    "atr": round(atr_val, 2), "score": score,
                                    "entry": entry, "sl": sl, "target": target,
                                    "rr": round(ATR_TARGET_MULT / ATR_STOP_MULT, 2),
                                    "mcap_cr": round(mcap, 0)}, None
    except Exception as e:
        reject = f"error"

    con.execute("""
        INSERT OR REPLACE INTO screener_cache
        (sym,price,daily_rsi,weekly_rsi,atr,score,entry,sl,target,reject,updated_at)
        VALUES (?,0,0,0,0,0,NULL,NULL,NULL,?,?)
    """, (sym, reject, now))
    return None, reject

# ── DIVERGENCE CHECK ──────────────────────────────────────────────────────────
def bearish_divergence(sym, lookback=10):
    try:
        df = yf.Ticker(sym + ".NS").history(period="30d", interval="1d",
                                             timeout=8, auto_adjust=True)
        if len(df) < lookback + 5:
            return False
        c = df["Close"].values.astype(float)
        r = rsi(c, RSI_PERIOD)
        if len(r) < lookback:
            return False
        price_now  = c[-1]
        price_prev = min(c[-(lookback + 1):-1])
        rsi_now    = r[-1]
        rsi_peak   = max(r[-(lookback + 1):-1])
        return price_now >= price_prev * 1.02 and rsi_now < rsi_peak * 0.95
    except Exception:
        return False

# ── POSITION MANAGEMENT ───────────────────────────────────────────────────────
def manage_positions(con):
    cols = ["id","sym","direction","entry","sl","target","qty",
            "risk_amt","target_gain","rr","score","status","pnl",
            "days_held","opened_at","closed_at","exit_reason"]
    rows = con.execute("SELECT * FROM trades WHERE status='open'").fetchall()
    if not rows:
        log.info("  No open positions")
        return
    seen_syms = set()
    for row in rows:
        t = dict(zip(cols, row))
        # Safety: if a duplicate open row for this symbol slipped through, only
        # process the first one this cycle and cancel the rest (prevents the
        # "sold multiple times" bug — same symbol selling on every duplicate row).
        if t["sym"] in seen_syms:
            con.execute(
                "UPDATE trades SET status='loss',pnl=0,closed_at=?,exit_reason='DUP_CANCELLED' WHERE id=?",
                (datetime.now().isoformat(), t["id"])
            )
            con.commit()
            log.warning(f"  {t['sym']}: duplicate open row cancelled (kept the first)")
            continue
        seen_syms.add(t["sym"])
        # Fetch live price
        try:
            hist = yf.Ticker(t["sym"] + ".NS").history(
                period="5d", interval="1d", timeout=8, auto_adjust=True)
            price = float(hist["Close"].iloc[-1]) if len(hist) > 0 else None
        except Exception:
            price = None
        # Backstop: never act on a missing or nan price (stale/after-hours data).
        if price is None or not math.isfinite(price):
            log.warning(f"  {t['sym']}: no valid price (got {price}) — skipping, no exit")
            continue
        # Update days held
        try:
            days = (datetime.now() - datetime.fromisoformat(t["opened_at"])).days
        except Exception:
            days = 0
        con.execute("UPDATE trades SET days_held=? WHERE id=?", (days, t["id"]))
        # Check exits
        reason = None
        if price <= t["sl"]:
            reason = "HARD_STOP"
        if not reason:
            try:
                df2 = yf.Ticker(t["sym"] + ".NS").history(
                    period="45d", interval="1d", timeout=8, auto_adjust=True)
                if len(df2) >= RSI_PERIOD + 2:
                    r2 = rsi(df2["Close"].values, RSI_PERIOD)
                    if r2 and float(r2[-1]) < DAILY_RSI_EXIT:
                        reason = f"DAILY_RSI({float(r2[-1]):.0f}<{DAILY_RSI_EXIT})"
            except Exception:
                pass
        if not reason:
            try:
                wr = weekly_rsi(t["sym"])
                if 0 < wr < WEEKLY_RSI_EXIT:
                    reason = f"WEEKLY_RSI({wr:.0f}<{WEEKLY_RSI_EXIT})"
            except Exception:
                pass
        if not reason and days >= MIN_DAYS_DIV:
            if bearish_divergence(t["sym"], DIV_LOOKBACK):
                reason = "DIVERGENCE"
        # Swing mode: carry positions across days. Exit only on a real signal
        # (hard stop, RSI fade, weekly fade, or divergence). No time-based cap.
        if reason:
            pnl    = round((price - t["entry"]) * (t["qty"] or 1), 2)
            status = "win" if pnl > 0 else "loss"
            # Route exit through broker — paper simulates, live places real Kite sell
            fill = broker.place_sell(t["sym"], t["qty"] or 1, price)
            if broker.live and fill.get("status") != "LIVE_PLACED":
                log.error(f"  LIVE SELL {t['sym']} failed — position still open. Fill: {fill}")
                continue
            con.execute(
                "UPDATE trades SET status=?,pnl=?,closed_at=?,exit_reason=? WHERE id=?",
                (status, pnl, datetime.now().isoformat(), reason, t["id"])
            )
            con.commit()
            ws = (date.today() - timedelta(days=date.today().weekday())).isoformat()
            con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
            con.execute(
                "UPDATE weekly_stats SET pnl=pnl+?,risk_used=risk_used+?,"
                "wins=wins+?,losses=losses+? WHERE week_start=?",
                (pnl, abs(pnl) if pnl < 0 else 0,
                 1 if pnl > 0 else 0, 0 if pnl > 0 else 1, ws)
            )
            con.commit()
            log.info(f"  CLOSED {t['sym']} [{reason}] @ Rs.{price:.2f}  P&L Rs.{pnl:+.2f}")
        else:
            unreal = round((price - t["entry"]) * (t["qty"] or 1), 0)
            log.info(f"  HOLD {t['sym']} @ Rs.{price:.2f}  "
                     f"unreal:Rs.{unreal:+.0f}  sl:Rs.{t['sl']:.2f}  day:{days}")

# ── TWO-PASS SCAN ─────────────────────────────────────────────────────────────
def quick_replace(con, slots_needed):
    """
    Fast slot replacement using screener cache.
    Ranks all cached valid setups by score and places orders immediately.
    No yfinance calls — runs in milliseconds.
    Returns number of slots filled.
    """
    cache_cutoff = (datetime.now() - timedelta(hours=1)).isoformat()  # 1h: cache survives restart
    # Get best cached setups not already in portfolio
    rows = con.execute("""
        SELECT sym, score, entry, sl, target, daily_rsi, weekly_rsi
        FROM screener_cache
        WHERE entry IS NOT NULL
          AND updated_at > ?
          AND daily_rsi  BETWEEN ? AND ?
          AND weekly_rsi BETWEEN ? AND ?
          AND sym NOT IN (SELECT sym FROM trades WHERE status='open')
        ORDER BY score DESC
        LIMIT ?
    """, (cache_cutoff, DAILY_RSI_MIN, DAILY_RSI_MAX,
          WEEKLY_RSI_MIN, WEEKLY_RSI_MAX, slots_needed * 3)).fetchall()

    if not rows:
        log.info("  Quick replace: no valid cache entries — doing full scan")
        return 0

    ws        = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    stats_row = con.execute("SELECT risk_used FROM weekly_stats WHERE week_start=?", (ws,)).fetchone()
    risk_used = float(stats_row[0]) if stats_row else 0.0
    risk_left = MAX_WEEKLY_RISK - risk_used

    placed = 0
    for sym, score, entry, sl, target, drsi, wrsi in rows:
        cur_open = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        if cur_open >= MAX_OPEN or placed >= slots_needed or risk_left <= 0:
            break
        if con.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'", (sym,)).fetchone():
            continue
        # Block re-entry of anything already closed today (any exit reason)
        if con.execute(
            "SELECT 1 FROM trades WHERE sym=? AND status IN ('win','loss') AND closed_at >= ?",
            (sym, date.today().isoformat())
        ).fetchone():
            continue
        if rp <= 0:
            continue
        qty         = max(1, int(min(risk_left, RISK_PER_TRADE) / rp))
        actual_risk = round(qty * rp, 2)
        rr          = round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)
        # Route through broker — paper simulates, live places real Kite order
        fill = broker.place_buy(sym, qty, entry)
        if broker.live and fill.get("status") != "LIVE_PLACED":
            log.error(f"  LIVE BUY {sym} failed — skipping. Fill: {fill}")
            continue
        try:
            con.execute("""
                INSERT INTO trades
                (id,sym,direction,entry,sl,target,qty,risk_amt,target_gain,rr,score,
                 status,pnl,days_held,opened_at,exit_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'open',0,0,?,?)
            """, (f"{sym}_{int(time.time())}", sym, "BUY",
                  entry, sl, target, qty, actual_risk,
                  round(qty * abs(entry - target), 2),
                  rr, score, datetime.now().isoformat(), ""))
            con.commit()
        except sqlite3.IntegrityError:
            log.warning(f"  {sym}: already has an open position — skipping duplicate buy")
            continue
        risk_left -= actual_risk
        placed    += 1
        log.info(f"  INSTANT BUY {sym}  qty:{qty}  @ Rs.{entry}"
                 f"  SL:Rs.{sl}  TGT:Rs.{target}  wRSI:{wrsi}  dRSI:{drsi}  score:{score}")
    return placed


def scan_and_trade(universe, con):
    cache_cutoff  = (datetime.now() - timedelta(hours=4)).isoformat()
    all_setups    = []
    reject_counts = {}
    total         = len(universe)

    # ── PASS 1: collect all setups ────────────────────────────────────────────
    log.info(f"  Pass 1: scanning {total} stocks...")
    for i in range(0, total, BATCH_SIZE):
        for sym in universe[i:i + BATCH_SIZE]:
            if con.execute(
                "SELECT 1 FROM trades WHERE sym=? AND status='open'", (sym,)
            ).fetchone():
                continue
            setup, reason = screen(sym, cache_cutoff, con)
            if setup:
                all_setups.append(setup)
            else:
                key = (reason or "unknown").replace("cached:", "").split("(")[0]
                reject_counts[key] = reject_counts.get(key, 0) + 1
        done = min(i + BATCH_SIZE, total)
        top_r = " | ".join(
            f"{k}:{v}" for k, v in
            sorted(reject_counts.items(), key=lambda x: -x[1])[:4]
        )
        log.info(f"  Scanned {done}/{total} — {len(all_setups)} setups | {top_r}")
        if i + BATCH_SIZE < total:
            time.sleep(BATCH_PAUSE)

    log.info("  Rejection summary:")
    for k, v in sorted(reject_counts.items(), key=lambda x: -x[1]):
        log.info(f"    {k:<35} {v:>5}")

    # Filter by minimum score — don't trade weak setups
    all_setups = [s for s in all_setups if s["score"] >= MIN_SCORE]
    if not all_setups:
        log.info(f"  No setups above MIN_SCORE={MIN_SCORE} — skipping cycle")
        return 0

    # ── PASS 2: rank and trade top N ─────────────────────────────────────────
    all_setups.sort(key=lambda x: x["score"], reverse=True)
    top = all_setups[:TOP_N]

    log.info(f"\n  Pass 2: top {len(top)} of {len(all_setups)} setups:")
    log.info(f"  {'#':<3} {'SYM':<14} {'SCORE':>6} {'wRSI':>6} {'dRSI':>6} {'ENTRY':>9}")
    for i, s in enumerate(top, 1):
        log.info(f"  {i:<3} {s['sym']:<14} {s['score']:>6} "
                 f"{s['weekly_rsi']:>6} {s['daily_rsi']:>6} "
                 f"Rs.{s['entry']:>8.2f}")

    ws        = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    stats_row = con.execute(
        "SELECT risk_used FROM weekly_stats WHERE week_start=?", (ws,)
    ).fetchone()
    risk_used = float(stats_row[0]) if stats_row else 0.0
    risk_left = MAX_WEEKLY_RISK - risk_used

    placed = 0
    for s in top:
        cur_open = con.execute(
            "SELECT COUNT(*) FROM trades WHERE status='open'"
        ).fetchone()[0]
        if cur_open >= MAX_OPEN:
            log.info(f"  Portfolio full ({cur_open}/{MAX_OPEN}) — stopping")
            break
        if risk_left <= 0:
            log.info("  Weekly risk budget exhausted")
            break
        if con.execute(
            "SELECT 1 FROM trades WHERE sym=? AND status='open'", (s["sym"],)
        ).fetchone():
            continue
        # Group concentration — max 1 stock per corporate group (blocks ADANI spam)
        _pfx = ''.join(c for c in s["sym"] if c.isalpha())[:5]
        _grp = con.execute(
            "SELECT COUNT(*) FROM trades WHERE status='open' AND sym LIKE ?",
            (_pfx[:4] + '%',)
        ).fetchone()[0]
        if _grp >= 1:
            log.info(f"  SKIP {s['sym']} — group limit (1 per group, {_pfx[:4]}* already held)")
            continue
        rp  = abs(s["entry"] - s["sl"])
        if rp <= 0:
            continue
        qty = max(1, int(min(risk_left, RISK_PER_TRADE) / rp))
        actual_risk = round(qty * rp, 2)
        # Route through broker — paper simulates, live places real Kite order
        fill = broker.place_buy(s["sym"], qty, s["entry"])
        if broker.live and fill.get("status") != "LIVE_PLACED":
            log.error(f"  LIVE BUY {s['sym']} failed — skipping. Fill: {fill}")
            continue
        try:
            con.execute("""
                INSERT INTO trades
                (id,sym,direction,entry,sl,target,qty,risk_amt,target_gain,rr,score,
                 status,pnl,days_held,opened_at,exit_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'open',0,0,?,?)
            """, (
                f"{s['sym']}_{int(time.time())}",
                s["sym"], "BUY",
                s["entry"], s["sl"], s["target"],
                qty, actual_risk,
                round(qty * abs(s["entry"] - s["target"]), 2),
                s["rr"], s["score"],
                datetime.now().isoformat(), ""
            ))
            con.commit()
        except sqlite3.IntegrityError:
            log.warning(f"  {s['sym']}: already has an open position — skipping duplicate buy")
            continue
        risk_left -= actual_risk
        placed    += 1
        log.info(
            f"  BUY {s['sym']}  qty:{qty}  @ Rs.{s['entry']}"
            f"  SL:Rs.{s['sl']}  TGT:Rs.{s['target']}"
            f"  R:R:{s['rr']}x  risk:Rs.{actual_risk}  score:{s['score']}"
        )

    log.info(f"  {placed} orders placed")
    return placed

# ── WEEKLY STATS ──────────────────────────────────────────────────────────────
def get_stats(con):
    ws = (date.today() - timedelta(days=date.today().weekday())).isoformat()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    con.commit()
    # risk_used is the only genuinely weekly metric (resets for the Rs.3000 cap)
    rw = con.execute(
        "SELECT risk_used FROM weekly_stats WHERE week_start=?", (ws,)
    ).fetchone()
    risk_used = float(rw[0]) if rw and rw[0] else 0.0

    # LIFETIME realised P&L — every genuine exit, including manual closes.
    # Excludes only the Day-1 cancelled/excess bug trades.
    REAL = (
        "status IN ('win','loss') "
        "AND exit_reason NOT LIKE '%EXCESS%' "
        "AND exit_reason NOT LIKE '%CANCEL%' "
        "AND exit_reason NOT LIKE '%BOOT%'"
    )
    pnl_row = con.execute(
        f"SELECT COALESCE(SUM(pnl),0) FROM trades WHERE {REAL}"
    ).fetchone()
    real_pnl = float(pnl_row[0]) if pnl_row and pnl_row[0] is not None else 0.0

    # LIFETIME wins/losses (not weekly)
    wl = con.execute(
        f"SELECT "
        f"SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), "
        f"SUM(CASE WHEN pnl<=0 THEN 1 ELSE 0 END) "
        f"FROM trades WHERE {REAL}"
    ).fetchone()
    wins   = int(wl[0] or 0)
    losses = int(wl[1] or 0)

    # This-week realised P&L (for a separate weekly view if wanted)
    week_pnl_row = con.execute(
        f"SELECT COALESCE(SUM(pnl),0) FROM trades WHERE {REAL} AND closed_at >= ?",
        (ws + "T00:00:00",)
    ).fetchone()
    week_pnl = float(week_pnl_row[0]) if week_pnl_row and week_pnl_row[0] is not None else 0.0

    cancelled_row = con.execute(
        "SELECT COALESCE(SUM(pnl),0) FROM trades "
        "WHERE exit_reason LIKE '%EXCESS%' OR exit_reason LIKE '%CANCEL%' OR exit_reason LIKE '%BOOT%'"
    ).fetchone()
    cancelled_pnl = float(cancelled_row[0]) if cancelled_row and cancelled_row[0] else 0.0

    return {
        "pnl":           real_pnl,    # lifetime realised
        "week_pnl":      week_pnl,    # this week realised
        "risk_used":     risk_used,
        "wins":          wins,        # lifetime
        "losses":        losses,      # lifetime
        "cancelled_pnl": cancelled_pnl,
    }

# ── DASHBOARD ─────────────────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>First-Orbit Trader PRO</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:      #080c12;
  --s1:      #0f1420;
  --s2:      #141926;
  --b1:      #1c2536;
  --b2:      #222f44;
  --t4:      #2a3a54;
  --t3:      #4a5a7a;
  --t2:      #7a8faa;
  --t1:      #b0c0d8;
  --white:   #e8f0fa;
  --green:   #10b981;
  --gbg:     rgba(16,185,129,.1);
  --gbr:     rgba(16,185,129,.25);
  --red:     #ef4444;
  --rbg:     rgba(239,68,68,.1);
  --rbr:     rgba(239,68,68,.25);
  --amber:   #f59e0b;
  --abg:     rgba(245,158,11,.1);
  --blue:    #60a5fa;
  --mono: 'JetBrains Mono', monospace;
  --sans: 'Inter', system-ui, sans-serif;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{font-size:14px;-webkit-text-size-adjust:100%}
body{background:var(--bg);color:var(--t1);font-family:var(--sans);min-height:100vh;overflow-x:hidden}

/* ── TOP BAR ─────────────────────────────────────────────── */
.nav{
  position:sticky;top:0;z-index:200;
  height:48px;padding:0 20px;
  background:var(--s1);border-bottom:1px solid var(--b1);
  display:flex;align-items:center;justify-content:space-between;gap:16px;
}
.nav-logo{font-family:var(--mono);font-size:12px;font-weight:600;color:var(--green);letter-spacing:2px;white-space:nowrap}
.nav-right{display:flex;align-items:center;gap:10px;flex-shrink:0}
.nav-clock{font-family:var(--mono);font-size:12px;color:var(--t3)}
.nav-dot{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 5px var(--green)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
.nav-dot.pulse{animation:blink 2s infinite}
.chip{font-size:10px;font-weight:500;padding:2px 8px;border-radius:4px;border:1px solid var(--b2);color:var(--t3);background:var(--s2);white-space:nowrap}
.chip.paper{color:var(--amber);border-color:rgba(245,158,11,.3);background:var(--abg)}

/* ── MARKET BANNER ───────────────────────────────────────── */
.mkt{
  padding:14px 20px;
  display:flex;align-items:center;justify-content:space-between;gap:16px;
  border-bottom:1px solid var(--b1);
  transition:background .4s;
}
.mkt.open  {background:rgba(16,185,129,.05)}
.mkt.closed{background:rgba(239,68,68,.03)}
.mkt.pre   {background:rgba(245,158,11,.05)}
.mkt-l{display:flex;align-items:center;gap:10px;flex:1}
.mkt-dot{width:9px;height:9px;border-radius:50%;flex-shrink:0}
.mkt.open   .mkt-dot{background:var(--green);box-shadow:0 0 7px var(--green);animation:blink 1.5s infinite}
.mkt.closed .mkt-dot{background:var(--red)}
.mkt.pre    .mkt-dot{background:var(--amber);animation:blink 2s infinite}
.mkt-name{font-size:14px;font-weight:600}
.mkt.open   .mkt-name{color:var(--green)}
.mkt.closed .mkt-name{color:var(--red)}
.mkt.pre    .mkt-name{color:var(--amber)}
.mkt-sub{font-size:11px;color:var(--t3);margin-top:1px}
.mkt-prog-wrap{height:2px;background:var(--b1);border-radius:1px;overflow:hidden;margin-top:7px}
.mkt-prog-fill{height:100%;border-radius:1px;transition:width 1s linear}
.mkt.open  .mkt-prog-fill{background:var(--green)}
.mkt.pre   .mkt-prog-fill{background:var(--amber)}
.mkt.closed .mkt-prog-fill{background:transparent}
.mkt-r{text-align:right;flex-shrink:0}
.mkt-cd{font-family:var(--mono);font-size:24px;font-weight:600;color:var(--white);letter-spacing:2px;line-height:1}
.mkt-cd-lbl{font-size:10px;color:var(--t4);letter-spacing:1px;text-transform:uppercase;margin-top:3px}

/* ── STRAT STRIP ─────────────────────────────────────────── */
.strat{background:var(--s2);border-bottom:1px solid var(--b1);padding:7px 20px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.strat-lbl{font-size:10px;color:var(--t4);letter-spacing:1.5px;text-transform:uppercase;margin-right:4px}
.sc{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:500;border:1px solid var(--b2);color:var(--t2);background:var(--s1)}
.sc.hi{color:var(--green);background:var(--gbg);border-color:var(--gbr)}

/* ── METRICS ─────────────────────────────────────────────── */
.metrics{display:grid;grid-template-columns:repeat(8,1fr);gap:1px;background:var(--b1);border-bottom:1px solid var(--b1)}
@media(max-width:1100px){.metrics{grid-template-columns:repeat(4,1fr)}}
@media(max-width:700px){.metrics{grid-template-columns:1fr 1fr 1fr}}
@media(max-width:420px){.metrics{grid-template-columns:1fr 1fr}}
.met{background:var(--s1);padding:14px 18px}
.met-lbl{font-size:10px;color:var(--t4);letter-spacing:1px;text-transform:uppercase;margin-bottom:6px}
.met-val{font-family:var(--mono);font-size:22px;font-weight:600;color:var(--white);line-height:1}
.met-val.g{color:var(--green)}.met-val.r{color:var(--red)}.met-val.a{color:var(--amber)}
.met-sub{font-size:10px;color:var(--t4);margin-top:4px;line-height:1.5}

/* ── BODY GRID ───────────────────────────────────────────── */
.body{display:grid;grid-template-columns:1fr 320px 240px;gap:1px;background:var(--b1)}
@media(max-width:1100px){.body{grid-template-columns:1fr 280px}}
@media(max-width:700px){.body{grid-template-columns:1fr}}
.col{background:var(--bg)}
.col-head{
  position:sticky;top:48px;z-index:50;
  padding:10px 16px;background:var(--s1);border-bottom:1px solid var(--b1);
  display:flex;align-items:center;justify-content:space-between;
}
.col-title{font-size:10px;font-weight:600;color:var(--t3);letter-spacing:1.5px;text-transform:uppercase}
.col-badge{font-family:var(--mono);font-size:10px;font-weight:600;padding:2px 8px;border-radius:4px}
.cb-full  {background:var(--rbg);color:var(--red);  border:1px solid var(--rbr)}
.cb-open  {background:var(--abg);color:var(--amber);border:1px solid rgba(245,158,11,.25)}
.cb-closed{background:var(--gbg);color:var(--green);border:1px solid var(--gbr)}

/* ── POSITION CARDS ──────────────────────────────────────── */
.pos-wrap{padding:12px;display:flex;flex-direction:column;gap:8px}
.pc{
  background:var(--s1);border:1px solid var(--b1);border-radius:8px;
  overflow:hidden;transition:border-color .15s;
}
.pc:hover{border-color:var(--b2)}
.pc-head{padding:12px 14px 0;display:flex;justify-content:space-between;align-items:flex-start}
.pc-sym{font-family:var(--mono);font-size:14px;font-weight:600;color:var(--white)}
.pc-tag{font-size:10px;color:var(--t3);margin-top:2px}
.pc-pnl{text-align:right}
.pc-pnl-v{font-family:var(--mono);font-size:15px;font-weight:600}
.pc-pnl-v.g{color:var(--green)}.pc-pnl-v.r{color:var(--red)}.pc-pnl-v.z{color:var(--t3)}
.pc-pnl-p{font-size:10px;color:var(--t3);margin-top:2px}
.pc-row{padding:8px 14px;display:grid;grid-template-columns:1fr 1fr;gap:8px}
.pc-item-lbl{font-size:10px;color:var(--t4);text-transform:uppercase;letter-spacing:.5px;margin-bottom:2px}
.pc-item-val{font-family:var(--mono);font-size:11px;color:var(--t1)}
.pc-item-val .v-white{color:var(--white)}.pc-item-val .v-red{color:var(--red)}.pc-item-val .v-green{color:var(--green)}.pc-item-val .v-amber{color:var(--amber)}
/* progress */
.pc-prog-section{padding:4px 14px 14px}
.pc-prog-lbls{display:flex;justify-content:space-between;font-family:var(--mono);font-size:9px;color:var(--t4);margin-bottom:4px}
.pc-prog-track{height:5px;background:var(--s2);border-radius:3px;position:relative;overflow:hidden}
.pc-prog-fill{height:100%;border-radius:3px;transition:width .5s}
.pc-prog-entry{position:absolute;top:0;width:2px;height:100%;background:var(--amber);opacity:.9}
/* close button */
.close-btn{
  display:block;width:100%;padding:8px;
  background:none;border:none;border-top:1px solid var(--b1);
  color:var(--t4);font-family:var(--mono);font-size:10px;
  cursor:pointer;transition:all .15s;letter-spacing:1px;
  text-transform:uppercase;
}
.close-btn:hover{background:var(--rbg);color:var(--red);border-top-color:var(--rbr)}
.close-btn:active{background:var(--red);color:#fff}
.closing{opacity:.5;pointer-events:none}


/* ── MARKET PANEL ────────────────────────────────────────────── */
.mkt-panel{background:var(--bg)}
.idx-wrap{padding:6px 8px;display:flex;flex-direction:column;gap:2px;overflow-y:auto}
.idx-card{
  background:var(--s1);border:1px solid var(--b1);border-radius:6px;
  padding:9px 12px;display:flex;justify-content:space-between;align-items:center;
  transition:border-color .15s;
}
.idx-card:hover{border-color:var(--b2)}
.idx-card.up  {border-left:3px solid var(--green)}
.idx-card.down{border-left:3px solid var(--red)}
.idx-card.flat{border-left:3px solid var(--t4)}
.idx-name{font-size:11px;font-weight:600;color:var(--t1)}
.idx-sub {font-size:10px;color:var(--t4);margin-top:1px}
.idx-r   {text-align:right}
.idx-val {font-family:var(--mono);font-size:12px;font-weight:600;color:var(--white)}
.idx-chg {font-family:var(--mono);font-size:10px;margin-top:1px}
.idx-chg.up  {color:var(--green)}
.idx-chg.down{color:var(--red)}
.idx-chg.flat{color:var(--t4)}
.idx-loading{text-align:center;padding:20px;font-size:11px;color:var(--t4);font-family:var(--mono)}
.idx-divider{height:1px;background:var(--b1);margin:4px 8px}
.idx-section-lbl{
  font-size:9px;letter-spacing:1.5px;text-transform:uppercase;
  color:var(--t4);padding:6px 12px 2px;font-family:var(--mono);
}
/* ── RISK BAR ────────────────────────────────────────────── */
.risk-section{padding:10px 16px 12px;border-top:1px solid var(--b1)}
.risk-lbl{display:flex;justify-content:space-between;font-size:11px;color:var(--t3);margin-bottom:5px}
.risk-track{height:4px;background:var(--s2);border-radius:2px;overflow:hidden}
.risk-fill{height:100%;border-radius:2px;transition:width .5s}

/* ── CLOSED LIST ─────────────────────────────────────────── */
.cl-wrap{padding:8px;display:flex;flex-direction:column;gap:4px;max-height:calc(100vh - 240px);overflow-y:auto}
/* collapsible closed trades */
.cl-col{background:var(--bg);transition:all .2s}
.cl-col.collapsed .cl-wrap{display:none}
.cl-col.collapsed{min-width:0}
.cl-col.collapsed .col-head{border-bottom:none}
.cl-toggle{
  background:none;border:none;cursor:pointer;
  color:var(--t3);font-size:13px;padding:2px 4px;
  border-radius:4px;transition:all .15s;line-height:1;
}
.cl-toggle:hover{background:var(--b1);color:var(--t1)}
.cl-col.collapsed .cl-toggle{transform:rotate(180deg)}
.cl-wrap::-webkit-scrollbar{width:3px}.cl-wrap::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}
.cl-card{
  background:var(--s1);border:1px solid var(--b1);border-radius:6px;
  padding:9px 12px;display:flex;justify-content:space-between;align-items:center;gap:8px;
}
.cl-card.win {border-left:3px solid var(--green)}
.cl-card.loss{border-left:3px solid var(--red)}
.cl-card.cancelled{border-left:3px solid var(--t4)}
.cl-sym{font-family:var(--mono);font-size:12px;font-weight:600;color:var(--white)}
.cl-why{font-size:10px;color:var(--t4);margin-top:2px}
.cl-pnl{font-family:var(--mono);font-size:12px;font-weight:600;white-space:nowrap}
.cl-pnl.g{color:var(--green)}.cl-pnl.r{color:var(--red)}.cl-pnl.z{color:var(--t4)}

/* ── LOG ─────────────────────────────────────────────────── */
.log-col{background:var(--bg);border-top:1px solid var(--b1);grid-column:1/-1}
.log-body{font-family:var(--mono);font-size:11px;line-height:1.9;padding:10px 20px;max-height:200px;overflow-y:auto}
.log-body::-webkit-scrollbar{width:3px}.log-body::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}
.lg{color:var(--green)}.lr{color:var(--red)}.la{color:var(--amber)}.lb{color:var(--blue)}.ld{color:var(--t4)}

/* ── EMPTY ───────────────────────────────────────────────── */
.empty{padding:40px 20px;text-align:center;color:var(--t4);font-size:13px;line-height:1.9}

/* ── MOBILE ──────────────────────────────────────────────── */
@media(max-width:700px){
  .nav{padding:0 12px}
  .mkt{padding:10px 12px}
  .mkt-cd{font-size:20px}
  .strat{display:none}
  .met-val{font-size:18px}
  .pos-wrap{padding:8px}
  .log-body{font-size:10px}
  .cl-wrap{max-height:none}
}
</style>
</head>
<body>

<!-- NAV -->
<nav class="nav">
  <div class="nav-logo">⬡ FIRST-ORBIT PRO</div>
  <div class="nav-right">
    <a href="/kite/login" target="_blank" id="kite-btn" style="font-size:10px;color:var(--amber);text-decoration:none;padding:3px 8px;border:1px solid rgba(245,158,11,.3);border-radius:4px;font-family:var(--mono);background:var(--abg)">⚿ AUTHENTICATE</a>
    <a href="/ledger" style="font-size:10px;color:var(--t3);text-decoration:none;padding:3px 8px;border:1px solid var(--b2);border-radius:4px;font-family:var(--mono)">LEDGER</a>
    <a href="/analytics" style="font-size:10px;color:var(--t3);text-decoration:none;padding:3px 8px;border:1px solid var(--b2);border-radius:4px;font-family:var(--mono)">ANALYTICS</a>
    <span class="chip paper" id="mode-chip">PAPER MODE</span>
    <span class="nav-clock" id="clk">--:--:-- IST</span>
    <span class="nav-dot pulse" id="dot"></span>
  </div>
</nav>

<!-- MARKET BANNER -->
<div class="mkt closed" id="mkt">
  <div class="mkt-l">
    <div class="mkt-dot"></div>
    <div>
      <div class="mkt-name" id="mkt-name">MARKET CLOSED</div>
      <div class="mkt-sub"  id="mkt-sub">NSE Mon–Fri 09:15–15:30 IST</div>
    </div>
    <div style="flex:1;margin-left:16px">
      <div class="mkt-prog-wrap"><div class="mkt-prog-fill" id="mkt-prog" style="width:0%"></div></div>
    </div>
  </div>
  <div class="mkt-r">
    <div class="mkt-cd"     id="mkt-cd">--:--:--</div>
    <div class="mkt-cd-lbl" id="mkt-cd-lbl">until open</div>
  </div>
</div>

<!-- STRATEGY STRIP -->
<div class="strat">
  <span class="strat-lbl">Strategy</span>
  <span class="sc hi">DUAL RSI MOMENTUM</span>
  <span class="sc">Weekly RSI &gt; 60</span>
  <span class="sc">Daily RSI 57–67</span>
  <span class="sc">MCap &gt; Rs.20,000 Cr</span>
  <span class="sc">TGT 3×ATR · Stop 2×ATR</span>
  <span class="sc">Exit: RSI fade · Divergence</span>
  <span class="sc">Top 5 · Nifty 500</span>
</div>

<!-- METRICS -->
<div class="metrics">
  <div class="met">
    <div class="met-lbl">Total Equity</div>
    <div class="met-val" id="m-equity">—</div>
    <div class="met-sub" id="m-equity-s">cash + holdings</div>
  </div>
  <div class="met">
    <div class="met-lbl">Free Cash</div>
    <div class="met-val" id="m-cash">—</div>
    <div class="met-sub" id="m-cash-s">available to trade</div>
  </div>
  <div class="met">
    <div class="met-lbl">Deployed</div>
    <div class="met-val" id="m-deployed">—</div>
    <div class="met-sub" id="m-deployed-s">in open positions</div>
  </div>
  <div class="met" style="border-left:2px solid var(--green);padding-left:16px">
    <div class="met-lbl">Lifetime P&amp;L</div>
    <div class="met-val g" id="m-pnl">—</div>
    <div class="met-sub" id="m-pnl-s">realised + unrealised</div>
  </div>
  <div class="met">
    <div class="met-lbl">Realised P&amp;L</div>
    <div class="met-val" id="m-real">Rs.0</div>
    <div class="met-sub" id="m-real-s">lifetime, all closed</div>
  </div>
  <div class="met">
    <div class="met-lbl">Unrealised P&amp;L</div>
    <div class="met-val" id="m-unreal">Rs.0</div>
    <div class="met-sub" id="m-unreal-s">open positions</div>
  </div>
  <div class="met">
    <div class="met-lbl">Win Rate (lifetime)</div>
    <div class="met-val"   id="m-wr">—</div>
    <div class="met-sub"   id="m-wr-s">0W / 0L</div>
  </div>
  <div class="met">
    <div class="met-lbl">Open / Risk</div>
    <div class="met-val a" id="m-open">—</div>
    <div class="met-sub"   id="m-risk-s">Rs.0 / Rs.3,000</div>
  </div>
</div>

<!-- BODY -->
<div class="body">

  <!-- OPEN POSITIONS -->
  <div class="col">
    <div class="col-head">
      <span class="col-title">Open Positions</span>
      <span class="col-badge cb-open" id="pos-badge">0 / 5</span>
    </div>
    <div class="pos-wrap" id="pos-list">
      <div class="empty">◎<br>No open positions<br><span style="font-size:11px">Scanning Nifty 500 for Dual RSI setups</span></div>
    </div>
    <div class="risk-section">
      <div class="risk-lbl">
        <span id="risk-lbl-txt">Weekly risk: Rs.0 of Rs.3,000</span>
        <span id="risk-lbl-pct" style="color:var(--t4)">0%</span>
      </div>
      <div class="risk-track"><div class="risk-fill" id="risk-fill" style="width:0%;background:var(--green)"></div></div>
    </div>
  </div>

  <!-- MARKET PANEL -->
  <div class="col mkt-panel">
    <div class="col-head">
      <span class="col-title">Market</span>
      <span style="font-size:10px;color:var(--t4)">60s · <span id="mkt-updated">—</span></span>
    </div>
    <div id="mkt-indices">
      <div class="idx-loading">Loading market data...</div>
    </div>
  </div>

  <!-- CLOSED TRADES -->
  <div class="col cl-col collapsed" id="cl-col">
    <div class="col-head" onclick="toggleClosed()" style="cursor:pointer;user-select:none">
      <div style="display:flex;align-items:center;gap:8px">
        <button class="cl-toggle" id="cl-toggle" title="Expand/collapse">⌃</button>
        <span class="col-title">Closed Trades</span>
      </div>
      <span class="col-badge cb-closed" id="cl-badge">0</span>
    </div>
    <div class="cl-wrap" id="cl-list">
      <div class="empty">○<br>No closed trades yet</div>
    </div>
  </div>

  <!-- LOG -->
  <div class="log-col">
    <div class="col-head" style="position:static">
      <span class="col-title">Bot Log</span>
      <span style="font-size:10px;color:var(--t4)">live · 5s refresh</span>
    </div>
    <div class="log-body" id="log-body"><span class="ld">Connecting...</span></div>
  </div>

</div>

<script>
// ── IST clock ────────────────────────────────────────────────────────────────
const IST_OFFSET = (5*60+30)*60000;
function nowIST(){ return new Date(Date.now()+IST_OFFSET); }
function pad(n){ return String(n).padStart(2,'0'); }
function fmtSecs(s){
  s=Math.max(0,Math.floor(s));
  const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;
  return h>0?`${pad(h)}:${pad(m)}:${pad(sc)}`:`${pad(m)}:${pad(sc)}`;
}

// ── Market state ─────────────────────────────────────────────────────────────
function mktState(){
  const t=nowIST(),day=t.getUTCDay(),h=t.getUTCHours(),m=t.getUTCMinutes();
  const mins=h*60+m,OPEN=9*60+15,CLOSE=15*60+30,PRE=OPEN-30;
  if(day===0||day===6)  return{s:'closed',n:'MARKET CLOSED',sub:'Reopens Monday 09:15 IST'};
  if(mins<PRE)          return{s:'closed',n:'MARKET CLOSED',sub:'NSE opens 09:15 IST'};
  if(mins<OPEN)         return{s:'pre',   n:'PRE-OPEN',     sub:'Call auction 09:00–09:15 IST'};
  if(mins<=CLOSE)       return{s:'open',  n:'MARKET OPEN',  sub:'NSE live · 09:15–15:30 IST'};
  return                      {s:'closed',n:'MARKET CLOSED',sub:'Reopens tomorrow 09:15 IST'};
}
function secsUntil(th,tm){
  const t=nowIST();
  let s=(th-t.getUTCHours())*3600+(tm-t.getUTCMinutes())*60-t.getUTCSeconds();
  if(s<0)s+=86400; return s;
}
function secsUntilOpen(){
  const t=nowIST();
  let d=new Date(Date.UTC(t.getUTCFullYear(),t.getUTCMonth(),t.getUTCDate()));
  for(let i=0;i<8;i++){
    if(d.getUTCDay()>=1&&d.getUTCDay()<=5){
      const tgt=new Date(d.getTime()+(9*60+15)*60000);
      const diff=(tgt-nowIST())/1000;
      if(diff>0)return diff;
    }
    d=new Date(d.getTime()+86400000);
  }
  return 86400;
}

function tickClock(){
  const t=nowIST();
  document.getElementById('clk').textContent=`${pad(t.getUTCHours())}:${pad(t.getUTCMinutes())}:${pad(t.getUTCSeconds())} IST`;
  const ms=mktState();
  const mkt=document.getElementById('mkt');
  mkt.className='mkt '+ms.s;
  document.getElementById('mkt-name').textContent=ms.n;
  document.getElementById('mkt-sub').textContent=ms.sub;
  const t2=nowIST(),mins=t2.getUTCHours()*60+t2.getUTCMinutes();
  let cd=0,lbl='until open',prog=0;
  if(ms.s==='open'){cd=secsUntil(15,30);lbl='until close';prog=Math.min(100,(mins-(9*60+15))/(6*60+15)*100);}
  else if(ms.s==='pre'){cd=secsUntil(9,15);lbl='until open';prog=Math.min(100,(1-cd/1800)*100);}
  else{cd=secsUntilOpen();lbl='until open';prog=0;}
  document.getElementById('mkt-cd').textContent=fmtSecs(cd);
  document.getElementById('mkt-cd-lbl').textContent=lbl;
  document.getElementById('mkt-prog').style.width=prog.toFixed(1)+'%';
}
setInterval(tickClock,1000);tickClock();

// ── Log colouring ────────────────────────────────────────────────────────────
function lc(l){
  if(/BUY|SETUP|TARGET|[+]Rs/.test(l))         return 'lg';
  if(/ERROR|STOP|[-]Rs|CLOSED.*loss/.test(l))   return 'lr';
  if(/HOLD|WARN|DIVERGE|CANCEL/.test(l))        return 'la';
  if(/Cycle|Scan|Pass|Scanned/.test(l))         return 'lb';
  return 'ld';
}

// ── Data refresh ─────────────────────────────────────────────────────────────
function toggleClosed() {
  const col = document.getElementById('cl-col');
  col.classList.toggle('collapsed');
  // Persist preference
  localStorage.setItem('cl_collapsed', col.classList.contains('collapsed') ? '1' : '0');
}

// Restore preference on load
(function(){
  const pref = localStorage.getItem('cl_collapsed');
  const col  = document.getElementById('cl-col');
  if (pref === '0') col.classList.remove('collapsed');
  // If there are closed trades, auto-expand
})();

async function manualClose(sym, btn) {
  if (!confirm(`Close ${sym} at current market price?\n\nThis cannot be undone.`)) return;
  btn.textContent = '⟳  Closing...';
  btn.classList.add('closing');
  try {
    const r = await fetch(`/api/close/${sym}`, {method:'POST'});
    const d = await r.json();
    if (d.error) {
      alert('Error: ' + d.error);
      btn.textContent = `✕  Close ${sym} manually`;
      btn.classList.remove('closing');
    } else {
      const sign = d.pnl >= 0 ? '+' : '';
      btn.textContent = `✓  Closed at Rs.${d.price}  P&L ${sign}Rs.${d.pnl}`;
      setTimeout(refresh, 800);
    }
  } catch(e) {
    alert('Network error — try again');
    btn.textContent = `✕  Close ${sym} manually`;
    btn.classList.remove('closing');
  }
}

async function refresh(){
  let d;
  try{
    d=await(await fetch('/api/status')).json();
    const dot=document.getElementById('dot');
    dot.style.background='var(--green)';dot.style.boxShadow='0 0 5px var(--green)';
    if(d.mode){const mc=document.getElementById('mode-chip');if(mc){mc.textContent=d.mode+' MODE';if(d.mode==='LIVE'){mc.style.color='var(--red)';mc.style.borderColor='var(--rbr)';mc.style.background='var(--rbg)';}}}
    const kb=document.getElementById('kite-btn');if(kb){if(d.kite_authed){kb.textContent='✓ KITE AUTHED';kb.style.color='var(--green)';kb.style.borderColor='var(--gbr)';kb.style.background='var(--gbg)';}else{kb.textContent='⚿ AUTHENTICATE';kb.style.color='var(--amber)';kb.style.borderColor='rgba(245,158,11,.3)';kb.style.background='var(--abg)';}}
  }catch(e){
    const dot=document.getElementById('dot');
    dot.style.background='var(--red)';dot.style.boxShadow='none';
    return;
  }

  const s=d.stats||{},open=d.open||[],closed=d.closed||[],logs=d.logs||[];
  const cap=d.capital||{};
  const unreal=cap.unreal_pnl!=null?cap.unreal_pnl:open.reduce((a,t)=>a+(t.unrealised||0),0);
  const realised=cap.realised!=null?cap.realised:(s.pnl||0);

  // Capital accounting — clear separation of cash vs deployed vs equity
  const equityEl=document.getElementById('m-equity');
  if(equityEl){
    equityEl.textContent='Rs.'+Math.round(cap.equity||0).toLocaleString('en-IN');
    document.getElementById('m-equity-s').textContent=
      `start Rs.${Math.round(cap.starting||100000).toLocaleString('en-IN')} · P&L ${(realised+unreal)>=0?'+':''}Rs.${Math.round(realised+unreal).toLocaleString('en-IN')}`;
  }
  const cashEl=document.getElementById('m-cash');
  if(cashEl){
    cashEl.textContent='Rs.'+Math.round(cap.free_cash||0).toLocaleString('en-IN');
    document.getElementById('m-cash-s').textContent=`available to trade`;
  }
  const depEl=document.getElementById('m-deployed');
  if(depEl){
    depEl.textContent='Rs.'+Math.round(cap.deployed||0).toLocaleString('en-IN');
    document.getElementById('m-deployed-s').textContent=
      `${cap.open_count||0} position${(cap.open_count===1)?'':'s'}`;
  }

  const tp=realised+unreal;

  // Total P&L
  const pnlEl=document.getElementById('m-pnl');
  pnlEl.textContent=(tp>=0?'+Rs.':'-Rs.')+Math.abs(Math.round(tp)).toLocaleString('en-IN');
  pnlEl.className='met-val '+(tp>=0?'g':'r');

  // Lifetime P&L subtitle: show this week's contribution
  const wpnl = s.week_pnl || 0;
  const pnlSub = document.getElementById('m-pnl-s');
  if (pnlSub) pnlSub.textContent = `lifetime · this week ${wpnl>=0?'+':''}Rs.${Math.round(wpnl).toLocaleString('en-IN')}`;

  // Realised P&L (lifetime, all genuine closes incl manual)
  const realEl=document.getElementById('m-real');
  realEl.textContent=(realised>=0?'Rs.':'-Rs.')+Math.abs(Math.round(realised)).toLocaleString('en-IN');
  realEl.className='met-val '+(realised>0?'g':realised<0?'r':'');
  document.getElementById('m-real-s').textContent=
    realised===0?'no exits yet':`${s.wins||0}W / ${s.losses||0}L`;

  // Unrealised P&L (open positions mark-to-market)
  const unrEl=document.getElementById('m-unreal');
  unrEl.textContent=(unreal>=0?'+Rs.':'-Rs.')+Math.abs(Math.round(unreal)).toLocaleString('en-IN');
  unrEl.className='met-val '+(unreal>0?'g':unreal<0?'r':'');
  document.getElementById('m-unreal-s').textContent=
    open.length?`across ${open.length} position${open.length>1?'s':''}`:'no open positions';

  const tot=(s.wins||0)+(s.losses||0);
  document.getElementById('m-wr').textContent=tot?Math.round(s.wins/tot*100)+'%':'—';
  document.getElementById('m-wr-s').textContent=`${s.wins||0}W / ${s.losses||0}L`;

  const rp=Math.round((s.risk_used||0)/3000*100);
  document.getElementById('m-open').textContent=`${open.length}/5`;
  document.getElementById('m-risk-s').textContent=`Rs.${s.risk_used||0} / Rs.3,000`;

  // Risk bar
  const rf=document.getElementById('risk-fill');
  rf.style.width=Math.min(100,rp)+'%';
  rf.style.background=rp>80?'var(--red)':rp>50?'var(--amber)':'var(--green)';
  document.getElementById('risk-lbl-txt').textContent=`Weekly risk: Rs.${s.risk_used||0} of Rs.3,000`;
  document.getElementById('risk-lbl-pct').textContent=rp+'%';

  // Position badge
  const pb=document.getElementById('pos-badge');
  pb.textContent=open.length>=5?`FULL ${open.length}/5`:`${open.length} / 5`;
  pb.className='col-badge '+(open.length>=5?'cb-full':'cb-open');

  // Open positions
  const pl=document.getElementById('pos-list');
  if(!open.length){
    pl.innerHTML='<div class="empty">◎<br>No open positions<br><span style="font-size:11px">Scanning Nifty 500 for Dual RSI setups</span></div>';
  }else{
    pl.innerHTML=open.map(t=>{
      const u=t.unrealised||0,lp=t.last_price||t.entry;
      const uc=u>0?'g':u<0?'r':'z',us=u>=0?'+':'';
      const bc=u>0?'var(--green)':u<0?'var(--red)':'var(--b2)';
      const rng=Math.abs(t.target-t.sl);
      const prog=rng>0?Math.min(100,Math.max(0,(lp-t.sl)/rng*100)):50;
      const ePct=rng>0?Math.min(100,Math.max(0,(t.entry-t.sl)/rng*100)):50;
      const fillC=u>0?'var(--green)':u<0?'var(--red)':'var(--amber)';
      return `<div class="pc" style="border-left:3px solid ${bc}">
        <div class="pc-head">
          <div>
            <div class="pc-sym">${t.sym}</div>
            <div class="pc-tag">BUY · Day ${t.days_held||0}</div>
          </div>
          <div class="pc-pnl">
            <div class="pc-pnl-v ${uc}">${us}Rs.${Math.abs(u).toLocaleString('en-IN')}</div>
            <div class="pc-pnl-p">${us}${t.unreal_pct||0}% · ${t.qty||1} qty</div>
          </div>
        </div>
        <div class="pc-row">
          <div>
            <div class="pc-item-lbl">Last · Qty</div>
            <div class="pc-item-val"><span class="v-white">Rs.${lp.toLocaleString('en-IN')}</span> <span style="color:var(--t4)">× ${t.qty||1}</span></div>
          </div>
          <div>
            <div class="pc-item-lbl">Entry · Value</div>
            <div class="pc-item-val"><span class="v-amber">Rs.${t.entry}</span> <span style="color:var(--t4)">(Rs.${Math.round(t.entry*(t.qty||1)).toLocaleString('en-IN')})</span></div>
          </div>
          <div>
            <div class="pc-item-lbl">Stop loss</div>
            <div class="pc-item-val"><span class="v-red">Rs.${t.sl}</span></div>
          </div>
          <div>
            <div class="pc-item-lbl">Target · R:R</div>
            <div class="pc-item-val"><span class="v-green">Rs.${t.target}</span> <span style="color:var(--t4)">${t.rr}×</span></div>
          </div>
        </div>
        <div class="pc-prog-section">
          <div class="pc-prog-lbls">
            <span style="color:var(--red)">SL ${t.sl}</span>
            <span style="color:var(--amber)">▲ Entry</span>
            <span style="color:var(--green)">TGT ${t.target}</span>
          </div>
          <div class="pc-prog-track">
            <div class="pc-prog-fill" style="width:${prog.toFixed(1)}%;background:${fillC}"></div>
            <div class="pc-prog-entry" style="left:${ePct.toFixed(1)}%"></div>
          </div>
        </div>
        <button class="close-btn" onclick="manualClose('${t.sym}', this)">
          ✕ &nbsp;Close ${t.sym} manually
        </button>
      </div>`;
    }).join('');
  }

  // Closed trades
  const cl=document.getElementById('cl-list');
  document.getElementById('cl-badge').textContent=closed.length;
  // Auto-expand if there are real strategy exits (not just CANCELLED)
  const realExits = closed.filter(t => t.exit_reason && !t.exit_reason.includes('EXCESS') && !t.exit_reason.includes('CANCEL'));
  if (realExits.length > 0) {
    document.getElementById('cl-col').classList.remove('collapsed');
  }
  if(!closed.length){
    cl.innerHTML='<div class="empty">○<br>No closed trades yet</div>';
  }else{
    cl.innerHTML=closed.map(t=>{
      const pnl=t.pnl||0;
      const cls=pnl>0?'g':pnl<0?'r':'z';
      const pStr=pnl>0?'+Rs.'+pnl.toLocaleString('en-IN'):pnl<0?'-Rs.'+Math.abs(pnl).toLocaleString('en-IN'):'Rs.0';
      const card=pnl>0?'win':pnl<0?'loss':'cancelled';
      const why=(t.exit_reason||'').replace('EXCESS_ON_BOOT','CANCELLED').replace('EXCESS_CANCELLED','CANCELLED');
      return `<div class="cl-card ${card}">
        <div>
          <div class="cl-sym">${t.sym}</div>
          <div class="cl-why">${why} · Rs.${t.entry}</div>
        </div>
        <div class="cl-pnl ${cls}">${pStr}</div>
      </div>`;
    }).join('');
  }

  // Market indices panel
  const idxEl  = document.getElementById('mkt-indices');
  const updEl  = document.getElementById('mkt-updated');
  const indices = d.indices || [];
  if (indices.length === 0) {
    idxEl.innerHTML = '<div class="idx-loading">Market data unavailable</div>';
  } else {
    updEl.textContent = 'as of ' + (d.time || '—');
    // Separate VIX from main indices
    const main  = indices.filter(i => i.sym !== '^INDIAVIX');
    const vix   = indices.find(i => i.sym === '^INDIAVIX');

    let html = '<div class="idx-section-lbl">Indices &amp; Sectors</div>';
    html += '<div class="idx-wrap">';
    main.forEach(idx => {
      const cls = idx.up ? 'up' : idx.down ? 'down' : 'flat';
      const sign = idx.chg >= 0 ? '+' : '';
      html += `<div class="idx-card ${cls}">
        <div>
          <div class="idx-name">${idx.name}</div>
          <div class="idx-sub">${idx.sector}</div>
        </div>
        <div class="idx-r">
          <div class="idx-val">${idx.price.toLocaleString('en-IN')}</div>
          <div class="idx-chg ${cls}">${sign}${idx.chg}%</div>
        </div>
      </div>`;
    });
    html += '</div>';

    if (vix) {
      const vixCls = vix.price > 20 ? 'down' : vix.price < 14 ? 'up' : 'flat';
      const vixLabel = vix.price > 20 ? 'High fear' : vix.price < 14 ? 'Low fear' : 'Neutral';
      html += '<div class="idx-divider"></div>';
      html += '<div class="idx-section-lbl">Volatility</div>';
      html += '<div class="idx-wrap">';
      html += `<div class="idx-card ${vixCls}">
        <div>
          <div class="idx-name">INDIA VIX</div>
          <div class="idx-sub">${vixLabel}</div>
        </div>
        <div class="idx-r">
          <div class="idx-val">${vix.price.toLocaleString('en-IN')}</div>
          <div class="idx-chg ${vixCls}">${vix.chg >= 0 ? '+' : ''}${vix.chg}%</div>
        </div>
      </div>`;
      html += '</div>';
    }
    idxEl.innerHTML = html;
  }

  // Log
  const lb=document.getElementById('log-body');
  lb.innerHTML=logs.length?logs.map(l=>`<div class="${lc(l)}">${l}</div>`).join(''):'<span class="ld">No log entries yet</span>';
}
refresh();setInterval(refresh,5000);
</script>
</body>
</html>"""


LEDGER_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>First-Orbit Lifetime Ledger</title>\n<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">\n<style>\n:root{--bg:#080c12;--s1:#0f1420;--s2:#141926;--b1:#1c2536;--b2:#222f44;\n--t4:#2a3a54;--t3:#4a5a7a;--t2:#7a8faa;--t1:#b0c0d8;--white:#e8f0fa;\n--green:#10b981;--red:#ef4444;--amber:#f59e0b;--blue:#60a5fa;\n--mono:\'JetBrains Mono\',monospace;--sans:\'Inter\',system-ui,sans-serif;}\n*{box-sizing:border-box;margin:0;padding:0}\nbody{background:var(--bg);color:var(--t1);font-family:var(--sans);font-size:14px}\n.nav{height:48px;background:var(--s1);border-bottom:1px solid var(--b1);padding:0 24px;display:flex;align-items:center;justify-content:space-between}\n.nav-logo{font-family:var(--mono);font-size:12px;font-weight:600;color:var(--green);letter-spacing:2px}\n.nav-back{font-size:12px;color:var(--t3);text-decoration:none}\n.nav-back:hover{color:var(--t1)}\n.page{max-width:1200px;margin:0 auto;padding:32px 24px}\n.page-title{font-size:22px;font-weight:600;color:var(--white);margin-bottom:4px}\n.page-sub{font-size:13px;color:var(--t3);margin-bottom:32px}\n.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px}\n@media(max-width:700px){.summary{grid-template-columns:1fr 1fr}}\n.scard{background:var(--s1);border:1px solid var(--b1);border-radius:8px;padding:16px}\n.scard-lbl{font-size:10px;color:var(--t3);letter-spacing:1px;text-transform:uppercase;margin-bottom:8px}\n.scard-val{font-family:var(--mono);font-size:24px;font-weight:600;color:var(--white)}\n.scard-val.g{color:var(--green)}.scard-val.r{color:var(--red)}.scard-val.a{color:var(--amber)}\n.scard-sub{font-size:11px;color:var(--t4);margin-top:4px}\n.tbl-wrap{background:var(--s1);border:1px solid var(--b1);border-radius:8px;overflow-x:auto}\n.tbl{width:100%;border-collapse:collapse;font-size:12.5px}\n.tbl th{font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--t3);padding:9px 14px;text-align:left;border-bottom:1px solid var(--b1);font-weight:500;white-space:nowrap}\n.tbl td{padding:9px 14px;border-bottom:1px solid rgba(28,37,54,.5);font-family:var(--mono);white-space:nowrap}\n.tbl tr:last-child td{border-bottom:none}\n.tbl tr:hover td{background:var(--s2)}\n.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--t3)}\n.tag{font-size:9px;padding:2px 6px;border-radius:3px;letter-spacing:.5px}\n.tag-open{background:rgba(96,165,250,.15);color:var(--blue)}\n.tag-real{background:rgba(16,185,129,.12);color:var(--green)}\n.tag-noise{background:rgba(74,90,122,.15);color:var(--t3)}\n.loading{text-align:center;padding:60px;color:var(--t4);font-family:var(--mono)}\n.filter-row{display:flex;gap:8px;margin-bottom:16px}\n.fbtn{font-size:11px;font-family:var(--mono);padding:5px 12px;border:1px solid var(--b2);border-radius:5px;background:transparent;color:var(--t2);cursor:pointer}\n.fbtn.active{border-color:var(--green);color:var(--green);background:rgba(16,185,129,.08)}\n</style>\n</head>\n<body>\n<nav class="nav">\n  <div class="nav-logo">HEXAGON FIRST-ORBIT PRO · LIFETIME LEDGER</div>\n  <a href="/" class="nav-back">&larr; Dashboard</a>\n</nav>\n<div class="page">\n  <div class="page-title">Lifetime Trade Ledger</div>\n  <div class="page-sub">Every trade ever recorded, with running lifetime P&L. Full audit trail — nothing resets.</div>\n  <div id="root"><div class="loading">Loading ledger...</div></div>\n</div>\n<script>\nlet ALL=[],FILTER=\'genuine\';\nasync function load(){\n  const root=document.getElementById(\'root\');\n  let d;\n  try{const r=await fetch(\'/api/ledger\');d=await r.json();}\n  catch(e){root.innerHTML=\'<div class="loading">Failed to load.</div>\';return;}\n  if(d.error){root.innerHTML=\'<div class="loading">\'+d.error+\'</div>\';return;}\n  ALL=d.trades;const s=d.summary;\n  const head=`<div class="summary">\n    <div class="scard"><div class="scard-lbl">Lifetime Realised P&L</div>\n      <div class="scard-val ${s.lifetime_pnl>=0?\'g\':\'r\'}">${s.lifetime_pnl>=0?\'+\':\'\'}Rs.${Math.round(s.lifetime_pnl).toLocaleString(\'en-IN\')}</div>\n      <div class="scard-sub">${s.realised_trades} genuine closed trades</div></div>\n    <div class="scard"><div class="scard-lbl">Win Rate</div>\n      <div class="scard-val ${s.win_rate>=50?\'g\':s.win_rate>=40?\'a\':\'r\'}">${s.win_rate}%</div>\n      <div class="scard-sub">${s.wins}W / ${s.losses}L</div></div>\n    <div class="scard"><div class="scard-lbl">Total Rows</div>\n      <div class="scard-val">${s.total_rows}</div>\n      <div class="scard-sub">incl. open & cancelled</div></div>\n    <div class="scard"><div class="scard-lbl">Avg per Trade</div>\n      <div class="scard-val ${s.lifetime_pnl>=0?\'g\':\'r\'}">${s.realised_trades?(s.lifetime_pnl>=0?\'+\':\'\')+\'Rs.\'+(s.lifetime_pnl/s.realised_trades).toFixed(1):\'—\'}</div>\n      <div class="scard-sub">expectancy</div></div>\n  </div>\n  <div class="filter-row">\n    <button class="fbtn ${FILTER===\'genuine\'?\'active\':\'\'}" onclick="setF(\'genuine\')">Genuine only</button>\n    <button class="fbtn ${FILTER===\'all\'?\'active\':\'\'}" onclick="setF(\'all\')">All rows</button>\n    <button class="fbtn ${FILTER===\'open\'?\'active\':\'\'}" onclick="setF(\'open\')">Open</button>\n  </div>`;\n  root.innerHTML=head+\'<div class="tbl-wrap"><table class="tbl"><thead><tr>\'+\n    \'<th>#</th><th>Symbol</th><th>Entry</th><th>Qty</th><th>P&L</th><th>Running P&L</th><th>Score</th><th>Exit</th><th>Status</th><th>Opened</th><th>Closed</th></tr></thead><tbody id="tb"></tbody></table></div>\';\n  render();\n}\nfunction setF(f){FILTER=f;document.querySelectorAll(\'.fbtn\').forEach(b=>b.classList.remove(\'active\'));event.target.classList.add(\'active\');render();}\nfunction render(){\n  let rows=ALL;\n  if(FILTER===\'genuine\')rows=ALL.filter(t=>t.genuine);\n  else if(FILTER===\'open\')rows=ALL.filter(t=>t.status===\'open\');\n  const fmt=x=>x==null?\'\':(x.length>16?x.slice(5,16).replace(\'T\',\' \'):x);\n  document.getElementById(\'tb\').innerHTML=rows.map((t,i)=>{\n    const tag=t.status===\'open\'?\'<span class="tag tag-open">OPEN</span>\':\n      t.genuine?\'<span class="tag tag-real">REAL</span>\':\'<span class="tag tag-noise">\'+(t.exit_reason||\'\').slice(0,8)+\'</span>\';\n    return `<tr>\n      <td class="neu">${i+1}</td>\n      <td style="color:var(--white)">${t.sym}</td>\n      <td>Rs.${t.entry}</td>\n      <td class="neu">${t.qty}</td>\n      <td class="${t.pnl>0?\'pos\':t.pnl<0?\'neg\':\'neu\'}">${t.pnl>0?\'+\':\'\'}${t.pnl?\'Rs.\'+t.pnl:\'—\'}</td>\n      <td class="${t.running_pnl>=0?\'pos\':\'neg\'}">${t.running_pnl==null?\'—\':(t.running_pnl>=0?\'+\':\'\')+\'Rs.\'+t.running_pnl}</td>\n      <td class="neu">${t.score||\'\'}</td>\n      <td class="neu" style="font-size:11px">${(t.exit_reason||\'\').slice(0,18)}</td>\n      <td>${tag}</td>\n      <td class="neu" style="font-size:11px">${fmt(t.opened_at)}</td>\n      <td class="neu" style="font-size:11px">${fmt(t.closed_at)}</td>\n    </tr>`;}).join(\'\');\n}\nload();\n</script>\n</body>\n</html>\n'

ANALYTICS_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>First-Orbit Analytics</title>\n<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">\n<style>\n:root{\n  --bg:#080c12;--s1:#0f1420;--s2:#141926;--b1:#1c2536;--b2:#222f44;\n  --t4:#2a3a54;--t3:#4a5a7a;--t2:#7a8faa;--t1:#b0c0d8;--white:#e8f0fa;\n  --green:#10b981;--red:#ef4444;--amber:#f59e0b;--blue:#60a5fa;--purple:#a78bfa;\n  --mono:\'JetBrains Mono\',monospace;--sans:\'Inter\',system-ui,sans-serif;\n}\n*{box-sizing:border-box;margin:0;padding:0}\nbody{background:var(--bg);color:var(--t1);font-family:var(--sans);font-size:14px}\n.nav{height:48px;background:var(--s1);border-bottom:1px solid var(--b1);padding:0 24px;display:flex;align-items:center;justify-content:space-between}\n.nav-logo{font-family:var(--mono);font-size:12px;font-weight:600;color:var(--green);letter-spacing:2px}\n.nav-back{font-size:12px;color:var(--t3);text-decoration:none;display:flex;align-items:center;gap:6px}\n.nav-back:hover{color:var(--t1)}\n.page{max-width:1100px;margin:0 auto;padding:32px 24px}\n.page-title{font-size:22px;font-weight:600;color:var(--white);margin-bottom:4px}\n.page-sub{font-size:13px;color:var(--t3);margin-bottom:32px}\n\n/* Summary cards */\n.summary{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:32px}\n@media(max-width:700px){.summary{grid-template-columns:1fr 1fr}}\n.scard{background:var(--s1);border:1px solid var(--b1);border-radius:8px;padding:16px}\n.scard-lbl{font-size:10px;color:var(--t3);letter-spacing:1px;text-transform:uppercase;margin-bottom:8px}\n.scard-val{font-family:var(--mono);font-size:24px;font-weight:600;color:var(--white)}\n.scard-val.g{color:var(--green)}.scard-val.r{color:var(--red)}.scard-val.a{color:var(--amber)}\n.scard-sub{font-size:11px;color:var(--t4);margin-top:4px}\n\n/* Sections */\n.section{margin-bottom:32px}\n.section-title{font-size:13px;font-weight:600;color:var(--t2);letter-spacing:1px;text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--b1)}\n.grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}\n@media(max-width:700px){.grid2{grid-template-columns:1fr}}\n\n/* Table */\n.tbl-wrap{background:var(--s1);border:1px solid var(--b1);border-radius:8px;overflow:hidden}\n.tbl{width:100%;border-collapse:collapse;font-size:13px}\n.tbl th{font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:var(--t3);padding:10px 16px;text-align:left;border-bottom:1px solid var(--b1);font-weight:500}\n.tbl td{padding:11px 16px;border-bottom:1px solid rgba(28,37,54,.6);font-family:var(--mono)}\n.tbl tr:last-child td{border-bottom:none}\n.tbl tr:hover td{background:var(--s2)}\n.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--t3)}\n\n/* Bar chart */\n.bar-chart{display:flex;flex-direction:column;gap:8px}\n.bar-row{display:flex;align-items:center;gap:10px}\n.bar-label{font-size:11px;color:var(--t2);min-width:60px;font-family:var(--mono)}\n.bar-track{flex:1;height:8px;background:var(--s2);border-radius:4px;overflow:hidden}\n.bar-fill{height:100%;border-radius:4px;transition:width .6s}\n.bar-val{font-size:11px;font-family:var(--mono);min-width:50px;text-align:right}\n\n/* Recommendations */\n.rec-list{display:flex;flex-direction:column;gap:8px}\n.rec-item{background:var(--s1);border:1px solid var(--b1);border-left:3px solid var(--amber);border-radius:6px;padding:12px 14px;font-size:13px;color:var(--t1)}\n.rec-item.good{border-left-color:var(--green)}\n.rec-item.info{border-left-color:var(--blue)}\n\n/* Trade card */\n.trade-row{display:flex;justify-content:space-between;align-items:center;padding:10px 16px;border-bottom:1px solid var(--b1)}\n.trade-row:last-child{border-bottom:none}\n.trade-sym{font-family:var(--mono);font-weight:500;color:var(--white)}\n.trade-meta{font-size:11px;color:var(--t3);margin-top:2px}\n\n/* Loading / error */\n.loading{text-align:center;padding:60px;color:var(--t4);font-family:var(--mono);font-size:13px}\n.insufficient{background:var(--s1);border:1px solid var(--b1);border-radius:8px;padding:40px;text-align:center;color:var(--t3);font-size:13px;line-height:1.8}\n</style>\n</head>\n<body>\n<nav class="nav">\n  <div class="nav-logo">⬡ FIRST-ORBIT PRO · ANALYTICS</div>\n  <a href="/" class="nav-back">← Dashboard</a>\n</nav>\n\n<div class="page">\n  <div class="page-title">Strategy Analytics</div>\n  <div class="page-sub">What the trade history is telling us — patterns, win rates, recommendations</div>\n\n  <div id="root"><div class="loading">Loading trade data...</div></div>\n</div>\n\n<script>\nasync function load() {\n  const root = document.getElementById(\'root\');\n  let d;\n  try {\n    const r = await fetch(\'/analytics\');\n    d = await r.json();\n  } catch(e) {\n    root.innerHTML = \'<div class="loading">Failed to load — is the bot running?</div>\';\n    return;\n  }\n\n  if (d.error) {\n    root.innerHTML = `<div class="insufficient">${d.error}</div>`;\n    return;\n  }\n\n  const s = d.summary;\n  const pf = s.profit_factor;\n\n  root.innerHTML = `\n    <!-- SUMMARY -->\n    <div class="summary">\n      <div class="scard">\n        <div class="scard-lbl">Total P&L</div>\n        <div class="scard-val ${s.total_pnl>=0?\'g\':\'r\'}">${s.total_pnl>=0?\'+\':\'\'}Rs.${Math.round(s.total_pnl).toLocaleString(\'en-IN\')}</div>\n        <div class="scard-sub">${s.total_trades} closed trades</div>\n      </div>\n      <div class="scard">\n        <div class="scard-lbl">Win Rate</div>\n        <div class="scard-val ${s.win_rate>=50?\'g\':s.win_rate>=35?\'a\':\'r\'}">${s.win_rate}%</div>\n        <div class="scard-sub">need >40% to be profitable</div>\n      </div>\n      <div class="scard">\n        <div class="scard-lbl">Expectancy</div>\n        <div class="scard-val ${s.expectancy>=0?\'g\':\'r\'}">${s.expectancy>=0?\'+\':\'\'}Rs.${s.expectancy}</div>\n        <div class="scard-sub">avg P&L per trade</div>\n      </div>\n      <div class="scard">\n        <div class="scard-lbl">Profit Factor</div>\n        <div class="scard-val ${pf>=1.5?\'g\':pf>=1?\'a\':\'r\'}">${pf}×</div>\n        <div class="scard-sub">gross wins ÷ gross losses</div>\n      </div>\n    </div>\n\n    <!-- AVG WIN/LOSS -->\n    <div class="summary" style="grid-template-columns:1fr 1fr 1fr;margin-bottom:32px">\n      <div class="scard">\n        <div class="scard-lbl">Avg Win</div>\n        <div class="scard-val g">+Rs.${s.avg_win}</div>\n        <div class="scard-sub">per winning trade</div>\n      </div>\n      <div class="scard">\n        <div class="scard-lbl">Avg Loss</div>\n        <div class="scard-val r">Rs.${s.avg_loss}</div>\n        <div class="scard-sub">per losing trade</div>\n      </div>\n      <div class="scard">\n        <div class="scard-lbl">Win:Loss Ratio</div>\n        <div class="scard-val ${s.avg_loss!==0&&Math.abs(s.avg_win/s.avg_loss)>=1.5?\'g\':\'a\'}">${s.avg_loss!==0?Math.abs(s.avg_win/s.avg_loss).toFixed(2):\'—\'}×</div>\n        <div class="scard-sub">target: 1.5×</div>\n      </div>\n    </div>\n\n    <div class="grid2">\n      <!-- WIN RATE BY SCORE -->\n      <div class="section">\n        <div class="section-title">Win Rate by Setup Score</div>\n        <div class="tbl-wrap">\n          <table class="tbl">\n            <thead><tr><th>Score</th><th>Trades</th><th>Win Rate</th><th>Avg P&L</th><th>Total</th></tr></thead>\n            <tbody>\n              ${Object.entries(d.by_score).map(([b,v])=>`\n                <tr>\n                  <td style="color:var(--white)">${b}</td>\n                  <td class="neu">${v.trades}</td>\n                  <td class="${v.win_rate>=50?\'pos\':v.win_rate>=35?\'\':\' neg\'}">${v.win_rate}%</td>\n                  <td class="${v.avg_pnl>=0?\'pos\':\'neg\'}">${v.avg_pnl>=0?\'+\':\'\'}Rs.${v.avg_pnl}</td>\n                  <td class="${v.total_pnl>=0?\'pos\':\'neg\'}">${v.total_pnl>=0?\'+\':\'\'}Rs.${v.total_pnl}</td>\n                </tr>`).join(\'\')}\n            </tbody>\n          </table>\n        </div>\n      </div>\n\n      <!-- WIN RATE BY DAYS HELD -->\n      <div class="section">\n        <div class="section-title">Win Rate by Days Held</div>\n        <div class="tbl-wrap">\n          <table class="tbl">\n            <thead><tr><th>Days</th><th>Trades</th><th>Win Rate</th><th>Avg P&L</th></tr></thead>\n            <tbody>\n              ${Object.entries(d.by_days_held).map(([b,v])=>`\n                <tr>\n                  <td style="color:var(--white)">${b===\'0\'?\'Same day\':b===\'1\'?\'1 day\':b+\' days\'}</td>\n                  <td class="neu">${v.trades}</td>\n                  <td class="${v.win_rate>=50?\'pos\':v.win_rate>=35?\'\':\' neg\'}">${v.win_rate}%</td>\n                  <td class="${v.avg_pnl>=0?\'pos\':\'neg\'}">${v.avg_pnl>=0?\'+\':\'\'}Rs.${v.avg_pnl}</td>\n                </tr>`).join(\'\')}\n            </tbody>\n          </table>\n        </div>\n      </div>\n    </div>\n\n    <!-- EXIT REASON BREAKDOWN -->\n    <div class="section">\n      <div class="section-title">Exit Signal Performance</div>\n      <div class="tbl-wrap">\n        <table class="tbl">\n          <thead><tr><th>Exit Reason</th><th>Wins</th><th>Losses</th><th>Total P&L</th><th>Assessment</th></tr></thead>\n          <tbody>\n            ${Object.entries(d.by_exit_reason).map(([reason, v])=>{\n              const total = v.wins + v.losses;\n              const wr = total ? Math.round(v.wins/total*100) : 0;\n              const assess = wr >= 60 ? \'✓ Working\' : wr >= 40 ? \'~ Neutral\' : \'✕ Review\';\n              const assessCls = wr >= 60 ? \'pos\' : wr >= 40 ? \'neu\' : \'neg\';\n              return `<tr>\n                <td style="color:var(--white)">${reason}</td>\n                <td class="pos">${v.wins}</td>\n                <td class="neg">${v.losses}</td>\n                <td class="${v.pnl>=0?\'pos\':\'neg\'}">${v.pnl>=0?\'+\':\'\'}Rs.${v.pnl}</td>\n                <td class="${assessCls}">${assess} (${wr}%)</td>\n              </tr>`;\n            }).join(\'\')}\n          </tbody>\n        </table>\n      </div>\n    </div>\n\n    <div class="grid2">\n      <!-- BEST TRADES -->\n      <div class="section">\n        <div class="section-title">Best Trades</div>\n        <div class="tbl-wrap">\n          ${d.best_trades.map(t=>`\n            <div class="trade-row">\n              <div>\n                <div class="trade-sym">${t.sym}</div>\n                <div class="trade-meta">${t.reason} · score ${t.score} · ${t.days}d</div>\n              </div>\n              <div class="pos">+Rs.${t.pnl.toLocaleString(\'en-IN\')}</div>\n            </div>`).join(\'\')}\n        </div>\n      </div>\n\n      <!-- WORST TRADES -->\n      <div class="section">\n        <div class="section-title">Worst Trades</div>\n        <div class="tbl-wrap">\n          ${[...d.worst_trades].reverse().map(t=>`\n            <div class="trade-row">\n              <div>\n                <div class="trade-sym">${t.sym}</div>\n                <div class="trade-meta">${t.reason} · score ${t.score} · ${t.days}d</div>\n              </div>\n              <div class="neg">Rs.${t.pnl.toLocaleString(\'en-IN\')}</div>\n            </div>`).join(\'\')}\n        </div>\n      </div>\n    </div>\n\n    <!-- RECOMMENDATIONS -->\n    <div class="section">\n      <div class="section-title">What the Data Suggests</div>\n      <div class="rec-list">\n        ${d.recommendations.map(r=>`<div class="rec-item">💡 ${r}</div>`).join(\'\')}\n      </div>\n      <div style="font-size:11px;color:var(--t4);margin-top:12px;padding:0 4px">\n        Recommendations require 20+ real strategy exits to be statistically meaningful. \n        Current count: ${s.total_trades} trades.\n      </div>\n    </div>\n  `;\n}\n\nload();\n</script>\n</body>\n</html>\n'


def start_dashboard():
    app = Flask(__name__)

    @app.route("/")
    def index():
        return render_template_string(DASHBOARD_HTML)

    @app.route("/analytics")
    def analytics_page():
        return ANALYTICS_HTML

    @app.route("/ledger")
    def ledger_page():
        return LEDGER_HTML



    @app.route("/health")
    def health():
        return {"status": "ok"}, 200

    @app.route("/ping")
    def ping():
        return "pong", 200

    @app.route("/kite/login")
    def kite_login():
        """Step 1: redirect the user to Kite's login page."""
        if not KITE_API_KEY:
            return "KITE_API_KEY not set in environment", 500
        url = f"https://kite.zerodha.com/connect/login?api_key={KITE_API_KEY}&v=3"
        return (f'<html><body style="font-family:monospace;background:#0a0d12;color:#b0c0d8;padding:40px">'
                f'<h2 style="color:#10b981">Kite Connect — Login</h2>'
                f'<p>Click below to authorise. After login, Kite redirects back and we exchange the token.</p>'
                f'<p><a href="{url}" style="color:#60a5fa">→ Log in to Kite</a></p>'
                f'<p style="color:#4a5a7a;font-size:12px">Bot stays in PAPER mode regardless. This only verifies auth works.</p>'
                f'</body></html>')

    @app.route("/kite/callback")
    def kite_callback():
        """Step 2: Kite redirects here with request_token. Exchange it for access token."""
        from flask import request
        request_token = request.args.get("request_token", "")
        if not request_token:
            return "No request_token in callback — login may have failed", 400
        api_secret = os.environ.get("KITE_API_SECRET", "")
        if not (KITE_API_KEY and api_secret):
            return "KITE_API_KEY / KITE_API_SECRET not set", 500
        try:
            from kiteconnect import KiteConnect
            kc = KiteConnect(api_key=KITE_API_KEY)
            data = kc.generate_session(request_token, api_secret=api_secret)
            access_token = data["access_token"]
            _save_token(access_token)  # persist so the running bot uses it today
            # Show it so the operator can set it as KITE_ACCESS_TOKEN env var.
            # NOTE: token expires daily ~6 AM IST; must be regenerated.
            return (f'<html><body style="font-family:monospace;background:#0a0d12;color:#b0c0d8;padding:40px">'
                    f'<h2 style="color:#10b981">✓ Kite auth successful</h2>'
                    f'<p>Connection works. Access token generated (valid until ~6 AM IST tomorrow).</p>'
                    f'<p style="color:#4a5a7a">Set this as <b>KITE_ACCESS_TOKEN</b> in Railway if/when you go live:</p>'
                    f'<p style="background:#141926;padding:12px;border-radius:6px;word-break:break-all;color:#e8f0fa">{access_token}</p>'
                    f'<p style="color:#f59e0b;font-size:12px">Bot is still in PAPER mode. No orders placed. '
                    f'Live trading requires FIRST_ORBIT_LIVE=1 AND a cleared DDPI mandate.</p>'
                    f'</body></html>')
        except Exception as e:
            return (f'<html><body style="font-family:monospace;background:#0a0d12;color:#ef4444;padding:40px">'
                    f'<h2>Kite auth failed</h2><p>{str(e)}</p>'
                    f'<p style="color:#4a5a7a">Check API key/secret and that the redirect URL matches exactly.</p>'
                    f'</body></html>'), 500


    @app.route("/debug/trades")
    def debug_trades():
        """Show all closed trades with their exit_reason and pnl — for debugging."""
        try:
            con2 = sqlite3.connect(DB_PATH)
            rows = con2.execute(
                "SELECT sym, status, pnl, exit_reason FROM trades "
                "WHERE status != 'open' ORDER BY closed_at DESC"
            ).fetchall()
            con2.close()
            result = [{"sym":r[0],"status":r[1],"pnl":r[2],"exit_reason":r[3]} for r in rows]
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": str(e)})

    @app.route("/debug/reopen/<sym>")
    def debug_reopen(sym):
        """One-shot: clean up bogus duplicate closes for a symbol and recreate
        ONE open position. Params: entry, qty, sl, target (query string).
        Example: /debug/reopen/GLAND?entry=2248&qty=4&sl=2082.31&target=2496.53
        Remove this route after use."""
        from flask import request
        sym = sym.upper()
        try:
            entry  = float(request.args.get("entry", 0))
            qty    = int(request.args.get("qty", 0))
            sl     = float(request.args.get("sl", 0))
            target = float(request.args.get("target", 0))
            if not (entry > 0 and qty > 0 and sl > 0 and target > 0):
                return jsonify({"error": "need entry, qty, sl, target as positive query params"}), 400

            con2 = sqlite3.connect(DB_PATH)
            # 1. Don't create a duplicate — bail if an open row already exists
            if con2.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'", (sym,)).fetchone():
                con2.close()
                return jsonify({"error": f"{sym} already has an open position — nothing to do"}), 400

            # 2. Purge the bogus duplicate closes (the after-hours DAILY_RSI sells)
            #    so they don't pollute the lifetime ledger.
            deleted = con2.execute(
                "DELETE FROM trades WHERE sym=? AND status IN ('win','loss')", (sym,)
            ).rowcount

            # 3. Recreate ONE clean open position
            rr = round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)
            risk_amt = round(qty * abs(entry - sl), 2)
            con2.execute("""
                INSERT INTO trades
                (id,sym,direction,entry,sl,target,qty,risk_amt,target_gain,rr,score,
                 status,pnl,days_held,opened_at,exit_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'open',0,0,?,?)
            """, (f"{sym}_{int(time.time())}", sym, "BUY",
                  entry, sl, target, qty, risk_amt,
                  round(qty * abs(entry - target), 2),
                  rr, 0, datetime.now().isoformat(), ""))
            con2.commit()
            con2.close()
            return jsonify({
                "ok": True, "sym": sym,
                "deleted_bogus_closes": deleted,
                "reopened": {"entry": entry, "qty": qty, "sl": sl, "target": target},
                "note": "Remove this route after use."
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/ledger")
    def api_ledger():
        """Complete lifetime trade ledger with running P&L — full audit trail."""
        try:
            con2 = sqlite3.connect(DB_PATH)
            rows = con2.execute(
                "SELECT sym, direction, entry, qty, pnl, score, exit_reason, "
                "status, opened_at, closed_at, days_held "
                "FROM trades ORDER BY COALESCE(closed_at, opened_at) ASC"
            ).fetchall()
            con2.close()

            ledger, running = [], 0.0
            realised_total = wins = losses = 0
            for r in rows:
                sym, direction, entry, qty, pnl, score, reason, status, opened, closed, days = r
                pnl = float(pnl or 0)
                is_closed = status in ("win", "loss")
                # Genuine realised trades only (exclude cancelled/excess/dup noise)
                genuine = is_closed and reason and not any(
                    x in reason for x in ("EXCESS", "CANCEL", "BOOT", "DUP")
                )
                if genuine:
                    running += pnl
                    realised_total += 1
                    if pnl > 0: wins += 1
                    else: losses += 1
                ledger.append({
                    "sym": sym, "direction": direction or "BUY",
                    "entry": round(float(entry or 0), 2),
                    "qty": int(qty or 0),
                    "pnl": round(pnl, 2),
                    "score": round(float(score or 0), 1),
                    "exit_reason": reason or "",
                    "status": status,
                    "opened_at": opened, "closed_at": closed,
                    "days_held": int(days or 0),
                    "genuine": genuine,
                    "running_pnl": round(running, 2) if genuine else None,
                })
            wr = round(wins / realised_total * 100, 1) if realised_total else 0
            return jsonify({
                "trades": ledger,
                "summary": {
                    "lifetime_pnl": round(running, 2),
                    "realised_trades": realised_total,
                    "wins": wins, "losses": losses, "win_rate": wr,
                    "total_rows": len(ledger),
                }
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/api/analytics")
    def analytics():
        """Strategy performance analytics — learns from trade history."""
        try:
            con2 = sqlite3.connect(DB_PATH)

            # Only real strategy exits — no cancelled/excess
            REAL = (
                "status IN ('win','loss') "
                "AND exit_reason NOT LIKE '%EXCESS%' "
                "AND exit_reason NOT LIKE '%CANCEL%' "
                "AND exit_reason NOT LIKE '%BOOT%' "
                "AND exit_reason NOT LIKE 'MANUAL_CLOSE'"
            )
            ALL_CLOSED = (
                "status IN ('win','loss') "
                "AND exit_reason NOT LIKE '%EXCESS%' "
                "AND exit_reason NOT LIKE '%CANCEL%' "
                "AND exit_reason NOT LIKE '%BOOT%'"
            )

            rows = con2.execute(
                f"SELECT sym, pnl, score, days_held, exit_reason, opened_at, closed_at, entry "
                f"FROM trades WHERE {ALL_CLOSED} ORDER BY closed_at DESC"
            ).fetchall()

            if not rows:
                return jsonify({"error": "No closed trades yet — need more data"}), 200

            trades = []
            for r in rows:
                sym, pnl, score, days, reason, opened, closed, entry = r
                trades.append({
                    "sym": sym, "pnl": float(pnl or 0),
                    "score": float(score or 0), "days": int(days or 0),
                    "reason": reason or "", "entry": float(entry or 0),
                })

            total     = len(trades)
            wins      = [t for t in trades if t["pnl"] > 0]
            losses    = [t for t in trades if t["pnl"] <= 0]
            win_rate  = round(len(wins) / total * 100, 1) if total else 0
            avg_win   = round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0
            avg_loss  = round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0
            total_pnl = round(sum(t["pnl"] for t in trades), 2)
            expectancy = round((win_rate/100 * avg_win) + ((1-win_rate/100) * avg_loss), 2)

            # Win rate by score bucket
            score_buckets = {"5-10": [], "10-15": [], "15-20": [], "20+": []}
            for t in trades:
                s = t["score"]
                if s < 10:    score_buckets["5-10"].append(t["pnl"])
                elif s < 15:  score_buckets["10-15"].append(t["pnl"])
                elif s < 20:  score_buckets["15-20"].append(t["pnl"])
                else:         score_buckets["20+"].append(t["pnl"])

            score_stats = {}
            for bucket, pnls in score_buckets.items():
                if pnls:
                    w = sum(1 for p in pnls if p > 0)
                    score_stats[bucket] = {
                        "trades": len(pnls),
                        "win_rate": round(w/len(pnls)*100, 1),
                        "avg_pnl": round(sum(pnls)/len(pnls), 2),
                        "total_pnl": round(sum(pnls), 2),
                    }

            # Win rate by days held
            days_buckets = {"0": [], "1": [], "2-3": [], "4+": []}
            for t in trades:
                d = t["days"]
                if d == 0:   days_buckets["0"].append(t["pnl"])
                elif d == 1: days_buckets["1"].append(t["pnl"])
                elif d <= 3: days_buckets["2-3"].append(t["pnl"])
                else:        days_buckets["4+"].append(t["pnl"])

            days_stats = {}
            for bucket, pnls in days_buckets.items():
                if pnls:
                    w = sum(1 for p in pnls if p > 0)
                    days_stats[bucket] = {
                        "trades": len(pnls),
                        "win_rate": round(w/len(pnls)*100, 1),
                        "avg_pnl": round(sum(pnls)/len(pnls), 2),
                    }

            # Exit reason breakdown
            exit_counts = {}
            for t in trades:
                r = t["reason"].split("(")[0]
                if r not in exit_counts:
                    exit_counts[r] = {"wins": 0, "losses": 0, "pnl": 0}
                if t["pnl"] > 0: exit_counts[r]["wins"] += 1
                else:             exit_counts[r]["losses"] += 1
                exit_counts[r]["pnl"] = round(exit_counts[r]["pnl"] + t["pnl"], 2)

            # Best and worst trades
            sorted_trades = sorted(trades, key=lambda x: x["pnl"], reverse=True)
            best  = sorted_trades[:3]
            worst = sorted_trades[-3:]

            # Recommendation engine — what does the data suggest?
            recommendations = []
            if score_stats.get("5-10", {}).get("win_rate", 100) < 30:
                recommendations.append("Low-score trades (5-10) underperforming — consider raising MIN_SCORE to 10")
            if score_stats.get("20+", {}).get("win_rate", 0) > 60:
                recommendations.append("High-score trades (20+) winning more — consider prioritising score > 20")
            if days_stats.get("0", {}).get("win_rate", 100) < 25:
                recommendations.append("Same-day exits losing — divergence exit may be triggering too early")
            if avg_loss != 0 and abs(avg_win / avg_loss) < 1.2:
                recommendations.append("R:R in practice below 1.2 — consider tightening stops or widening targets")
            if win_rate < 35 and total >= 10:
                recommendations.append("Win rate below 35% — strategy needs refinement or market is in wrong regime")
            if not recommendations:
                recommendations.append("Not enough data yet — need 20+ trades for reliable patterns")

            con2.close()
            return jsonify({
                "summary": {
                    "total_trades": total,
                    "win_rate": win_rate,
                    "total_pnl": total_pnl,
                    "avg_win": avg_win,
                    "avg_loss": avg_loss,
                    "expectancy": expectancy,
                    "profit_factor": round(abs(sum(t["pnl"] for t in wins) / sum(t["pnl"] for t in losses)), 2) if losses and sum(t["pnl"] for t in losses) != 0 else 0,
                },
                "by_score": score_stats,
                "by_days_held": days_stats,
                "by_exit_reason": exit_counts,
                "best_trades": best,
                "worst_trades": worst,
                "recommendations": recommendations,
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/close/<sym>", methods=["POST"])
    def manual_close(sym):
        """Manually close an open position at current market price."""
        try:
            con2 = sqlite3.connect(DB_PATH)
            row = con2.execute(
                "SELECT id, entry, qty FROM trades WHERE sym=? AND status='open'",
                (sym.upper(),)
            ).fetchone()
            if not row:
                con2.close()
                return jsonify({"error": f"{sym} not found in open positions"}), 404
            tid, entry, qty = row
            # Fetch current price
            price = entry  # fallback
            try:
                h = yf.Ticker(sym.upper() + ".NS").history(
                    period="1d", interval="1m", timeout=5, auto_adjust=True)
                if h is not None and len(h) > 0:
                    price = round(float(h["Close"].iloc[-1]), 2)
            except Exception:
                pass
            qty   = qty or 1
            pnl   = round((price - entry) * qty, 2)
            status = "win" if pnl > 0 else "loss"
            # Route through broker — paper simulates, live places real Kite sell
            fill = broker.place_sell(sym.upper(), qty, price)
            if broker.live and fill.get("status") != "LIVE_PLACED":
                con2.close()
                return jsonify({"error": f"Live sell failed for {sym}", "fill": fill}), 500
            con2.execute(
                "UPDATE trades SET status=?,pnl=?,closed_at=?,exit_reason=? WHERE id=?",
                (status, pnl, datetime.now().isoformat(), "MANUAL_CLOSE", tid)
            )
            # Update weekly stats
            ws2 = (date.today()-timedelta(days=date.today().weekday())).isoformat()
            con2.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)",(ws2,))
            con2.execute(
                "UPDATE weekly_stats SET pnl=pnl+?,wins=wins+?,losses=losses+? WHERE week_start=?",
                (pnl, 1 if pnl>0 else 0, 0 if pnl>0 else 1, ws2)
            )
            # Invalidate screener cache — prevents immediate re-entry after manual close
            con2.execute("DELETE FROM screener_cache WHERE sym=?", (sym.upper(),))
            con2.commit()
            con2.close()
            log.info(f"  MANUAL CLOSE {sym.upper()} @ Rs.{price:.2f}  P&L Rs.{pnl:+.2f}")
            return jsonify({"sym": sym.upper(), "price": price, "pnl": pnl, "status": status})
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    @app.route("/api/status")
    def api_status():
        try:
            con = sqlite3.connect(DB_PATH)

            # Use the SAME stats function the cycle log uses — lifetime P&L,
            # lifetime W/L, including manual closes. (Previously read weekly_stats
            # directly here, which is why the dashboard disagreed with the log.)
            stats = get_stats(con)

            # Open positions — fetch live 1-min price
            open_rows = con.execute(
                "SELECT sym,entry,sl,target,rr,risk_amt,days_held,qty "
                "FROM trades WHERE status='open'"
            ).fetchall()
            open_list = []
            for r in open_rows:
                sym, entry, sl, target, rr, risk_amt, days_held, qty = r
                # Try live 1-min price
                last_price = entry
                try:
                    h = yf.Ticker(sym + ".NS").history(
                        period="1d", interval="1m", timeout=5, auto_adjust=True)
                    if h is not None and len(h) > 0:
                        lp = round(float(h["Close"].iloc[-1]), 2)
                        if math.isfinite(lp):
                            last_price = lp
                        else:
                            raise ValueError("nan price")
                    else:
                        raise ValueError("empty")
                except Exception:
                    # Fall back to screener cache
                    cr = con.execute(
                        "SELECT price FROM screener_cache WHERE sym=?", (sym,)
                    ).fetchone()
                    if cr and cr[0] and math.isfinite(float(cr[0])):
                        last_price = round(float(cr[0]), 2)
                qty     = qty or 1
                unreal  = round((last_price - entry) * qty, 2)
                # % move on total position value (entry × qty), not just per share
                position_value = entry * qty
                upct    = round((last_price - entry) * qty / position_value * 100, 2) if position_value else 0
                open_list.append({
                    "sym":        sym,
                    "entry":      entry,
                    "sl":         sl,
                    "target":     target,
                    "rr":         rr,
                    "risk_amt":   risk_amt,
                    "days_held":  days_held or 0,
                    "qty":        qty,
                    "last_price": last_price,
                    "unrealised": unreal,
                    "unreal_pct": upct,
                })

            # Closed trades — compute real P&L for EXCESS_CANCELLED
            closed_rows = con.execute(
                "SELECT sym,entry,pnl,status,exit_reason,qty "
                "FROM trades WHERE status != 'open' "
                "ORDER BY closed_at DESC LIMIT 25"
            ).fetchall()
            closed_list = []
            needs_commit = False
            for r in closed_rows:
                sym, entry, pnl, status, exit_reason, qty = r
                qty = qty or 1
                pnl = pnl if pnl is not None else 0.0

                # Recalculate P&L for cancelled trades ONLY if not yet computed
                if exit_reason and "EXCESS" in exit_reason and pnl == 0:
                    last_price = None
                    # Try screener cache
                    cr = con.execute(
                        "SELECT price FROM screener_cache WHERE sym=?", (sym,)
                    ).fetchone()
                    if cr and cr[0]:
                        last_price = float(cr[0])
                    # Try yfinance
                    if not last_price:
                        try:
                            h = yf.Ticker(sym + ".NS").history(
                                period="5d", interval="1d", timeout=5, auto_adjust=True)
                            if h is not None and len(h) > 0:
                                last_price = float(h["Close"].iloc[-1])
                        except Exception:
                            pass
                    if last_price:
                        pnl    = round((last_price - entry) * qty, 2)
                        status = "win" if pnl > 0 else ("loss" if pnl < 0 else "cancelled")
                        con.execute(
                            "UPDATE trades SET pnl=?, status=? "
                            "WHERE sym=? AND exit_reason LIKE '%EXCESS%'",
                            (pnl, status, sym)
                        )
                        needs_commit = True

                closed_list.append({
                    "sym":         sym,
                    "entry":       entry,
                    "pnl":         float(pnl),
                    "status":      status or "cancelled",
                    "exit_reason": exit_reason or "",
                })

            if needs_commit:
                # Update weekly_stats with newly computed P&L
                ws2 = (date.today()-timedelta(days=date.today().weekday())).isoformat()
                con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)",(ws2,))
                # Sum all EXCESS_CANCELLED P&L from closed_list
                excess_pnl = sum(
                    t["pnl"] for t in closed_list
                    if t.get("exit_reason","") and "EXCESS" in t["exit_reason"]
                )
                if excess_pnl != 0:
                    con.execute(
                        "UPDATE weekly_stats SET pnl=? WHERE week_start=?",
                        (excess_pnl, ws2)
                    )
                con.commit()
            con.close()

            # Logs
            logs = []
            try:
                with open(LOG_PATH) as f:
                    logs = [l.strip() for l in f.readlines()[-60:]][::-1]
            except Exception:
                logs = ["Bot starting up..."]

            # Fetch market indices — cached 60s so we don't hammer Yahoo every 5s
            indices = []
            _now_ts = time.time()
            _cache  = getattr(api_status, '_idx_cache', {"ts": 0, "data": []})
            if _now_ts - _cache["ts"] > 60:
                import yfinance as _yfi2
                import logging as _lg2
                from curl_cffi.requests import Session as _CS
                _lg2.getLogger("yfinance").setLevel(_lg2.CRITICAL)
                _sess = _CS(impersonate="chrome110")
                _yfi2.set_tz_cache_location("/tmp/yf_tz")
                INDICES = [
                    ("^NSEI",      "NIFTY 50",      "Benchmark"),
                    ("^NSEBANK",   "BANK NIFTY",    "Banking"),
                    ("^CNXIT",     "NIFTY IT",       "Technology"),
                    ("^CNXPHARMA", "NIFTY PHARMA",  "Pharma"),
                    ("^CNXAUTO",   "NIFTY AUTO",    "Auto"),
                    ("^CNXFMCG",   "NIFTY FMCG",   "FMCG"),
                    ("^CNXMETAL",  "NIFTY METAL",   "Metal"),
                    ("^CNXREALTY", "NIFTY REALTY",  "Realty"),
                    ("^CNXSC",     "NIFTY SMALLCAP","Small Cap"),
                    ("^INDIAVIX",  "INDIA VIX",     "Volatility"),
                ]
                _fresh = []
                for _sym, _name, _sector in INDICES:
                    try:
                        _t = _yfi2.Ticker(_sym, session=_sess)
                        _h = _t.history(period="5d", interval="1d",
                                        auto_adjust=True, timeout=6)
                        _h = _h.dropna(subset=["Close"])
                        if len(_h) >= 2:
                            _prev = float(_h["Close"].iloc[-2])
                            _curr = float(_h["Close"].iloc[-1])
                            _chg  = round((_curr - _prev) / _prev * 100, 2)
                            _fresh.append({
                                "sym": _sym, "name": _name, "sector": _sector,
                                "price": round(_curr, 2), "chg": _chg,
                                "up": _chg > 0.1, "down": _chg < -0.1,
                            })
                    except Exception:
                        pass
                _lg2.getLogger("yfinance").setLevel(_lg2.ERROR)
                api_status._idx_cache = {"ts": _now_ts, "data": _fresh}
                indices = _fresh
            else:
                indices = _cache["data"]

            # ── CAPITAL ACCOUNTING ────────────────────────────────────────────
            # Make it unambiguous where money is — critical before going live.
            #   deployed   = capital tied up in open positions (entry × qty)
            #   unreal_pnl = mark-to-market gain/loss on those open positions
            #   realised   = lifetime closed P&L (already in stats['pnl'])
            #   free_cash  = starting capital + realised − deployed
            #   equity     = free_cash + deployed + unreal_pnl  (total net worth)
            deployed   = round(sum(p["entry"] * p["qty"] for p in open_list), 2)
            unreal_pnl = round(sum(p["unrealised"] for p in open_list), 2)
            realised   = float(stats.get("pnl", 0) or 0)
            free_cash  = round(CAPITAL + realised - deployed, 2)
            equity     = round(free_cash + deployed + unreal_pnl, 2)
            capital = {
                "starting":   CAPITAL,
                "free_cash":  free_cash,
                "deployed":   deployed,
                "unreal_pnl": unreal_pnl,
                "realised":   round(realised, 2),
                "equity":     equity,
                "open_count": len(open_list),
            }

            return jsonify({
                "stats":   stats,
                "capital": capital,
                "open":    open_list,
                "closed":  closed_list,
                "logs":    logs,
                "indices": indices,
                "mode":    broker.mode_label(),
                "kite_authed": _get_saved_token() is not None,
                "time":    datetime.now().strftime("%H:%M:%S"),
            })

        except Exception as e:
            return jsonify({
                "stats":  {"pnl": 0, "risk_used": 0, "wins": 0, "losses": 0},
                "open":   [],
                "closed": [],
                "logs":   [f"API error: {e}"],
                "time":   "--:--:--",
            })

    port = int(os.environ.get("PORT", 8080))
    log.info(f"Dashboard on http://0.0.0.0:{port}")
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port,
                               debug=False, use_reloader=False),
        daemon=True
    ).start()


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 52)
    log.info("  First-Orbit Trader PRO")
    log.info("  Strategy : Dual RSI Momentum + MCap Filter")
    log.info(f"  Capital  : Rs.{CAPITAL:,}   Risk/week: Rs.{MAX_WEEKLY_RISK:,}")
    log.info(f"  DB       : {DB_PATH}")
    log.info("=" * 52)

    try:
        import curl_cffi
        log.info(f"  curl_cffi {curl_cffi.__version__} — Yahoo cloud fix active")
    except ImportError:
        log.warning("  curl_cffi not found — add to requirements.txt")

    con = init_db()

    # Drop and recreate screener_cache to ensure correct schema
    # (safe to drop — rebuilt automatically on next scan)
    con.execute("DROP TABLE IF EXISTS screener_cache")
    con.execute("""CREATE TABLE IF NOT EXISTS screener_cache (
        sym TEXT PRIMARY KEY, price REAL, daily_rsi REAL,
        weekly_rsi REAL, atr REAL, score REAL,
        entry REAL, sl REAL, target REAL,
        reject TEXT, updated_at TEXT
    )""")
    con.commit()
    log.info("  Screener cache reset — fresh build on next scan")

    # Migrate weekly_stats schema if needed (add time_exits if missing)
    cols = [r[1] for r in con.execute("PRAGMA table_info(weekly_stats)").fetchall()]
    if "time_exits" not in cols:
        con.execute("ALTER TABLE weekly_stats ADD COLUMN time_exits INTEGER DEFAULT 0")
        con.commit()
        log.info("  Migrated weekly_stats: added time_exits column")

    # Repair NULL qty from legacy schema — prevents manage_positions crash
    repaired = con.execute(
        "UPDATE trades SET qty=1 WHERE qty IS NULL AND status='open'"
    ).rowcount
    con.commit()
    if repaired:
        log.info(f"  Repaired {repaired} trades with NULL qty")

    # Dedupe: if multiple OPEN rows exist for the same symbol (race condition in
    # an earlier build), keep the most recent and cancel the rest. This is what
    # caused a symbol to be "sold multiple times" — each duplicate row sold itself.
    dupes = con.execute(
        "SELECT sym, COUNT(*) c FROM trades WHERE status='open' "
        "GROUP BY sym HAVING c > 1"
    ).fetchall()
    for sym, cnt in dupes:
        ids = [r[0] for r in con.execute(
            "SELECT id FROM trades WHERE sym=? AND status='open' ORDER BY opened_at DESC",
            (sym,)
        ).fetchall()]
        keep, drop = ids[0], ids[1:]
        for tid in drop:
            con.execute(
                "UPDATE trades SET status='loss',pnl=0,closed_at=?,exit_reason='DUP_CANCELLED' WHERE id=?",
                (datetime.now().isoformat(), tid)
            )
        log.warning(f"  Deduped {sym}: kept 1, cancelled {len(drop)} duplicate open row(s)")
    con.commit()

    # Enforce MAX_OPEN on boot — keep top N by score, cancel rest with real P&L
    open_rows = con.execute(
        "SELECT id,sym,score,entry,qty FROM trades WHERE status='open' ORDER BY score DESC"
    ).fetchall()
    if len(open_rows) > MAX_OPEN:
        excess = open_rows[MAX_OPEN:]
        ws = (date.today() - timedelta(days=date.today().weekday())).isoformat()
        con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
        net_pnl = 0.0
        for tid, sym, score, entry, qty in excess:
            qty = qty or 1
            # Get last price from cache
            cr = con.execute(
                "SELECT price FROM screener_cache WHERE sym=?", (sym,)
            ).fetchone()
            last = float(cr[0]) if cr and cr[0] else entry
            pnl  = round((last - entry) * qty, 2)
            stat = "win" if pnl > 0 else ("loss" if pnl < 0 else "cancelled")
            con.execute(
                "UPDATE trades SET status=?,pnl=?,closed_at=?,exit_reason=? WHERE id=?",
                (stat, pnl, datetime.now().isoformat(), "EXCESS_CANCELLED", tid)
            )
            net_pnl += pnl
            log.info(f"  CANCELLED {sym} @ Rs.{last:.2f}  P&L Rs.{pnl:+.2f}  (score:{score})")
        con.execute(
            "UPDATE weekly_stats SET pnl=pnl+? WHERE week_start=?", (net_pnl, ws)
        )
        con.commit()
        log.info(f"  Enforced MAX_OPEN={MAX_OPEN}: kept top {MAX_OPEN}, "
                 f"cancelled {len(excess)}  net P&L Rs.{net_pnl:+.2f}")

    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n== Cycle #{cycle} — {datetime.now().strftime('%H:%M:%S %d-%b')} ==")

        # Repair NULL qty from legacy trades before managing positions
        fixed = con.execute("UPDATE trades SET qty=1 WHERE qty IS NULL AND status='open'").rowcount
        con.commit()
        if fixed:
            log.info(f"  Repaired {fixed} NULL qty trades")

        # Manage open positions — ONLY when market is open. No trading of any
        # kind after hours (after-hours prices are stale/nan and must never
        # trigger an exit or any order).
        if is_market_open():
            log.info("-- Position management")
            manage_positions(con)
        else:
            log.info("-- Market closed — no position management, no trading")

        # Check capacity and market hours
        stats    = get_stats(con)
        open_cnt = con.execute(
            "SELECT COUNT(*) FROM trades WHERE status='open'"
        ).fetchone()[0]

        if not is_market_open():
            log.info(f"  NSE closed — opens in {fmt_time(secs_to_open())}")
        elif stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning("  Weekly risk limit reached — no new trades")
        elif open_cnt >= MAX_OPEN:
            log.info(f"  Portfolio full ({open_cnt}/{MAX_OPEN}) — monitoring only")
        else:
            slots = MAX_OPEN - open_cnt
            log.info(f"-- Market scan ({slots} slot{'s' if slots > 1 else ''} open)")
            # INSTANT REPLACEMENT: try screener cache first (no yfinance calls)
            # This fills a newly-opened slot in seconds, not 8 minutes
            filled = quick_replace(con, slots)
            if filled < slots:
                # Cache didn't have enough — do full universe scan for remaining slots
                universe = fetch_universe()
                scan_and_trade(universe, con)
            else:
                log.info(f"  Instant replacement: filled {filled} slot(s) from cache")

        # Summary
        stats    = get_stats(con)
        open_cnt = con.execute(
            "SELECT COUNT(*) FROM trades WHERE status='open'"
        ).fetchone()[0]
        total = stats["wins"] + stats["losses"]
        wr    = round(stats["wins"] / total * 100) if total else 0
        log.info(
            f"\n  P&L: Rs.{stats['pnl']:+.0f}  "
            f"Risk: Rs.{stats['risk_used']:.0f}/Rs.{MAX_WEEKLY_RISK}  "
            f"W/L: {stats['wins']}/{stats['losses']} ({wr}%)  "
            f"Open: {open_cnt}/{MAX_OPEN}"
        )
        log.info(f"  Sleeping {SCAN_INTERVAL}s\n")
        time.sleep(SCAN_INTERVAL)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    start_dashboard()

    # Self keep-alive — prevents Railway from sleeping the container
    def _keepalive():
        import time as _t
        _t.sleep(30)
        port = int(os.environ.get("PORT", 8080))
        while True:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=5)
            except Exception:
                pass
            _t.sleep(240)

    threading.Thread(target=_keepalive, daemon=True).start()
    run()
