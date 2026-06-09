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

import time, sqlite3, os, logging, io
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
MAX_OPEN         = 5           # hard cap on concurrent positions
TOP_N            = 5           # take top N setups by score each scan
SCAN_INTERVAL    = 300         # seconds between cycles
BATCH_SIZE       = 50
BATCH_PAUSE      = 1

# Quality gates
MIN_PRICE        = 50
MAX_PRICE        = 50_000
MIN_AVG_VOL      = 200_000
MIN_MCAP_CR      = 20_000      # Rs. Crores

# Strategy parameters
RSI_PERIOD       = 14
WEEKLY_RSI_MIN   = 60          # entry: weekly RSI must be above this
DAILY_RSI_MIN    = 60          # entry: daily RSI must be above this
DAILY_RSI_EXIT   = 50          # exit: daily RSI drops below this
WEEKLY_RSI_EXIT  = 55          # exit: weekly RSI drops below this
ATR_PERIOD       = 14
ATR_STOP_MULT    = 2.0         # stop  = entry - 2*ATR
ATR_TARGET_MULT  = 3.0         # target = entry + 3*ATR  → R:R = 1.5
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
    return con

# ── MARKET HOURS ─────────────────────────────────────────────────────────────
def ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

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
                    elif float(d_rsi[-1]) <= DAILY_RSI_MIN:
                        reject = f"daily_rsi_low({float(d_rsi[-1]):.0f})"
                    else:
                        drsi_val = float(d_rsi[-1])
                        wrsi_val = weekly_rsi(sym)
                        if wrsi_val <= WEEKLY_RSI_MIN:
                            reject = f"weekly_rsi_low({wrsi_val:.0f})"
                        else:
                            atr_val = atr(h, lo, c, ATR_PERIOD)
                            entry   = round(price, 2)
                            sl      = round(entry - ATR_STOP_MULT * atr_val, 2)
                            target  = round(entry + ATR_TARGET_MULT * atr_val, 2)
                            score   = round(
                                (wrsi_val  - 60) * 0.5 +
                                (drsi_val  - 60) * 0.5 +
                                min(20, (mcap / 100_000) * 5), 1
                            )
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
    for row in rows:
        t = dict(zip(cols, row))
        # Fetch live price
        try:
            hist = yf.Ticker(t["sym"] + ".NS").history(
                period="5d", interval="1d", timeout=8, auto_adjust=True)
            price = float(hist["Close"].iloc[-1]) if len(hist) > 0 else None
        except Exception:
            price = None
        if price is None:
            log.warning(f"  {t['sym']}: price fetch failed")
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
        if reason:
            pnl    = round((price - t["entry"]) * t["qty"], 2)
            status = "win" if pnl > 0 else "loss"
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
            unreal = round((price - t["entry"]) * t["qty"], 0)
            log.info(f"  HOLD {t['sym']} @ Rs.{price:.2f}  "
                     f"unreal:Rs.{unreal:+.0f}  sl:Rs.{t['sl']:.2f}  day:{days}")

# ── TWO-PASS SCAN ─────────────────────────────────────────────────────────────
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

    if not all_setups:
        log.info("  No setups found this cycle")
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
        rp  = abs(s["entry"] - s["sl"])
        if rp <= 0:
            continue
        qty = max(1, int(min(risk_left, RISK_PER_TRADE) / rp))
        actual_risk = round(qty * rp, 2)
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
    r = con.execute(
        "SELECT risk_used,wins,losses FROM weekly_stats WHERE week_start=?", (ws,)
    ).fetchone()
    # Compute P&L directly from closed trades this week — ground truth
    week_start_dt = ws + "T00:00:00"
    pnl_row = con.execute(
        "SELECT COALESCE(SUM(pnl),0) FROM trades "
        "WHERE status IN ('win','loss','cancelled') "
        "AND closed_at >= ?", (week_start_dt,)
    ).fetchone()
    real_pnl = float(pnl_row[0]) if pnl_row and pnl_row[0] is not None else 0.0
    return {
        "pnl":       real_pnl,
        "risk_used": float(r[0] or 0),
        "wins":      int(r[1] or 0),
        "losses":    int(r[2] or 0),
    }

# ── DASHBOARD ─────────────────────────────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>First-Orbit Trader PRO</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#080b0f;color:#a8b4c0;font-family:'IBM Plex Mono',monospace;font-size:12px;min-height:100vh}
/* top bar */
.bar{background:#0d1520;border-bottom:1px solid #162030;padding:9px 18px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:99}
.logo{color:#00e676;font-weight:600;letter-spacing:3px;font-size:13px}
.bar-r{display:flex;align-items:center;gap:14px;font-size:10px;color:#3a5060}
.ping{width:6px;height:6px;border-radius:50%;background:#00e676;display:inline-block;margin-right:5px;animation:blink 2s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.15}}
/* market banner */
.mkt{padding:10px 18px;border-bottom:1px solid #162030;display:flex;align-items:center;justify-content:space-between;transition:background .4s}
.mkt.open{background:#031a0d}.mkt.closed{background:#0e0a02}.mkt.pre{background:#030e1a}
.mkt-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;margin-right:12px}
.mkt.open .mkt-dot{background:#00e676;box-shadow:0 0 8px #00e676;animation:blink 1.5s infinite}
.mkt.closed .mkt-dot{background:#ff5252}
.mkt.pre .mkt-dot{background:#ffab40;animation:blink 2s infinite}
.mkt-label{font-size:13px;font-weight:600}
.mkt.open .mkt-label{color:#00e676}.mkt.closed .mkt-label{color:#ff5252}.mkt.pre .mkt-label{color:#ffab40}
.mkt-sub{font-size:10px;color:#3a5060;margin-top:1px}
.mkt-cd{font-size:26px;font-weight:600;color:#c8d8e8;letter-spacing:3px;text-align:right;font-variant-numeric:tabular-nums}
.mkt-cd-lbl{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;text-align:right;margin-top:2px}
.mkt-prog-wrap{height:3px;background:#0d1520;border-radius:2px;overflow:hidden;margin-top:7px;width:100%}
.mkt-prog{height:100%;border-radius:2px;transition:width 1s linear}
.mkt.open .mkt-prog{background:#00e676}.mkt.pre .mkt-prog{background:#ffab40}.mkt.closed .mkt-prog{background:#ff5252;width:0%!important}
/* strategy strip */
.strat{background:#0a0f15;border-bottom:1px solid #162030;padding:7px 18px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.strat-lbl{font-size:9px;color:#2a5060;letter-spacing:2px;text-transform:uppercase;white-space:nowrap}
.badge{padding:2px 9px;border-radius:3px;font-size:10px;font-weight:600;letter-spacing:1px;white-space:nowrap}
.b-g{background:#031a0d;color:#00e676;border:1px solid #0d3a1a}
.b-d{background:#0d1520;color:#8a9aaa;border:1px solid #1e2d3d}
/* metrics */
.metrics{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:#162030}
@media(max-width:640px){.metrics{grid-template-columns:repeat(2,1fr)}}
.met{background:#0d1520;padding:12px 14px}
.ml{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px}
.mv{font-size:20px;font-weight:600;color:#c8d8e8}
.mv.g{color:#00e676}.mv.r{color:#ff5252}.mv.a{color:#ffab40}
.ms{font-size:10px;color:#2a4050;margin-top:2px}
/* body grid */
.body{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#162030;margin-top:1px}
@media(max-width:680px){.body{grid-template-columns:1fr}}
.panel{background:#080b0f;padding:14px}
.pt{font-size:9px;color:#2a5060;letter-spacing:2px;text-transform:uppercase;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}
.pt-tag{font-size:9px;font-weight:600;padding:2px 8px;border-radius:3px}
.tag-full{background:#2a0808;color:#ff5252;border:1px solid #4a1010}
.tag-open{background:#1a1000;color:#ffab40;border:1px solid #3a2800}
.tag-closed{background:#001a08;color:#00e676;border:1px solid #003a15}
/* trade cards */
.tc{background:#0d1520;border-radius:6px;padding:10px 12px;margin-bottom:6px;border-left:3px solid #1e2d3d}
.tc.open{border-left-color:#ffab40}.tc.win{border-left-color:#00e676}.tc.loss{border-left-color:#ff5252}.tc.cancelled{border-left-color:#3a4a5a}
.tc-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}
.tc-sym{font-weight:600;color:#c8d8e8;font-size:12px}
.tc-pnl{font-weight:600;font-size:13px}
.pnl-pos{color:#00e676}.pnl-neg{color:#ff5252}.pnl-zero{color:#4a5a6a}
.tc-meta{color:#3a5060;line-height:1.7;font-size:11px}
/* progress bar */
.pbar-outer{position:relative;height:6px;background:#0a1520;border-radius:3px;overflow:hidden;margin-top:7px}
.pbar-fill{position:absolute;left:0;top:0;height:100%;border-radius:3px;transition:width .6s}
.pbar-entry{position:absolute;top:0;width:2px;height:100%;background:#ffab40;opacity:.8}
.pbar-labels{display:flex;justify-content:space-between;font-size:9px;color:#2a4050;margin-top:3px}
/* risk bar */
.risk-bar-outer{height:4px;background:#0d1520;border-radius:2px;overflow:hidden;margin-top:8px}
.risk-bar-fill{height:100%;border-radius:2px;transition:width .6s}
/* log */
.log-section{background:#080b0f;padding:14px;border-top:1px solid #162030;margin-top:1px}
.log-box{background:#0d1520;border-radius:6px;padding:10px 12px;font-size:10px;line-height:1.9;max-height:280px;overflow-y:auto}
.log-box::-webkit-scrollbar{width:3px}.log-box::-webkit-scrollbar-thumb{background:#1e3040}
.lg{color:#2a4050}.lb{color:#40c4ff}.lg2{color:#00e676}.lr{color:#ff5252}.la{color:#ffab40}
.empty{color:#1e3040;text-align:center;padding:28px 0;font-size:11px;line-height:1.8}
</style>
</head>
<body>

<!-- TOP BAR -->
<div class="bar">
  <div class="logo">⬡ FIRST-ORBIT TRADER PRO</div>
  <div class="bar-r">
    <span><span class="ping"></span>◉ PAPER MODE</span>
    <span id="ist-clock">--:--:-- IST</span>
    <span id="conn-dot" style="font-size:14px;color:#1a4030">●</span>
  </div>
</div>

<!-- MARKET STATUS BANNER -->
<div class="mkt closed" id="mkt-banner">
  <div style="flex:1">
    <div style="display:flex;align-items:center">
      <div class="mkt-dot" id="mkt-dot"></div>
      <div>
        <div class="mkt-label" id="mkt-label">MARKET CLOSED</div>
        <div class="mkt-sub"  id="mkt-sub">NSE Mon–Fri 09:15–15:30 IST</div>
      </div>
    </div>
    <div class="mkt-prog-wrap"><div class="mkt-prog" id="mkt-prog" style="width:0%"></div></div>
  </div>
  <div style="margin-left:24px;text-align:right">
    <div class="mkt-cd"     id="mkt-cd">--:--:--</div>
    <div class="mkt-cd-lbl" id="mkt-cd-lbl">until open</div>
  </div>
</div>

<!-- STRATEGY STRIP -->
<div class="strat">
  <span class="strat-lbl">Strategy</span>
  <span class="badge b-g">DUAL RSI MOMENTUM</span>
  <span class="badge b-d">Weekly RSI &gt; 60 · Daily RSI &gt; 60 · MCap &gt; Rs.20,000 Cr</span>
  <span class="badge b-d">Stop: Entry − 2×ATR</span>
  <span class="badge b-d">Exit: Daily RSI &lt; 50 · Weekly RSI &lt; 55 · Divergence</span>
  <span class="badge b-d">Top 5 per scan · Nifty 500</span>
</div>

<!-- METRICS -->
<div class="metrics">
  <div class="met">
    <div class="ml">Portfolio Value</div>
    <div class="mv" id="m-port">—</div>
    <div class="ms" id="m-port-s">base Rs.1,00,000</div>
  </div>
  <div class="met">
    <div class="ml">Week P&amp;L</div>
    <div class="mv g" id="m-pnl">—</div>
    <div class="ms" id="m-pnl-s">—</div>
  </div>
  <div class="met">
    <div class="ml">Win Rate</div>
    <div class="mv" id="m-wr">—</div>
    <div class="ms" id="m-wr-s">—</div>
  </div>
  <div class="met">
    <div class="ml">Risk Used</div>
    <div class="mv a" id="m-risk">—</div>
    <div class="ms" id="m-risk-s">—</div>
  </div>
  <div class="met">
    <div class="ml">Open Positions</div>
    <div class="mv a" id="m-open">—</div>
    <div class="ms">max 5 · top 5 per scan</div>
  </div>
</div>

<!-- MAIN BODY -->
<div class="body">
  <!-- Open Positions -->
  <div class="panel">
    <div class="pt">
      Open Positions
      <span class="pt-tag tag-open" id="tag-open">0 / 5</span>
    </div>
    <div id="open-list"><div class="empty">No open positions<br><small>Bot scanning for Dual RSI setups</small></div></div>
    <div style="font-size:10px;color:#2a4050;margin-top:10px" id="risk-label">Risk: Rs.0 of Rs.3,000 weekly budget (0%)</div>
    <div class="risk-bar-outer"><div class="risk-bar-fill" id="risk-bar" style="width:0%;background:#00e676"></div></div>
  </div>

  <!-- Closed Trades -->
  <div class="panel">
    <div class="pt">
      Closed Trades
      <span class="pt-tag tag-closed" id="tag-closed">0 CLOSED</span>
    </div>
    <div id="closed-list" style="max-height:420px;overflow-y:auto"><div class="empty">No closed trades yet</div></div>
  </div>
</div>

<!-- BOT LOG -->
<div class="log-section">
  <div class="pt">Bot Log <span style="font-size:9px;color:#1a3040">5s refresh</span></div>
  <div class="log-box" id="log-box"><div class="lg">Connecting...</div></div>
</div>

<script>
// ── IST helpers ───────────────────────────────────────────────────────────────
function nowIST() {
  return new Date(Date.now() + (5 * 60 + 30) * 60000);
}
function pad(n) { return String(n).padStart(2, '0'); }
function fmtSecs(s) {
  s = Math.max(0, Math.floor(s));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sc = s % 60;
  return h > 0 ? pad(h)+':'+pad(m)+':'+pad(sc) : pad(m)+':'+pad(sc);
}

// ── Market state ──────────────────────────────────────────────────────────────
function marketState() {
  const t   = nowIST();
  const day = t.getUTCDay();
  const h = t.getUTCHours(), m = t.getUTCMinutes();
  const mins = h * 60 + m;
  const OPEN = 9 * 60 + 15, CLOSE = 15 * 60 + 30, PRE = OPEN - 30;

  if (day === 0 || day === 6)
    return { state: 'closed', label: 'MARKET CLOSED', sub: 'Reopens Monday 09:15 IST' };
  if (mins < PRE)
    return { state: 'closed', label: 'MARKET CLOSED', sub: 'NSE opens 09:15 IST' };
  if (mins < OPEN)
    return { state: 'pre', label: 'PRE-OPEN', sub: 'Call auction 09:00–09:15 IST' };
  if (mins <= CLOSE)
    return { state: 'open', label: 'MARKET OPEN', sub: 'NSE live · 09:15–15:30 IST' };
  return { state: 'closed', label: 'MARKET CLOSED', sub: 'Reopens tomorrow 09:15 IST' };
}

function secsUntil(th, tm) {
  const t = nowIST();
  let s = (th - t.getUTCHours()) * 3600 + (tm - t.getUTCMinutes()) * 60 - t.getUTCSeconds();
  if (s < 0) s += 86400;
  return s;
}

function secsUntilNextWeekdayOpen() {
  const t   = nowIST();
  let   d   = new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate()));
  for (let i = 0; i < 8; i++) {
    const day  = d.getUTCDay();
    if (day >= 1 && day <= 5) {
      const target = new Date(d.getTime() + (9 * 60 + 15) * 60000);
      const diff   = (target - nowIST()) / 1000;
      if (diff > 0) return diff;
    }
    d = new Date(d.getTime() + 86400000);
  }
  return 86400;
}

// ── Clock + market banner ─────────────────────────────────────────────────────
function tickClock() {
  const t = nowIST();
  document.getElementById('ist-clock').textContent =
    pad(t.getUTCHours()) + ':' + pad(t.getUTCMinutes()) + ':' + pad(t.getUTCSeconds()) + ' IST';

  const ms  = marketState();
  const ban = document.getElementById('mkt-banner');
  ban.className = 'mkt ' + ms.state;
  document.getElementById('mkt-label').textContent = ms.label;
  document.getElementById('mkt-sub').textContent   = ms.sub;

  const t2   = nowIST();
  const mins = t2.getUTCHours() * 60 + t2.getUTCMinutes();
  let cd = 0, cdLbl = 'until open', prog = 0;

  if (ms.state === 'open') {
    cd    = secsUntil(15, 30);
    cdLbl = 'until close';
    prog  = Math.min(100, (mins - (9 * 60 + 15)) / (6 * 60 + 15) * 100);
  } else if (ms.state === 'pre') {
    cd    = secsUntil(9, 15);
    cdLbl = 'until open';
    prog  = Math.min(100, (1 - cd / 1800) * 100);
  } else {
    cd    = secsUntilNextWeekdayOpen();
    cdLbl = 'until open';
    prog  = 0;
  }

  document.getElementById('mkt-cd').textContent     = fmtSecs(cd);
  document.getElementById('mkt-cd-lbl').textContent = cdLbl;
  document.getElementById('mkt-prog').style.width   = prog.toFixed(1) + '%';
}
setInterval(tickClock, 1000);
tickClock();

// ── Log line colouring ────────────────────────────────────────────────────────
function logClass(l) {
  if (/BUY|SETUP|[+]Rs/.test(l)) return 'lg2';
  if (/ERROR|STOP|[-]Rs|CLOSED.*loss/.test(l))          return 'lr';
  if (/HOLD|WARNING|DIVERGENCE|CANCELLED/.test(l))     return 'la';
  if (/Cycle|Scan|Pass|Pass 1|Pass 2/.test(l))         return 'lb';
  return 'lg';
}

// ── Main data refresh ─────────────────────────────────────────────────────────
async function refresh() {
  let data;
  try {
    const resp = await fetch('/api/status');
    data = await resp.json();
  } catch (e) {
    document.getElementById('conn-dot').style.color = '#ff5252';
    return;
  }
  document.getElementById('conn-dot').style.color = '#00e676';

  const s      = data.stats  || {};
  const open   = data.open   || [];
  const closed = data.closed || [];
  const logs   = data.logs   || [];

  const CAP    = 100000;
  const unreal = open.reduce((sum, t) => sum + (t.unrealised || 0), 0);

  // Metrics
  const portVal = CAP + (s.pnl || 0) + unreal;
  document.getElementById('m-port').textContent   = 'Rs.' + Math.round(portVal).toLocaleString('en-IN');
  document.getElementById('m-port-s').textContent = 'base Rs.1,00,000 · unreal ' +
    (unreal >= 0 ? '+' : '') + 'Rs.' + Math.abs(Math.round(unreal)).toLocaleString('en-IN');

  const pnlEl = document.getElementById('m-pnl');
  const totalPnl = (s.pnl || 0) + unreal;
  pnlEl.textContent  = (totalPnl >= 0 ? '+Rs.' : '-Rs.') + Math.abs(Math.round(totalPnl)).toLocaleString('en-IN');
  pnlEl.className    = 'mv ' + (totalPnl >= 0 ? 'g' : 'r');
  document.getElementById('m-pnl-s').textContent =
    'closed ' + (s.pnl >= 0 ? '+' : '') + 'Rs.' + Math.round(s.pnl || 0).toLocaleString('en-IN') +
    ' · unreal ' + (unreal >= 0 ? '+' : '') + 'Rs.' + Math.abs(Math.round(unreal)).toLocaleString('en-IN');

  const tot = (s.wins || 0) + (s.losses || 0);
  document.getElementById('m-wr').textContent   = tot ? Math.round(s.wins / tot * 100) + '%' : '—';
  document.getElementById('m-wr-s').textContent = (s.wins || 0) + 'W / ' + (s.losses || 0) + 'L';

  const rPct = Math.round((s.risk_used || 0) / 3000 * 100);
  const rEl  = document.getElementById('m-risk');
  rEl.textContent  = rPct + '%';
  rEl.className    = 'mv ' + (rPct > 80 ? 'r' : rPct > 50 ? 'a' : 'a');
  document.getElementById('m-risk-s').textContent = 'Rs.' + (s.risk_used || 0) + ' / Rs.3,000';

  document.getElementById('m-open').textContent = open.length;

  // Risk bar
  const rb = document.getElementById('risk-bar');
  rb.style.width      = Math.min(100, rPct) + '%';
  rb.style.background = rPct > 80 ? '#ff5252' : rPct > 50 ? '#ffab40' : '#00e676';
  document.getElementById('risk-label').textContent =
    'Risk: Rs.' + (s.risk_used || 0) + ' of Rs.3,000 weekly budget (' + rPct + '%)';

  // Open positions badge
  const tagOpen = document.getElementById('tag-open');
  tagOpen.textContent = open.length >= 5 ? 'FULL (5/5)' : open.length + ' / 5';
  tagOpen.className   = 'pt-tag ' + (open.length >= 5 ? 'tag-full' : 'tag-open');

  // Open positions
  const openEl = document.getElementById('open-list');
  if (!open.length) {
    openEl.innerHTML = '<div class="empty">No open positions<br><small>Bot scanning for Dual RSI setups</small></div>';
  } else {
    openEl.innerHTML = open.map(t => {
      const u    = t.unrealised || 0;
      const uc   = u > 0 ? '#00e676' : u < 0 ? '#ff5252' : '#4a5a6a';
      const us   = u >= 0 ? '+' : '';
      const lp   = t.last_price || t.entry;
      const rng  = Math.abs(t.target - t.sl);
      const prog = rng > 0 ? Math.min(100, Math.max(0, (lp - t.sl) / rng * 100)) : 50;
      const ePct = rng > 0 ? Math.min(100, Math.max(0, (t.entry - t.sl) / rng * 100)) : 50;
      return `
        <div class="tc open">
          <div class="tc-top">
            <span class="tc-sym">${t.sym} <span style="font-size:9px;color:#3a5060">BUY · Day ${t.days_held}/5</span></span>
            <span class="tc-pnl" style="color:${uc}">${us}Rs.${Math.abs(u).toLocaleString('en-IN')} (${us}${t.unreal_pct || 0}%)</span>
          </div>
          <div class="tc-meta">
            Last <b style="color:#c8d8e8">Rs.${lp.toLocaleString('en-IN')}</b>
            &nbsp;·&nbsp; Entry Rs.${t.entry}
            &nbsp;·&nbsp; Qty ${t.qty || '—'}<br>
            SL <span style="color:#ff5252">Rs.${t.sl}</span>
            &nbsp;·&nbsp; TGT <span style="color:#00e676">Rs.${t.target}</span>
            &nbsp;·&nbsp; R:R ${t.rr}x
          </div>
          <div class="pbar-outer">
            <div class="pbar-fill"  style="width:${prog.toFixed(1)}%;background:${uc}"></div>
            <div class="pbar-entry" style="left:${ePct.toFixed(1)}%"></div>
          </div>
          <div class="pbar-labels">
            <span>SL Rs.${t.sl}</span>
            <span style="color:#ffab40">▲ Entry</span>
            <span>TGT Rs.${t.target}</span>
          </div>
        </div>`;
    }).join('');
  }

  // Closed trades
  const closedEl = document.getElementById('closed-list');
  document.getElementById('tag-closed').textContent = closed.length + ' CLOSED';
  if (!closed.length) {
    closedEl.innerHTML = '<div class="empty">No closed trades yet</div>';
  } else {
    closedEl.innerHTML = closed.map(t => {
      const pnl    = t.pnl || 0;
      const pClass = pnl > 0 ? 'pnl-pos' : pnl < 0 ? 'pnl-neg' : 'pnl-zero';
      const pStr   = pnl > 0 ? '+Rs.' + pnl.toLocaleString('en-IN')
                   : pnl < 0 ? '-Rs.' + Math.abs(pnl).toLocaleString('en-IN')
                   : 'Rs.0';
      const reason = (t.exit_reason || t.status || '')
        .replace('EXCESS_ON_BOOT', 'CANCELLED')
        .replace('EXCESS_CANCELLED', 'CANCELLED');
      const cls = t.status === 'win' ? 'win' : t.status === 'loss' ? 'loss' : 'cancelled';
      return `
        <div class="tc ${cls}">
          <div class="tc-top">
            <span class="tc-sym">${t.sym}</span>
            <span class="tc-pnl ${pClass}">${pStr}</span>
          </div>
          <div class="tc-meta">${reason} · Entry Rs.${t.entry}</div>
        </div>`;
    }).join('');
  }

  // Log
  const logEl = document.getElementById('log-box');
  logEl.innerHTML = logs.length
    ? logs.map(l => `<div class="${logClass(l)}">${l}</div>`).join('')
    : '<div class="lg">No log entries yet</div>';
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


def start_dashboard():
    app = Flask(__name__)

    @app.route("/")
    def index():
        return render_template_string(DASHBOARD_HTML)

    @app.route("/health")
    def health():
        return {"status": "ok"}, 200

    @app.route("/ping")
    def ping():
        return "pong", 200

    @app.route("/api/status")
    def api_status():
        try:
            con = sqlite3.connect(DB_PATH)

            # Weekly stats
            ws = (date.today() - timedelta(days=date.today().weekday())).isoformat()
            con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
            con.commit()
            row = con.execute(
                "SELECT pnl,risk_used,wins,losses FROM weekly_stats WHERE week_start=?", (ws,)
            ).fetchone()
            stats = {
                "pnl":       float(row[0] or 0),
                "risk_used": float(row[1] or 0),
                "wins":      int(row[2] or 0),
                "losses":    int(row[3] or 0),
            }

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
                        last_price = round(float(h["Close"].iloc[-1]), 2)
                    else:
                        raise ValueError("empty")
                except Exception:
                    # Fall back to screener cache
                    cr = con.execute(
                        "SELECT price FROM screener_cache WHERE sym=?", (sym,)
                    ).fetchone()
                    if cr and cr[0]:
                        last_price = round(float(cr[0]), 2)
                qty     = qty or 1
                unreal  = round((last_price - entry) * qty, 2)
                upct    = round((last_price - entry) / entry * 100, 2) if entry else 0
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

                # Recalculate P&L for cancelled trades
                if exit_reason and "EXCESS" in exit_reason:
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

            return jsonify({
                "stats":  stats,
                "open":   open_list,
                "closed": closed_list,
                "logs":   logs,
                "time":   datetime.now().strftime("%H:%M:%S"),
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

        # Manage open positions
        log.info("-- Position management")
        manage_positions(con)

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
            universe = fetch_universe()
            scan_and_trade(universe, con)

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
