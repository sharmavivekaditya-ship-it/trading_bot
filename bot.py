"""
First-Orbit Trader PRO
NSE Swing Trading Bot — Pure Algo, No AI API

STRATEGY: Dual RSI Momentum + Market Cap Filter
  Entry:  Weekly RSI(14) > 60 AND Daily RSI(14) > 60 AND MCap > Rs.20000 Cr
  Stop:   Entry - 2.0 x ATR (hard floor)
  Target: Entry + 3.0 x ATR
  Exit:   Daily RSI < 50 | Weekly RSI < 55 | Bearish divergence (min 2 days held) | Hard stop
  Size:   qty = Rs.800 / (entry - stop)
  Scan:   Two-pass — collect ALL setups, rank by score, trade TOP 5 only
"""

import time, json, sqlite3, os, logging, io, math
from datetime import datetime, date, timedelta
import urllib.request

# ── DB PATH (Railway volume at /data, else local) ─────────────────────────────
DB_PATH = os.environ.get("DB_PATH",
    "/data/trades.db" if os.path.isdir("/data") else "trades.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.FileHandler(
        "/data/claudebot.log" if os.path.isdir("/data") else "claudebot.log"
    ), logging.StreamHandler()]
)
log = logging.getLogger("bot")
LOG_PATH = "/data/claudebot.log" if os.path.isdir("/data") else "claudebot.log"

# ── INSTALL DEPS ──────────────────────────────────────────────────────────────
def ensure_deps():
    import importlib, subprocess, sys
    for pkg in ["yfinance", "curl_cffi", "pandas", "numpy", "flask"]:
        try: importlib.import_module(pkg.replace("-","_"))
        except ImportError:
            log.info(f"pip install {pkg}...")
            subprocess.check_call([sys.executable,"-m","pip","install",pkg,"-q"])
ensure_deps()

import yfinance as yf
import pandas as pd
import numpy as np

# ── CONFIG ────────────────────────────────────────────────────────────────────
CAPITAL          = 100_000
MAX_WEEKLY_RISK  = 3_000
RISK_PER_TRADE   = 800
SCAN_INTERVAL    = 300
BATCH_SIZE       = 50
BATCH_PAUSE      = 1
TOP_N            = 5          # trade top 5 setups by score
MAX_OPEN         = 5          # hard cap — stop scanning when 5 positions open

MIN_PRICE        = 50
MAX_PRICE        = 50_000
MIN_AVG_VOL      = 200_000
MIN_MCAP_CR      = 20_000

RSI_PERIOD       = 14
WEEKLY_RSI_ENTRY = 60
DAILY_RSI_ENTRY  = 60
DAILY_RSI_EXIT   = 50
WEEKLY_RSI_EXIT  = 55
ATR_PERIOD       = 14
ATR_STOP_MULT    = 2.0
ATR_TARGET_MULT  = 3.0
DIVERGENCE_LOOKBACK = 10
MIN_DAYS_FOR_DIV    = 2

NIFTY500_CSV = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","ITC","LT","AXISBANK","ASIANPAINT","MARUTI","SUNPHARMA","TATAMOTORS",
    "ULTRACEMCO","WIPRO","NESTLEIND","POWERGRID","NTPC","TECHM","HCLTECH","BAJFINANCE",
    "BAJAJFINSV","TITAN","ADANIPORTS","ONGC","DIVISLAB","DRREDDY","CIPLA","COALINDIA",
    "JSWSTEEL","TATASTEEL","INDUSINDBK","HINDALCO","BPCL","GRASIM","BRITANNIA",
    "EICHERMOT","HEROMOTOCO","M&M","APOLLOHOSP","TATACONSUM","DABUR","PIDILITIND",
    "BERGEPAINT","LUPIN","TORNTPHARM","MUTHOOTFIN","CHOLAFIN","SBILIFE","HDFCLIFE",
    "ICICIGI","BANDHANBNK","FEDERALBNK","IDFCFIRSTB","PNB","CANBK","BANKBARODA",
    "PERSISTENT","LTIM","MPHASIS","COFORGE","ZOMATO","IRCTC","TATAPOWER","PFC",
    "RECLTD","BEL","HAL","BHEL","SAIL","NMDC","VEDL","POLYCAB","HAVELLS","VOLTAS",
    "ABB","SIEMENS","CUMMINSIND","DIXON","TATAELXSI","KPITTECH","CYIENT","LTTS",
    "APOLLOTYRE","MRF","CEAT","BAJAJ-AUTO","TVSMOTOR","ASHOKLEY","ALKEM","AUROPHARMA",
    "IPCA","NATCOPHARM","GRANULES","AJANTPHARM","LAURUS","DIVI","BIOCON","STRIDES",
]

# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY, sym TEXT, direction TEXT,
        entry REAL, sl REAL, target REAL, trail_sl REAL,
        qty INTEGER, risk_amt REAL, target_gain REAL, rr REAL,
        status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
        score REAL DEFAULT 0, days_held INTEGER DEFAULT 0,
        opened_at TEXT, closed_at TEXT, exit_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        pnl REAL DEFAULT 0, risk_used REAL DEFAULT 0,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        time_exits INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS screener_cache (
        sym TEXT PRIMARY KEY, score REAL, rsi REAL, rsi_prev REAL,
        vol_ratio REAL, atr_pct REAL, price REAL, ema20 REAL, ema50 REAL,
        entry REAL, sl REAL, target REAL, rr REAL,
        reject_reason TEXT, updated_at TEXT
    );
    """)
    con.commit()
    return con

# ── MARKET HOURS ─────────────────────────────────────────────────────────────
def ist_now():
    return datetime.now(tz=__import__("datetime").timezone.utc).replace(tzinfo=None) + timedelta(hours=5, minutes=30)

def is_market_open():
    t = ist_now()
    if t.weekday() >= 5: return False
    o = t.replace(hour=9, minute=15, second=0, microsecond=0)
    c = t.replace(hour=15, minute=30, second=0, microsecond=0)
    return o <= t <= c

def time_to_open():
    t = ist_now()
    c = t.replace(hour=9, minute=15, second=0, microsecond=0)
    if t >= c: c += timedelta(days=1)
    while c.weekday() >= 5: c += timedelta(days=1)
    s = int((c - t).total_seconds())
    return f"{s//3600}h {(s%3600)//60}m"

# ── UNIVERSE ──────────────────────────────────────────────────────────────────
def fetch_universe():
    try:
        req = urllib.request.Request(NIFTY500_CSV, headers={"User-Agent":"Mozilla/5.0","Referer":"https://nseindia.com"})
        with urllib.request.urlopen(req, timeout=15) as r:
            df = pd.read_csv(io.StringIO(r.read().decode("latin-1")))
        col = [c for c in df.columns if "symbol" in c.lower()][0]
        syms = [s for s in df[col].dropna().str.strip().tolist()
                if s and not s.upper().startswith("DUMMY") and len(s) <= 20]
        log.info(f"Nifty 500 universe: {len(syms)} symbols")
        return syms
    except Exception as e:
        log.warning(f"Nifty500 CSV failed ({e}) — using fallback {len(FALLBACK)} symbols")
        return FALLBACK

# ── INDICATORS ────────────────────────────────────────────────────────────────
def calc_rsi(close, period=14):
    close = close.astype(float)
    delta = np.diff(close)
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    ag, al = gain[:period].mean(), loss[:period].mean()
    rsi_vals = []
    for i in range(period, len(gain)):
        ag = (ag*(period-1) + gain[i]) / period
        al = (al*(period-1) + loss[i]) / period
        rsi_vals.append(100 - 100/(1 + ag/al) if al else 100.0)
    return rsi_vals

def calc_ema(close, span):
    return float(pd.Series(close).ewm(span=span, adjust=False).mean().iloc[-1])

def calc_atr(high, low, close, period=14):
    h, l, c = high[1:], low[1:], close[:-1]
    tr = np.maximum(h-l, np.maximum(abs(h-c), abs(l-c)))
    return float(tr[-period:].mean())

def get_mcap_cr(sym):
    try:
        info = yf.Ticker(sym+".NS").fast_info
        mcap = getattr(info, "market_cap", None) or 0
        return mcap / 1e7
    except Exception:
        return 0

def calc_weekly_rsi(sym, period=14):
    try:
        df = yf.Ticker(sym+".NS").history(period="2y", interval="1wk", timeout=12, auto_adjust=True)
        if df is None or len(df) < period + 5: return 0.0, []
        rsi = calc_rsi(df["Close"].values, period)
        return (float(rsi[-1]) if rsi else 0.0), rsi
    except Exception:
        return 0.0, []

# ── SCREENER ──────────────────────────────────────────────────────────────────
def screen(sym, cache_cutoff, con):
    cached = con.execute(
        "SELECT score,rsi,rsi_prev,vol_ratio,atr_pct,price,ema20,ema50,entry,sl,target,rr,reject_reason "
        "FROM screener_cache WHERE sym=? AND updated_at>?", (sym, cache_cutoff)
    ).fetchone()
    if cached:
        score,rsi_n,rsi_p,vol_r,atr_p,price,e20,e50,entry,sl,tgt,rr,rej = cached
        if entry:
            return {"sym":sym,"score":score,"rsi":rsi_n,"rsi_prev":rsi_p,"vol_ratio":vol_r,
                    "atr_pct":atr_p,"price":price,"ema20":e20,"ema50":e50,
                    "entry":entry,"sl":sl,"target":tgt,"rr":rr}, None
        return None, f"cached:{rej}"

    now, reject = datetime.now().isoformat(), None
    try:
        df = yf.Ticker(sym+".NS").history(period="90d", interval="1d", timeout=12, auto_adjust=True)
        if df is None or len(df) < RSI_PERIOD + 5:
            reject = "insufficient_data"
        else:
            c = df["Close"].values.astype(float)
            h = df["High"].values.astype(float)
            l = df["Low"].values.astype(float)
            v = df["Volume"].values.astype(float)
            price   = float(c[-1])
            avg_vol = float(v[-20:].mean())
            vol_r   = float(v[-1]) / avg_vol if avg_vol else 0

            if   price < MIN_PRICE:    reject = f"price_low(Rs.{price:.0f})"
            elif price > MAX_PRICE:    reject = f"price_high(Rs.{price:.0f})"
            elif avg_vol < MIN_AVG_VOL: reject = f"low_vol({int(avg_vol):,})"
            else:
                mcap = get_mcap_cr(sym)
                if mcap < MIN_MCAP_CR:
                    reject = f"small_cap(Rs.{mcap:.0f}Cr)"
                else:
                    d_rsi = calc_rsi(c, RSI_PERIOD)
                    if not d_rsi:
                        reject = "rsi_error"
                    elif float(d_rsi[-1]) <= DAILY_RSI_ENTRY:
                        reject = f"daily_rsi_low({float(d_rsi[-1]):.1f})"
                    else:
                        daily_rsi = float(d_rsi[-1])
                        weekly_rsi, _ = calc_weekly_rsi(sym)
                        if weekly_rsi <= WEEKLY_RSI_ENTRY:
                            reject = f"weekly_rsi_low({weekly_rsi:.1f})"
                        else:
                            entry  = round(price, 2)
                            atr    = calc_atr(h, l, c, ATR_PERIOD)
                            sl     = round(entry - ATR_STOP_MULT * atr, 2)
                            target = round(entry + ATR_TARGET_MULT * atr, 2)
                            rr     = round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)
                            atr_pct = atr / price * 100
                            ema20  = calc_ema(c, 20)
                            ema50  = calc_ema(c, 50)
                            score  = round(
                                (weekly_rsi - 60) * 0.5 +
                                (daily_rsi  - 60) * 0.5 +
                                min(20, (mcap / 100_000) * 5), 1
                            )
                            con.execute("""INSERT OR REPLACE INTO screener_cache
                                (sym,score,rsi,rsi_prev,vol_ratio,atr_pct,price,ema20,ema50,
                                 entry,sl,target,rr,reject_reason,updated_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)""",
                                (sym,score,round(daily_rsi,1),round(weekly_rsi,1),
                                 round(vol_r,2),round(atr_pct,2),price,
                                 round(ema20,2),round(ema50,2),entry,sl,target,rr,now))
                            con.commit()
                            return {"sym":sym,"score":score,"rsi":round(daily_rsi,1),
                                    "rsi_prev":round(weekly_rsi,1),"vol_ratio":round(vol_r,2),
                                    "atr_pct":round(atr_pct,2),"price":price,
                                    "ema20":round(ema20,2),"ema50":round(ema50,2),
                                    "entry":entry,"sl":sl,"target":target,"rr":rr,
                                    "mcap_cr":round(mcap,0)}, None
    except Exception as e:
        reject = f"error:{str(e)[:40]}"

    con.execute("""INSERT OR REPLACE INTO screener_cache
        (sym,score,rsi,rsi_prev,vol_ratio,atr_pct,price,ema20,ema50,
         entry,sl,target,rr,reject_reason,updated_at)
        VALUES (?,0,0,0,0,0,0,0,0,NULL,NULL,NULL,NULL,?,?)""", (sym,reject,now))
    return None, reject

# ── DIVERGENCE ────────────────────────────────────────────────────────────────
def check_rsi_divergence(sym, lookback=10):
    try:
        df = yf.Ticker(sym+".NS").history(period="30d", interval="1d", timeout=8, auto_adjust=True)
        if len(df) < lookback + 5: return False, "insufficient"
        c = df["Close"].values.astype(float)
        rsi = calc_rsi(c, RSI_PERIOD)
        if len(rsi) < lookback: return False, "rsi short"
        price_now  = c[-1]
        price_prev = min(c[-(lookback+1):-1])
        rsi_now    = rsi[-1]
        rsi_high   = max(rsi[-(lookback+1):-1])
        if price_now >= price_prev * 1.02 and rsi_now < rsi_high * 0.95:
            return True, f"price+{((price_now/price_prev-1)*100):.1f}% rsi-{(rsi_high-rsi_now):.1f}pts"
        return False, "no divergence"
    except Exception as e:
        return False, str(e)[:30]

# ── POSITION MANAGEMENT ───────────────────────────────────────────────────────
def manage_positions(con):
    cols = ["id","sym","direction","entry","sl","target","trail_sl","qty",
            "risk_amt","target_gain","rr","status","pnl","score","days_held",
            "opened_at","closed_at","exit_reason"]
    rows = con.execute("SELECT * FROM trades WHERE status='open'").fetchall()
    if not rows:
        log.info("  No open positions")
        return
    for row in rows:
        t = dict(zip(cols, row))
        try:
            hist = yf.Ticker(t["sym"]+".NS").history(period="5d",interval="1d",timeout=8,auto_adjust=True)
            price = float(hist["Close"].iloc[-1]) if len(hist) > 0 else None
        except Exception:
            price = None
        if price is None:
            log.warning(f"  {t['sym']}: price fetch failed")
            continue
        try:
            days = (datetime.now() - datetime.fromisoformat(t["opened_at"])).days
        except Exception:
            days = 0
        con.execute("UPDATE trades SET days_held=? WHERE id=?", (days, t["id"]))
        exit_reason = None
        if price <= t["sl"]:
            exit_reason = "HARD_STOP"
        if not exit_reason:
            try:
                df = yf.Ticker(t["sym"]+".NS").history(period="45d",interval="1d",timeout=8,auto_adjust=True)
                if len(df) >= RSI_PERIOD + 2:
                    d_rsi = calc_rsi(df["Close"].values, RSI_PERIOD)
                    if d_rsi and float(d_rsi[-1]) < DAILY_RSI_EXIT:
                        exit_reason = f"DAILY_RSI_EXIT({float(d_rsi[-1]):.1f})"
            except Exception:
                pass
        if not exit_reason:
            try:
                wr, _ = calc_weekly_rsi(t["sym"])
                if wr > 0 and wr < WEEKLY_RSI_EXIT:
                    exit_reason = f"WEEKLY_RSI_EXIT({wr:.1f})"
            except Exception:
                pass
        if not exit_reason and days >= MIN_DAYS_FOR_DIV:
            div, desc = check_rsi_divergence(t["sym"], DIVERGENCE_LOOKBACK)
            if div:
                exit_reason = f"DIVERGENCE({desc})"
        if exit_reason:
            pnl = round((price - t["entry"]) * t["qty"], 2)
            status = "win" if pnl > 0 else "loss"
            con.execute("UPDATE trades SET status=?,pnl=?,closed_at=?,exit_reason=? WHERE id=?",
                        (status, pnl, datetime.now().isoformat(), exit_reason, t["id"]))
            con.commit()
            upd_stats(con, pnl=pnl, risk_used=abs(pnl) if pnl<0 else 0,
                      wins=1 if pnl>0 else 0, losses=0 if pnl>0 else 1, time_exits=0)
            log.info(f"  CLOSED {t['sym']} [{exit_reason}] @ Rs.{price:.2f} P&L Rs.{pnl:+.2f}")
        else:
            pnl_u = round((price - t["entry"]) * t["qty"], 0)
            log.info(f"  HOLD {t['sym']} @ Rs.{price:.2f} unrealised:Rs.{pnl_u:+.0f} sl:Rs.{t['sl']:.2f} day:{days}")

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
def execute_order(setup, con, risk_left):
    rp = abs(setup["entry"] - setup["sl"])
    if rp <= 0: return False
    qty = max(1, int(min(risk_left, RISK_PER_TRADE) / rp))
    con.execute("""INSERT INTO trades
        (id,sym,direction,entry,sl,target,trail_sl,qty,risk_amt,target_gain,rr,
         score,days_held,opened_at,exit_reason)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)""",
        (f"{setup['sym']}_{int(time.time())}", setup["sym"], "BUY",
         setup["entry"], setup["sl"], setup["target"], setup["sl"],
         qty, round(qty*rp,2), round(qty*abs(setup["entry"]-setup["target"]),2),
         setup["rr"], setup["score"], datetime.now().isoformat(), ""))
    con.commit()
    log.info(f"  BUY {setup['sym']} qty:{qty} @ Rs.{setup['entry']} "
             f"SL:Rs.{setup['sl']} TGT:Rs.{setup['target']} "
             f"R:R:{setup['rr']}x risk:Rs.{round(qty*rp,2)} score:{setup['score']}")
    return True

# ── TWO-PASS SCAN ─────────────────────────────────────────────────────────────
def scan_and_trade(universe, con, risk_left):
    cache_cutoff  = (datetime.now()-timedelta(hours=4)).isoformat()
    reject_counts = {}
    all_setups    = []
    total         = len(universe)

    log.info(f"  Pass 1: scanning {total} stocks...")
    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i+BATCH_SIZE]
        for sym in batch:
            if con.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'",(sym,)).fetchone():
                continue
            setup, reason = screen(sym, cache_cutoff, con)
            if setup:
                all_setups.append(setup)
                log.info(f"  SETUP {sym} wRSI:{setup['rsi_prev']} dRSI:{setup['rsi']} "
                         f"mcap:Rs.{setup.get('mcap_cr',0):.0f}Cr score:{setup['score']}")
            else:
                key = (reason or "unknown").replace("cached:","").split("(")[0]
                reject_counts[key] = reject_counts.get(key, 0) + 1
        done = min(i+BATCH_SIZE, total)
        top_r = " | ".join(f"{k}:{v}" for k,v in sorted(reject_counts.items(),key=lambda x:-x[1])[:4])
        log.info(f"  Scanned {done}/{total} — {len(all_setups)} setups | {top_r}")
        if i+BATCH_SIZE < total:
            time.sleep(BATCH_PAUSE)

    log.info("  ── Rejections ──")
    for k,v in sorted(reject_counts.items(), key=lambda x:-x[1]):
        log.info(f"     {k:<35} {v:>5}")

    if not all_setups:
        log.info("  No setups this cycle — market not in momentum zone")
        return 0

    all_setups.sort(key=lambda x: x["score"], reverse=True)
    top5 = all_setups[:TOP_N]
    log.info(f"\n  Pass 2: top {len(top5)} of {len(all_setups)} setups:")
    log.info(f"  {'#':<3} {'SYM':<14} {'SCORE':>6} {'wRSI':>6} {'dRSI':>6} {'MCAP':>10} {'ENTRY':>8}")
    log.info("  " + "-"*60)
    for i, s in enumerate(top5, 1):
        log.info(f"  {i:<3} {s['sym']:<14} {s['score']:>6} {s['rsi_prev']:>6} {s['rsi']:>6} "
                 f"{s.get('mcap_cr',0):>10.0f} Rs.{s['entry']:>7.2f}")

    placed = 0
    for s in top5:
        # Hard stop — never exceed MAX_OPEN positions
        current_open = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        if current_open >= MAX_OPEN:
            log.info(f"  Portfolio full ({current_open}/{MAX_OPEN}) — stopping")
            break
        if risk_left <= 0:
            log.info("  Risk budget exhausted")
            break
        if con.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'",(s["sym"],)).fetchone():
            log.info(f"  SKIP {s['sym']} — already open")
            continue
        if execute_order(s, con, risk_left):
            rp = abs(s["entry"]-s["sl"])
            qty = max(1, int(min(risk_left, RISK_PER_TRADE)/max(0.01,rp)))
            risk_left -= min(RISK_PER_TRADE, qty * rp)
            placed += 1
    log.info(f"  {placed} orders placed | portfolio now {con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]}/{MAX_OPEN}")
    return placed

# ── STATS ─────────────────────────────────────────────────────────────────────
def week_start():
    t = date.today()
    return (t - timedelta(days=t.weekday())).isoformat()

def get_stats(con):
    ws = week_start()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    con.commit()
    r = con.execute("SELECT pnl,risk_used,wins,losses,time_exits FROM weekly_stats WHERE week_start=?",(ws,)).fetchone()
    return {"pnl":r[0],"risk_used":r[1],"wins":r[2],"losses":r[3],"time_exits":r[4]}

def upd_stats(con, **kw):
    ws = week_start()
    sets = ",".join(f"{k}={k}+?" for k in kw)
    con.execute(f"UPDATE weekly_stats SET {sets} WHERE week_start=?",(*kw.values(),ws))
    con.commit()

# ── DASHBOARD ─────────────────────────────────────────────────────────────────
def start_dashboard():
    from flask import Flask, jsonify, render_template_string
    import threading

    app = Flask(__name__)

    HTML = """<!DOCTYPE html>
<html><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>First-Orbit Trader PRO</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#080b0f;color:#a8b4c0;font-family:'IBM Plex Mono',monospace;font-size:12px}
.bar{background:#0d1520;border-bottom:1px solid #162030;padding:9px 18px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:99}
.logo{color:#00e676;font-weight:600;letter-spacing:3px;font-size:13px}
.bar-r{display:flex;align-items:center;gap:14px;font-size:10px;color:#3a5060}
.ping{width:6px;height:6px;border-radius:50%;background:#00e676;display:inline-block;margin-right:5px;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.15}}
.mkt{display:flex;align-items:center;justify-content:space-between;padding:10px 18px;border-bottom:1px solid #162030;transition:background .5s}
.mkt.open{background:#031a0d}.mkt.closed{background:#0e0a02}.mkt.pre{background:#030e1a}
.mkt-left{display:flex;align-items:center;gap:12px}
.mkt-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.mkt.open .mkt-dot{background:#00e676;box-shadow:0 0 8px #00e676;animation:pulse 1.5s infinite}
.mkt.closed .mkt-dot{background:#ff5252}.mkt.pre .mkt-dot{background:#ffab40;animation:pulse 2s infinite}
.mkt-status{font-size:13px;font-weight:600}
.mkt.open .mkt-status{color:#00e676}.mkt.closed .mkt-status{color:#ff5252}.mkt.pre .mkt-status{color:#ffab40}
.mkt-sub{font-size:10px;color:#3a5060;margin-top:1px}
.mkt-cd{font-size:22px;font-weight:600;color:#c8d8e8;letter-spacing:2px;text-align:right}
.mkt-cd-label{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;text-align:right;margin-top:2px}
.mkt-prog{height:3px;background:#0d1520;border-radius:2px;overflow:hidden;margin-top:8px}
.mkt-prog-fill{height:100%;border-radius:2px;transition:width 1s linear}
.mkt.open .mkt-prog-fill{background:#00e676}.mkt.closed .mkt-prog-fill{background:#ff5252}.mkt.pre .mkt-prog-fill{background:#ffab40}
.strat{background:#0a0f15;border-bottom:1px solid #162030;padding:8px 18px;display:flex;flex-wrap:wrap;gap:12px;align-items:center}
.strat-label{font-size:9px;color:#2a5060;letter-spacing:2px;text-transform:uppercase}
.badge{padding:2px 8px;border-radius:3px;font-size:10px;font-weight:600;letter-spacing:1px}
.b-green{background:#031a0d;color:#00e676;border:1px solid #0d3a1a}
.b-gray{background:#0d1520;color:#a8b4c0;border:1px solid #1e2d3d}
.metrics{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:#162030}
@media(max-width:600px){.metrics{grid-template-columns:repeat(2,1fr)}}
.met{background:#0d1520;padding:12px 14px}
.ml{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px}
.mv{font-size:20px;font-weight:600;color:#c8d8e8}
.mv.g{color:#00e676}.mv.r{color:#ff5252}.mv.a{color:#ffab40}
.ms{font-size:10px;color:#2a4050;margin-top:2px}
.body{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#162030;margin-top:1px}
@media(max-width:600px){.body{grid-template-columns:1fr}}
.panel{background:#080b0f;padding:14px}
.pt{font-size:9px;color:#2a5060;letter-spacing:2px;text-transform:uppercase;margin-bottom:10px;display:flex;justify-content:space-between}
.tc{background:#0d1520;border-radius:6px;padding:10px 12px;margin-bottom:5px;border-left:3px solid #1e2d3d;font-size:11px}
.tc.open{border-left-color:#ffab40}.tc.win{border-left-color:#00e676}.tc.loss{border-left-color:#ff5252}
.tr{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.ts{color:#c8d8e8;font-weight:600}
.tp.p{color:#00e676}.tp.n{color:#ff5252}.tp.o{color:#ffab40}
.tm{color:#3a5060;line-height:1.7}
.pbar{height:3px;background:#0a1520;border-radius:2px;overflow:hidden;margin-top:6px}
.pbar-fill{height:100%;border-radius:2px;transition:width .5s}
.rb{height:4px;background:#0d1520;border-radius:2px;overflow:hidden;margin-top:8px}
.rf{height:100%;border-radius:2px;transition:width .6s}
.full{background:#080b0f;padding:14px;border-top:1px solid #162030;margin-top:1px}
.log{background:#0d1520;border-radius:6px;padding:10px;font-size:10px;line-height:1.9;max-height:280px;overflow-y:auto}
.log::-webkit-scrollbar{width:3px}.log::-webkit-scrollbar-thumb{background:#1e3040}
.g{color:#00e676}.r{color:#ff5252}.a{color:#ffab40}.b{color:#40c4ff}.d{color:#2a4050}
.empty{color:#1e3040;text-align:center;padding:24px 0;font-size:11px}
</style></head><body>
<div class="bar">
  <div class="logo">⬡ FIRST-ORBIT TRADER PRO</div>
  <div class="bar-r">
    <span><span class="ping"></span>◉ PAPER MODE</span>
    <span id="ist">--:--:-- IST</span>
    <span id="conn" style="color:#1a4030;font-size:14px">●</span>
  </div>
</div>
<div class="mkt closed" id="mkt">
  <div style="flex:1">
    <div class="mkt-left">
      <div class="mkt-dot"></div>
      <div><div class="mkt-status" id="mkt-s">MARKET CLOSED</div><div class="mkt-sub" id="mkt-sub">NSE Mon-Fri 09:15-15:30 IST</div></div>
    </div>
    <div class="mkt-prog"><div class="mkt-prog-fill" id="mkt-prog" style="width:0%"></div></div>
  </div>
  <div style="margin-left:24px">
    <div class="mkt-cd" id="mkt-cd">--:--:--</div>
    <div class="mkt-cd-label" id="mkt-cd-l">until open</div>
  </div>
</div>
<div class="strat">
  <span class="strat-label">Strategy</span>
  <span class="badge b-green">DUAL RSI MOMENTUM</span>
  <span class="badge b-gray">Weekly RSI &gt; 60 · Daily RSI &gt; 60 · MCap &gt; Rs.20,000 Cr</span>
  <span class="badge b-gray">Stop: Entry - 2×ATR</span>
  <span class="badge b-gray">Exit: Daily RSI &lt; 50 · Weekly RSI &lt; 55 · Divergence</span>
  <span class="badge b-gray">Top 5 setups per scan · Nifty 500</span>
</div>
<div class="metrics">
  <div class="met"><div class="ml">Portfolio</div><div class="mv" id="mp">—</div><div class="ms" id="mp-s">base Rs.1,00,000</div></div>
  <div class="met"><div class="ml">Week P&L</div><div class="mv g" id="mw">—</div><div class="ms" id="mw-s">—</div></div>
  <div class="met"><div class="ml">Win Rate</div><div class="mv" id="mwr">—</div><div class="ms" id="mwr-s">—</div></div>
  <div class="met"><div class="ml">Risk Used</div><div class="mv a" id="mr">—</div><div class="ms" id="mr-s">—</div></div>
  <div class="met"><div class="ml">Open</div><div class="mv a" id="mo">—</div><div class="ms">top 5 per cycle</div></div>
</div>
<div class="body">
  <div class="panel">
    <div class="pt"><span>Open Positions</span><span id="op-b" style="font-size:9px;color:#ffab40">0 OPEN</span></div>
    <div id="op"><div class="empty">No open positions</div></div>
    <div class="rb"><div class="rf" id="rf" style="width:0%;background:#00e676"></div></div>
    <div style="font-size:10px;color:#2a4050;margin-top:5px" id="rl">Risk: Rs.0 of Rs.3,000 weekly budget</div>
  </div>
  <div class="panel">
    <div class="pt"><span>Closed Trades</span><span id="ct-b" style="font-size:9px;color:#00e676">0 CLOSED</span></div>
    <div id="ct"><div class="empty">No closed trades yet</div></div>
  </div>
</div>
<div class="full">
  <div class="pt"><span>Bot Log</span><span style="font-size:9px;color:#1a3040">live · 10s refresh</span></div>
  <div class="log" id="lg"><div class="d">connecting...</div></div>
</div>
<script>
const CAP=100000;
function nowIST(){return new Date(Date.now()+(5*60+30)*60000);}
function fmt(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sc=s%60;return h>0?`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sc).padStart(2,'0')}`:`${String(m).padStart(2,'0')}:${String(sc).padStart(2,'0')}`;}
function mktState(){
  const t=nowIST(),day=t.getUTCDay(),h=t.getUTCHours(),m=t.getUTCMinutes();
  const mins=h*60+m,open=9*60+15,close=15*60+30;
  if(day===0||day===6)return{state:'closed',label:'MARKET CLOSED',sub:'Reopens Monday 09:15 IST'};
  if(mins<open-30)return{state:'closed',label:'MARKET CLOSED',sub:'NSE opens 09:15 IST'};
  if(mins<open)return{state:'pre',label:'PRE-OPEN',sub:'Call auction 09:00-09:15 IST'};
  if(mins<=close)return{state:'open',label:'MARKET OPEN',sub:'NSE live · 09:15-15:30 IST'};
  return{state:'closed',label:'MARKET CLOSED',sub:'Reopens tomorrow 09:15 IST'};
}
function secUntil(th,tm){const t=nowIST();let s=(th-t.getUTCHours())*3600+(tm-t.getUTCMinutes())*60-t.getUTCSeconds();if(s<0)s+=86400;return s;}
function secUntilNextWeekday(th,tm){
  const t=nowIST();
  let d=new Date(t.getTime());
  for(let i=0;i<8;i++){
    const day=d.getUTCDay();
    if(day>=1&&day<=5){
      const h=d.getUTCHours(),m=d.getUTCMinutes(),s=d.getUTCSeconds();
      const targetMins=th*60+tm, curMins=h*60+m;
      if(i===0&&curMins<targetMins){
        return Math.max(0,(th-h)*3600+(tm-m)*60-s);
      } else if(i>0){
        // Next weekday — time from start of that day to target
        const secsUntilMidnight=(23-h)*3600+(59-m)*60+(60-s);
        const secsFromMidnight=th*3600+tm*60;
        return secsUntilMidnight+secsFromMidnight;
      }
    }
    d=new Date(d.getTime()+86400000);
    d=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate(),0,0,0));
  }
  return 0;
}
function tick(){
  const t=nowIST();
  document.getElementById('ist').textContent=`${String(t.getUTCHours()).padStart(2,'0')}:${String(t.getUTCMinutes()).padStart(2,'0')}:${String(t.getUTCSeconds()).padStart(2,'0')} IST`;
  const ms=mktState();
  const mkt=document.getElementById('mkt');
  mkt.className='mkt '+ms.state;
  document.getElementById('mkt-s').textContent=ms.label;
  document.getElementById('mkt-sub').textContent=ms.sub;
  const t2=nowIST(),h=t2.getUTCHours(),m=t2.getUTCMinutes(),mins=h*60+m;
  let cd=0,label='until open',prog=0;
  if(ms.state==='open'){cd=secUntil(15,30);label='until close';prog=Math.min(100,(mins-9*60-15)/(6*60+15)*100);}
  else if(ms.state==='pre'){cd=secUntil(9,15);label='until open';prog=Math.min(100,(1-cd/1800)*100);}
  else{cd=secUntilNextWeekday(9,15);label='until open';prog=0;}
  document.getElementById('mkt-cd').textContent=fmt(cd);
  document.getElementById('mkt-cd-l').textContent=label;
  document.getElementById('mkt-prog').style.width=prog.toFixed(1)+'%';
}
setInterval(tick,1000);tick();
function lc(l){
  if(l.includes('BUY')||l.includes('SETUP')||l.includes('+Rs'))return 'g';
  if(l.includes('ERROR')||l.includes('STOP')||l.includes('-Rs'))return 'r';
  if(l.includes('HOLD')||l.includes('WARNING')||l.includes('DIVERGENCE'))return 'a';
  if(l.includes('Cycle')||l.includes('Scan')||l.includes('Pass'))return 'b';
  return 'd';
}
async function refresh(){
  try{
    const d=await(await fetch('/api/status')).json();
    const s=d.stats;
    document.getElementById('conn').style.color='#00e676';
    const unreal=d.open.reduce((sum,t)=>sum+(t.unrealised||0),0);
    const port=CAP+s.pnl+unreal;
    document.getElementById('mp').textContent='Rs.'+port.toLocaleString('en-IN');
    document.getElementById('mp-s').textContent=`base Rs.1,00,000 · unreal ${unreal>=0?'+':''}Rs.${Math.abs(Math.round(unreal)).toLocaleString('en-IN')}`;
    const mw=document.getElementById('mw');
    const totalPnL = s.pnl + unreal;
    mw.textContent=(totalPnL>=0?'+Rs.':'-Rs.')+Math.abs(Math.round(totalPnL)).toLocaleString('en-IN');
    mw.className='mv '+(totalPnL>=0?'g':'r');
    document.getElementById('mw-s').textContent=
      `closed ${s.pnl>=0?'+':''}Rs.${Math.round(s.pnl).toLocaleString('en-IN')} · unreal ${unreal>=0?'+':''}Rs.${Math.abs(Math.round(unreal)).toLocaleString('en-IN')}`;
    const tot=s.wins+s.losses;
    document.getElementById('mwr').textContent=tot?Math.round(s.wins/tot*100)+'%':'—';
    document.getElementById('mwr-s').textContent=s.wins+'W / '+s.losses+'L';
    const rp=Math.round(s.risk_used/3000*100);
    const mr=document.getElementById('mr');
    mr.textContent=rp+'%';mr.className='mv '+(rp>80?'r':rp>50?'a':'a');
    document.getElementById('mr-s').textContent='Rs.'+s.risk_used+' / Rs.3,000';
    document.getElementById('mo').textContent=d.open.length;
    const rf=document.getElementById('rf');
    rf.style.width=Math.min(100,rp)+'%';
    rf.style.background=rp>80?'#ff5252':rp>50?'#ffab40':'#00e676';
    document.getElementById('rl').textContent='Risk: Rs.'+s.risk_used+' of Rs.3,000 weekly budget ('+rp+'%)';
    document.getElementById('op-b').textContent=
      d.open.length>=5 ? '⬛ FULL ('+d.open.length+'/5)' : d.open.length+'/5 OPEN';
    document.getElementById('op-b').style.color = d.open.length>=5 ? '#ff5252' : '#ffab40';
    document.getElementById('op').innerHTML=d.open.length?d.open.map(t=>{
      const u=t.unrealised||0,uc=u>=0?'#00e676':'#ff5252',us=u>=0?'+':'';
      const totalRange=Math.abs(t.target-t.sl);
      const pricePos=(t.last_price||t.entry)-t.sl;
      const prog=totalRange>0?Math.min(100,Math.max(0,pricePos/totalRange*100)):50;
      const entryPct=totalRange>0?Math.min(100,Math.max(0,(t.entry-t.sl)/totalRange*100)):50;
      return `<div class="tc open">
        <div class="tr">
          <span class="ts">${t.sym} <span style="font-size:9px;color:#3a5060">BUY · Day ${t.days_held}/5</span></span>
          <span style="color:${uc};font-weight:600;font-size:13px">${us}Rs.${Math.abs(u).toLocaleString('en-IN')} <span style="font-size:10px">(${us}${t.unreal_pct||0}%)</span></span>
        </div>
        <div class="tm" style="margin:4px 0">
          Last <b style="color:#c8d8e8">Rs.${(t.last_price||t.entry).toLocaleString('en-IN')}</b> &nbsp;·&nbsp; Entry Rs.${t.entry} &nbsp;·&nbsp; Qty ${t.qty||'—'}<br>
          SL <span style="color:#ff5252">Rs.${t.sl}</span> &nbsp;·&nbsp; TGT <span style="color:#00e676">Rs.${t.target}</span> &nbsp;·&nbsp; R:R ${t.rr}x
        </div>
        <div style="position:relative;height:6px;background:#0a1520;border-radius:3px;overflow:hidden;margin-top:6px">
          <div style="position:absolute;left:0;top:0;height:100%;width:${prog.toFixed(1)}%;background:${uc};border-radius:3px;transition:width .5s"></div>
          <div style="position:absolute;left:${entryPct.toFixed(1)}%;top:0;width:2px;height:100%;background:#ffab40"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:9px;color:#2a4050;margin-top:2px">
          <span>SL Rs.${t.sl}</span><span style="color:#ffab40">&#9650; Entry</span><span>TGT Rs.${t.target}</span>
        </div>
      </div>`;
    }).join(''):'<div class="empty">No open positions<br><span style="font-size:10px;color:#1a3020">Bot scanning for top 5 Dual RSI setups</span></div>';
    const wins=d.closed.filter(t=>t.status==='win').length;
    document.getElementById('ct-b').textContent=d.closed.length+' CLOSED';
    document.getElementById('ct').innerHTML=d.closed.length?d.closed.map(t=>{
      const pc=t.pnl>=0?'#00e676':'#ff5252';
      const ps=t.pnl>=0?'+Rs.':'-Rs.';
      const reason=(t.exit_reason||t.status||'').replace('EXCESS_ON_BOOT','CANCELLED').replace('EXCESS_CANCELLED','CANCELLED');
      return `<div class="tc ${t.status||'loss'}">
        <div class="tr">
          <span class="ts">${t.sym}</span>
          <span style="color:${pc};font-weight:600">${ps}${Math.abs(t.pnl).toLocaleString('en-IN')}</span>
        </div>
        <div class="tm">${reason} · Entry Rs.${t.entry}</div>
      </div>`;
    }).join(''):'<div class="empty">No closed trades yet</div>';
    document.getElementById('lg').innerHTML=d.logs.length?d.logs.map(l=>`<div class="${lc(l)}">${l}</div>`).join(''):'<div class="d">No log entries yet</div>';
  }catch(e){document.getElementById('conn').style.color='#ff5252';}
}
refresh();setInterval(refresh,5000);
</script></body></html>"""

    @app.route("/")
    def index():
        return render_template_string(HTML)

    @app.route("/health")
    def health():
        return {"status": "ok", "bot": "First-Orbit Trader PRO"}, 200

    @app.route("/ping")
    def ping():
        return "pong", 200

    @app.route("/api/status")
    def api_status():
        try:
            con = sqlite3.connect(DB_PATH)
            con.row_factory = sqlite3.Row
            ws = (date.today()-timedelta(days=date.today().weekday())).isoformat()
            con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)",(ws,))
            con.commit()
            s = dict(con.execute(
                "SELECT pnl,risk_used,wins,losses,time_exits FROM weekly_stats WHERE week_start=?",(ws,)
            ).fetchone() or {"pnl":0,"risk_used":0,"wins":0,"losses":0,"time_exits":0})
            open_t = [dict(r) for r in con.execute(
                "SELECT sym,entry,sl,target,rr,risk_amt,days_held,qty FROM trades WHERE status='open'"
            ).fetchall()]
            # Fetch live prices for open positions
            for t in open_t:
                try:
                    import yfinance as _yf
                    hist = _yf.Ticker(t["sym"]+".NS").history(period="1d",interval="1m",timeout=5,auto_adjust=True)
                    if hist is not None and len(hist) > 0:
                        t["last_price"] = round(float(hist["Close"].iloc[-1]),2)
                    else:
                        row = con.execute("SELECT price FROM screener_cache WHERE sym=?",(t["sym"],)).fetchone()
                        t["last_price"] = round(float(row[0]),2) if row and row[0] else t["entry"]
                except Exception:
                    row = con.execute("SELECT price FROM screener_cache WHERE sym=?",(t["sym"],)).fetchone()
                    t["last_price"] = round(float(row[0]),2) if row and row[0] else t["entry"]
                t["unrealised"] = round((t["last_price"]-t["entry"])*t["qty"],2)
                t["unreal_pct"] = round((t["last_price"]-t["entry"])/t["entry"]*100,2)
            closed_rows = con.execute(
                "SELECT sym,entry,pnl,status,exit_reason,qty FROM trades WHERE status!='open' ORDER BY closed_at DESC LIMIT 20"
            ).fetchall()
            closed = []
            for r in closed_rows:
                sym,entry,pnl,status,exit_reason,qty = r
                # Retroactively compute P&L for EXCESS_ON_BOOT trades with pnl=0
                if pnl == 0 and exit_reason and 'EXCESS' in exit_reason and qty:
                    cached = con.execute("SELECT price FROM screener_cache WHERE sym=?",(sym,)).fetchone()
                    if cached and cached[0]:
                        pnl = round((float(cached[0]) - entry) * qty, 2)
                closed.append({"sym":sym,"entry":entry,"pnl":pnl,
                               "status":status,"exit_reason":exit_reason})
            con.close()
            logs = []
            try:
                with open(LOG_PATH) as f:
                    logs = [l.strip() for l in f.readlines()[-50:]][::-1]
            except Exception:
                logs = ["Bot starting up..."]
            return jsonify({"stats":s,"open":open_t,"closed":closed,"logs":logs,
                            "time":datetime.now().strftime("%H:%M:%S")})
        except Exception as e:
            return jsonify({"error":str(e),"stats":{"pnl":0,"risk_used":0,"wins":0,"losses":0},
                            "open":[],"closed":[],"logs":[],"time":"--:--:--"})

    port = int(os.environ.get("PORT", 8080))
    log.info(f"Dashboard on port {port}")
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True
    ).start()

# ── MAIN ──────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 50)
    log.info("  First-Orbit Trader PRO")
    log.info("  Strategy: Dual RSI Momentum + MCap Filter")
    log.info(f"  Capital: Rs.{CAPITAL:,}  Risk/week: Rs.{MAX_WEEKLY_RISK:,}")
    log.info(f"  DB: {DB_PATH}")
    log.info("=" * 50)

    try:
        import curl_cffi
        log.info(f"  curl_cffi {curl_cffi.__version__} active")
    except ImportError:
        log.warning("  curl_cffi missing")

    con = init_db()
    wiped = con.execute(
        "DELETE FROM screener_cache WHERE reject_reason='insufficient_data' OR reject_reason LIKE 'error:%'"
    ).rowcount
    con.commit()
    if wiped: log.info(f"  Cleared {wiped} stale cache entries")

    # Enforce MAX_OPEN — keep only top MAX_OPEN positions by score, cancel excess
    open_trades = con.execute(
        "SELECT id, sym, score, entry, qty FROM trades WHERE status='open' ORDER BY score DESC"
    ).fetchall()
    if len(open_trades) > MAX_OPEN:
        excess = open_trades[MAX_OPEN:]
        total_cancelled_pnl = 0
        for row in excess:
            tid, sym, score, entry, qty = row
            # Fetch last known price to compute actual P&L
            try:
                cached = con.execute(
                    "SELECT price FROM screener_cache WHERE sym=?", (sym,)
                ).fetchone()
                last_price = float(cached[0]) if cached and cached[0] else entry
                pnl = round((last_price - entry) * qty, 2)
            except Exception:
                pnl = 0.0
                last_price = entry
            status = 'win' if pnl > 0 else ('loss' if pnl < 0 else 'cancelled')
            con.execute(
                "UPDATE trades SET status=?, pnl=?, closed_at=?, exit_reason='EXCESS_CANCELLED' WHERE id=?",
                (status, pnl, datetime.now().isoformat(), tid)
            )
            total_cancelled_pnl += pnl
            log.info(f"  CANCELLED {sym} @ Rs.{last_price:.2f} P&L Rs.{pnl:+.2f} (score:{score})")
        con.commit()
        # Update weekly stats with cancelled P&L
        ws = (date.today()-timedelta(days=date.today().weekday())).isoformat()
        con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)",(ws,))
        con.execute(
            "UPDATE weekly_stats SET pnl=pnl+? WHERE week_start=?",
            (round(total_cancelled_pnl,2), ws)
        )
        con.commit()
        log.info(
            f"  Enforced MAX_OPEN={MAX_OPEN}: kept top {MAX_OPEN}, "            f"cancelled {len(excess)} excess | net P&L Rs.{total_cancelled_pnl:+.2f}"
        )

    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n== Cycle #{cycle} — {datetime.now().strftime('%H:%M:%S %d-%b')} ==")

        log.info("-- Position management")
        manage_positions(con)

        stats  = get_stats(con)
        open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        if not is_market_open():
            log.info(f"  NSE closed — next open in {time_to_open()}")
        elif stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning("  Weekly risk limit reached — no new trades this week")
        elif open_c >= MAX_OPEN:
            log.info(f"  Portfolio full ({open_c}/{MAX_OPEN}) — monitoring only, no scan")
        else:
            slots = MAX_OPEN - open_c
            log.info(f"-- Market scan ({slots} slot{'s' if slots>1 else ''} available)")
            universe  = fetch_universe()
            risk_left = MAX_WEEKLY_RISK - stats["risk_used"]
            scan_and_trade(universe, con, risk_left)

        stats  = get_stats(con)
        open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        total  = stats["wins"] + stats["losses"]
        wr     = round(stats["wins"]/total*100) if total else 0
        # Compute total unrealised from open positions
        open_rows = con.execute("SELECT entry,qty FROM trades WHERE status='open'").fetchall()
        log.info(f"\n  P&L: Rs.{stats['pnl']:+.0f}  Risk: Rs.{stats['risk_used']:.0f}/Rs.{MAX_WEEKLY_RISK}"
                 f"  W/L: {stats['wins']}/{stats['losses']} ({wr}%)  Open: {open_c}")
        log.info(f"  Sleeping {SCAN_INTERVAL}s\n")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    import threading as _th

    start_dashboard()

    # Self keep-alive ping every 4 minutes
    def _keepalive():
        import time as _t, urllib.request as _u
        _t.sleep(30)
        port = int(os.environ.get("PORT", 8080))
        while True:
            try: _u.urlopen(f"http://127.0.0.1:{port}/health", timeout=5)
            except Exception: pass
            _t.sleep(240)
    _th.Thread(target=_keepalive, daemon=True).start()

    run()
