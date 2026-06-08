"""
ClaudeBot v4 — Pure Algo NSE Swing Trader
Fixes: batch scanning, Railway-safe, rejection logging, clean progress

STRATEGY: RSI(14) cross above 32 + EMA20>EMA50 uptrend + volume surge + ATR range
STOP:     entry − 1.5×ATR
TARGET:   entry + 3.0×ATR  (R:R = 2.0 always)
TRAIL:    move stop to breakeven once price > entry + 1×ATR
TIME:     force exit after 5 trading days
SIZE:     qty = ₹800 ÷ (entry − stop)
"""

import time, json, sqlite3, os, logging, io, math
from datetime import datetime, date, timedelta
import urllib.request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.FileHandler("claudebot.log"), logging.StreamHandler()]
)
log = logging.getLogger("bot")

# ── INSTALL DEPS ──────────────────────────────────────────────────────────────
def ensure_deps():
    import importlib, subprocess, sys
    for pkg in ["yfinance","curl_cffi","pandas","numpy"]:
        try: importlib.import_module(pkg)
        except ImportError:
            log.info(f"pip install {pkg}…")
            subprocess.check_call([sys.executable,"-m","pip","install",pkg,"-q","--quiet"])
ensure_deps()

import yfinance as yf
import pandas as pd
import numpy as np

# ── CONFIG ────────────────────────────────────────────────────────────────────
CAPITAL         = 100_000
MAX_WEEKLY_RISK = 3_000
RISK_PER_TRADE  = 800
MAX_OPEN        = 3
SCAN_INTERVAL   = 300       # seconds between cycles
TOP_N           = 3         # max new trades per cycle
BATCH_SIZE      = 30        # symbols per batch (Railway memory-safe)
BATCH_PAUSE     = 2         # seconds between batches

# Liquidity gates
MIN_PRICE       = 50         # lowered: include mid-caps from ₹50
MAX_PRICE       = 8_000
MIN_AVG_VOL     = 150_000    # lowered: include liquid mid-caps

# Strategy params
RSI_PERIOD      = 14
RSI_ENTRY_LOW   = 28         # RSI zone bottom — oversold
RSI_ENTRY_HIGH  = 42         # RSI zone top — still cheap
RSI_EXIT_OB     = 70         # exit if RSI hits overbought
EMA_FAST        = 20
EMA_SLOW        = 50
VOL_MULT        = 1.2        # volume must be this × 20d avg (relaxed)
ATR_PERIOD      = 14
ATR_MIN_PCT     = 1.0        # min daily ATR% (relaxed)
ATR_MAX_PCT     = 6.0        # max daily ATR% (relaxed slightly)
ATR_STOP_MULT   = 1.5
ATR_TARGET_MULT = 3.0        # guarantees R:R = 2.0
TIME_STOP_DAYS  = 5

NSE_CSV = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

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
    "APOLLOTYRE","MRF","CEAT","TVS","BAJAJ-AUTO","TVSMOTOR","ESCORTS","ASHOKLEY",
    "ALKEM","AUROPHARMA","IPCA","NATCOPHARM","GRANULES","AJANTPHARM","LAURUS",
    "DIVI","BIOCON","STRIDES","GLAND","PFIZER","SANOFI","BALRAMCHIN","TRIVENI",
]

# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect("trades.db")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY, sym TEXT, direction TEXT,
        entry REAL, sl REAL, target REAL, trail_sl REAL,
        qty INTEGER, risk_amt REAL, target_gain REAL, rr REAL,
        status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
        score REAL DEFAULT 0, days_held INTEGER DEFAULT 0,
        opened_at TEXT, closed_at TEXT, exit_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS scan_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, sym TEXT, signal TEXT,
        score REAL, rsi REAL, atr_pct REAL,
        entry REAL, sl REAL, target REAL, rr REAL,
        reject_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        pnl REAL DEFAULT 0, risk_used REAL DEFAULT 0,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        time_exits INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS screener_cache (
        sym TEXT PRIMARY KEY, score REAL,
        rsi REAL, rsi_prev REAL,
        vol_ratio REAL, atr_pct REAL, price REAL,
        ema20 REAL, ema50 REAL,
        entry REAL, sl REAL, target REAL, rr REAL,
        reject_reason TEXT, updated_at TEXT
    );
    """)
    con.commit()
    return con

# ── UNIVERSE ──────────────────────────────────────────────────────────────────
def fetch_universe():
    try:
        req = urllib.request.Request(NSE_CSV, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            df = pd.read_csv(io.StringIO(r.read().decode("latin-1")))
        syms = df["SYMBOL"].dropna().str.strip().tolist()
        log.info(f"NSE universe: {len(syms)} symbols")
        return syms
    except Exception as e:
        log.warning(f"NSE CSV failed ({e}) — fallback {len(FALLBACK)} symbols")
        return FALLBACK

# ── INDICATORS ────────────────────────────────────────────────────────────────
def calc_rsi(close, period=14):
    delta = np.diff(close.astype(float))
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    # Use simple rolling mean for first value, then Wilder smoothing
    ag = np.mean(gain[:period])
    al = np.mean(loss[:period])
    rsi_vals = []
    for i in range(period, len(gain)):
        ag = (ag * (period-1) + gain[i]) / period
        al = (al * (period-1) + loss[i]) / period
        rs = ag / al if al != 0 else 100
        rsi_vals.append(100 - 100/(1+rs))
    return rsi_vals   # most recent is last

def calc_ema(close, span):
    return float(pd.Series(close).ewm(span=span, adjust=False).mean().iloc[-1])

def calc_atr(high, low, close, period=14):
    h, l, c = high[1:], low[1:], close[:-1]
    tr = np.maximum(h-l, np.maximum(np.abs(h-c), np.abs(l-c)))
    return float(np.mean(tr[-period:]))

# ── SCREEN ONE SYMBOL ─────────────────────────────────────────────────────────
def screen(sym, cache_cutoff, con):
    """
    Returns (setup_dict, reject_reason).
    setup_dict is None if no valid entry.
    Logs exactly why each stock was rejected.
    """
    # Check 4-hour cache first
    cached = con.execute(
        "SELECT score,rsi,rsi_prev,vol_ratio,atr_pct,price,ema20,ema50,"
        "entry,sl,target,rr,reject_reason "
        "FROM screener_cache WHERE sym=? AND updated_at>?",
        (sym, cache_cutoff)
    ).fetchone()
    if cached:
        score,rsi_n,rsi_p,vol_r,atr_p,price,e20,e50,entry,sl,tgt,rr,rej = cached
        if entry:   # was a valid setup
            return {"sym":sym,"score":score,"rsi":rsi_n,"rsi_prev":rsi_p,
                    "vol_ratio":vol_r,"atr_pct":atr_p,"price":price,
                    "ema20":e20,"ema50":e50,"entry":entry,"sl":sl,
                    "target":tgt,"rr":rr}, None
        return None, f"cached:{rej}"

    now = datetime.now().isoformat()
    reject = None

    try:
        df = yf.Ticker(sym+".NS").history(period="90d", interval="1d",
                                           timeout=10, auto_adjust=True)
        if df is None or len(df) < EMA_SLOW + 5:
            reject = "insufficient_data"
        else:
            c = df["Close"].values.astype(float)
            h = df["High"].values.astype(float)
            l = df["Low"].values.astype(float)
            v = df["Volume"].values.astype(float)
            price     = float(c[-1])
            avg_vol   = float(np.mean(v[-20:]))
            today_vol = float(v[-1])

            # Gate 1: price range
            if   price < MIN_PRICE:  reject = f"price_low(₹{price:.0f})"
            elif price > MAX_PRICE:  reject = f"price_high(₹{price:.0f})"
            # Gate 2: liquidity
            elif avg_vol < MIN_AVG_VOL: reject = f"low_vol({avg_vol:.0f})"
            else:
                rsi_list  = calc_rsi(c, RSI_PERIOD)
                if len(rsi_list) < 2:
                    reject = "rsi_calc_error"
                else:
                    rsi_now  = rsi_list[-1]
                    rsi_prev = rsi_list[-2]
                    ema20    = calc_ema(c, EMA_FAST)
                    ema50    = calc_ema(c, EMA_SLOW)
                    atr_val  = calc_atr(h, l, c, ATR_PERIOD)
                    atr_pct  = atr_val / price * 100
                    vol_ratio = today_vol / avg_vol

                    # Gate 3: RSI in oversold-recovery zone
                    # Condition: RSI currently between 28-42 AND was below 45 yesterday
                    # This catches: fresh crosses, ongoing recoveries, and dip bounces
                    rsi_in_zone = RSI_ENTRY_LOW <= rsi_now <= RSI_ENTRY_HIGH
                    rsi_was_low = rsi_prev < 45
                    if not (rsi_in_zone and rsi_was_low):
                        reject = f"rsi_not_setup(prev:{rsi_prev:.1f} now:{rsi_now:.1f} zone:{RSI_ENTRY_LOW}-{RSI_ENTRY_HIGH})"
                    # Gate 4: uptrend (price > EMA50, EMA20 > EMA50)
                    elif not (price > ema50 and ema20 > ema50):
                        reject = f"no_uptrend(p:{price:.0f} e20:{ema20:.0f} e50:{ema50:.0f})"
                    # Gate 5: volume above threshold
                    elif vol_ratio < VOL_MULT:
                        reject = f"low_vol_ratio({vol_ratio:.2f}x<{VOL_MULT}x)"
                    # Gate 6: ATR in swingable range
                    elif not (ATR_MIN_PCT <= atr_pct <= ATR_MAX_PCT):
                        reject = f"atr_oor({atr_pct:.1f}% need {ATR_MIN_PCT}-{ATR_MAX_PCT}%)"
                    else:
                        # ALL CONDITIONS MET — compute setup
                        entry  = round(price, 2)
                        sl     = round(entry - ATR_STOP_MULT * atr_val, 2)
                        target = round(entry + ATR_TARGET_MULT * atr_val, 2)
                        rr     = round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)

                        rsi_score   = max(0, 100-(rsi_now-RSI_ENTRY_LOW)*5)  # best score at low end of zone
                        vol_score   = min(100, (vol_ratio-VOL_MULT)/2*100)
                        atr_score   = max(0, 100-abs(atr_pct-2.5)*15)
                        ema_gap     = (ema20-ema50)/ema50*100
                        trend_score = min(100, ema_gap*20)
                        score       = round(rsi_score*0.35+vol_score*0.30+
                                            trend_score*0.20+atr_score*0.15, 1)

                        con.execute("""INSERT OR REPLACE INTO screener_cache
                            (sym,score,rsi,rsi_prev,vol_ratio,atr_pct,price,
                             ema20,ema50,entry,sl,target,rr,reject_reason,updated_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)""",
                            (sym,score,round(rsi_now,1),round(rsi_prev,1),
                             round(vol_ratio,2),round(atr_pct,2),round(price,2),
                             round(ema20,2),round(ema50,2),entry,sl,target,rr,now))
                        con.commit()
                        return {"sym":sym,"score":score,"rsi":round(rsi_now,1),
                                "rsi_prev":round(rsi_prev,1),"vol_ratio":round(vol_ratio,2),
                                "atr_pct":round(atr_pct,2),"price":price,
                                "ema20":round(ema20,2),"ema50":round(ema50,2),
                                "entry":entry,"sl":sl,"target":target,"rr":rr}, None

    except Exception as e:
        reject = f"error:{str(e)[:40]}"

    # Cache the rejection
    con.execute("""INSERT OR REPLACE INTO screener_cache
        (sym,score,rsi,rsi_prev,vol_ratio,atr_pct,price,
         ema20,ema50,entry,sl,target,rr,reject_reason,updated_at)
        VALUES (?,0,0,0,0,0,0,0,0,NULL,NULL,NULL,NULL,?,?)""",
        (sym, reject, now))
    return None, reject

# ── MARKET HOURS CHECK ───────────────────────────────────────────────────────
def is_market_open() -> bool:
    """NSE is open Mon–Fri 09:15–15:30 IST (UTC+5:30)."""
    ist_offset = timedelta(hours=5, minutes=30)
    now_ist = datetime.utcnow() + ist_offset
    if now_ist.weekday() >= 5:          # Saturday=5, Sunday=6
        return False
    market_open  = now_ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    market_close = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now_ist <= market_close

def time_to_open() -> str:
    """Human-readable time until next NSE open."""
    ist_offset = timedelta(hours=5, minutes=30)
    now_ist = datetime.utcnow() + ist_offset
    # Find next weekday 09:15
    candidate = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    if now_ist >= candidate:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    delta = candidate - now_ist
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m = rem // 60
    return f"{h}h {m}m"

# ── SCAN MARKET (batched) ─────────────────────────────────────────────────────
def scan_market(universe, con):
    cache_cutoff = (datetime.now()-timedelta(hours=4)).isoformat()
    setups = []
    reject_counts = {}
    total = len(universe)

    for i in range(0, total, BATCH_SIZE):
        batch = universe[i:i+BATCH_SIZE]
        for sym in batch:
            setup, reason = screen(sym, cache_cutoff, con)
            if setup:
                setups.append(setup)
                log.info(f"  ✓ SETUP {sym} RSI:{setup['rsi_prev']}→{setup['rsi']} "
                         f"vol:{setup['vol_ratio']}x atr:{setup['atr_pct']}% "
                         f"score:{setup['score']}")
            else:
                # strip cache: prefix to get real reason
                real = reason.replace("cached:","") if reason else "unknown"
                key  = real.split("(")[0]
                reject_counts[key] = reject_counts.get(key, 0) + 1

        done = min(i+BATCH_SIZE, total)
        # Print rejection breakdown every 30 symbols so you see it mid-scan
        top_rejects = " | ".join(
            f"{k}:{v}" for k,v in
            sorted(reject_counts.items(), key=lambda x:-x[1])[:4]
        )
        log.info(f"  Scanned {done}/{total} — {len(setups)} setups | {top_rejects}")
        if i+BATCH_SIZE < total:
            time.sleep(BATCH_PAUSE)

    log.info("  ── Final rejection breakdown ──")
    for k,v in sorted(reject_counts.items(), key=lambda x:-x[1]):
        log.info(f"     {k:<30} {v:>5} stocks")

    setups.sort(key=lambda x: x["score"], reverse=True)
    top = setups[:TOP_N]
    if top:
        log.info(f"  Top {len(top)} setups selected:")
        for s in top:
            log.info(f"    {s['sym']} score:{s['score']} "
                     f"entry:₹{s['entry']} sl:₹{s['sl']} tgt:₹{s['target']}")
    else:
        log.info("  No setups passed all filters this cycle")
    return top

# ── MANAGE OPEN POSITIONS ─────────────────────────────────────────────────────
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
            hist = yf.Ticker(t["sym"]+".NS").history(
                period="2d", interval="5m", timeout=8, auto_adjust=True)
            price = float(hist["Close"].iloc[-1]) if len(hist) > 0 else None
        except Exception:
            price = None

        if price is None:
            log.warning(f"  {t['sym']}: price fetch failed")
            continue

        # Days held
        try:
            days = (datetime.now() - datetime.fromisoformat(t["opened_at"])).days
        except Exception:
            days = 0
        con.execute("UPDATE trades SET days_held=? WHERE id=?", (days, t["id"]))

        # Trailing stop: move to breakeven once price > entry + 1×ATR
        atr_val     = abs(t["entry"] - t["sl"]) / ATR_STOP_MULT
        be_trigger  = t["entry"] + atr_val
        trail_sl    = t["trail_sl"]
        if price >= be_trigger and trail_sl < t["entry"]:
            trail_sl = t["entry"]
            con.execute("UPDATE trades SET trail_sl=? WHERE id=?", (trail_sl, t["id"]))
            con.commit()
            log.info(f"  TRAIL {t['sym']}: stop → breakeven ₹{trail_sl}")

        effective_sl = max(t["sl"], trail_sl)

        # RSI overbought check
        rsi_exit = False
        try:
            df = yf.Ticker(t["sym"]+".NS").history(
                period="30d", interval="1d", timeout=8, auto_adjust=True)
            if len(df) >= RSI_PERIOD + 2:
                rsi_list = calc_rsi(df["Close"].values, RSI_PERIOD)
                if rsi_list and rsi_list[-1] >= RSI_EXIT_OB:
                    rsi_exit = True
        except Exception:
            pass

        # Determine exit
        if   price <= effective_sl:      reason = "SL_HIT"
        elif price >= t["target"]:       reason = "TARGET_HIT"
        elif days  >= TIME_STOP_DAYS:    reason = "TIME_STOP(5d)"
        elif rsi_exit:                   reason = f"RSI_OB"
        else:
            pnl_unreal = round((price-t["entry"])*t["qty"], 0)
            log.info(f"  HOLD {t['sym']} @ ₹{price:.2f} "
                     f"unrealised:₹{pnl_unreal:+.0f} "
                     f"sl:₹{effective_sl:.2f} tgt:₹{t['target']} day:{days}/{TIME_STOP_DAYS}")
            continue

        pnl    = round((price-t["entry"])*t["qty"], 2)
        status = "win" if pnl > 0 else "loss"
        con.execute("""UPDATE trades SET status=?,pnl=?,closed_at=?,exit_reason=?
                       WHERE id=?""",
                    (status, pnl, datetime.now().isoformat(), reason, t["id"]))
        con.commit()
        upd_stats(con, pnl=pnl,
                  risk_used=abs(pnl) if pnl<0 else 0,
                  wins=1 if pnl>0 else 0,
                  losses=0 if pnl>0 else 1,
                  time_exits=1 if "TIME" in reason else 0)
        log.info(f"  CLOSED {t['sym']} [{reason}] @ ₹{price:.2f} P&L ₹{pnl:+.2f}")

# ── EXECUTE PAPER ORDER ───────────────────────────────────────────────────────
def execute_order(setup, con, risk_left):
    risk_per = abs(setup["entry"] - setup["sl"])
    if risk_per <= 0: return False
    qty = max(1, int(min(risk_left, RISK_PER_TRADE) / risk_per))
    actual_risk = round(qty * risk_per, 2)

    con.execute("""INSERT INTO trades
        (id,sym,direction,entry,sl,target,trail_sl,qty,risk_amt,target_gain,rr,
         score,days_held,opened_at,exit_reason)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)""",
        (f"{setup['sym']}_{int(time.time())}",
         setup["sym"], "BUY", setup["entry"], setup["sl"], setup["target"],
         setup["sl"],   # trail starts at sl
         qty, actual_risk,
         round(qty*abs(setup["entry"]-setup["target"]),2),
         setup["rr"], setup["score"],
         datetime.now().isoformat(), ""))
    con.commit()
    log.info(f"  ◈ BUY {setup['sym']} qty:{qty} @ ₹{setup['entry']} "
             f"SL:₹{setup['sl']} TGT:₹{setup['target']} "
             f"R:R:{setup['rr']}x risk:₹{actual_risk} score:{setup['score']}")
    return True

# ── STATS ─────────────────────────────────────────────────────────────────────
def week_start():
    t = date.today()
    return (t - timedelta(days=t.weekday())).isoformat()

def get_stats(con):
    ws = week_start()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    con.commit()
    r = con.execute(
        "SELECT pnl,risk_used,wins,losses,time_exits FROM weekly_stats WHERE week_start=?",(ws,)
    ).fetchone()
    return {"pnl":r[0],"risk_used":r[1],"wins":r[2],"losses":r[3],"time_exits":r[4]}

def upd_stats(con, **kw):
    ws = week_start()
    sets = ",".join(f"{k}={k}+?" for k in kw)
    con.execute(f"UPDATE weekly_stats SET {sets} WHERE week_start=?",(*kw.values(),ws))
    con.commit()

# ── MAIN ──────────────────────────────────────────────────────────────────────
def run():
    log.info("════════════════════════════════════════════")
    log.info("  ClaudeBot v4 · Pure Algo · NSE Swing")
    log.info("  Strategy: RSI cross + EMA trend + ATR size")
    log.info(f"  Capital:₹{CAPITAL:,}  Risk/week:₹{MAX_WEEKLY_RISK:,}")
    log.info(f"  Params: RSI_zone:{RSI_ENTRY_LOW}-{RSI_ENTRY_HIGH} EMA:{EMA_FAST}/{EMA_SLOW} "
             f"Vol:{VOL_MULT}x ATR:{ATR_MIN_PCT}-{ATR_MAX_PCT}%")
    log.info("════════════════════════════════════════════")

    con = init_db()

    # Wipe stale cache entries caused by Yahoo 403 on previous runs
    wiped = con.execute(
        "DELETE FROM screener_cache WHERE reject_reason='insufficient_data' "
        "OR reject_reason LIKE 'error:%' OR reject_reason LIKE 'cached:insufficient%'"
    ).rowcount
    con.commit()
    if wiped:
        log.info(f"  Cleared {wiped} stale cache entries from previous runs")

    # Verify curl_cffi (fixes Yahoo Finance 403 on Railway/cloud IPs)
    try:
        import curl_cffi
        log.info(f"  curl_cffi {curl_cffi.__version__} — Yahoo Finance cloud fix active")
    except ImportError:
        log.warning("  curl_cffi missing — add to requirements.txt!")

    cycle = 0

    while True:
        cycle += 1
        now_str = datetime.now().strftime("%H:%M:%S %d-%b")
        log.info(f"\n══ Cycle #{cycle} — {now_str} ══")

        # 1. Manage open positions
        log.info("── Position management")
        manage_positions(con)

        # 2. Check market hours + capacity
        stats   = get_stats(con)
        open_c  = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        if not is_market_open():
            log.info(f"  NSE closed — next open in {time_to_open()} · sleeping {SCAN_INTERVAL}s")
        elif stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning(f"  Weekly risk limit u20b9{MAX_WEEKLY_RISK} reached — no new trades")
        elif open_c >= MAX_OPEN:
            log.info(f"  Positions full ({open_c}/{MAX_OPEN}) — skipping scan")
        else:
            # 3. Scan market
            log.info(f"── Market scan (capacity: {MAX_OPEN-open_c} slots)")
            universe = fetch_universe()
            setups   = scan_market(universe, con)
            risk_left = MAX_WEEKLY_RISK - stats["risk_used"]

            entered = 0
            for s in setups:
                open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
                if open_c >= MAX_OPEN: break
                if con.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'", (s["sym"],)).fetchone():
                    continue
                if execute_order(s, con, risk_left):
                    risk_left -= min(RISK_PER_TRADE, abs(s["entry"]-s["sl"]))
                    entered += 1
            if not entered:
                log.info("  No new positions opened this cycle")

        # 4. Weekly summary
        stats  = get_stats(con)
        open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        total  = stats["wins"]+stats["losses"]
        wr     = round(stats["wins"]/total*100) if total else 0
        log.info(
            f"\n  ── Weekly summary ──\n"
            f"  P&L      : ₹{stats['pnl']:+.0f}\n"
            f"  Risk used: ₹{stats['risk_used']:.0f} / ₹{MAX_WEEKLY_RISK}\n"
            f"  Win rate : {wr}% ({stats['wins']}W / {stats['losses']}L)\n"
            f"  Open     : {open_c} / {MAX_OPEN}\n"
            f"  TimeExits: {stats['time_exits']}\n"
            f"  Next scan: {SCAN_INTERVAL}s"
        )
        time.sleep(SCAN_INTERVAL)

# ── EMBEDDED DASHBOARD (Flask, runs in background thread) ────────────────────
def start_dashboard():
    from flask import Flask, jsonify, render_template_string
    import threading

    app = Flask(__name__)
    DB  = "trades.db"

    DASH_HTML = """<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClaudeBot · NSE</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:ital,wght@0,400;0,500;0,600;1,400&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#080b0f;color:#a8b4c0;font-family:'IBM Plex Mono',monospace;font-size:12px}

/* ── TOP BAR ── */
.bar{background:#0d1520;border-bottom:1px solid #162030;padding:9px 18px;
     display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:99}
.logo{color:#00e676;font-weight:600;letter-spacing:3px;font-size:13px}
.bar-r{display:flex;align-items:center;gap:14px;font-size:10px;color:#3a5060}
.ping{width:6px;height:6px;border-radius:50%;background:#00e676;display:inline-block;
      margin-right:5px;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.15}}

/* ── MARKET STATUS BANNER ── */
.mkt-banner{display:flex;align-items:center;justify-content:space-between;
            padding:10px 18px;border-bottom:1px solid #162030;transition:background .5s}
.mkt-banner.open{background:#031a0d}
.mkt-banner.closed{background:#0e0a02}
.mkt-banner.pre{background:#030e1a}
.mkt-left{display:flex;align-items:center;gap:12px}
.mkt-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.mkt-banner.open   .mkt-dot{background:#00e676;box-shadow:0 0 8px #00e676;animation:pulse 1.5s infinite}
.mkt-banner.closed .mkt-dot{background:#ff5252}
.mkt-banner.pre    .mkt-dot{background:#ffab40;animation:pulse 2s infinite}
.mkt-status{font-size:13px;font-weight:600}
.mkt-banner.open   .mkt-status{color:#00e676}
.mkt-banner.closed .mkt-status{color:#ff5252}
.mkt-banner.pre    .mkt-status{color:#ffab40}
.mkt-sub{font-size:10px;color:#3a5060;margin-top:1px}
.mkt-right{text-align:right}
.mkt-cd{font-size:22px;font-weight:600;color:#c8d8e8;letter-spacing:2px;font-variant-numeric:tabular-nums}
.mkt-cd-label{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;margin-top:2px}
.mkt-prog{height:3px;background:#0d1520;border-radius:2px;overflow:hidden;margin-top:8px;width:100%}
.mkt-prog-fill{height:100%;border-radius:2px;transition:width 1s linear}
.mkt-banner.open   .mkt-prog-fill{background:#00e676}
.mkt-banner.closed .mkt-prog-fill{background:#ff5252}
.mkt-banner.pre    .mkt-prog-fill{background:#ffab40}

/* ── IST CLOCK (updates every second in JS) ── */
.ist{font-size:11px;color:#4a6070}

/* ── METRICS ── */
.metrics{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:#162030}
@media(max-width:640px){.metrics{grid-template-columns:repeat(2,1fr)}}
.met{background:#0d1520;padding:12px 14px}
.ml{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:4px}
.mv{font-size:20px;font-weight:600;color:#c8d8e8}
.mv.g{color:#00e676}.mv.r{color:#ff5252}.mv.a{color:#ffab40}
.ms{font-size:10px;color:#2a4050;margin-top:2px}

/* ── BODY GRID ── */
.body{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#162030;margin-top:1px}
@media(max-width:640px){.body{grid-template-columns:1fr}}
.panel{background:#080b0f;padding:14px}
.pt{font-size:9px;color:#2a5060;letter-spacing:2px;text-transform:uppercase;margin-bottom:10px;
    display:flex;justify-content:space-between;align-items:center}
.pt-badge{font-size:9px;padding:2px 7px;border-radius:3px;letter-spacing:1px;font-weight:600}
.pt-badge.open{background:#1a1000;color:#ffab40;border:1px solid #3a2800}
.pt-badge.closed-ok{background:#001a08;color:#00e676;border:1px solid #003a15}
.pt-badge.closed-no{background:#1a0505;color:#ff5252;border:1px solid #3a0808}

/* ── TRADE CARDS ── */
.tc{background:#0d1520;border-radius:6px;padding:10px 12px;margin-bottom:5px;
    border-left:3px solid #1e2d3d;font-size:11px}
.tc.open {border-left-color:#ffab40}
.tc.win  {border-left-color:#00e676}
.tc.loss {border-left-color:#ff5252}
.tr{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}
.ts{color:#c8d8e8;font-weight:600}
.tp.p{color:#00e676}.tp.n{color:#ff5252}.tp.o{color:#ffab40}
.tm{color:#3a5060;line-height:1.7}
.pbar-wrap{margin-top:6px}
.pbar{height:3px;background:#0a1520;border-radius:2px;overflow:hidden}
.pbar-fill{height:100%;border-radius:2px;transition:width .5s}

/* ── RISK BAR ── */
.rb{height:4px;background:#0d1520;border-radius:2px;overflow:hidden;margin-top:8px}
.rf{height:100%;border-radius:2px;transition:width .6s}

/* ── LOG ── */
.full{background:#080b0f;padding:14px;border-top:1px solid #162030;margin-top:1px}
.log{background:#0d1520;border-radius:6px;padding:10px;font-size:10px;
     line-height:1.9;max-height:280px;overflow-y:auto}
.log::-webkit-scrollbar{width:3px}
.log::-webkit-scrollbar-thumb{background:#1e3040}
.g{color:#00e676}.r{color:#ff5252}.a{color:#ffab40}.b{color:#40c4ff}.d{color:#2a4050}
.empty{color:#1e3040;text-align:center;padding:24px 0;font-size:11px}
</style></head><body>

<!-- TOP BAR -->
<div class="bar">
  <div class="logo">◈ CLAUDEBOT · NSE SWING</div>
  <div class="bar-r">
    <span><span class="ping"></span>PAPER MODE</span>
    <span class="ist" id="ist-clock">--:--:-- IST</span>
    <span id="conn" style="color:#1a4030;font-size:14px">●</span>
  </div>
</div>

<!-- MARKET STATUS BANNER -->
<div class="mkt-banner closed" id="mkt-banner">
  <div style="flex:1">
    <div class="mkt-left">
      <div class="mkt-dot"></div>
      <div>
        <div class="mkt-status" id="mkt-status">MARKET CLOSED</div>
        <div class="mkt-sub" id="mkt-sub">NSE · Mon–Fri 09:15–15:30 IST</div>
      </div>
    </div>
    <div class="mkt-prog"><div class="mkt-prog-fill" id="mkt-prog" style="width:0%"></div></div>
  </div>
  <div class="mkt-right" style="margin-left:24px">
    <div class="mkt-cd" id="mkt-cd">--:--:--</div>
    <div class="mkt-cd-label" id="mkt-cd-label">until open</div>
  </div>
</div>

<!-- METRICS -->
<div class="metrics">
  <div class="met"><div class="ml">Portfolio</div><div class="mv" id="mp">—</div><div class="ms">base ₹1,00,000</div></div>
  <div class="met"><div class="ml">Week P&L</div><div class="mv g" id="mw">—</div><div class="ms" id="mws">—</div></div>
  <div class="met"><div class="ml">Win Rate</div><div class="mv" id="mwr">—</div><div class="ms" id="mwrs">—</div></div>
  <div class="met"><div class="ml">Risk Used</div><div class="mv a" id="mr">—</div><div class="ms" id="mrs">—</div></div>
  <div class="met"><div class="ml">Open Trades</div><div class="mv a" id="mo">—</div><div class="ms">max 3 concurrent</div></div>
</div>

<!-- TRADES -->
<div class="body">
  <div class="panel">
    <div class="pt">
      Open Positions
      <span class="pt-badge open" id="op-badge">0 OPEN</span>
    </div>
    <div id="op"><div class="empty">No open positions</div></div>
    <div class="rb"><div class="rf" id="rf" style="width:0%;background:#00e676"></div></div>
    <div style="font-size:10px;color:#2a4050;margin-top:5px" id="risk-label">Risk: ₹0 of ₹3,000 weekly budget used</div>
  </div>
  <div class="panel">
    <div class="pt">
      Closed Trades
      <span class="pt-badge closed-ok" id="ct-badge">0 CLOSED</span>
    </div>
    <div id="ct"><div class="empty">No closed trades yet</div></div>
  </div>
</div>

<!-- LOG -->
<div class="full">
  <div class="pt">
    Bot Activity Log
    <span style="font-size:9px;color:#1a3040">auto-refresh 10s</span>
  </div>
  <div class="log" id="lg"><div class="d">connecting…</div></div>
</div>

<script>
const CAP = 100000;
const OPEN_START  = { h:9,  m:15 };   // IST
const OPEN_END    = { h:15, m:30 };
const PRE_MINUTES = 30;               // show "pre-open" window before open

function nowIST() {
  const now = new Date();
  // UTC + 5:30
  const ist = new Date(now.getTime() + (5*60+30)*60000);
  return ist;
}

function marketState() {
  const t   = nowIST();
  const day = t.getUTCDay();   // 0=Sun,6=Sat
  const h   = t.getUTCHours(), m = t.getUTCMinutes(), s = t.getUTCSeconds();
  const mins = h*60+m;
  const openMins  = OPEN_START.h*60+OPEN_START.m;
  const closeMins = OPEN_END.h*60+OPEN_END.m;

  if (day === 0 || day === 6) return { state:'closed', label:'MARKET CLOSED', sub:'Reopens Monday 09:15 IST' };
  if (mins < openMins - PRE_MINUTES) return { state:'closed', label:'MARKET CLOSED', sub:'NSE opens 09:15 IST' };
  if (mins < openMins) return { state:'pre', label:'PRE-OPEN', sub:'Market opens soon · 09:00–09:15 call auction' };
  if (mins <= closeMins) return { state:'open', label:'MARKET OPEN', sub:'NSE live · 09:15–15:30 IST' };
  return { state:'closed', label:'MARKET CLOSED', sub:'Reopens tomorrow 09:15 IST' };
}

function secUntilNext(targetH, targetM) {
  const t    = nowIST();
  const h    = t.getUTCHours(), m = t.getUTCMinutes(), s = t.getUTCSeconds();
  let secs   = (targetH - h)*3600 + (targetM - m)*60 - s;
  if (secs < 0) secs += 86400;
  return secs;
}

function secUntilNextWeekday(targetH, targetM) {
  // Find next Mon–Fri at targetH:targetM IST
  const t   = nowIST();
  let d     = new Date(t);
  for (let i = 0; i < 7; i++) {
    const day  = d.getUTCDay();
    const h    = d.getUTCHours(), m = d.getUTCMinutes(), s = d.getUTCSeconds();
    const mins = h*60+m;
    const tMins = targetH*60+targetM;
    if (day >= 1 && day <= 5 && (i > 0 || mins < tMins)) {
      let secs = (targetH-h)*3600 + (targetM-m)*60 - s;
      if (secs < 0 && i === 0) secs += 86400;
      return Math.max(0, secs);
    }
    d = new Date(d.getTime() + 86400000);
  }
  return 0;
}

function fmt(secs) {
  const h = Math.floor(secs/3600);
  const m = Math.floor((secs%3600)/60);
  const s = secs%60;
  if (h > 0) return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
  return `${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
}

function updateMarketBanner() {
  const ms  = marketState();
  const ban = document.getElementById('mkt-banner');
  ban.className = 'mkt-banner ' + ms.state;
  document.getElementById('mkt-status').textContent = ms.label;
  document.getElementById('mkt-sub').textContent    = ms.sub;

  const t    = nowIST();
  const day  = t.getUTCDay();
  const h    = t.getUTCHours(), m = t.getUTCMinutes();
  const mins = h*60+m;
  const openMins  = OPEN_START.h*60+OPEN_START.m;
  const closeMins = OPEN_END.h*60+OPEN_END.m;

  let cdSecs = 0, cdLabel = '', prog = 0;
  if (ms.state === 'open') {
    cdSecs  = secUntilNext(OPEN_END.h, OPEN_END.m);
    cdLabel = 'until close';
    const sessionLen = (closeMins - openMins) * 60;
    const elapsed    = (mins - openMins)*60 + t.getUTCSeconds();
    prog = Math.min(100, elapsed/sessionLen*100);
  } else if (ms.state === 'pre') {
    cdSecs  = secUntilNext(OPEN_START.h, OPEN_START.m);
    cdLabel = 'until open';
    prog = Math.min(100, (1 - cdSecs/(PRE_MINUTES*60))*100);
  } else {
    cdSecs  = secUntilNextWeekday(OPEN_START.h, OPEN_START.m);
    cdLabel = 'until open';
    prog = 0;
  }

  document.getElementById('mkt-cd').textContent       = fmt(cdSecs);
  document.getElementById('mkt-cd-label').textContent = cdLabel;
  document.getElementById('mkt-prog').style.width     = prog.toFixed(1)+'%';
}

// IST clock ticking every second
function tickIST() {
  const t = nowIST();
  const hh = String(t.getUTCHours()).padStart(2,'0');
  const mm = String(t.getUTCMinutes()).padStart(2,'0');
  const ss = String(t.getUTCSeconds()).padStart(2,'0');
  document.getElementById('ist-clock').textContent = hh+':'+mm+':'+ss+' IST';
  updateMarketBanner();
}
setInterval(tickIST, 1000);
tickIST();

// ── LOG COLOURING ──
function logClass(l){
  if (l.includes('◈')||l.includes('+₹')||l.includes('SETUP')||l.includes('TARGET_HIT')) return 'g';
  if (l.includes('ERROR')||l.includes('SL_HIT')||l.includes('-₹'))                      return 'r';
  if (l.includes('WARNING')||l.includes('HOLD')||l.includes('TIME_STOP'))               return 'a';
  if (l.includes('Cycle')||l.includes('Scanned')||l.includes('Progress'))               return 'b';
  return 'd';
}

// ── API REFRESH ──
async function refresh(){
  try {
    const d = await (await fetch('/api/status')).json();
    const s = d.stats;
    document.getElementById('conn').style.color = '#00e676';

    // Metrics
    document.getElementById('mp').textContent = '₹'+(CAP+s.pnl).toLocaleString('en-IN');
    const mw = document.getElementById('mw');
    mw.textContent  = (s.pnl>=0?'+₹':'-₹') + Math.abs(s.pnl).toLocaleString('en-IN');
    mw.className    = 'mv '+(s.pnl>=0?'g':'r');
    document.getElementById('mws').textContent = ((s.pnl/CAP)*100).toFixed(2)+'% of capital';
    const tot = s.wins+s.losses;
    document.getElementById('mwr').textContent  = tot ? Math.round(s.wins/tot*100)+'%' : '—';
    document.getElementById('mwrs').textContent = s.wins+'W / '+s.losses+'L';
    const rp = Math.round(s.risk_used/3000*100);
    const mr = document.getElementById('mr');
    mr.textContent  = rp+'%';
    mr.className    = 'mv '+(rp>80?'r':rp>50?'a':'a');
    document.getElementById('mrs').textContent  = '₹'+s.risk_used+' / ₹3,000';
    document.getElementById('mo').textContent   = d.open.length;

    // Risk bar
    const rf = document.getElementById('rf');
    rf.style.width      = Math.min(100,rp)+'%';
    rf.style.background = rp>80?'#ff5252':rp>50?'#ffab40':'#00e676';
    document.getElementById('risk-label').textContent =
      'Risk: ₹'+s.risk_used+' of ₹3,000 weekly budget used ('+rp+'%)';

    // Open trades badge
    document.getElementById('op-badge').textContent = d.open.length+' OPEN';

    // Open positions
    document.getElementById('op').innerHTML = d.open.length
      ? d.open.map(t => {
          const range    = Math.abs(t.target - t.sl);
          const progress = range > 0 ? Math.min(100, Math.max(0,
            (t.entry - t.sl) / range * 100)) : 0;
          return `<div class="tc open">
            <div class="tr">
              <span class="ts">${t.sym} <span style="font-size:9px;color:#3a5060">BUY</span></span>
              <span class="tp o">OPEN · Day ${t.days_held}/5</span>
            </div>
            <div class="tm">
              Entry ₹${t.entry} &nbsp;·&nbsp; SL ₹${t.sl} &nbsp;·&nbsp; TGT ₹${t.target}<br>
              R:R ${t.rr}x &nbsp;·&nbsp; Risk ₹${t.risk_amt}
            </div>
            <div class="pbar-wrap">
              <div class="pbar"><div class="pbar-fill" style="width:${progress}%;background:#ffab40"></div></div>
            </div>
          </div>`;
        }).join('')
      : '<div class="empty">No open positions<br><span style="font-size:10px;color:#1a3020">Bot is scanning for RSI cross + uptrend setups</span></div>';

    // Closed trades
    const wins  = d.closed.filter(t=>t.status==='win').length;
    const total = d.closed.length;
    document.getElementById('ct-badge').textContent = total+' CLOSED';
    document.getElementById('ct-badge').className   =
      'pt-badge '+(wins>total/2?'closed-ok':'closed-no');
    document.getElementById('ct').innerHTML = d.closed.length
      ? d.closed.map(t =>
          `<div class="tc ${t.status}">
            <div class="tr">
              <span class="ts">${t.sym}</span>
              <span class="tp ${t.pnl>=0?'p':'n'}">${t.pnl>=0?'+₹':'-₹'}${Math.abs(t.pnl).toLocaleString('en-IN')}</span>
            </div>
            <div class="tm">${t.exit_reason||t.status.toUpperCase()} &nbsp;·&nbsp; Entry ₹${t.entry}</div>
          </div>`
        ).join('')
      : '<div class="empty">No closed trades yet</div>';

    // Log
    document.getElementById('lg').innerHTML = d.logs.length
      ? d.logs.map(l=>`<div class="${logClass(l)}">${l}</div>`).join('')
      : '<div class="d">No log entries yet</div>';

  } catch(e) {
    document.getElementById('conn').style.color = '#ff5252';
  }
}
refresh();
setInterval(refresh, 10000);
</script></body></html>"""

    @app.route("/")
    def index():
        return render_template_string(DASH_HTML)

    @app.route("/api/status")
    def status():
        try:
            con = sqlite3.connect(DB)
            con.row_factory = sqlite3.Row
            ws = (date.today()-timedelta(days=date.today().weekday())).isoformat()
            con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)",(ws,))
            con.commit()
            s = dict(con.execute(
                "SELECT pnl,risk_used,wins,losses,time_exits FROM weekly_stats WHERE week_start=?",(ws,)
            ).fetchone() or {"pnl":0,"risk_used":0,"wins":0,"losses":0,"time_exits":0})
            open_t = [dict(r) for r in con.execute(
                "SELECT sym,entry,sl,target,rr,risk_amt,days_held FROM trades WHERE status='open'"
            ).fetchall()]
            closed = [dict(r) for r in con.execute(
                "SELECT sym,entry,pnl,status,exit_reason FROM trades "
                "WHERE status!='open' ORDER BY closed_at DESC LIMIT 15"
            ).fetchall()]
            con.close()
            logs = []
            try:
                with open("claudebot.log") as f:
                    logs = [l.strip() for l in f.readlines()[-50:]][::-1]
            except Exception:
                logs = ["Log file not yet created — bot starting up"]
            return jsonify({"stats":s,"open":open_t,"closed":closed,"logs":logs,
                            "time":datetime.now().strftime("%H:%M:%S")})
        except Exception as e:
            return jsonify({"error":str(e),
                            "stats":{"pnl":0,"risk_used":0,"wins":0,"losses":0},
                            "open":[],"closed":[],"logs":[],"time":"--:--:--"})

    port = int(os.environ.get("PORT", 8000))
    log.info(f"Dashboard starting on port {port}")
    # Run Flask in a daemon thread so it doesn't block the bot
    t = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True
    )
    t.start()

if __name__ == "__main__":
    start_dashboard()   # starts web server in background thread
    run()               # runs bot in main thread