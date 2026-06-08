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
    for pkg in ["yfinance","pandas","numpy"]:
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
MIN_PRICE       = 100
MAX_PRICE       = 8_000
MIN_AVG_VOL     = 300_000

# Strategy params
RSI_PERIOD      = 14
RSI_ENTRY       = 32        # RSI must cross UP through this level
RSI_EXIT_OB     = 72        # exit if RSI hits overbought
EMA_FAST        = 20
EMA_SLOW        = 50
VOL_MULT        = 1.5       # volume must be this × 20d avg
ATR_PERIOD      = 14
ATR_MIN_PCT     = 1.5       # min daily ATR%
ATR_MAX_PCT     = 5.0       # max daily ATR%
ATR_STOP_MULT   = 1.5
ATR_TARGET_MULT = 3.0       # guarantees R:R = 2.0
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

                    # Gate 3: RSI cross (was below 32, now above)
                    if not (rsi_prev < RSI_ENTRY <= rsi_now):
                        reject = f"no_rsi_cross(prev:{rsi_prev:.1f}→now:{rsi_now:.1f})"
                    # Gate 4: uptrend
                    elif not (price > ema50 and ema20 > ema50):
                        reject = f"no_uptrend(p:{price:.0f} e20:{ema20:.0f} e50:{ema50:.0f})"
                    # Gate 5: volume
                    elif vol_ratio < VOL_MULT:
                        reject = f"low_vol_ratio({vol_ratio:.2f}x)"
                    # Gate 6: ATR range
                    elif not (ATR_MIN_PCT <= atr_pct <= ATR_MAX_PCT):
                        reject = f"atr_oor({atr_pct:.1f}%)"
                    else:
                        # ALL CONDITIONS MET — compute setup
                        entry  = round(price, 2)
                        sl     = round(entry - ATR_STOP_MULT * atr_val, 2)
                        target = round(entry + ATR_TARGET_MULT * atr_val, 2)
                        rr     = round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)

                        rsi_score   = max(0, 100-(rsi_now-RSI_ENTRY)*4)
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
                # Bucket rejection reasons for summary
                key = reason.split("(")[0] if reason else "unknown"
                reject_counts[key] = reject_counts.get(key, 0) + 1

        done = min(i+BATCH_SIZE, total)
        log.info(f"  Scanned {done}/{total} — {len(setups)} setups found")
        if i+BATCH_SIZE < total:
            time.sleep(BATCH_PAUSE)

    # Rejection summary
    log.info("  Rejection breakdown: " +
             " | ".join(f"{k}:{v}" for k,v in
                        sorted(reject_counts.items(), key=lambda x:-x[1])[:6]))

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
    log.info(f"  Params: RSI_entry:{RSI_ENTRY} EMA:{EMA_FAST}/{EMA_SLOW} "
             f"Vol:{VOL_MULT}x ATR:{ATR_MIN_PCT}-{ATR_MAX_PCT}%")
    log.info("════════════════════════════════════════════")

    con   = init_db()
    cycle = 0

    while True:
        cycle += 1
        now_str = datetime.now().strftime("%H:%M:%S %d-%b")
        log.info(f"\n══ Cycle #{cycle} — {now_str} ══")

        # 1. Manage open positions
        log.info("── Position management")
        manage_positions(con)

        # 2. Check capacity
        stats   = get_stats(con)
        open_c  = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        if stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning(f"  Weekly risk limit ₹{MAX_WEEKLY_RISK} reached — no new trades")
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
                if con.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'",(s["sym"],)).fetchone():
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

if __name__ == "__main__":
    run()