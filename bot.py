"""
ClaudeBot v3 — Pure Algo NSE Swing Trader
NO AI / NO API CALLS — fully deterministic rule-based strategy

STRATEGY: RSI Mean-Reversion in Uptrend + ATR-based sizing
─────────────────────────────────────────────────────────────
UNIVERSE:  NSE EQUITY_L.csv → liquidity filter (~400 stocks)
ENTRY:     RSI(14) crosses above 32 from oversold
           + Price > EMA50 (uptrend)
           + EMA20 > EMA50 (momentum)
           + Volume > 1.5x 20d avg (institutional)
           + ATR% 1.5–5% (swingable range)
STOP:      Entry − 1.5 × ATR  (volatility-adjusted)
TARGET:    Entry + 3.0 × ATR  (2:1 R:R guaranteed)
TRAIL:     Move stop to breakeven once price + 1×ATR
TIME EXIT: Force close after 5 trading days
SIZING:    qty = RISK_PER_TRADE / (entry − stop)
RANK:      Composite score → top 3 per cycle
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
    for pkg in ["yfinance", "pandas", "numpy"]:
        try: importlib.import_module(pkg)
        except ImportError:
            log.info(f"pip install {pkg}…")
            subprocess.check_call([sys.executable,"-m","pip","install",pkg,"-q"])
ensure_deps()

import yfinance as yf
import pandas as pd
import numpy as np

# ── CONFIG ────────────────────────────────────────────────────────────────────
CAPITAL          = 100_000
MAX_WEEKLY_RISK  = 3_000
RISK_PER_TRADE   = 800        # ₹ risked per trade
MAX_OPEN         = 3          # max concurrent positions
SCAN_INTERVAL    = 300        # seconds between cycles (5 min)
TOP_N            = 3          # take top N ranked setups per cycle

# Liquidity gates
MIN_PRICE        = 100
MAX_PRICE        = 8_000
MIN_AVG_VOL      = 300_000    # shares/day

# Strategy parameters
RSI_PERIOD       = 14
RSI_ENTRY        = 32         # RSI must cross UP through this
RSI_EXIT_OB      = 72         # exit if RSI hits overbought
EMA_FAST         = 20
EMA_SLOW         = 50
VOL_MULTIPLIER   = 1.5        # volume must be this × 20d avg
ATR_PERIOD       = 14
ATR_MIN_PCT      = 1.5        # min daily range % (too quiet = skip)
ATR_MAX_PCT      = 5.0        # max daily range % (too wild = skip)
ATR_STOP_MULT    = 1.5        # stop = entry − N×ATR
ATR_TARGET_MULT  = 3.0        # target = entry + N×ATR  → R:R = 2.0
TIME_STOP_DAYS   = 5          # force exit after N trading days

NSE_CSV = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

NIFTY500_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","ITC","LT","AXISBANK","ASIANPAINT","MARUTI","SUNPHARMA","TATAMOTORS",
    "ULTRACEMCO","WIPRO","NESTLEIND","POWERGRID","NTPC","TECHM","HCLTECH","BAJFINANCE",
    "BAJAJFINSV","TITAN","ADANIPORTS","ONGC","DIVISLAB","DRREDDY","CIPLA","COALINDIA",
    "JSWSTEEL","TATASTEEL","INDUSINDBK","HINDALCO","BPCL","GRASIM","SHREECEM","UPL",
    "BRITANNIA","EICHERMOT","HEROMOTOCO","M&M","APOLLOHOSP","TATACONSUM","DABUR",
    "PIDILITIND","BERGEPAINT","LUPIN","TORNTPHARM","MUTHOOTFIN","CHOLAFIN","SBILIFE",
    "HDFCLIFE","ICICIGI","BANDHANBNK","FEDERALBNK","IDFCFIRSTB","PNB","CANBK","BANKBARODA",
    "MOTHERSON","BALKRISIND","PERSISTENT","LTIM","MPHASIS","COFORGE","ZOMATO","IRCTC",
    "TATAPOWER","TORNTPOWER","PFC","RECLTD","NHPC","IRFC","BEL","HAL","BHEL","SAIL",
    "NMDC","VEDL","POLYCAB","HAVELLS","VOLTAS","ABB","SIEMENS","CUMMINSIND","THERMAX",
    "DIXON","AMBER","KAYNES","SYRMA","TATAELXSI","KPITTECH","CYIENT","LTTS","HAPPSTMNDS",
    "INTELLECT","MASTEK","HEXAWARE","OFSS","NIITTECH","RATEGAIN","ZENSAR","BIRLASOFT",
    "SONACOMS","SCHAEFFLER","TIMKEN","GRINDWELL","FINCABLES","SUPREMEIND","ASTRAL",
    "PRINCEPIPE","FINOLEX","NILKAMAL","GREENPLY","CENTURYPLY","GREENPANEL","CENTURY",
    "JKCEMENT","RAMCOCEM","HEIDELBERG","ORIENTCEM","NUVOCO","BIRLACORPN","DALMIACEM",
    "STARCEMENT","MANGCMFG","PRSMJOHNSN","KAJARIA","SOMANYCER","CERA","HSIL",
    "APLAPOLLO","RATNAMANI","APL","JINDALSTEL","WELCORP","TINPLATE","MSTC",
    "BALRAMCHIN","TRIVENI","EIDPARRY","DHAMPUR","UTTAMSUGAR","RENUKA","GMRINFRA",
    "ADANIENT","ADANITRANS","ADANIGREEN","ADANIPOWER","ATGL","APSEZ","ADANIPORTS",
    "NYKAA","DELHIVERY","PAYTM","POLICYBZR","CARTRADE","EASEMYTRIP","IXIGO",
    "DEVYANI","SAPPHIRE","JUBLFOOD","WESTLIFE","BARBEQUE","EAZYDINER",
    "PAGEIND","MANYAVAR","VEDANT","ABFRL","TRENT","SHOPERSTOP","VLCL","METRO",
    "RAJESHEXPO","KALYANKJIL","SENCO","PCJEWELLER","GITANJALI",
    "APOLLOTYRE","MRF","CEAT","GOODYEAR","TVS","BAJAJ-AUTO","TVSMOTOR","ESCORTS",
    "FORCEMOT","ASHOKLEY","VOLVO","SML","TIINDIA","SUPRAJIT","ENDURANCE","MINDA",
    "SANDHAR","GABRIAL","UCAL","SHARDAMOTR","MINDAIND","SUNDRM","WABCO","SUBROS",
    "ALKEM","AUROPHARMA","CADILAHC","IPCA","NATCOPHARM","GRANULES","AJANTPHARM",
    "LAUREATE","SEQUENT","LAURUS","SUVEN","SOLARA","HIKAL","NEULANDLAB","DIVI",
    "BIOCON","STRIDES","GLAND","PHIBRO","PFIZER","SANOFI","ABBOT","GLAXO","NOVARTIS",
]

# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect("trades.db")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        sym TEXT, direction TEXT,
        entry REAL, sl REAL, target REAL, trail_sl REAL,
        qty INTEGER, risk_amt REAL, target_gain REAL,
        rr REAL, status TEXT DEFAULT 'open',
        pnl REAL DEFAULT 0, score REAL DEFAULT 0,
        days_held INTEGER DEFAULT 0,
        opened_at TEXT, closed_at TEXT, exit_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS scan_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, sym TEXT, signal TEXT,
        score REAL, rsi REAL, atr_pct REAL,
        entry REAL, sl REAL, target REAL, rr REAL
    );
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        pnl REAL DEFAULT 0, risk_used REAL DEFAULT 0,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        time_exits INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS screener_cache (
        sym TEXT PRIMARY KEY,
        score REAL, rsi REAL, rsi_prev REAL,
        trend TEXT, vol_ratio REAL, atr REAL, atr_pct REAL,
        price REAL, ema20 REAL, ema50 REAL,
        entry REAL, sl REAL, target REAL, rr REAL,
        updated_at TEXT
    );
    """)
    con.commit()
    return con

# ── FETCH NSE UNIVERSE ────────────────────────────────────────────────────────
def fetch_universe() -> list[str]:
    try:
        req = urllib.request.Request(NSE_CSV, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            df = pd.read_csv(io.StringIO(r.read().decode("latin-1")))
        syms = df["SYMBOL"].dropna().str.strip().tolist()
        log.info(f"NSE universe: {len(syms)} symbols")
        return syms
    except Exception as e:
        log.warning(f"NSE CSV failed ({e}) — using {len(NIFTY500_FALLBACK)}-stock fallback")
        return NIFTY500_FALLBACK

# ── INDICATOR ENGINE ──────────────────────────────────────────────────────────
def rsi(close: np.ndarray, period=14) -> np.ndarray:
    delta = np.diff(close.astype(float))
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    avg_g = np.convolve(gain, np.ones(period)/period, mode='valid')
    avg_l = np.convolve(loss, np.ones(period)/period, mode='valid')
    rs    = np.divide(avg_g, avg_l, out=np.ones_like(avg_g)*100, where=avg_l!=0)
    return 100 - (100 / (1 + rs))

def ema(close: np.ndarray, span: int) -> float:
    return float(pd.Series(close).ewm(span=span, adjust=False).mean().iloc[-1])

def atr(high, low, close, period=14) -> float:
    h, l, c = high[1:], low[1:], close[:-1]
    tr = np.maximum(h-l, np.maximum(np.abs(h-c), np.abs(l-c)))
    return float(np.mean(tr[-period:]))

# ── CORE SCREENER ─────────────────────────────────────────────────────────────
def screen_symbol(sym: str) -> dict | None:
    """
    Download 90 days OHLCV, apply all filters, compute score.
    Returns setup dict if valid entry signal, else None.
    """
    try:
        df = yf.Ticker(sym+".NS").history(period="90d", interval="1d", timeout=10)
        if df is None or len(df) < EMA_SLOW + 5:
            return None

        c = df["Close"].values.astype(float)
        h = df["High"].values.astype(float)
        l = df["Low"].values.astype(float)
        v = df["Volume"].values.astype(float)

        price     = c[-1]
        avg_vol   = float(np.mean(v[-20:]))
        today_vol = float(v[-1])

        # ── STAGE 1: LIQUIDITY GATE ───────────────────────────────────────
        if price < MIN_PRICE or price > MAX_PRICE:
            return None
        if avg_vol < MIN_AVG_VOL:
            return None

        # ── COMPUTE INDICATORS ────────────────────────────────────────────
        rsi_arr  = rsi(c, RSI_PERIOD)
        rsi_now  = float(rsi_arr[-1])
        rsi_prev = float(rsi_arr[-2])
        ema20    = ema(c, EMA_FAST)
        ema50    = ema(c, EMA_SLOW)
        atr_val  = atr(h, l, c, ATR_PERIOD)
        atr_pct  = atr_val / price * 100
        vol_ratio = today_vol / avg_vol

        # ── STAGE 2: ENTRY SIGNAL CONDITIONS ─────────────────────────────
        # Condition A — RSI CROSS: was below entry threshold, now above
        rsi_cross = (rsi_prev < RSI_ENTRY) and (rsi_now >= RSI_ENTRY)
        # Condition B — UPTREND confirmed
        uptrend   = (price > ema50) and (ema20 > ema50)
        # Condition C — VOLUME surge
        vol_surge = vol_ratio >= VOL_MULTIPLIER
        # Condition D — ATR in swing-friendly range
        atr_ok    = ATR_MIN_PCT <= atr_pct <= ATR_MAX_PCT

        if not (rsi_cross and uptrend and vol_surge and atr_ok):
            return None

        # ── COMPUTE LEVELS ────────────────────────────────────────────────
        entry  = round(price, 2)
        sl     = round(entry - ATR_STOP_MULT * atr_val, 2)
        target = round(entry + ATR_TARGET_MULT * atr_val, 2)
        rr     = round(ATR_TARGET_MULT / ATR_STOP_MULT, 2)   # always 2.0

        # ── COMPOSITE SCORE (0–100) ───────────────────────────────────────
        # RSI score: strongest signal just above 32 (fresh cross), weakens higher
        rsi_score  = max(0, 100 - (rsi_now - RSI_ENTRY) * 4)
        # Volume score: more surge = more conviction
        vol_score  = min(100, (vol_ratio - VOL_MULTIPLIER) / 2 * 100)
        # ATR score: 2.5% is ideal swing range
        atr_score  = max(0, 100 - abs(atr_pct - 2.5) * 15)
        # EMA gap score: bigger gap between ema20 and ema50 = stronger trend
        ema_gap_pct = (ema20 - ema50) / ema50 * 100
        trend_score = min(100, ema_gap_pct * 20)

        score = (rsi_score * 0.35 + vol_score * 0.30 +
                 trend_score * 0.20 + atr_score * 0.15)

        return {
            "sym":      sym,
            "price":    price,
            "rsi":      round(rsi_now, 1),
            "rsi_prev": round(rsi_prev, 1),
            "ema20":    round(ema20, 2),
            "ema50":    round(ema50, 2),
            "atr":      round(atr_val, 2),
            "atr_pct":  round(atr_pct, 2),
            "vol_ratio": round(vol_ratio, 2),
            "avg_vol":  int(avg_vol),
            "entry":    entry,
            "sl":       sl,
            "target":   target,
            "rr":       rr,
            "score":    round(score, 1),
        }

    except Exception:
        return None

# ── FULL MARKET SCAN ──────────────────────────────────────────────────────────
def scan_market(universe: list[str], con) -> list[dict]:
    """
    Screen all symbols, cache results, return top N setups.
    Skips symbols cached within last 4 hours (no fresh signal).
    """
    log.info(f"Scanning {len(universe)} symbols…")
    setups = []
    cache_cutoff = (datetime.now() - timedelta(hours=4)).isoformat()

    for i, sym in enumerate(universe):
        # Check if we already have a cached setup for this symbol
        cached = con.execute(
            "SELECT score,rsi,rsi_prev,trend,vol_ratio,atr_pct,price,entry,sl,target,rr,atr "
            "FROM screener_cache WHERE sym=? AND updated_at>? AND entry IS NOT NULL",
            (sym, cache_cutoff)
        ).fetchone()

        if cached:
            score,rsi_n,rsi_p,trend,vol_r,atr_p,price,entry,sl,target,rr,atr_v = cached
            setups.append({"sym":sym,"score":score,"rsi":rsi_n,"rsi_prev":rsi_p,
                           "vol_ratio":vol_r,"atr_pct":atr_p,"price":price,
                           "entry":entry,"sl":sl,"target":target,"rr":rr,"atr":atr_v})
            continue

        result = screen_symbol(sym)
        now = datetime.now().isoformat()

        if result:
            con.execute("""INSERT OR REPLACE INTO screener_cache
                (sym,score,rsi,rsi_prev,trend,vol_ratio,atr,atr_pct,
                 price,ema20,ema50,entry,sl,target,rr,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sym, result["score"], result["rsi"], result["rsi_prev"],
                 "Uptrend", result["vol_ratio"], result["atr"], result["atr_pct"],
                 result["price"], result["ema20"], result["ema50"],
                 result["entry"], result["sl"], result["target"],
                 result["rr"], now))
            con.commit()
            setups.append(result)
            log.info(f"  ✓ SETUP: {sym} score:{result['score']} "
                     f"RSI:{result['rsi_prev']}→{result['rsi']} "
                     f"vol:{result['vol_ratio']}x atr:{result['atr_pct']}%")
        else:
            # Cache the miss too (avoid re-screening)
            con.execute("INSERT OR REPLACE INTO screener_cache "
                        "(sym,score,rsi,rsi_prev,trend,vol_ratio,atr,atr_pct,price,ema20,ema50,updated_at) "
                        "VALUES (?,0,0,0,'',0,0,0,0,0,?,?)",
                        (sym, 0, now))
            if (i+1) % 100 == 0:
                con.commit()

        if (i+1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(universe)} scanned, {len(setups)} setups found")
            time.sleep(1)   # gentle rate limiting

    setups.sort(key=lambda x: x["score"], reverse=True)
    top = setups[:TOP_N]
    log.info(f"Scan complete: {len(setups)} setups → top {len(top)} selected")
    for s in top:
        log.info(f"  #{setups.index(s)+1} {s['sym']} score:{s['score']} "
                 f"entry:₹{s['entry']} sl:₹{s['sl']} tgt:₹{s['target']} R:R:{s['rr']}x")
    return top

# ── POSITION MANAGEMENT ───────────────────────────────────────────────────────
def manage_positions(con) -> None:
    """
    For each open trade:
    1. Fetch latest price via yfinance
    2. Update trailing stop (move to breakeven once price > entry + 1×ATR)
    3. Check SL / target / time stop
    4. RSI overbought exit
    """
    open_trades = con.execute("SELECT * FROM trades WHERE status='open'").fetchall()
    cols = ["id","sym","direction","entry","sl","target","trail_sl","qty",
            "risk_amt","target_gain","rr","status","pnl","score","days_held",
            "opened_at","closed_at","exit_reason"]

    for row in open_trades:
        t = dict(zip(cols, row))

        # Fetch live price
        try:
            hist = yf.Ticker(t["sym"]+".NS").history(period="2d", interval="5m", timeout=8)
            price = float(hist["Close"].iloc[-1]) if len(hist) > 0 else None
        except Exception:
            price = None

        if price is None:
            log.warning(f"  Price fetch failed for {t['sym']} — skipping")
            continue

        # Update days held
        try:
            opened = datetime.fromisoformat(t["opened_at"])
            days = (datetime.now() - opened).days
        except Exception:
            days = 0
        con.execute("UPDATE trades SET days_held=? WHERE id=?", (days, t["id"]))

        # ── TRAILING STOP ─────────────────────────────────────────────────
        # Once price crosses entry + 1×ATR, move stop to breakeven
        atr_val = abs(t["entry"] - t["sl"]) / ATR_STOP_MULT
        be_trigger = t["entry"] + atr_val   # breakeven trigger level
        if price >= be_trigger and t["trail_sl"] < t["entry"]:
            new_trail = t["entry"]
            con.execute("UPDATE trades SET trail_sl=? WHERE id=?", (new_trail, t["id"]))
            con.commit()
            log.info(f"  TRAIL {t['sym']}: stop moved to breakeven ₹{new_trail}")
            t["trail_sl"] = new_trail

        effective_sl = max(t["sl"], t["trail_sl"])

        # ── EXIT CONDITIONS ───────────────────────────────────────────────
        exit_reason = None
        if price <= effective_sl:
            exit_reason = "SL_HIT"
        elif price >= t["target"]:
            exit_reason = "TARGET_HIT"
        elif days >= TIME_STOP_DAYS:
            exit_reason = "TIME_STOP"
        else:
            # RSI overbought exit
            try:
                df = yf.Ticker(t["sym"]+".NS").history(period="30d", interval="1d", timeout=8)
                if len(df) >= RSI_PERIOD + 2:
                    rsi_arr = rsi(df["Close"].values, RSI_PERIOD)
                    if float(rsi_arr[-1]) >= RSI_EXIT_OB:
                        exit_reason = f"RSI_OB({float(rsi_arr[-1]):.0f})"
            except Exception:
                pass

        if exit_reason:
            pnl = round((price - t["entry"]) * t["qty"], 2)
            status = "win" if pnl > 0 else "loss"
            con.execute("UPDATE trades SET status=?,pnl=?,closed_at=?,exit_reason=? WHERE id=?",
                        (status, pnl, datetime.now().isoformat(), exit_reason, t["id"]))
            con.commit()
            _upd_stats(con, pnl=pnl, risk_used=abs(pnl) if pnl<0 else 0,
                       wins=1 if pnl>0 else 0, losses=0 if pnl>0 else 1,
                       time_exits=1 if exit_reason=="TIME_STOP" else 0)
            log.info(f"  CLOSED {t['sym']} [{exit_reason}] @ ₹{price:.2f} P&L ₹{pnl:+.0f}")
        else:
            log.info(f"  HOLD {t['sym']} @ ₹{price:.2f} "
                     f"(entry ₹{t['entry']} SL ₹{effective_sl:.2f} TGT ₹{t['target']}) "
                     f"days:{days}/{TIME_STOP_DAYS}")

# ── EXECUTE PAPER ORDER ───────────────────────────────────────────────────────
def execute_order(setup: dict, con, risk_remaining: float) -> bool:
    entry     = setup["entry"]
    sl        = setup["sl"]
    risk_per  = abs(entry - sl)
    if risk_per <= 0: return False
    qty = max(1, int(min(risk_remaining, RISK_PER_TRADE) / risk_per))
    actual_risk  = round(qty * risk_per, 2)
    target_gain  = round(qty * abs(entry - setup["target"]), 2)

    tid = f"{setup['sym']}_{int(time.time())}"
    con.execute("""INSERT INTO trades
        (id,sym,direction,entry,sl,target,trail_sl,qty,risk_amt,target_gain,rr,
         score,days_held,opened_at,exit_reason)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)""",
        (tid, setup["sym"], "BUY", entry, sl, setup["target"],
         sl,   # trail_sl starts at sl
         qty, actual_risk, target_gain, setup["rr"],
         setup["score"], datetime.now().isoformat(), ""))
    con.commit()

    log.info(f"  ◈ BUY {setup['sym']} qty:{qty} @ ₹{entry} "
             f"SL:₹{sl} TGT:₹{setup['target']} R:R:{setup['rr']}x "
             f"risk:₹{actual_risk} score:{setup['score']}")

    # ── LIVE MODE: replace this block with your broker API call ──────────
    # from kiteconnect import KiteConnect
    # kite = KiteConnect(api_key=os.environ["KITE_API_KEY"])
    # kite.set_access_token(os.environ["KITE_ACCESS_TOKEN"])
    # kite.place_order(
    #     tradingsymbol=setup["sym"], exchange="NSE",
    #     transaction_type="BUY", quantity=qty,
    #     order_type="LIMIT", price=entry, product="CNC"
    # )
    return True

# ── STATS HELPERS ─────────────────────────────────────────────────────────────
def _week_start():
    today = date.today()
    return (today - timedelta(days=today.weekday())).isoformat()

def _get_stats(con):
    ws = _week_start()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    con.commit()
    r = con.execute(
        "SELECT pnl,risk_used,wins,losses,time_exits FROM weekly_stats WHERE week_start=?", (ws,)
    ).fetchone()
    return {"pnl":r[0],"risk_used":r[1],"wins":r[2],"losses":r[3],"time_exits":r[4]}

def _upd_stats(con, **kw):
    ws = _week_start()
    sets = ",".join(f"{k}={k}+?" for k in kw)
    con.execute(f"UPDATE weekly_stats SET {sets} WHERE week_start=?", (*kw.values(), ws))
    con.commit()

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def run():
    log.info("════════════════════════════════════════════")
    log.info("  ClaudeBot v3 · Pure Algo · NSE Swing")
    log.info("  Strategy: RSI cross + EMA trend + ATR size")
    log.info(f"  Capital:₹{CAPITAL:,}  Risk/week:₹{MAX_WEEKLY_RISK:,}")
    log.info("════════════════════════════════════════════")

    con   = init_db()
    cycle = 0

    while True:
        cycle += 1
        log.info(f"\n══ Cycle #{cycle} — {datetime.now().strftime('%H:%M:%S')} ══")
        stats = get_stats_safe(con)

        # 1. Manage existing positions (exits, trailing stops)
        log.info("── Position check")
        manage_positions(con)

        # 2. Scan for new entries (if capacity available)
        stats = get_stats_safe(con)
        open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        if stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning("  Weekly risk exhausted — no new entries")
        elif open_c >= MAX_OPEN:
            log.info(f"  Max positions open ({open_c}/{MAX_OPEN}) — skipping scan")
        else:
            universe = fetch_universe()
            setups = scan_market(universe, con)
            risk_left = MAX_WEEKLY_RISK - stats["risk_used"]

            entered = 0
            for setup in setups:
                open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
                if open_c >= MAX_OPEN: break
                already = con.execute(
                    "SELECT 1 FROM trades WHERE sym=? AND status='open'", (setup["sym"],)
                ).fetchone()
                if already: continue
                if execute_order(setup, con, risk_left):
                    risk_left -= min(RISK_PER_TRADE, abs(setup["entry"]-setup["sl"]))
                    entered += 1

            if not entered:
                log.info("  No qualifying setups this cycle")

        # 3. Print weekly summary
        stats = get_stats_safe(con)
        open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        total  = stats["wins"] + stats["losses"]
        wr     = round(stats["wins"]/total*100) if total else 0
        log.info(
            f"\n  P&L:₹{stats['pnl']:+.0f}  "
            f"Risk:₹{stats['risk_used']:.0f}/₹{MAX_WEEKLY_RISK}  "
            f"W:{stats['wins']} L:{stats['losses']} ({wr}% WR)  "
            f"Open:{open_c}  TimeExits:{stats['time_exits']}"
        )
        log.info(f"  Sleeping {SCAN_INTERVAL}s…\n")
        time.sleep(SCAN_INTERVAL)

def get_stats_safe(con):
    ws = _week_start()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
    con.commit()
    r = con.execute(
        "SELECT pnl,risk_used,wins,losses,time_exits FROM weekly_stats WHERE week_start=?", (ws,)
    ).fetchone()
    return {"pnl":r[0],"risk_used":r[1],"wins":r[2],"losses":r[3],"time_exits":r[4]}

if __name__ == "__main__":
    run()
