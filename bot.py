"""
ClaudeBot v2 — Full NSE Market Scanner
Scans entire NSE universe → multi-stage algo filter → Claude signal → auto trade

DATA:  yfinance (free, no API key)
UNIVERSE: NSE EQUITY_L.csv (~2000 stocks) → filtered to liquid mid/large caps
PIPELINE:
  Stage 1 — Liquidity filter     (price, volume, market cap proxy) — zero API cost
  Stage 2 — Technical screener   (RSI, EMA trend, volume surge, ATR) — zero API cost
  Stage 3 — Momentum scorer      (ranks survivors by composite score) — zero API cost
  Stage 4 — Claude signal        (top N candidates only) — Haiku, ~80 tokens each
  Stage 5 — Risk-sized execution (paper or live broker)
"""

import time, json, sqlite3, os, logging, math, io
from datetime import datetime, date, timedelta
import urllib.request, urllib.error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.FileHandler("claudebot.log"), logging.StreamHandler()]
)
log = logging.getLogger("claudebot")

# ── INSTALL DEPS AT RUNTIME IF MISSING ───────────────────────────────────────
def ensure_deps():
    import importlib, subprocess, sys
    for pkg, imp in [("yfinance","yfinance"), ("pandas","pandas"), ("numpy","numpy")]:
        try: importlib.import_module(imp)
        except ImportError:
            log.info(f"Installing {pkg}...")
            subprocess.check_call([sys.executable,"-m","pip","install",pkg,"-q"])
ensure_deps()

import yfinance as yf
import pandas as pd
import numpy as np

# ── CONFIG ────────────────────────────────────────────────────────────────────
CAPITAL          = 100_000
MAX_WEEKLY_RISK  = 3_000
RISK_PER_TRADE   = 800
MIN_RR           = 2.0
MAX_OPEN         = 3
SCAN_INTERVAL    = 300          # 5 min between full scan cycles
TOP_N_FOR_CLAUDE = 5            # only top N algo-ranked stocks sent to Claude
HOLD_SKIP_CYCLES = 6

# Liquidity thresholds (Stage 1)
MIN_PRICE        = 50           # skip penny stocks
MAX_PRICE        = 10_000       # skip stocks too expensive to buy qty>1
MIN_AVG_VOLUME   = 200_000      # avg daily volume

# NSE symbol list URL (official NSE CSV)
NSE_EQUITY_CSV   = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

# ── DB ────────────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect("trades.db")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY, sym TEXT, direction TEXT,
        entry REAL, sl REAL, target REAL,
        qty INTEGER, risk_amt REAL, target_gain REAL,
        rr REAL, status TEXT DEFAULT 'open', pnl REAL DEFAULT 0,
        score REAL DEFAULT 0,
        opened_at TEXT, closed_at TEXT, claude_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS scan_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, sym TEXT, signal TEXT, rr REAL,
        skipped INTEGER DEFAULT 0, model_used TEXT,
        tokens_used INTEGER DEFAULT 0, algo_score REAL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        pnl REAL DEFAULT 0, risk_used REAL DEFAULT 0,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        total_tokens INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS screener_cache (
        sym TEXT PRIMARY KEY, score REAL, rsi REAL,
        trend TEXT, vol_ratio REAL, atr_pct REAL,
        price REAL, updated_at TEXT
    );
    """)
    con.commit()
    return con

# ── STAGE 1: GET NSE UNIVERSE ─────────────────────────────────────────────────
def fetch_nse_universe() -> list[str]:
    """
    Download NSE's official equity list and return liquid symbols.
    Falls back to a curated Nifty 200 list if the CSV download fails.
    """
    try:
        req = urllib.request.Request(
            NSE_EQUITY_CSV,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("latin-1")
        df = pd.read_csv(io.StringIO(raw))
        # NSE CSV has column "SYMBOL"
        syms = df["SYMBOL"].dropna().tolist()
        log.info(f"NSE universe loaded: {len(syms)} symbols")
        return syms
    except Exception as e:
        log.warning(f"NSE CSV fetch failed ({e}) — using Nifty 200 fallback")
        return NIFTY200_FALLBACK

# Curated Nifty 200 fallback (used if NSE CSV unreachable)
NIFTY200_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","ITC","LT","AXISBANK","ASIANPAINT","MARUTI","SUNPHARMA","TATAMOTORS",
    "ULTRACEMCO","WIPRO","NESTLEIND","POWERGRID","NTPC","TECHM","HCLTECH","BAJFINANCE",
    "BAJAJFINSV","TITAN","ADANIPORTS","ONGC","DIVISLAB","DRREDDY","CIPLA","COALINDIA",
    "JSWSTEEL","TATASTEEL","INDUSINDBK","HINDALCO","BPCL","GRASIM","SHREECEM","UPL",
    "BRITANNIA","EICHERMOT","HEROMOTOCO","M&M","APOLLOHOSP","TATACONSUM","DABUR",
    "PIDILITIND","BERGEPAINT","LUPIN","TORNTPHARM","MUTHOOTFIN","CHOLAFIN","SBILIFE",
    "HDFCLIFE","ICICIGI","BANDHANBNK","FEDERALBNK","IDFCFIRSTB","PNB","CANBK","BANKBARODA",
    "MOTHERSON","BALKRISIND","PERSISTENT","LTIM","MPHASIS","COFORGE","ZOMATO","NYKAA",
    "DELHIVERY","PAYTM","POLICYBZR","IRCTC","GMRINFRA","ADANIENT","ADANITRANS",
    "ADANIGREEN","ADANIPOWER","TATAPOWER","TORNTPOWER","CESC","IEX","PFC","RECLTD",
    "NHPC","SJVN","IRFC","HUDCO","RVNL","RAILTEL","NBCC","BEL","HAL","BHEL","SAIL",
    "NMDC","MOIL","NATIONALUM","VEDL","HINDCOPPER","RATNAMANI","APL","JINDALSTEL",
    "WELCORP","APLAPOLLO","POLYCAB","HAVELLS","VOLTAS","BLUESTAR","CROMPTON","KEI",
    "SCHNEIDER","ABB","SIEMENS","CUMMINSIND","THERMAX","KSB","GRINDWELL","TIMKEN",
    "SCHAEFFLER","SKFINDIA","FINCABLES","GMMPFAUDLR","TDPOWERSYS","ELECON","ISGEC",
]

# ── STAGE 2 & 3: TECHNICAL SCREENER + MOMENTUM SCORER ────────────────────────
def compute_indicators(sym: str) -> dict | None:
    """
    Download 60 days of daily OHLCV for one symbol.
    Compute: RSI-14, EMA20/EMA50, volume ratio, ATR%, price.
    Returns None if data insufficient or stock fails liquidity filter.
    """
    try:
        ticker = yf.Ticker(sym + ".NS")
        df = ticker.history(period="60d", interval="1d", timeout=10)
        if df is None or len(df) < 20:
            return None

        close = df["Close"].values
        volume = df["Volume"].values
        high = df["High"].values
        low = df["Low"].values
        price = float(close[-1])

        # Stage 1: liquidity
        avg_vol = float(np.mean(volume[-20:]))
        if price < MIN_PRICE or price > MAX_PRICE or avg_vol < MIN_AVG_VOLUME:
            return None

        # RSI-14
        delta = np.diff(close)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        avg_gain = np.mean(gain[-14:])
        avg_loss = np.mean(loss[-14:])
        rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100

        # EMA trend
        ema20 = float(pd.Series(close).ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(pd.Series(close).ewm(span=50, adjust=False).mean().iloc[-1])
        if price > ema20 > ema50:
            trend = "Uptrend"
        elif price < ema20 < ema50:
            trend = "Downtrend"
        elif price > ema50 and ema20 > ema50:
            trend = "Recovering"
        else:
            trend = "Sideways"

        # Volume ratio (today vs 20d avg)
        vol_ratio = float(volume[-1]) / avg_vol if avg_vol > 0 else 1.0

        # ATR% (volatility proxy — good for swing)
        tr = np.maximum(high[1:]-low[1:],
             np.maximum(abs(high[1:]-close[:-1]), abs(low[1:]-close[:-1])))
        atr = float(np.mean(tr[-14:]))
        atr_pct = atr / price * 100

        # ── MOMENTUM SCORE (0–100) ──────────────────────────────────────────
        # Components:
        #  RSI score   — oversold (30-45) = bullish setup = high score
        #  Trend score — Uptrend/Recovering > Sideways > Downtrend
        #  Volume score— surge (>1.5x) = institutional interest
        #  ATR score   — sweet spot 1.5–4% daily range (good for swing)

        # RSI: best buy zone 28–42, best sell zone 62–75
        if 28 <= rsi <= 42:
            rsi_score = 90 - (rsi - 28) * 2        # buy candidate
            direction_hint = "BUY"
        elif 62 <= rsi <= 75:
            rsi_score = 90 - (rsi - 62) * 2        # sell/short candidate
            direction_hint = "SELL"
        elif 42 < rsi < 55:
            rsi_score = 30                           # neutral
            direction_hint = "HOLD"
        else:
            rsi_score = 10
            direction_hint = "HOLD"

        trend_score = {"Uptrend":85,"Recovering":65,"Sideways":20,"Downtrend":40}.get(trend, 20)
        vol_score   = min(100, vol_ratio * 40)       # 2.5x volume = 100 pts
        atr_score   = 100 - abs(atr_pct - 2.5) * 15  # ideal ~2.5% daily range
        atr_score   = max(0, min(100, atr_score))

        composite = (rsi_score*0.35 + trend_score*0.30 + vol_score*0.20 + atr_score*0.15)

        return {
            "sym": sym,
            "price": round(price, 2),
            "rsi": round(rsi, 1),
            "trend": trend,
            "vol_ratio": round(vol_ratio, 2),
            "atr_pct": round(atr_pct, 2),
            "ema20": round(ema20, 2),
            "ema50": round(ema50, 2),
            "score": round(composite, 1),
            "direction_hint": direction_hint,
            "avg_vol": int(avg_vol),
        }
    except Exception:
        return None

def run_screener(universe: list[str], con) -> list[dict]:
    """
    Stage 2+3: Screen entire universe, return top N by momentum score.
    Batched with delays to be polite to Yahoo Finance.
    Caches results in DB to avoid re-fetching within same cycle.
    """
    log.info(f"Screening {len(universe)} symbols…")
    results = []
    batch_size = 20
    now = datetime.now().isoformat()

    for i, sym in enumerate(universe):
        # Check cache (fresh within 4 hours)
        cached = con.execute(
            "SELECT score,rsi,trend,vol_ratio,atr_pct,price FROM screener_cache WHERE sym=? AND updated_at>?",
            (sym, (datetime.now()-timedelta(hours=4)).isoformat())
        ).fetchone()

        if cached:
            score, rsi, trend, vol_ratio, atr_pct, price = cached
            if score >= 40:  # only keep cache hits above threshold
                direction_hint = "BUY" if rsi < 45 else ("SELL" if rsi > 60 else "HOLD")
                results.append({"sym":sym,"score":score,"rsi":rsi,"trend":trend,
                                 "vol_ratio":vol_ratio,"atr_pct":atr_pct,"price":price,
                                 "direction_hint":direction_hint})
            continue

        data = compute_indicators(sym)
        if data:
            con.execute("""
                INSERT OR REPLACE INTO screener_cache
                (sym,score,rsi,trend,vol_ratio,atr_pct,price,updated_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, (sym, data["score"], data["rsi"], data["trend"],
                  data["vol_ratio"], data["atr_pct"], data["price"], now))
            con.commit()
            if data["score"] >= 40:
                results.append(data)

        # Rate limiting — batch pause
        if (i+1) % batch_size == 0:
            time.sleep(2)
            log.info(f"  Screened {i+1}/{len(universe)} — {len(results)} candidates so far…")

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"Screener done: {len(results)} candidates → top {TOP_N_FOR_CLAUDE} to Claude")
    return results[:TOP_N_FOR_CLAUDE]

# ── STAGE 4: CREDIT-EFFICIENT CLAUDE ─────────────────────────────────────────
class Claude:
    def __init__(self):
        from anthropic import Anthropic
        self.client = Anthropic()
        self.hold_cache = {}
        self.total_tokens = 0

    def get_signal(self, stock: dict, risk_remaining: float) -> dict:
        if self.hold_cache.get(stock["sym"], 0) > 0:
            self.hold_cache[stock["sym"]] -= 1
            return {"signal":"SKIP","reason":"hold cache","tokens":0,"model":"none"}

        prompt = (
            f"NSE swing trade. JSON only, no markdown.\n"
            f"{stock['sym']} ₹{stock['price']} RSI:{stock['rsi']} "
            f"trend:{stock['trend']} vol:{stock['vol_ratio']}x ATR:{stock['atr_pct']}% "
            f"algo_score:{stock['score']}/100 hint:{stock['direction_hint']}\n"
            f"Risk:₹{risk_remaining:.0f} min_rr:{MIN_RR} hold:2-5days\n"
            f"{{\"signal\":\"BUY\"|\"SELL\"|\"HOLD\","
            f"\"entry\":N,\"sl\":N,\"target\":N,\"rr\":N,\"reason\":\"<8 words\"}}"
        )
        resp = self.client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[{"role":"user","content":prompt}]
        )
        tok = resp.usage.input_tokens + resp.usage.output_tokens
        self.total_tokens += tok
        raw = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        data = json.loads(raw)
        if data.get("signal") == "HOLD":
            self.hold_cache[stock["sym"]] = HOLD_SKIP_CYCLES
        data["tokens"] = tok
        data["model"] = "haiku"
        return data

    def check_exit(self, trade: dict, price: float) -> dict:
        pnl = (price-trade["entry"])*trade["qty"] if trade["direction"]=="BUY" \
              else (trade["entry"]-price)*trade["qty"]
        model = "claude-sonnet-4-20250514" if pnl > trade["risk_amt"] else "claude-haiku-4-5-20251001"
        resp = self.client.messages.create(
            model=model, max_tokens=60,
            messages=[{"role":"user","content":
                f"Exit? JSON {{\"action\":\"EXIT\"|\"HOLD\",\"reason\":\"<6 words\"}}\n"
                f"{trade['direction']} {trade['sym']} entry:₹{trade['entry']} "
                f"sl:₹{trade['sl']} tgt:₹{trade['target']} now:₹{price:.2f} pnl:₹{pnl:.0f}"}]
        )
        tok = resp.usage.input_tokens + resp.usage.output_tokens
        self.total_tokens += tok
        raw = resp.content[0].text.strip().replace("```json","").replace("```","")
        data = json.loads(raw.strip())
        data["tokens"] = tok
        data["pnl"] = pnl
        return data

# ── HELPERS ───────────────────────────────────────────────────────────────────
def week_start():
    today = date.today()
    return (today - timedelta(days=today.weekday())).isoformat()

def get_stats(con):
    ws = week_start()
    con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)",(ws,))
    con.commit()
    r = con.execute("SELECT pnl,risk_used,wins,losses,total_tokens FROM weekly_stats WHERE week_start=?",(ws,)).fetchone()
    return {"pnl":r[0],"risk_used":r[1],"wins":r[2],"losses":r[3],"tokens":r[4]}

def upd_stats(con, **kw):
    ws = week_start()
    sets = ",".join(f"{k}={k}+?" for k in kw)
    con.execute(f"UPDATE weekly_stats SET {sets} WHERE week_start=?", (*kw.values(), ws))
    con.commit()

def size_qty(entry, sl, budget):
    rp = abs(entry - sl)
    return max(1, int(min(budget, RISK_PER_TRADE) / rp)) if rp > 0 else 0

def close_trade(con, t, reason, custom_pnl=None):
    cols = ["id","sym","direction","entry","sl","target","qty","risk_amt","target_gain","rr"]
    if isinstance(t, sqlite3.Row): t = dict(zip(cols+["status","pnl","score","opened_at","closed_at","claude_reason"], t))
    pnl = custom_pnl if custom_pnl is not None else \
          (t["target_gain"] if "TARGET" in reason else -t["risk_amt"])
    status = "win" if pnl > 0 else "loss"
    con.execute("UPDATE trades SET status=?,pnl=?,closed_at=? WHERE id=?",
                (status, round(pnl,2), datetime.now().isoformat(), t["id"]))
    con.commit()
    upd_stats(con, pnl=round(pnl,2),
              risk_used=abs(pnl) if pnl<0 else 0,
              wins=1 if pnl>0 else 0,
              losses=0 if pnl>0 else 1)
    log.info(f"  CLOSED {t['sym']} [{reason}] P&L ₹{pnl:+.0f}")

# ── MAIN LOOP ─────────────────────────────────────────────────────────────────
def run():
    log.info("══════════════════════════════════════════")
    log.info("  ClaudeBot v2 · Full NSE Scanner · PAPER")
    log.info(f"  Capital ₹{CAPITAL:,} · Risk ₹{MAX_WEEKLY_RISK:,}/week")
    log.info("══════════════════════════════════════════")

    con = init_db()
    ai  = Claude()
    cycle = 0

    while True:
        cycle += 1
        log.info(f"\n══ Cycle #{cycle} — {datetime.now().strftime('%H:%M:%S')} ══")
        stats = get_stats(con)

        # ── MANAGE OPEN POSITIONS ─────────────────────────────────────────
        open_trades = con.execute("SELECT * FROM trades WHERE status='open'").fetchall()
        for t in open_trades:
            t = dict(zip(["id","sym","direction","entry","sl","target","qty",
                           "risk_amt","target_gain","rr","status","pnl","score",
                           "opened_at","closed_at","claude_reason"], t))
            # Fetch live price
            try:
                tk = yf.Ticker(t["sym"]+".NS")
                hist = tk.history(period="1d", interval="5m", timeout=8)
                price = float(hist["Close"].iloc[-1]) if len(hist) > 0 else t["entry"]
            except Exception:
                price = t["entry"]

            # Hard SL / target
            if t["direction"] == "BUY":
                if price <= t["sl"]:   close_trade(con, t, "SL_HIT"); continue
                if price >= t["target"]: close_trade(con, t, "TARGET_HIT"); continue
            else:
                if price >= t["sl"]:   close_trade(con, t, "SL_HIT"); continue
                if price <= t["target"]: close_trade(con, t, "TARGET_HIT"); continue

            # Claude exit check every 3 cycles
            if cycle % 3 == 0:
                try:
                    ex = ai.check_exit(t, price)
                    upd_stats(con, total_tokens=ex["tokens"])
                    if ex["action"] == "EXIT":
                        close_trade(con, t, "CLAUDE_EXIT", round(ex["pnl"],2))
                    else:
                        log.info(f"  HOLD {t['sym']} @ ₹{price:.0f} P&L:₹{ex['pnl']:.0f} — {ex['reason']}")
                except Exception as e:
                    log.warning(f"  Exit check {t['sym']}: {e}")

        # ── SCAN MARKET ───────────────────────────────────────────────────
        stats = get_stats(con)
        open_count = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        if stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning("  Weekly risk limit hit — no new entries this week")
        elif open_count >= MAX_OPEN:
            log.info(f"  {open_count}/{MAX_OPEN} positions open — holding")
        else:
            universe = fetch_nse_universe()
            candidates = run_screener(universe, con)

            risk_remaining = MAX_WEEKLY_RISK - stats["risk_used"]
            for stock in candidates:
                open_count = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
                if open_count >= MAX_OPEN: break
                if con.execute("SELECT 1 FROM trades WHERE sym=? AND status='open'",(stock["sym"],)).fetchone(): continue

                try:
                    sig = ai.get_signal(stock, risk_remaining)
                    con.execute(
                        "INSERT INTO scan_log (ts,sym,signal,rr,skipped,model_used,tokens_used,algo_score) VALUES (?,?,?,?,?,?,?,?)",
                        (datetime.now().isoformat(), stock["sym"],
                         sig.get("signal","?"), sig.get("rr",0),
                         1 if sig["signal"] in ("SKIP","HOLD") else 0,
                         sig.get("model","?"), sig.get("tokens",0), stock["score"])
                    )
                    con.commit()
                    upd_stats(con, total_tokens=sig.get("tokens",0))

                    if sig.get("signal") in ("BUY","SELL") and sig.get("rr",0) >= MIN_RR:
                        qty = size_qty(sig["entry"], sig["sl"], risk_remaining)
                        if qty < 1: continue
                        actual_risk = round(qty * abs(sig["entry"]-sig["sl"]), 2)
                        tid = f"{stock['sym']}_{int(time.time())}"
                        con.execute("""
                            INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,'open',0,?,?,NULL,?)
                        """, (tid, stock["sym"], sig["signal"],
                              sig["entry"], sig["sl"], sig["target"],
                              qty, actual_risk,
                              round(qty*abs(sig["entry"]-sig["target"]),2),
                              sig["rr"], stock["score"],
                              datetime.now().isoformat(), sig.get("reason","")))
                        con.commit()
                        risk_remaining -= actual_risk
                        log.info(
                            f"  ◈ {sig['signal']} {stock['sym']} score:{stock['score']} "
                            f"qty:{qty} @ ₹{sig['entry']} SL:₹{sig['sl']} TGT:₹{sig['target']} "
                            f"R:R:{sig['rr']}x risk:₹{actual_risk} [{sig['tokens']}tok]"
                        )
                    else:
                        log.info(f"  {stock['sym']} score:{stock['score']} → {sig.get('signal')} (R:R {sig.get('rr',0)}x)")

                except Exception as e:
                    log.error(f"  Signal error {stock['sym']}: {e}")
                time.sleep(1)

        # ── STATUS ────────────────────────────────────────────────────────
        stats = get_stats(con)
        oc = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        total = stats["wins"]+stats["losses"]
        wr = round(stats["wins"]/total*100) if total else 0
        log.info(
            f"\n  P&L:₹{stats['pnl']:+.0f}  Risk:₹{stats['risk_used']:.0f}/₹{MAX_WEEKLY_RISK}"
            f"  W/L:{stats['wins']}/{stats['losses']}({wr}%)  Open:{oc}  Tokens:{stats['tokens']:,}"
        )
        log.info(f"  Next scan in {SCAN_INTERVAL}s…\n")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    run()
