"""
ClaudeBot — NSE Swing Trading Bot
Always-running, credit-efficient autonomous paper trader
"""
import time, json, sqlite3, os, logging
from datetime import datetime, date
from anthropic import Anthropic

# ── CONFIG ────────────────────────────────────────────────────────────────────
CAPITAL        = 100_000
MAX_WEEKLY_RISK = 3_000
RISK_PER_TRADE  = 800
MIN_RR          = 2.0
MAX_OPEN        = 3
SCAN_INTERVAL   = 60        # seconds between full scan cycles
HOLD_SKIP_CYCLES = 4        # skip re-scanning a HOLD for N cycles

WATCHLIST = [
    {"sym": "RELIANCE",   "sector": "Energy",  "rsi": 42, "trend": "Uptrend",   "vol": "Above average", "price": 2847},
    {"sym": "TCS",        "sector": "IT",       "rsi": 55, "trend": "Sideways",  "vol": "Average",       "price": 3920},
    {"sym": "HDFCBANK",   "sector": "Banking",  "rsi": 36, "trend": "Uptrend",   "vol": "Above average", "price": 1680},
    {"sym": "INFY",       "sector": "IT",       "rsi": 61, "trend": "Breakout",  "vol": "Spike (3x+)",   "price": 1745},
    {"sym": "TATAMOTORS", "sector": "Auto",     "rsi": 29, "trend": "Downtrend", "vol": "Average",       "price": 924 },
    {"sym": "WIPRO",      "sector": "IT",       "rsi": 48, "trend": "Sideways",  "vol": "Average",       "price": 487 },
    {"sym": "AXISBANK",   "sector": "Banking",  "rsi": 34, "trend": "Uptrend",   "vol": "Above average", "price": 1102},
    {"sym": "SUNPHARMA",  "sector": "Pharma",   "rsi": 58, "trend": "Breakout",  "vol": "Above average", "price": 1634},
]

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler("claudebot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("claudebot")

# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect("trades.db")
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        sym TEXT, direction TEXT,
        entry REAL, sl REAL, target REAL,
        qty INTEGER, risk_amt REAL, target_gain REAL,
        rr REAL, status TEXT DEFAULT 'open',
        pnl REAL DEFAULT 0,
        opened_at TEXT, closed_at TEXT,
        claude_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS scan_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT, sym TEXT,
        signal TEXT, rr REAL, skipped INTEGER DEFAULT 0,
        model_used TEXT, tokens_used INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS weekly_stats (
        week_start TEXT PRIMARY KEY,
        pnl REAL DEFAULT 0,
        risk_used REAL DEFAULT 0,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        total_tokens INTEGER DEFAULT 0
    );
    """)
    con.commit()
    return con

# ── CREDIT-EFFICIENT CLAUDE CALLER ───────────────────────────────────────────
class CreditEfficientClaude:
    """
    Two-tier model usage:
      - Haiku  → routine scan signals (cheap, fast)
      - Sonnet → exit decisions on profitable trades (accurate)
    Pre-filter: skip Claude entirely if rules don't align.
    Hold cache: skip re-scanning HOLDs for N cycles.
    """
    def __init__(self):
        self.client    = Anthropic()          # reads ANTHROPIC_API_KEY from env
        self.hold_cache = {}                  # sym -> cycles_remaining
        self.total_tokens = 0

    # ── PRE-FILTER (zero API cost) ─────────────────────────────────────────
    def passes_prefilter(self, stock: dict) -> tuple[bool, str]:
        rsi   = stock.get("rsi", 50)
        trend = stock.get("trend", "")
        vol   = stock.get("vol", "")

        if trend == "Sideways" and (35 < rsi < 62):
            return False, f"RSI {rsi} + Sideways — no setup"
        if vol == "Below average":
            return False, "Low volume — skip"
        if self.hold_cache.get(stock["sym"], 0) > 0:
            self.hold_cache[stock["sym"]] -= 1
            return False, f"HOLD cache ({self.hold_cache[stock['sym']]} cycles left)"
        return True, "passes"

    # ── SCAN SIGNAL (Haiku) ────────────────────────────────────────────────
    def get_signal(self, stock: dict, risk_remaining: float) -> dict:
        ok, reason = self.passes_prefilter(stock)
        if not ok:
            log.info(f"  SKIP {stock['sym']}: {reason}")
            return {"signal": "SKIP", "reason": reason, "tokens": 0, "model": "none"}

        prompt = (
            f"NSE swing trade signal. Respond ONLY valid JSON, no markdown.\n"
            f"Stock: {stock['sym']} ₹{stock['price']} RSI:{stock['rsi']} "
            f"trend:{stock['trend']} vol:{stock['vol']} sector:{stock['sector']}\n"
            f"Risk available: ₹{risk_remaining:.0f} | Min R:R: {MIN_RR}\n"
            f"Rules: BUY if RSI<40+uptrend, SELL if RSI>62+downtrend, else HOLD.\n"
            f"JSON: {{\"signal\":\"BUY\"|\"SELL\"|\"HOLD\","
            f"\"entry\":number,\"sl\":number,\"target\":number,"
            f"\"rr\":number,\"reason\":\"<10 words\"}}"
        )

        resp = self.client.messages.create(
            model="claude-haiku-4-5-20251001",        # cheapest model
            max_tokens=120,
            messages=[{"role": "user", "content": prompt}]
        )
        tokens = resp.usage.input_tokens + resp.usage.output_tokens
        self.total_tokens += tokens
        raw  = resp.content[0].text.strip()
        data = json.loads(raw.replace("```json","").replace("```","").strip())

        if data.get("signal") == "HOLD":
            self.hold_cache[stock["sym"]] = HOLD_SKIP_CYCLES

        data["tokens"] = tokens
        data["model"]  = "haiku"
        return data

    # ── EXIT CHECK (Sonnet — only when trade is profitable) ───────────────
    def check_exit(self, trade: dict, current_price: float) -> dict:
        pnl = (current_price - trade["entry"]) * trade["qty"] \
              if trade["direction"] == "BUY" \
              else (trade["entry"] - current_price) * trade["qty"]

        # Only escalate to Sonnet if trade is in profit and worth re-evaluating
        if pnl < trade["risk_amt"] * 0.5:
            # Use Haiku for small/negative P&L exit checks
            model = "claude-haiku-4-5-20251001"
            max_tok = 60
        else:
            model = "claude-sonnet-4-20250514"
            max_tok = 80

        prompt = (
            f"Exit decision. JSON only: {{\"action\":\"EXIT\"|\"HOLD\",\"reason\":\"<8 words\"}}\n"
            f"{trade['direction']} {trade['sym']} entry:₹{trade['entry']} "
            f"sl:₹{trade['sl']} target:₹{trade['target']} "
            f"current:₹{current_price:.2f} pnl:₹{pnl:.0f}"
        )
        resp = self.client.messages.create(
            model=model, max_tokens=max_tok,
            messages=[{"role": "user", "content": prompt}]
        )
        tokens = resp.usage.input_tokens + resp.usage.output_tokens
        self.total_tokens += tokens
        raw  = resp.content[0].text.strip()
        data = json.loads(raw.replace("```json","").replace("```","").strip())
        data["tokens"] = tokens
        data["model"]  = model.split("-")[1]
        data["pnl"]    = pnl
        return data

# ── POSITION SIZER ────────────────────────────────────────────────────────────
def size_position(entry: float, sl: float, risk_budget: float) -> int:
    risk_per_share = abs(entry - sl)
    if risk_per_share <= 0:
        return 0
    return max(1, int(min(risk_budget, RISK_PER_TRADE) / risk_per_share))

# ── WEEKLY STATS HELPER ───────────────────────────────────────────────────────
def get_week_start():
    today = date.today()
    return (today - __import__('datetime').timedelta(days=today.weekday())).isoformat()

def get_weekly_stats(con):
    ws = get_week_start()
    row = con.execute(
        "SELECT pnl, risk_used, wins, losses, total_tokens FROM weekly_stats WHERE week_start=?",
        (ws,)
    ).fetchone()
    if not row:
        con.execute("INSERT OR IGNORE INTO weekly_stats VALUES (?,0,0,0,0,0)", (ws,))
        con.commit()
        return {"pnl": 0, "risk_used": 0, "wins": 0, "losses": 0, "tokens": 0}
    return {"pnl": row[0], "risk_used": row[1], "wins": row[2], "losses": row[3], "tokens": row[4]}

def update_weekly_stats(con, pnl_delta=0, risk_delta=0, win_delta=0, loss_delta=0, token_delta=0):
    ws = get_week_start()
    con.execute("""
        UPDATE weekly_stats SET
            pnl=pnl+?, risk_used=risk_used+?,
            wins=wins+?, losses=losses+?, total_tokens=total_tokens+?
        WHERE week_start=?
    """, (pnl_delta, risk_delta, win_delta, loss_delta, token_delta, ws))
    con.commit()

# ── MAIN BOT LOOP ─────────────────────────────────────────────────────────────
def run_bot():
    log.info("═══════════════════════════════════════")
    log.info("  ClaudeBot NSE Swing — PAPER MODE")
    log.info(f"  Capital ₹{CAPITAL:,}  |  Max weekly risk ₹{MAX_WEEKLY_RISK:,}")
    log.info("═══════════════════════════════════════")

    con    = init_db()
    ai     = CreditEfficientClaude()
    cycle  = 0

    while True:
        cycle += 1
        log.info(f"\n── Scan cycle #{cycle} ─────────────────────────")
        stats = get_weekly_stats(con)

        # ── 1. MANAGE OPEN POSITIONS ──────────────────────────────────────
        open_trades = con.execute(
            "SELECT * FROM trades WHERE status='open'"
        ).fetchall()
        cols = ["id","sym","direction","entry","sl","target","qty",
                "risk_amt","target_gain","rr","status","pnl",
                "opened_at","closed_at","claude_reason"]
        open_trades = [dict(zip(cols, t)) for t in open_trades]

        for t in open_trades:
            # Simulate price movement (replace with real feed)
            import random
            drift  = 1.002 if t["direction"] == "BUY" else 0.998
            noise  = random.gauss(0, 0.008)
            current = t["entry"] * (drift + noise)

            # Hard SL / target check (no API cost)
            if t["direction"] == "BUY":
                if current <= t["sl"]:
                    _close_trade(con, t, current, "SL_HIT", stats); continue
                if current >= t["target"]:
                    _close_trade(con, t, current, "TARGET_HIT", stats); continue
            else:
                if current >= t["sl"]:
                    _close_trade(con, t, current, "SL_HIT", stats); continue
                if current <= t["target"]:
                    _close_trade(con, t, current, "TARGET_HIT", stats); continue

            # Claude exit check every 5 cycles (further reduces API calls)
            if cycle % 5 == 0:
                try:
                    result = ai.check_exit(t, current)
                    update_weekly_stats(con, token_delta=result["tokens"])
                    log.info(f"  EXIT CHECK {t['sym']}: {result['action']} — {result['reason']} [{result['model']}]")
                    if result["action"] == "EXIT":
                        _close_trade(con, t, current, "CLAUDE_EXIT", stats)
                except Exception as e:
                    log.warning(f"  Exit check error {t['sym']}: {e}")

        # ── 2. SCAN FOR NEW ENTRIES ───────────────────────────────────────
        stats = get_weekly_stats(con)  # refresh after exits
        open_count = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]

        if stats["risk_used"] >= MAX_WEEKLY_RISK:
            log.warning("  Weekly risk limit reached — skipping new entries")
        elif open_count >= MAX_OPEN:
            log.info(f"  Max {MAX_OPEN} positions open — skipping scan")
        else:
            risk_remaining = MAX_WEEKLY_RISK - stats["risk_used"]
            for stock in WATCHLIST:
                open_count = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
                if open_count >= MAX_OPEN: break
                already_open = con.execute(
                    "SELECT 1 FROM trades WHERE sym=? AND status='open'", (stock["sym"],)
                ).fetchone()
                if already_open: continue

                try:
                    sig = ai.get_signal(stock, risk_remaining)
                    con.execute(
                        "INSERT INTO scan_log (ts,sym,signal,rr,skipped,model_used,tokens_used) VALUES (?,?,?,?,?,?,?)",
                        (datetime.now().isoformat(), stock["sym"],
                         sig.get("signal","?"), sig.get("rr",0),
                         1 if sig["signal"] in ("SKIP","HOLD") else 0,
                         sig.get("model","?"), sig.get("tokens",0))
                    )
                    con.commit()
                    update_weekly_stats(con, token_delta=sig.get("tokens",0))

                    if sig["signal"] in ("BUY","SELL") and sig.get("rr",0) >= MIN_RR:
                        qty = size_position(sig["entry"], sig["sl"], risk_remaining)
                        if qty < 1: continue
                        actual_risk = round(qty * abs(sig["entry"] - sig["sl"]), 2)
                        trade_id = f"{stock['sym']}_{int(time.time())}"
                        con.execute("""
                            INSERT INTO trades VALUES (?,?,?,?,?,?,?,?,?,?,'open',0,?,NULL,?)
                        """, (
                            trade_id, stock["sym"], sig["signal"],
                            sig["entry"], sig["sl"], sig["target"],
                            qty, actual_risk,
                            round(qty * abs(sig["entry"] - sig["target"]), 2),
                            sig["rr"], datetime.now().isoformat(), sig.get("reason","")
                        ))
                        con.commit()
                        risk_remaining -= actual_risk
                        log.info(
                            f"  ◈ {sig['signal']} {stock['sym']} qty:{qty} "
                            f"@ ₹{sig['entry']} SL:₹{sig['sl']} TGT:₹{sig['target']} "
                            f"R:R:{sig['rr']}x risk:₹{actual_risk} [{sig['model']} {sig['tokens']}tok]"
                        )
                    elif sig["signal"] not in ("SKIP",):
                        log.info(f"  {sig['signal']} {stock['sym']}: {sig.get('reason','')} [{sig.get('model','?')} {sig.get('tokens',0)}tok]")

                except Exception as e:
                    log.error(f"  Scan error {stock['sym']}: {e}")
                time.sleep(0.5)  # gentle rate limiting

        # ── 3. STATUS SUMMARY ─────────────────────────────────────────────
        stats = get_weekly_stats(con)
        open_c = con.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()[0]
        log.info(
            f"\n  Week P&L: ₹{stats['pnl']:+.0f}  "
            f"Risk used: ₹{stats['risk_used']:.0f}/₹{MAX_WEEKLY_RISK}  "
            f"W/L: {stats['wins']}/{stats['losses']}  "
            f"Open: {open_c}  "
            f"Tokens this week: {stats['tokens']:,}"
        )

        log.info(f"  Sleeping {SCAN_INTERVAL}s until next cycle…\n")
        time.sleep(SCAN_INTERVAL)

def _close_trade(con, t, current_price, reason, stats):
    pnl = round(
        (current_price - t["entry"]) * t["qty"] if t["direction"] == "BUY"
        else (t["entry"] - current_price) * t["qty"], 2
    )
    status = "win" if pnl > 0 else "loss"
    con.execute(
        "UPDATE trades SET status=?, pnl=?, closed_at=? WHERE id=?",
        (status, pnl, datetime.now().isoformat(), t["id"])
    )
    con.commit()
    risk_delta = abs(pnl) if pnl < 0 else 0
    update_weekly_stats(con, pnl_delta=pnl, risk_delta=risk_delta,
                        win_delta=1 if pnl>0 else 0,
                        loss_delta=0 if pnl>0 else 1)
    log.info(f"  CLOSED {t['sym']} {reason}: P&L ₹{pnl:+.0f}")

if __name__ == "__main__":
    run_bot()
