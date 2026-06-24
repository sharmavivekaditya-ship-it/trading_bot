"""
First-Orbit Trader PRO — Historical Backtester
================================================
Runs the EXACT strategy logic from bot.py over historical data, walking
bar-by-bar (no look-ahead bias), and compares it against a simple
Daily+Weekly+Monthly RSI>60 baseline over the same period and universe.

RUN THIS WHERE YAHOO FINANCE IS REACHABLE (your machine or Railway):
    pip install yfinance curl_cffi pandas numpy
    python3 backtest.py

It cannot run in Claude's sandbox (Yahoo is not in the network allowlist),
which is why it's a standalone script you run yourself.

IMPORTANT HONESTY NOTE: A backtest cannot prove future performance. Its job
is to (a) CHEAPLY REJECT a broken strategy, (b) reveal the STRUCTURE of the
risk (drawdown, realized R:R), and (c) expose whether exits fire before the
target is reached. Treat a good result as "not yet falsified," not "proven."
"""

import sys, time
import numpy as np
import pandas as pd

try:
    from curl_cffi import requests as cr
    SESSION = cr.Session(impersonate="chrome110")
except Exception:
    SESSION = None
import yfinance as yf

# ── STRATEGY CONFIG (mirrors bot.py exactly) ──────────────────────────────────
WEEKLY_RSI_MIN  = 60
DAILY_RSI_MIN   = 57
DAILY_RSI_MAX   = 67
MIN_ADX         = 20.0
ATR_PERIOD      = 14
ATR_STOP_MULT   = 1.0
ATR_TARGET_MULT = 2.0
DAILY_RSI_EXIT  = 52
WEEKLY_RSI_EXIT = 52
MIN_DAYS_DIV    = 2
RISK_PER_TRADE  = 800
MAX_OPEN        = 5
CAPITAL         = 100_000

# Costs — realistic NSE delivery round-trip (brokerage + STT + charges + slippage)
COST_PCT        = 0.0015   # ~0.15% per side ≈ 0.3% round trip (conservative)

# Backtest window & universe
YEARS           = 3
# A representative slice of liquid Nifty large/mid caps. Expand as you like.
UNIVERSE = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
    "BHARTIARTL","KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN",
    "SUNPHARMA","ULTRACEMCO","WIPRO","NESTLEIND","BAJFINANCE","HCLTECH","NTPC",
    "POWERGRID","TATAMOTORS","TATASTEEL","ADANIENT","ADANIGREEN","ADANIENSOL",
    "JSWSTEEL","GRASIM","HINDALCO","CIPLA","DRREDDY","BAJAJFINSV","COALINDIA",
    "BPCL","DIVISLAB","BRITANNIA","EICHERMOT","HEROMOTOCO","SHYAMMETL","POLYCAB",
    "MOTHERSON","KPRMILL","LAURUSLABS","TATACOMM","APOLLOHOSP","ZYDUSLIFE",
    "TORNTPHARM","SAILIFE","CGPOWER","KIMS","ACMESOLAR","TATATECH","EXIDEIND",
]


# ── INDICATORS (identical to bot.py) ──────────────────────────────────────────
def rsi_series(close, period=14):
    close = np.asarray(close, float)
    d = np.diff(close)
    g = np.where(d > 0, d, 0.0)
    l = np.where(d < 0, -d, 0.0)
    if len(g) < period:
        return []
    ag, al = g[:period].mean(), l[:period].mean()
    vals = [np.nan] * period
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
        vals.append(100.0 - 100.0 / (1.0 + ag / al) if al > 0 else 100.0)
    return [np.nan] + vals  # align to close length


def atr_at(high, low, close, i, period=14):
    if i < period + 1:
        return None
    h = high[i - period + 1:i + 1]
    l = low[i - period + 1:i + 1]
    cprev = close[i - period:i]
    tr = np.maximum(h - l, np.maximum(np.abs(h - cprev), np.abs(l - cprev)))
    return float(tr.mean())


def adx_at(high, low, close, i, period=14):
    if i < period * 2:
        return 0.0
    h = high[:i + 1]; l = low[:i + 1]; c = close[:i + 1]
    up = h[1:] - h[:-1]; down = l[:-1] - l[1:]
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    atr_s = pd.Series(tr).rolling(period).mean()
    pdi = 100 * pd.Series(plus_dm).rolling(period).mean() / atr_s
    mdi = 100 * pd.Series(minus_dm).rolling(period).mean() / atr_s
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    v = dx.rolling(period).mean().iloc[-1]
    return float(v) if pd.notna(v) else 0.0


def ema_at(close, span, i):
    return float(pd.Series(close[:i + 1]).ewm(span=span, adjust=False).mean().iloc[-1])


# ── DATA ──────────────────────────────────────────────────────────────────────
def fetch(sym):
    try:
        kw = dict(period=f"{YEARS}y", interval="1d", auto_adjust=True, timeout=15)
        if SESSION:
            df = yf.Ticker(sym + ".NS", session=SESSION).history(**kw)
        else:
            df = yf.Ticker(sym + ".NS").history(**kw)
        if df is None or len(df) < 260:
            return None
        df = df.dropna()
        return df
    except Exception as e:
        print(f"  {sym}: fetch failed ({e})")
        return None


def weekly_rsi_aligned(df):
    """Weekly RSI resampled, forward-filled to daily index (no look-ahead:
    uses only completed weeks)."""
    wk = df["Close"].resample("W").last().dropna()
    wr = rsi_series(wk.values, 14)
    wk_rsi = pd.Series(wr, index=wk.index)
    return wk_rsi.reindex(df.index, method="ffill")


def monthly_rsi_aligned(df):
    mo = df["Close"].resample("ME").last().dropna()
    mr = rsi_series(mo.values, 14)
    mo_rsi = pd.Series(mr, index=mo.index)
    return mo_rsi.reindex(df.index, method="ffill")


# ── BACKTEST ENGINES ──────────────────────────────────────────────────────────
def backtest_strategy(data, strategy="full"):
    """
    Walk every trading day across the whole universe. Portfolio-level:
    max MAX_OPEN concurrent positions, picked by score when multiple qualify.
    strategy='full'  → exact bot.py logic
    strategy='rsi'   → simple Daily+Weekly+Monthly RSI>60 buy, same exits
    """
    # Build a unified date index
    all_dates = sorted(set().union(*[set(df.index) for df in data.values()]))
    open_pos = {}          # sym -> dict
    closed = []            # list of trade dicts
    equity = CAPITAL
    equity_curve = []

    # Precompute indicators per symbol
    ind = {}
    for sym, df in data.items():
        c = df["Close"].values; h = df["High"].values; l = df["Low"].values
        ind[sym] = {
            "close": c, "high": h, "low": l,
            "drsi": rsi_series(c, 14),
            "wrsi": weekly_rsi_aligned(df),
            "mrsi": monthly_rsi_aligned(df),
            "idx": {d: k for k, d in enumerate(df.index)},
        }

    for date in all_dates:
        # 1) Manage open positions (exits) — check stop/target/RSI fade
        for sym in list(open_pos.keys()):
            if date not in ind[sym]["idx"]:
                continue
            i = ind[sym]["idx"][date]
            p = open_pos[sym]
            px = ind[sym]["close"][i]
            hi = ind[sym]["high"][i]; lo = ind[sym]["low"][i]
            days_held = p["days"]
            reason = None
            # Intrabar: stop and target. Conservative — if both touched same day,
            # assume stop hit first (worst case).
            if lo <= p["sl"]:
                reason, exit_px = "HARD_STOP", p["sl"]
            elif hi >= p["target"]:
                reason, exit_px = "TARGET_HIT", p["target"]
            else:
                dr = ind[sym]["drsi"][i] if i < len(ind[sym]["drsi"]) else np.nan
                wr = ind[sym]["wrsi"].iloc[i] if i < len(ind[sym]["wrsi"]) else np.nan
                if days_held >= 1 and not np.isnan(dr) and dr < DAILY_RSI_EXIT:
                    reason, exit_px = "DAILY_RSI", px
                elif days_held >= 1 and not np.isnan(wr) and 0 < wr < WEEKLY_RSI_EXIT:
                    reason, exit_px = "WEEKLY_RSI", px
            if reason:
                gross = (exit_px - p["entry"]) * p["qty"]
                cost = (p["entry"] + exit_px) * p["qty"] * COST_PCT
                pnl = gross - cost
                equity += pnl
                closed.append({"sym": sym, "entry": p["entry"], "exit": exit_px,
                               "qty": p["qty"], "pnl": pnl, "reason": reason,
                               "days": days_held, "score": p["score"]})
                del open_pos[sym]
            else:
                p["days"] += 1

        # 2) Look for entries if slots open
        slots = MAX_OPEN - len(open_pos)
        if slots > 0:
            candidates = []
            for sym, df in data.items():
                if sym in open_pos or date not in ind[sym]["idx"]:
                    continue
                i = ind[sym]["idx"][date]
                if i < 60:   # need history for EMA50/ADX
                    continue
                c = ind[sym]["close"]; h = ind[sym]["high"]; l = ind[sym]["low"]
                drsi = ind[sym]["drsi"][i] if i < len(ind[sym]["drsi"]) else np.nan
                wrsi = ind[sym]["wrsi"].iloc[i]
                mrsi = ind[sym]["mrsi"].iloc[i]
                price = c[i]
                if np.isnan(drsi) or np.isnan(wrsi):
                    continue

                if strategy == "full":
                    # Exact bot.py entry gates
                    if wrsi < WEEKLY_RSI_MIN: continue
                    if not (DAILY_RSI_MIN <= drsi <= DAILY_RSI_MAX): continue
                    e9, e21, e50 = ema_at(c, 9, i), ema_at(c, 21, i), ema_at(c, 50, i)
                    if not (e9 > e21 > e50): continue
                    if price < e9 * 0.99: continue
                    if adx_at(h, l, c, i, ATR_PERIOD) < MIN_ADX: continue
                    atr_val = atr_at(h, l, c, i, ATR_PERIOD)
                    if not atr_val or atr_val <= 0: continue
                    # Score (mirrors bot.py bell-curve)
                    w_q = max(0, 5 - abs(wrsi - 62)) / 5
                    d_q = max(0, 5 - abs(drsi - 62)) / 5
                    score = (w_q + d_q) * 10
                    candidates.append((score, sym, i, price, atr_val))
                else:  # simple RSI baseline
                    if np.isnan(mrsi): continue
                    if drsi > 60 and wrsi > 60 and mrsi > 60:
                        atr_val = atr_at(h, l, c, i, ATR_PERIOD)
                        if not atr_val or atr_val <= 0: continue
                        score = drsi  # rank by daily RSI
                        candidates.append((score, sym, i, price, atr_val))

            candidates.sort(reverse=True)
            for score, sym, i, price, atr_val in candidates[:slots]:
                sl = price - ATR_STOP_MULT * atr_val
                target = price + ATR_TARGET_MULT * atr_val
                rp = price - sl
                qty = max(1, int(RISK_PER_TRADE / rp))
                open_pos[sym] = {"entry": price, "sl": sl, "target": target,
                                 "qty": qty, "days": 0, "score": round(score, 1)}

        equity_curve.append((date, equity + sum(
            (ind[s]["close"][ind[s]["idx"][date]] - p["entry"]) * p["qty"]
            for s, p in open_pos.items() if date in ind[s]["idx"]
        )))

    return closed, equity_curve


def report(name, closed, equity_curve):
    if not closed:
        print(f"\n{name}: no trades.")
        return
    pnls = np.array([t["pnl"] for t in closed])
    wins = pnls[pnls > 0]; losses = pnls[pnls <= 0]
    total = pnls.sum()
    wr = len(wins) / len(pnls) * 100
    avg_w = wins.mean() if len(wins) else 0
    avg_l = losses.mean() if len(losses) else 0
    pf = wins.sum() / abs(losses.sum()) if losses.sum() != 0 else float("inf")
    eq = np.array([e for _, e in equity_curve])
    peak = np.maximum.accumulate(eq)
    dd = ((eq - peak) / peak * 100).min() if len(eq) else 0
    ret_pct = (eq[-1] - CAPITAL) / CAPITAL * 100 if len(eq) else 0

    reasons = {}
    for t in closed:
        reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1

    print(f"\n{'='*60}\n{name}\n{'='*60}")
    print(f"  Trades            : {len(closed)}")
    print(f"  Win rate          : {wr:.1f}%  ({len(wins)}W / {len(losses)}L)")
    print(f"  Total P&L         : Rs.{total:,.0f}")
    print(f"  Return on capital : {ret_pct:+.1f}%  over {YEARS}y")
    print(f"  Avg win / loss    : +Rs.{avg_w:,.0f} / Rs.{avg_l:,.0f}")
    print(f"  Realized R:R      : {abs(avg_w/avg_l):.2f}" if avg_l else "  Realized R:R      : n/a")
    print(f"  Profit factor     : {pf:.2f}")
    print(f"  Max drawdown      : {dd:.1f}%")
    print(f"  Exit breakdown    : {reasons}")
    # The structural question: how often does TARGET actually get hit?
    th = reasons.get("TARGET_HIT", 0)
    print(f"  >> TARGET_HIT rate: {th/len(closed)*100:.1f}%  "
          f"(if near 0, the target is unreachable / exits fire first)")


def main():
    print("Fetching historical data (this takes a few minutes)...")
    data = {}
    for k, sym in enumerate(UNIVERSE):
        df = fetch(sym)
        if df is not None and len(df) > 260:
            data[sym] = df
        if (k + 1) % 10 == 0:
            print(f"  ...{k+1}/{len(UNIVERSE)} fetched")
        time.sleep(0.3)
    print(f"\nUsable symbols: {len(data)}/{len(UNIVERSE)}")
    if len(data) < 10:
        print("Not enough data — aborting. (Is Yahoo reachable here?)")
        sys.exit(1)

    print("\nRunning FULL strategy backtest...")
    c1, e1 = backtest_strategy(data, "full")
    print("Running SIMPLE RSI baseline backtest...")
    c2, e2 = backtest_strategy(data, "rsi")

    report("FULL STRATEGY (RSI+EMA+ADX, 1xATR stop / 2xATR target)", c1, e1)
    report("SIMPLE BASELINE (Daily+Weekly+Monthly RSI > 60)", c2, e2)

    print(f"\n{'='*60}\nHONEST CAVEATS\n{'='*60}")
    print("  - Costs modeled at ~0.3% round trip. Real slippage may be worse.")
    print("  - Survivorship: universe is today's liquid names; delisted losers")
    print("    are absent, so BOTH results are optimistic.")
    print("  - One historical period = one regime sample. Not a guarantee.")
    print("  - A good result here means 'not yet falsified', NOT 'proven'.")


if __name__ == "__main__":
    main()
