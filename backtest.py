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

# ── COSTS ─────────────────────────────────────────────────────────────────────
# Costs — realistic NSE delivery round-trip (brokerage + STT + charges + slippage)
COST_PCT        = 0.0015   # ~0.15% per side ≈ 0.3% round trip (conservative)

# Backtest window
YEARS           = 3

# ── UNIVERSE: full Nifty 500, fetched live from NSE (same source as bot.py) ────
import io, urllib.request
NIFTY500_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
FALLBACK_UNIVERSE = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","ITC","SBIN",
    "BHARTIARTL","KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN",
    "SUNPHARMA","ULTRACEMCO","WIPRO","NESTLEIND","BAJFINANCE","HCLTECH","NTPC",
    "POWERGRID","TATAMOTORS","TATASTEEL","ADANIENT","ADANIGREEN","ADANIENSOL",
    "JSWSTEEL","GRASIM","HINDALCO","CIPLA","DRREDDY","BAJAJFINSV","COALINDIA",
    "BPCL","DIVISLAB","BRITANNIA","EICHERMOT","HEROMOTOCO","SHYAMMETL","POLYCAB",
    "MOTHERSON","KPRMILL","LAURUSLABS","TATACOMM","APOLLOHOSP","ZYDUSLIFE",
    "TORNTPHARM","SAILIFE","CGPOWER","KIMS","ACMESOLAR","TATATECH","EXIDEIND",
]

def get_universe():
    try:
        import ssl
        # macOS Python often lacks a cert bundle → NSE fetch fails with
        # CERTIFICATE_VERIFY_FAILED. Try certifi; if absent, fall back to an
        # unverified context (this is a public CSV, no sensitive data).
        try:
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
        except Exception:
            ctx = ssl._create_unverified_context()
        req = urllib.request.Request(
            NIFTY500_URL,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://nseindia.com"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            df = pd.read_csv(io.StringIO(r.read().decode("latin-1")))
        col = next(c for c in df.columns if "symbol" in c.lower())
        syms = [s for s in df[col].dropna().str.strip()
                if s and not s.upper().startswith("DUMMY") and len(s) <= 20]
        print(f"Universe: {len(syms)} Nifty 500 symbols (live from NSE)")
        return syms
    except Exception as e:
        print(f"NSE fetch failed ({e}) — using {len(FALLBACK_UNIVERSE)}-stock fallback")
        return FALLBACK_UNIVERSE


# ── STRATEGY PARAMETERS (defaults; grid search overrides these) ───────────────
# Bundled in a dict so the optimizer can swap them cleanly.
DEFAULT_PARAMS = {
    "MIN_ADX":         20.0,
    "ATR_STOP_MULT":   1.0,
    "ATR_TARGET_MULT": 2.0,
    "WEEKLY_RSI_MIN":  60,
    "DAILY_RSI_EXIT":  52,
    "WEEKLY_RSI_EXIT": 52,
}
ATR_PERIOD     = 14
MIN_DAYS_DIV   = 2
RISK_PER_TRADE = 800
MAX_OPEN       = 5
CAPITAL        = 100_000


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
def backtest_strategy(data, ind, strategy="discount", params=None,
                      date_from=None, date_to=None):
    """
    Walk every trading day. Portfolio-level: max MAX_OPEN concurrent positions.
    strategy='discount' → buy strength on prior-week-midline pullback + resumption
    strategy='rsi'      → simple Daily+Weekly+Monthly RSI>60 baseline
    params: dict overriding strategy thresholds (for grid search)
    date_from/date_to: restrict the trading window (for in/out-of-sample split)
    `ind` is the precomputed-indicator dict (built once, reused across grid runs).
    """
    P = params or DEFAULT_PARAMS
    MIN_ADX_P    = P["MIN_ADX"]
    STOP_P       = P["ATR_STOP_MULT"]
    TGT_P        = P["ATR_TARGET_MULT"]
    WK_MIN_P     = P["WEEKLY_RSI_MIN"]
    D_EXIT_P     = P["DAILY_RSI_EXIT"]
    W_EXIT_P     = P["WEEKLY_RSI_EXIT"]

    all_dates = sorted(set().union(*[set(df.index) for df in data.values()]))
    if date_from: all_dates = [d for d in all_dates if d >= date_from]
    if date_to:   all_dates = [d for d in all_dates if d <= date_to]

    open_pos, closed, equity, equity_curve = {}, [], CAPITAL, []

    for date in all_dates:
        # 1) Manage open positions (exits)
        for sym in list(open_pos.keys()):
            if date not in ind[sym]["idx"]:
                continue
            i = ind[sym]["idx"][date]
            p = open_pos[sym]
            px = ind[sym]["close"][i]; hi = ind[sym]["high"][i]; lo = ind[sym]["low"][i]
            days_held = p["days"]; reason = None
            if lo <= p["sl"]:
                reason, exit_px = "HARD_STOP", p["sl"]
            elif hi >= p["target"]:
                reason, exit_px = "TARGET_HIT", p["target"]
            else:
                dr = ind[sym]["drsi"][i] if i < len(ind[sym]["drsi"]) else np.nan
                wr = ind[sym]["wrsi"].iloc[i] if i < len(ind[sym]["wrsi"]) else np.nan
                if days_held >= 1 and not np.isnan(dr) and dr < D_EXIT_P:
                    reason, exit_px = "DAILY_RSI", px
                elif days_held >= 1 and not np.isnan(wr) and 0 < wr < W_EXIT_P:
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

        # 2) Entries
        slots = MAX_OPEN - len(open_pos)
        if slots > 0:
            candidates = []
            for sym, df in data.items():
                if sym in open_pos or date not in ind[sym]["idx"]:
                    continue
                i = ind[sym]["idx"][date]
                if i < 60:
                    continue
                c = ind[sym]["close"]; h = ind[sym]["high"]; l = ind[sym]["low"]
                drsi = ind[sym]["drsi"][i] if i < len(ind[sym]["drsi"]) else np.nan
                wrsi = ind[sym]["wrsi"].iloc[i]; mrsi = ind[sym]["mrsi"].iloc[i]
                price = c[i]
                if np.isnan(drsi) or np.isnan(wrsi):
                    continue

                if strategy == "discount":
                    if wrsi < WK_MIN_P: continue
                    e9, e21, e50 = ema_at(c, 9, i), ema_at(c, 21, i), ema_at(c, 50, i)
                    if not (e9 > e21 > e50): continue
                    if adx_at(h, l, c, i, ATR_PERIOD) < MIN_ADX_P: continue
                    mid = ind[sym]["midline"].iloc[i]
                    if np.isnan(mid): continue
                    prev_close = c[i-1]
                    if not (prev_close < mid and price > prev_close and price >= mid):
                        continue
                    atr_val = atr_at(h, l, c, i, ATR_PERIOD)
                    if not atr_val or atr_val <= 0: continue
                    score = adx_at(h, l, c, i, ATR_PERIOD)
                    candidates.append((score, sym, i, price, atr_val))
                else:  # rsi baseline
                    if np.isnan(mrsi): continue
                    if drsi > 60 and wrsi > 60 and mrsi > 60:
                        atr_val = atr_at(h, l, c, i, ATR_PERIOD)
                        if not atr_val or atr_val <= 0: continue
                        candidates.append((drsi, sym, i, price, atr_val))

            candidates.sort(reverse=True)
            for score, sym, i, price, atr_val in candidates[:slots]:
                sl = price - STOP_P * atr_val
                target = price + TGT_P * atr_val
                qty = max(1, int(RISK_PER_TRADE / (price - sl)))
                open_pos[sym] = {"entry": price, "sl": sl, "target": target,
                                 "qty": qty, "days": 0, "score": round(score, 1)}

        equity_curve.append((date, equity + sum(
            (ind[s]["close"][ind[s]["idx"][date]] - p["entry"]) * p["qty"]
            for s, p in open_pos.items() if date in ind[s]["idx"]
        )))

    return closed, equity_curve


def build_indicators(data):
    """Precompute per-symbol indicators ONCE so the grid search can reuse them
    across dozens of parameter combos without recomputing (huge speedup)."""
    ind = {}
    for sym, df in data.items():
        c = df["Close"].values; h = df["High"].values; l = df["Low"].values
        wk_hi = df["High"].resample("W").max()
        wk_lo = df["Low"].resample("W").min()
        prev_hi = wk_hi.shift(1).reindex(df.index, method="ffill")
        prev_lo = wk_lo.shift(1).reindex(df.index, method="ffill")
        ind[sym] = {
            "close": c, "high": h, "low": l,
            "drsi": rsi_series(c, 14),
            "wrsi": weekly_rsi_aligned(df),
            "mrsi": monthly_rsi_aligned(df),
            "midline": (prev_hi + prev_lo) / 2.0,
            "idx": {d: k for k, d in enumerate(df.index)},
        }
    return ind


def metrics(closed, equity_curve):
    """Compute the summary metrics dict for a run."""
    if not closed:
        return None
    pnls = np.array([t["pnl"] for t in closed])
    wins = pnls[pnls > 0]; losses = pnls[pnls <= 0]
    eq = np.array([e for _, e in equity_curve])
    peak = np.maximum.accumulate(eq)
    dd = ((eq - peak) / peak * 100).min() if len(eq) else 0
    pf = wins.sum() / abs(losses.sum()) if losses.sum() != 0 else float("inf")
    th = sum(1 for t in closed if t["reason"] == "TARGET_HIT")
    return {
        "trades": len(closed),
        "win_rate": len(wins) / len(pnls) * 100,
        "total_pnl": pnls.sum(),
        "ret_pct": (eq[-1] - CAPITAL) / CAPITAL * 100 if len(eq) else 0,
        "avg_win": wins.mean() if len(wins) else 0,
        "avg_loss": losses.mean() if len(losses) else 0,
        "pf": pf,
        "max_dd": dd,
        "target_hit_rate": th / len(closed) * 100,
    }


def report(name, closed, equity_curve):
    m = metrics(closed, equity_curve)
    if not m:
        print(f"\n{name}: no trades.")
        return
    reasons = {}
    for t in closed:
        reasons[t["reason"]] = reasons.get(t["reason"], 0) + 1
    rr = abs(m["avg_win"]/m["avg_loss"]) if m["avg_loss"] else 0
    print(f"\n{'='*64}\n{name}\n{'='*64}")
    print(f"  Trades            : {m['trades']}")
    print(f"  Win rate          : {m['win_rate']:.1f}%")
    print(f"  Total P&L         : Rs.{m['total_pnl']:,.0f}")
    print(f"  Return on capital : {m['ret_pct']:+.1f}%")
    print(f"  Avg win / loss    : +Rs.{m['avg_win']:,.0f} / Rs.{m['avg_loss']:,.0f}")
    print(f"  Realized R:R      : {rr:.2f}")
    print(f"  Profit factor     : {m['pf']:.2f}")
    print(f"  Max drawdown      : {m['max_dd']:.1f}%")
    print(f"  TARGET_HIT rate   : {m['target_hit_rate']:.1f}%")
    print(f"  Exit breakdown    : {reasons}")


def main():
    import itertools
    universe = get_universe()
    print(f"Fetching {len(universe)} symbols (this takes several minutes)...")
    data = {}
    for k, sym in enumerate(universe):
        df = fetch(sym)
        if df is not None and len(df) > 260:
            data[sym] = df
        if (k + 1) % 25 == 0:
            print(f"  ...{k+1}/{len(universe)} fetched ({len(data)} usable)")
        time.sleep(0.25)
    print(f"\nUsable symbols: {len(data)}/{len(universe)}")
    if len(data) < 30:
        print("Not enough data — aborting.")
        sys.exit(1)

    print("Precomputing indicators (once, reused across all grid runs)...")
    ind = build_indicators(data)

    # ── IN-SAMPLE / OUT-OF-SAMPLE SPLIT ───────────────────────────────────────
    all_dates = sorted(set().union(*[set(df.index) for df in data.values()]))
    split = all_dates[int(len(all_dates) * 0.66)]   # first 2/3 train, last 1/3 test
    print(f"\nSplit date: {split.date()}  "
          f"(in-sample {all_dates[0].date()}→{split.date()}, "
          f"out-of-sample {split.date()}→{all_dates[-1].date()})")

    # ── GRID: 3 key parameters, coarse values (54 combos) ─────────────────────
    grid = {
        "MIN_ADX":         [15.0, 20.0, 25.0],
        "ATR_STOP_MULT":   [1.0, 1.5],
        "ATR_TARGET_MULT": [2.0, 2.5, 3.0],
    }
    fixed = {"WEEKLY_RSI_MIN": 60, "DAILY_RSI_EXIT": 52, "WEEKLY_RSI_EXIT": 52}
    combos = list(itertools.product(*grid.values()))
    print(f"\nGrid-searching {len(combos)} parameter combos on IN-SAMPLE data...")

    results = []
    for vals in combos:
        params = dict(zip(grid.keys(), vals)); params.update(fixed)
        closed_is, eq_is = backtest_strategy(data, ind, "discount", params,
                                             date_to=split)
        m_is = metrics(closed_is, eq_is)
        if m_is and m_is["trades"] >= 20:
            results.append((params, m_is))

    if not results:
        print("No combo produced enough in-sample trades. Aborting.")
        sys.exit(1)

    # Rank by in-sample profit factor (robust metric, not raw return)
    results.sort(key=lambda x: x[1]["pf"], reverse=True)

    print(f"\n{'='*64}\nTOP 5 IN-SAMPLE (ranked by profit factor)\n{'='*64}")
    print(f"  {'ADX':>4} {'Stop':>5} {'Tgt':>4} | {'Trades':>6} {'Win%':>5} {'PF':>5} {'Ret%':>7} {'MaxDD':>6}")
    for params, m in results[:5]:
        print(f"  {params['MIN_ADX']:>4.0f} {params['ATR_STOP_MULT']:>5.1f} "
              f"{params['ATR_TARGET_MULT']:>4.1f} | {m['trades']:>6} "
              f"{m['win_rate']:>5.1f} {m['pf']:>5.2f} {m['ret_pct']:>+7.1f} {m['max_dd']:>6.1f}")

    # ── OUT-OF-SAMPLE VALIDATION of the best in-sample combo ──────────────────
    best_params = results[0][0]
    print(f"\n{'='*64}\nOUT-OF-SAMPLE TEST of best in-sample params\n{'='*64}")
    print(f"  Params: ADX>{best_params['MIN_ADX']:.0f}, "
          f"stop {best_params['ATR_STOP_MULT']}xATR, "
          f"target {best_params['ATR_TARGET_MULT']}xATR")
    closed_oos, eq_oos = backtest_strategy(data, ind, "discount", best_params,
                                           date_from=split)
    report("BEST PARAMS — OUT-OF-SAMPLE (data optimizer never saw)",
           closed_oos, eq_oos)

    # Also show DEFAULT params out-of-sample, for honest comparison
    closed_def, eq_def = backtest_strategy(data, ind, "discount", DEFAULT_PARAMS,
                                           date_from=split)
    report("DEFAULT PARAMS — OUT-OF-SAMPLE (current bot settings)",
           closed_def, eq_def)

    # ── Baseline over the FULL period for reference ───────────────────────────
    closed_b, eq_b = backtest_strategy(data, ind, "rsi", DEFAULT_PARAMS)
    report("SIMPLE BASELINE (RSI>60) — full period", closed_b, eq_b)

    # ── Discount with DEFAULT params over FULL period ─────────────────────────
    closed_full, eq_full = backtest_strategy(data, ind, "discount", DEFAULT_PARAMS)
    report("DISCOUNT (default params) — full period", closed_full, eq_full)

    # ── VERDICT ───────────────────────────────────────────────────────────────
    m_oos = metrics(closed_oos, eq_oos)
    m_def_oos = metrics(closed_def, eq_def)
    print(f"\n{'='*64}\nVERDICT\n{'='*64}")
    if m_oos and m_def_oos:
        print(f"  Best-params out-of-sample PF : {m_oos['pf']:.2f}  "
              f"(win {m_oos['win_rate']:.1f}%, ret {m_oos['ret_pct']:+.1f}%)")
        print(f"  Default-params out-of-sample : {m_def_oos['pf']:.2f}  "
              f"(win {m_def_oos['win_rate']:.1f}%, ret {m_def_oos['ret_pct']:+.1f}%)")
        if m_oos["pf"] > m_def_oos["pf"] * 1.1:
            print("  → Optimized params held up out-of-sample AND beat defaults.")
            print("    Worth adopting — but still paper-trade before live.")
        elif m_oos["pf"] < 1.0:
            print("  → Best in-sample params FAILED out-of-sample (PF < 1).")
            print("    This is overfitting. Keep DEFAULT params, do NOT adopt.")
        else:
            print("  → Optimized params did not clearly beat defaults out-of-sample.")
            print("    Keep defaults — the 'improvement' was likely in-sample noise.")

    print(f"\n{'='*64}\nHONEST CAVEATS\n{'='*64}")
    print("  - The out-of-sample number is what matters. In-sample is fittable.")
    print("  - Survivorship: today's Nifty 500 excludes delisted losers — optimistic.")
    print("  - One regime (mostly bullish). Bear/sideways behavior unknown.")
    print("  - Even an out-of-sample win = 'not falsified', not 'proven'. Paper-trade next.")


if __name__ == "__main__":
    main()