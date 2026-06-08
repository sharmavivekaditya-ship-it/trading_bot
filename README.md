# ClaudeBot — NSE Swing Paper Trader

## Quick start (local)
```bash
pip install anthropic
export ANTHROPIC_API_KEY=sk-ant-...
python bot.py
```

## Deploy to Railway (free, always-on)
1. Push this folder to a GitHub repo
2. Go to railway.app → New Project → Deploy from GitHub
3. Add env var: ANTHROPIC_API_KEY=your_key
4. Done — bot runs 24/7, restarts on crash

## Deploy to Render.com (free alternative)
1. Push to GitHub
2. Render → New Background Worker → connect repo
3. Add ANTHROPIC_API_KEY env var
4. Free tier: always-on background worker

## Credit optimisation built in
- Pre-filter: skips ~70-80% of scans with zero API cost
- Haiku model for routine signals (~25x cheaper than Sonnet)
- Sonnet only for high-stakes exit decisions
- HOLD cache: skips re-scanning HOLDs for 4 cycles
- Exit check only every 5 scan cycles

## Reports
```bash
python report.py   # weekly P&L, win rate, token usage
```

## Connect real broker (when ready)
In bot.py, find the comment:
  # [LIVE] Place order here
Replace with your broker's order call.

Zerodha example:
  from kiteconnect import KiteConnect
  kite = KiteConnect(api_key="xxx")
  kite.place_order(tradingsymbol=sym, exchange="NSE",
      transaction_type=direction, quantity=qty,
      order_type="LIMIT", price=entry)
