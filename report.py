"""
Daily report generator — run manually or cron it.
Prints P&L, win rate, top trades, and Claude credit usage.
"""
import sqlite3, json
from datetime import date, timedelta

con = sqlite3.connect("trades.db")

def report():
    week_start = (date.today() - timedelta(days=date.today().weekday())).isoformat()

    print("\n╔══════════════════════════════════════╗")
    print("  ClaudeBot — Paper Trading Report")
    print(f"  Week of {week_start}")
    print("╚══════════════════════════════════════╝\n")

    # Weekly stats
    row = con.execute(
        "SELECT pnl, risk_used, wins, losses, total_tokens FROM weekly_stats WHERE week_start=?",
        (week_start,)
    ).fetchone()
    if row:
        pnl, risk, wins, losses, tokens = row
        total = wins + losses
        wr = round(wins/total*100) if total else 0
        print(f"  P&L       : ₹{pnl:+,.0f}  ({pnl/100000*100:.2f}%)")
        print(f"  Risk used : ₹{risk:,.0f} / ₹3,000")
        print(f"  Win rate  : {wr}%  ({wins}W / {losses}L)")
        print(f"  API tokens: {tokens:,}  (~${tokens/1_000_000*0.25:.3f} USD)")

    # All trades this week
    trades = con.execute(
        "SELECT sym, direction, entry, target, sl, rr, pnl, status, opened_at FROM trades WHERE opened_at >= ? ORDER BY opened_at DESC",
        (week_start,)
    ).fetchall()

    print(f"\n  {'SYM':<12} {'DIR':<5} {'ENTRY':>7} {'TGT':>7} {'SL':>7} {'RR':>4} {'P&L':>8} {'STATUS'}")
    print("  " + "─"*68)
    for t in trades:
        sym,dir_,entry,tgt,sl,rr,pnl,status,ts = t
        pnl_s = f"₹{pnl:+.0f}" if pnl else "open"
        print(f"  {sym:<12} {dir_:<5} {entry:>7.0f} {tgt:>7.0f} {sl:>7.0f} {rr:>4.1f} {pnl_s:>8} {status}")

    # Scan efficiency
    total_scans = con.execute("SELECT COUNT(*) FROM scan_log WHERE ts >= ?", (week_start,)).fetchone()[0]
    skipped     = con.execute("SELECT COUNT(*) FROM scan_log WHERE ts >= ? AND skipped=1", (week_start,)).fetchone()[0]
    if total_scans:
        print(f"\n  Scan efficiency: {skipped}/{total_scans} skipped by pre-filter ({skipped/total_scans*100:.0f}% saved)")

    print("\n  Model breakdown:")
    for row in con.execute("SELECT model_used, COUNT(*), SUM(tokens_used) FROM scan_log WHERE ts >= ? GROUP BY model_used", (week_start,)):
        print(f"    {row[0]:<10} {row[1]:>4} calls  {row[2]:>8,} tokens")

    print()

if __name__ == "__main__":
    report()
