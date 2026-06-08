"""
ClaudeBot Live Dashboard — Flask web UI
Deploy alongside bot.py on Railway.
Access from phone/browser at your Railway public URL.
"""
from flask import Flask, jsonify, render_template_string
import sqlite3, json, os
from datetime import date, timedelta, datetime

app = Flask(__name__)
DB = os.environ.get("DB_PATH", "trades.db")
LOG = os.environ.get("LOG_PATH", "claudebot.log")

def db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def week_start():
    today = date.today()
    return (today - timedelta(days=today.weekday())).isoformat()

# ── API ENDPOINTS (used by dashboard JS) ─────────────────────────────────────

@app.route("/api/status")
def status():
    con = db()
    ws = week_start()

    stats = con.execute(
        "SELECT * FROM weekly_stats WHERE week_start=?", (ws,)
    ).fetchone()
    stats = dict(stats) if stats else {"pnl":0,"risk_used":0,"wins":0,"losses":0,"total_tokens":0}

    open_trades = [dict(r) for r in con.execute(
        "SELECT sym,direction,entry,sl,target,rr,risk_amt,opened_at FROM trades WHERE status='open' ORDER BY opened_at DESC"
    ).fetchall()]

    closed = [dict(r) for r in con.execute(
        "SELECT sym,direction,entry,target,pnl,status,closed_at FROM trades WHERE status!='open' ORDER BY closed_at DESC LIMIT 20"
    ).fetchall()]

    last_scans = [dict(r) for r in con.execute(
        "SELECT sym,signal,rr,model_used,tokens_used,ts FROM scan_log ORDER BY id DESC LIMIT 30"
    ).fetchall()]

    # Read last 40 log lines
    log_lines = []
    try:
        with open(LOG, "r") as f:
            lines = f.readlines()
            log_lines = [l.strip() for l in lines[-40:]][::-1]
    except:
        log_lines = ["Log file not found — bot may still be starting up"]

    total = stats["wins"] + stats["losses"]
    win_rate = round(stats["wins"]/total*100) if total else 0

    # Scan efficiency this week
    scans_total = con.execute("SELECT COUNT(*) FROM scan_log WHERE ts>=?", (ws,)).fetchone()[0]
    scans_skipped = con.execute("SELECT COUNT(*) FROM scan_log WHERE ts>=? AND skipped=1", (ws,)).fetchone()[0]
    skip_pct = round(scans_skipped/scans_total*100) if scans_total else 0

    # Token cost estimate (Haiku ~$0.25/1M input, Sonnet ~$3/1M)
    haiku_tok = con.execute("SELECT COALESCE(SUM(tokens_used),0) FROM scan_log WHERE ts>=? AND model_used='haiku'", (ws,)).fetchone()[0]
    sonnet_tok = con.execute("SELECT COALESCE(SUM(tokens_used),0) FROM scan_log WHERE ts>=? AND model_used LIKE '%sonnet%'", (ws,)).fetchone()[0]
    est_cost_usd = (haiku_tok/1_000_000*0.25) + (sonnet_tok/1_000_000*3.0)

    return jsonify({
        "stats": stats,
        "win_rate": win_rate,
        "open_trades": open_trades,
        "closed_trades": closed,
        "last_scans": last_scans,
        "log_lines": log_lines,
        "scan_efficiency": {"total": scans_total, "skipped": scans_skipped, "pct": skip_pct},
        "cost": {"haiku_tokens": haiku_tok, "sonnet_tokens": sonnet_tok, "est_usd": round(est_cost_usd,4)},
        "server_time": datetime.now().strftime("%H:%M:%S"),
        "capital": 100000
    })

# ── MAIN HTML DASHBOARD ───────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClaudeBot Dashboard</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#080b0f;color:#a8b4c0;font-family:'IBM Plex Mono',monospace;font-size:12px}
.bar{background:#0d1520;border-bottom:1px solid #162030;padding:10px 16px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:10}
.logo{color:#00e676;font-weight:600;letter-spacing:3px;font-size:13px}
.bar-meta{display:flex;gap:16px;font-size:10px;color:#3a5060}
.dot{display:inline-block;width:6px;height:6px;border-radius:50%;background:#00e676;margin-right:4px;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}
.metrics{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:#162030;border-bottom:1px solid #162030}
@media(max-width:700px){.metrics{grid-template-columns:repeat(3,1fr)}}
.met{background:#0d1520;padding:10px 12px}
.met-l{font-size:9px;color:#3a5060;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:3px}
.met-v{font-size:18px;font-weight:600;color:#c8d8e8}
.met-v.g{color:#00e676}.met-v.r{color:#ff5252}.met-v.a{color:#ffab40}
.met-s{font-size:10px;color:#2a4050;margin-top:1px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#162030;margin-top:1px}
@media(max-width:700px){.grid{grid-template-columns:1fr}}
.panel{background:#080b0f;padding:14px}
.panel-full{background:#080b0f;padding:14px;border-top:1px solid #162030}
.sec{font-size:9px;color:#2a5060;letter-spacing:2px;text-transform:uppercase;margin-bottom:10px}
.trade{background:#0d1520;border-radius:6px;padding:9px 11px;margin-bottom:5px;border-left:3px solid #1e2d3d}
.trade.open{border-left-color:#ffab40}
.trade.win{border-left-color:#00e676}
.trade.loss{border-left-color:#ff5252}
.trade-top{display:flex;justify-content:space-between;margin-bottom:3px}
.trade-sym{font-weight:600;color:#c8d8e8}
.trade-pnl.p{color:#00e676}.trade-pnl.n{color:#ff5252}.trade-pnl.o{color:#ffab40}
.trade-meta{color:#3a5060;line-height:1.6;font-size:10px}
.log-box{background:#0d1520;border-radius:6px;padding:10px;font-size:10px;line-height:1.8;max-height:220px;overflow-y:auto;color:#4a6070}
.log-box::-webkit-scrollbar{width:3px}.log-box::-webkit-scrollbar-thumb{background:#1e3040}
.log-g{color:#00e676}.log-r{color:#ff5252}.log-a{color:#ffab40}.log-b{color:#40c4ff}.log-d{color:#2a4050}
.scan-row{display:flex;align-items:center;gap:6px;padding:4px 0;border-bottom:1px solid #0d1520;font-size:10px}
.scan-sym{width:88px;color:#c8d8e8;font-weight:500}
.scan-sig{padding:2px 7px;border-radius:3px;font-size:9px;font-weight:600;letter-spacing:1px}
.s-buy{background:#003020;color:#00e676;border:1px solid #005030}
.s-sell{background:#2a0808;color:#ff5252;border:1px solid #500010}
.s-hold{background:#1a1a08;color:#ffab40;border:1px solid #3a3010}
.s-skip{background:#0d1520;color:#2a4050;border:1px solid #162030}
.scan-meta{color:#2a4050;flex:1}
.scan-tok{color:#1a3040;font-size:9px}
.risk-wrap{margin-top:10px}
.risk-labels{display:flex;justify-content:space-between;font-size:9px;color:#2a4050;margin-bottom:4px}
.risk-bar{height:5px;background:#0d1520;border-radius:3px;overflow:hidden}
.risk-fill{height:100%;border-radius:3px;transition:width .8s}
.cost-row{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid #0d1520;font-size:10px}
.cost-k{color:#3a5060}.cost-v{color:#c8d8e8}
.badge{display:inline-block;padding:1px 6px;border-radius:3px;font-size:9px;margin-left:4px}
.b-h{background:#0a1530;color:#40c4ff;border:1px solid #103050}
.b-s{background:#1a0a30;color:#c060ff;border:1px solid #300860}
.empty{color:#1e3040;text-align:center;padding:20px 0}
</style>
</head>
<body>

<div class="bar">
  <div class="logo">◈ CLAUDEBOT · LIVE</div>
  <div class="bar-meta">
    <span><span class="dot"></span><span id="mode">PAPER MODE</span></span>
    <span id="server-time">--:--:--</span>
    <span id="refresh-label">refreshing…</span>
  </div>
</div>

<div class="metrics">
  <div class="met"><div class="met-l">Portfolio</div><div class="met-v" id="m-port">—</div><div class="met-s" id="m-port-s">—</div></div>
  <div class="met"><div class="met-l">Week P&L</div><div class="met-v" id="m-pnl">—</div><div class="met-s" id="m-pnl-s">—</div></div>
  <div class="met"><div class="met-l">Win Rate</div><div class="met-v" id="m-wr">—</div><div class="met-s" id="m-wr-s">—</div></div>
  <div class="met"><div class="met-l">Risk Used</div><div class="met-v a" id="m-risk">—</div><div class="met-s" id="m-risk-s">—</div></div>
  <div class="met"><div class="met-l">Open Trades</div><div class="met-v a" id="m-open">—</div><div class="met-s">max 3</div></div>
  <div class="met"><div class="met-l">API Cost (week)</div><div class="met-v" id="m-cost">—</div><div class="met-s" id="m-cost-s">—</div></div>
</div>

<div class="grid">

  <!-- Open positions -->
  <div class="panel">
    <div class="sec">Open Positions</div>
    <div id="open-trades"><div class="empty">loading…</div></div>
    <div class="risk-wrap">
      <div class="risk-labels"><span>Weekly risk</span><span id="risk-label-r">—</span></div>
      <div class="risk-bar"><div class="risk-fill" id="risk-fill" style="width:0%;background:#00e676"></div></div>
    </div>
  </div>

  <!-- Recent scan signals -->
  <div class="panel">
    <div class="sec">Recent Signals <span id="scan-eff" style="color:#1a3040;font-size:9px"></span></div>
    <div id="scan-list"><div class="empty">loading…</div></div>
  </div>

  <!-- Closed trades -->
  <div class="panel">
    <div class="sec">Closed Trades</div>
    <div id="closed-trades"><div class="empty">loading…</div></div>
  </div>

  <!-- API cost breakdown -->
  <div class="panel">
    <div class="sec">API Credit Usage (this week)</div>
    <div id="cost-breakdown"></div>
    <div class="sec" style="margin-top:14px">Scan Efficiency</div>
    <div id="eff-breakdown"></div>
  </div>

</div>

<!-- Live log -->
<div class="panel-full">
  <div class="sec">Bot Log (live)</div>
  <div class="log-box" id="log-box">loading…</div>
</div>

<script>
const CAPITAL = 100000;
let lastRefresh = Date.now();

function colorLog(line) {
  if (line.includes('◈') || line.includes('BUY') || line.includes('WIN') || line.includes('+₹')) return 'log-g';
  if (line.includes('SL_HIT') || line.includes('ERROR') || line.includes('error') || line.includes('-₹')) return 'log-r';
  if (line.includes('WARNING') || line.includes('risk limit') || line.includes('HOLD')) return 'log-a';
  if (line.includes('Scan cycle') || line.includes('scanning') || line.includes('Claude')) return 'log-b';
  return 'log-d';
}

async function refresh() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    const s = d.stats;

    document.getElementById('server-time').textContent = d.server_time;
    document.getElementById('refresh-label').textContent = 'live · 10s';

    // Metrics
    const port = CAPITAL + s.pnl;
    const pct = (s.pnl/CAPITAL*100).toFixed(2);
    document.getElementById('m-port').textContent = '₹'+port.toLocaleString('en-IN');
    document.getElementById('m-port-s').textContent = 'base ₹1,00,000';

    const pnlEl = document.getElementById('m-pnl');
    pnlEl.textContent = (s.pnl>=0?'+₹':'-₹') + Math.abs(s.pnl).toLocaleString('en-IN');
    pnlEl.className = 'met-v '+(s.pnl>=0?'g':'r');
    document.getElementById('m-pnl-s').textContent = pct+'% of capital';

    document.getElementById('m-wr').textContent = d.win_rate+'%';
    document.getElementById('m-wr-s').textContent = s.wins+'W / '+s.losses+'L';

    const rPct = Math.round(s.risk_used/3000*100);
    document.getElementById('m-risk').textContent = rPct+'%';
    document.getElementById('m-risk').className = 'met-v '+(rPct>80?'r':rPct>50?'a':'a');
    document.getElementById('m-risk-s').textContent = '₹'+s.risk_used+' / ₹3,000';

    document.getElementById('m-open').textContent = d.open_trades.length;
    document.getElementById('m-cost').textContent = '$'+d.cost.est_usd;
    document.getElementById('m-cost-s').textContent = s.total_tokens.toLocaleString()+' tokens';

    // Risk bar
    const fill = document.getElementById('risk-fill');
    fill.style.width = Math.min(100,rPct)+'%';
    fill.style.background = rPct>80?'#ff5252':rPct>50?'#ffab40':'#00e676';
    document.getElementById('risk-label-r').textContent = '₹'+s.risk_used+' / ₹3,000';

    // Open trades
    const ot = document.getElementById('open-trades');
    ot.innerHTML = d.open_trades.length ? d.open_trades.map(t=>`
      <div class="trade open">
        <div class="trade-top"><span class="trade-sym">${t.sym} <span style="font-size:9px;color:#3a5060">${t.direction}</span></span>
        <span class="trade-pnl o">OPEN</span></div>
        <div class="trade-meta">Entry ₹${t.entry} · SL ₹${t.sl} · TGT ₹${t.target}<br>R:R ${t.rr}x · Risk ₹${t.risk_amt}</div>
      </div>`).join('') : '<div class="empty">No open positions</div>';

    // Closed trades
    const ct = document.getElementById('closed-trades');
    ct.innerHTML = d.closed_trades.length ? d.closed_trades.map(t=>`
      <div class="trade ${t.status}">
        <div class="trade-top"><span class="trade-sym">${t.sym} <span style="font-size:9px;color:#3a5060">${t.direction}</span></span>
        <span class="trade-pnl ${t.pnl>=0?'p':'n'}">${t.pnl>=0?'+₹':'-₹'}${Math.abs(t.pnl)}</span></div>
        <div class="trade-meta">${t.status.toUpperCase()} · Entry ₹${t.entry}</div>
      </div>`).join('') : '<div class="empty">No closed trades yet</div>';

    // Scan list
    const sl = document.getElementById('scan-list');
    sl.innerHTML = d.last_scans.length ? d.last_scans.slice(0,12).map(s=>{
      const cls = s.signal==='BUY'?'s-buy':s.signal==='SELL'?'s-sell':s.signal==='HOLD'?'s-hold':'s-skip';
      const model = s.model_used==='haiku'?'<span class="badge b-h">H</span>':'<span class="badge b-s">S</span>';
      return `<div class="scan-row">
        <span class="scan-sym">${s.sym}</span>
        <span class="scan-sig ${cls}">${s.signal}</span>
        ${s.rr?`<span style="color:#2a4050">${s.rr}x</span>`:''}
        ${model}
        <span class="scan-tok">${s.tokens_used}tok</span>
        <span class="scan-meta" style="text-align:right">${s.ts.slice(11,19)}</span>
      </div>`;
    }).join('') : '<div class="empty">No scans yet</div>';

    // Efficiency
    const eff = d.scan_efficiency;
    document.getElementById('scan-eff').textContent = eff.total ? `${eff.pct}% skipped (${eff.skipped}/${eff.total})` : '';

    // Cost breakdown
    document.getElementById('cost-breakdown').innerHTML = `
      <div class="cost-row"><span class="cost-k">Haiku tokens</span><span class="cost-v">${d.cost.haiku_tokens.toLocaleString()} <span class="badge b-h">cheap</span></span></div>
      <div class="cost-row"><span class="cost-k">Sonnet tokens</span><span class="cost-v">${d.cost.sonnet_tokens.toLocaleString()} <span class="badge b-s">accurate</span></span></div>
      <div class="cost-row"><span class="cost-k">Est. total cost</span><span class="cost-v" style="color:#00e676">$${d.cost.est_usd} USD</span></div>
      <div class="cost-row"><span class="cost-k">In rupees</span><span class="cost-v">~₹${Math.round(d.cost.est_usd*84)}</span></div>`;

    document.getElementById('eff-breakdown').innerHTML = `
      <div class="cost-row"><span class="cost-k">Total scans</span><span class="cost-v">${eff.total}</span></div>
      <div class="cost-row"><span class="cost-k">Pre-filtered</span><span class="cost-v" style="color:#00e676">${eff.skipped} (${eff.pct}% free)</span></div>
      <div class="cost-row"><span class="cost-k">Reached Claude</span><span class="cost-v">${eff.total-eff.skipped}</span></div>`;

    // Log
    const lb = document.getElementById('log-box');
    lb.innerHTML = d.log_lines.map(l=>`<div class="${colorLog(l)}">${l}</div>`).join('');

  } catch(e) {
    document.getElementById('refresh-label').textContent = 'error — retrying';
  }
}

refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(HTML)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
