"""Patch panel.html: replace Hinglish renderUserManual + remove modal."""
import re

with open('panel.html', encoding='utf-8') as f:
    lines = f.readlines()

NEW_FUNC = r"""    function _renderUserManual() {
      const root = el('manual-root');
      if (!root || root.innerHTML.trim()) return;
      root.innerHTML = `
<style>
.um-h1{font-size:16px;font-weight:bold;color:#60a5fa;border-bottom:2px solid #1e3a5f;padding-bottom:6px;margin:20px 0 10px}
.um-h2{font-size:13px;font-weight:bold;color:#4ade80;margin:14px 0 5px}
.um-h3{font-size:11px;font-weight:bold;color:#f59e0b;margin:10px 0 3px}
.um-p{font-size:11px;color:#d1d5db;margin:4px 0 8px;line-height:1.6}
.um-box{background:#080f1d;border:1px solid #1a2540;border-left:3px solid #374151;border-radius:4px;padding:8px 12px;margin:8px 0;font-size:11px;color:#d1d5db;line-height:1.6}
.um-ex{background:#050a14;border:1px solid #1a2540;border-radius:3px;padding:6px 10px;margin:6px 0;font-family:monospace;font-size:10px;color:#9ca3af;white-space:pre-wrap}
.um-tag{background:#1e3a5f;color:#93c5fd;border-radius:3px;padding:1px 6px;font-size:10px;font-family:monospace}
.um-warn{background:#1c0a0a;border:1px solid #7f1d1d;border-radius:4px;padding:8px 12px;margin:8px 0;color:#fca5a5;font-size:11px}
.um-ok{background:#052e16;border:1px solid #166534;border-radius:4px;padding:8px 12px;margin:8px 0;color:#86efac;font-size:11px}
.um-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:8px 0}
</style>

<div style="font-size:20px;font-weight:bold;color:#f59e0b;margin-bottom:4px">&#128218; Hedge Trader &mdash; User Manual</div>
<div style="font-size:11px;color:#4b5563;margin-bottom:20px">BTC options hedging platform &mdash; paper trading, virtual accounts, 3 independent traders</div>

<div style="background:#050a14;border:1px solid #1a2540;border-radius:6px;padding:12px 16px;margin-bottom:20px">
  <div style="font-size:9px;color:#4b5563;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px">Contents</div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:3px;font-size:10px;color:#60a5fa">
    <span>1. Platform Overview</span><span>2. Overview Tab</span><span>3. BTC Technicals</span>
    <span>4. Bullish Trader</span><span>5. Bearish Trader</span><span>6. Volatile Event Trader</span>
    <span>7. Order Blocks</span><span>8. Virtual Accounts</span><span>9. Session Management</span>
    <span>10. Key Calculations</span><span>11. Settings Reference</span><span>12. Tips &amp; Warnings</span>
  </div>
</div>

<div class="um-h1">1. Platform Overview</div>
<div class="um-p">This is an automated <b style="color:#4ade80">paper trading platform</b> for Bitcoin (BTC). Three independent traders run simultaneously, each with a dedicated virtual $100,000 USDT balance. All orders are simulated against live Binance market data. No real money is involved.</div>
<div class="um-box"><b style="color:#60a5fa">Core strategy &mdash; Hedge Trading:</b> Each trader identifies a key price level (Support or Resistance), then enters a directional futures position and simultaneously buys an options contract as downside insurance. The option limits losses if the trade moves against you.</div>
<div class="um-grid">
  <div class="um-box" style="border-left-color:#4ade80"><b style="color:#4ade80">Bullish Trader</b><br>LONG Futures + ITM PUT hedge<br>Profits when BTC rises from support</div>
  <div class="um-box" style="border-left-color:#f87171"><b style="color:#f87171">Bearish Trader</b><br>SHORT Futures + ITM CALL hedge<br>Profits when BTC falls from resistance</div>
  <div class="um-box" style="border-left-color:#f59e0b"><b style="color:#f59e0b">Volatile Event Trader</b><br>PUT + CALL straddle around events<br>Profits from large moves in either direction</div>
  <div class="um-box" style="border-left-color:#60a5fa"><b style="color:#60a5fa">Virtual Accounts</b><br>$100k USDT per trader, fully independent<br>Complete PnL tracking: realized, unrealized, equity</div>
</div>

<div class="um-h1">2. Overview Tab</div>
<div class="um-h2">BTC Spot Price</div>
<div class="um-p">Current BTC price on Binance spot market. <b>Bid</b> = highest buy order. <b>Ask</b> = lowest sell order. Updated in real-time via WebSocket.</div>
<div class="um-h2">BTC Futures Perpetual (USDT-M)</div>
<div class="um-p"><b>Mark Price</b> is the fair value used for settlement and unrealized PnL &mdash; averaged across exchanges to prevent manipulation. <b>Premium</b> = (Futures &minus; Spot) / Spot &times; 100%. Positive = futures trading above spot (bullish market sentiment).</div>
<div class="um-h2">Options Chain</div>
<div class="um-p">Shows nearest ITM (in-the-money) CALL and PUT options. Key columns:</div>
<div class="um-box">
  <b>Strike</b> &mdash; Price at which the option gives the right to buy/sell BTC<br>
  <b>Bid / Ask</b> &mdash; Current market prices (Bid = where you sell, Ask = where you buy)<br>
  <b>IV (Implied Volatility)</b> &mdash; Market expectation of future price movement; higher IV = more expensive options<br>
  <b>Intrinsic Value</b> &mdash; PUT: max(Strike &minus; Spot, 0) &nbsp;&nbsp; CALL: max(Spot &minus; Strike, 0)<br>
  <b>Time Value (TV)</b> &mdash; Ask &minus; Intrinsic; the extra premium you pay for time and volatility
</div>

<div class="um-h1">3. BTC Technicals Tab</div>
<div class="um-p">Live candlestick chart with Binance futures data. Each candle = Open / High / Low / Close for the selected timeframe (1m, 3m, 5m, 15m, 1h, 4h, 1d).</div>
<div class="um-h2">Support and Resistance Lines</div>
<div class="um-p">The Analyst agent runs daily at the configured time (default 18:00 IST). It detects price levels where BTC has repeatedly bounced, then draws them on the chart. Bull/Bear traders use these as their primary entry reference.</div>
<div class="um-box" style="border-left-color:#4ade80"><b style="color:#4ade80">Green line</b> = Support &mdash; price has bounced upward from this level (Bull trader targets this)</div>
<div class="um-box" style="border-left-color:#f87171"><b style="color:#f87171">Red line</b> = Resistance &mdash; price has reversed downward from this level (Bear trader targets this)</div>

<div class="um-h1">4. Bullish Trader</div>
<div class="um-h2">Strategy</div>
<div class="um-p">Waits for BTC price to approach the nearest <b>Demand Order Block</b> (OB). When within the configured distance (max_distance_from_line pts), checks that all 4 option conditions are satisfied, then enters simultaneously:</div>
<div class="um-box">1. <b>BUY ITM PUT option</b> &mdash; the hedge; profits if BTC drops (limits downside)<br>2. <b>BUY LONG Futures</b> &mdash; the trade; profits if BTC rises</div>
<div class="um-h2">Workflow</div>
<div class="um-ex">SLEEP &rarr; (window opens) &rarr; CHECK_ELIGIBILITY &rarr; (OB zone found) &rarr; VERIFY_HEDGE_LOOP &rarr; (all 4 pass) &rarr; EXECUTE &rarr; MANAGING_POSITION &rarr; (target hit) &rarr; SLEEP</div>
<div class="um-h2">4 Entry Conditions (all must pass at the same instant)</div>
<div class="um-box">
  1. <b>Proximity</b>: Mark price within &plusmn;max_distance_from_line pts of the OB mid<br>
  2. <b>Premium</b>: PUT ask &le; max_premium USDT<br>
  3. <b>Time Value</b>: PUT TV &le; max_time_value USDT (not overpaying for time)<br>
  4. <b>Spread</b>: |Ask &minus; Mark| / Mark &le; price_diff_percent% (liquid, tight market)
</div>
<div class="um-h2">Exit Logic</div>
<div class="um-box">
  <b>Partial Close</b>: Futures PnL &ge; premium &times; partial_profit_ratio &rarr; sell 50% futures, place rebuy limit<br>
  <b>Full Close</b>: Session futures PnL &ge; session_pnl_target &rarr; close all futures (option held until squareoff)<br>
  <b>Force Close</b>: At force_close_h:m IST &rarr; sell most-profitable leg first, then the other
</div>
<div class="um-h2">Example</div>
<div class="um-ex">BTC mark price = $72,500. Nearest demand OB mid = $72,400 (distance = 100 pts, within tolerance).
Nearest ITM PUT: strike $73,000, ask $320, intrinsic = $73,000 - $72,500 = $500, TV = $320 - $500 = negative (deep ITM, TV = 0).
All 4 conditions pass &rarr; BUY PUT @ $320 + BUY 1 BTC LONG futures @ $72,500.
BTC rises to $73,200. Futures PnL = (73,200 - 72,500) &times; 1 = +$700. Session target = $600. Full close fires.
Net profit = $700 (futures) - $320 (premium cost) = +$380 USDT.</div>

<div class="um-h1">5. Bearish Trader</div>
<div class="um-p">Mirror of the Bullish trader &mdash; watches the nearest <b>Supply Order Block</b>.</div>
<div class="um-box">1. <b>BUY ITM CALL option</b> &mdash; hedges against an upward spike<br>2. <b>SELL SHORT Futures</b> &mdash; profits if BTC falls</div>
<div class="um-h2">Example</div>
<div class="um-ex">BTC = $74,800. Supply OB mid = $75,000 (distance = 200 pts, within tolerance).
Nearest ITM CALL: strike $74,000, spot $74,800, intrinsic = $74,800 - $74,000 = $800. Ask = $950, TV = $150.
BUY CALL @ $950 + SHORT 1 BTC futures @ $74,800.
BTC drops to $73,200. Futures PnL = (74,800 - 73,200) &times; 1 = +$1,600. Target hit.
Net profit = $1,600 - $950 = +$650 USDT.</div>

<div class="um-h1">6. Volatile Event Trader</div>
<div class="um-h2">Options Straddle Strategy</div>
<div class="um-p">Designed for high-impact macro events (FOMC, CPI, etc.) where a large BTC move is expected but direction is unknown. Buys both a PUT and a CALL simultaneously. Profits if BTC moves significantly in either direction.</div>
<div class="um-box">
  <b>Entry &mdash; Strict ITM Pair:</b><br>
  &bull; PUT strike &gt; current spot &gt; CALL strike (both options are in-the-money)<br>
  &bull; PUT strike &minus; CALL strike &asymp; configured gap (&plusmn; tolerance, default 500 pts &plusmn; 50 pts)<br>
  &bull; Combined ask (PUT + CALL) &le; combined_premium_max<br>
  &bull; Each leg ask quantity &ge; min_ask_qty
</div>
<div class="um-h2">How to Use</div>
<div class="um-p">Add an event in the Volatile Event tab with its name and exact IST timestamp. The trader begins searching for a qualifying straddle when the event time arrives. The search window closes at the configured hour next morning (default 06:00 IST). If a valid pair is found, it enters and monitors both legs.</div>
<div class="um-h2">Take Profit</div>
<div class="um-ex">TP per leg = (PUT_ask + CALL_ask) &times; tp_multiplier
Example: PUT = $620, CALL = $390, combined = $1,010, tp_multiplier = 1.10
TP = $1,010 &times; 1.10 = $1,111 per leg
When bid on PUT or CALL &ge; $1,111 &rarr; that leg is sold at market.</div>

<div class="um-h1">7. Order Blocks Tab</div>
<div class="um-p">Detects institutional order blocks (OB) &mdash; price zones where large orders caused a sharp price reversal. These are the primary entry reference for Bull/Bear traders.</div>
<div class="um-box">
  <b>Demand OB</b> (green) &mdash; Price fell into this zone then reversed sharply upward. Bull trader enters near the nearest demand OB.<br>
  <b>Supply OB</b> (red) &mdash; Price rose into this zone then reversed sharply downward. Bear trader enters near the nearest supply OB.
</div>
<div class="um-h2">Quality Grades</div>
<div class="um-ex">A+  score &ge; 86  Very strong: large displacement, fresh, close to current price
A   score &ge; 74  Strong zone
B   score &ge; 58  Valid zone (minimum threshold used by traders)</div>
<div class="um-p">At each window open, the trader fetches the nearest qualifying OB, locks its midpoint as the entry reference, and displays the zone on the mini-chart. The zone stays locked for the rest of that session.</div>

<div class="um-h1">8. Virtual Accounts Tab</div>
<div class="um-p">Unified view of all 3 traders' paper accounts with complete financial data, updated every 10 seconds.</div>
<div class="um-h2">PnL Breakdown</div>
<div class="um-box">
  <b>Balance</b> &mdash; Cash after option premiums paid and futures PnL settled. Starts at $100,000.<br>
  <b>Unrealized PnL</b> &mdash; How much open positions have moved: options MTM change + futures MTM.<br>
  <b>Equity</b> &mdash; Balance + current market value of open options + futures MTM = total account value.<br>
  <b>Realized PnL</b> &mdash; Gains/losses from <b>closed</b> positions only (sold options + closed futures).<br>
  <b>Total PnL</b> &mdash; Equity &minus; $100,000 = complete picture (Realized + Unrealized).
</div>
<div class="um-ok">Buying an option is capital deployment, not a realized loss. The premium paid appears in your Balance (cash is lower), but Realized PnL stays $0 until the option is sold. Total PnL shows the full change including current option market value.</div>
<div class="um-h2">Trade Log Views</div>
<div class="um-box">
  <b>Live</b> &mdash; Current server session trades (in-memory, real-time, exact execution timestamps)<br>
  <b>History</b> &mdash; All trades from the database (persists across restarts, exact execution timestamps)<br>
  <b>Sessions</b> &mdash; Completed trading sessions with entry/close times and net PnL
</div>

<div class="um-h1">9. Session Management</div>
<div class="um-h2">Trading Window</div>
<div class="um-p">Each trader searches for entries only during its configured window (trade_start to trade_end IST). Outside the window, state = SLEEP. At force_close time, any open position is automatically closed.</div>
<div class="um-h2">Session Boundaries</div>
<div class="um-p">Binance options expire daily at 13:30 IST. A "session" runs 13:31 IST to 13:30 IST the next day. A window of 16:00&rarr;02:00 is fully valid &mdash; both times are within the same session. The blue banner at the top always shows the current session.</div>
<div class="um-h2">Calendar and Blackout Dates</div>
<div class="um-p">Each Bull/Bear trader has a Trading Calendar inside its Settings panel. Toggle weekends off, or click specific dates to mark them as blackout days. The trader will skip those days entirely (stays SLEEP).</div>
<div class="um-h2">Clear Memory</div>
<div class="um-warn">Clear Memory will: (1) close all open positions without sending orders, (2) delete the full trade history from the database, (3) reset the virtual balance to $100,000. This is permanent and cannot be undone.</div>
<div class="um-h2">Force Close</div>
<div class="um-p">Manually triggers end-of-day squareoff. Sells the more profitable leg first, then the other. The session is marked "force-closed" and will not appear in the journal history. Only naturally completed sessions are recorded.</div>

<div class="um-h1">10. Key Calculations</div>
<div class="um-h2">Intrinsic Value</div>
<div class="um-ex">PUT intrinsic  = max(Strike - Spot, 0)    e.g. Strike=$74,000, Spot=$73,500  &rarr;  $500
CALL intrinsic = max(Spot - Strike, 0)    e.g. Strike=$72,000, Spot=$73,500  &rarr;  $1,500</div>
<div class="um-h2">Time Value</div>
<div class="um-ex">Time Value = Ask - Intrinsic
Example: Ask=$650, Intrinsic=$500  &rarr;  TV=$150  (you pay $150 for time and implied volatility)</div>
<div class="um-h2">Futures Unrealized PnL</div>
<div class="um-ex">LONG:   PnL = (Current Mark - Entry Price) &times; Qty    e.g. (73,200 - 72,500) &times; 1 = +$700
SHORT:  PnL = (Entry Price - Current Mark) &times; Qty    e.g. (74,800 - 73,200) &times; 1 = +$1,600</div>
<div class="um-h2">Partial Booking Trigger Price</div>
<div class="um-ex">LONG trigger  = Entry + (premium_paid &times; partial_ratio) / qty
Example: Entry=$72,500, premium=$320, ratio=1.10, qty=1
Trigger = $72,500 + ($352 / 1) = $72,852
When futures price reaches $72,852 &rarr; sell 50% of the position.</div>
<div class="um-h2">Rebuy After Partial</div>
<div class="um-ex">After the 50% partial sell, a limit rebuy is placed at:
  LONG  rebuy = avg_entry - (2 &times; time_value)    [buys the dip, lowers average cost]
  SHORT rebuy = avg_entry + (2 &times; time_value)
  This reduces the effective average entry and lowers the full-close target price.</div>
<div class="um-h2">Equity Formula</div>
<div class="um-ex">Equity = Cash Balance + Current Option Market Value + Futures Unrealized PnL
Cash Balance = $100k - premiums paid + option sale proceeds + realized futures PnL</div>

<div class="um-h1">11. Settings Reference</div>
<div class="um-h2">Time Window</div>
<div class="um-box">
  <b>trade_start_h/m</b> &mdash; Window open time (IST). Trader activates and begins analysis.<br>
  <b>trade_end_h/m</b> &mdash; Window close time. No new entries allowed after this.<br>
  <b>force_close_h/m</b> &mdash; Hard stop time. All open positions closed at this time regardless.<br>
  <i>Times are session-aware: a window of 16:00&rarr;02:00 is valid (spans midnight within same options session).</i>
</div>
<div class="um-h2">Entry Thresholds</div>
<div class="um-box">
  <b>max_premium</b> &mdash; Maximum option ask in USDT. Higher = accepts more expensive options.<br>
  <b>max_time_value</b> &mdash; Maximum TV allowed. Lower = only deep ITM options with minimal theta decay.<br>
  <b>price_diff_percent</b> &mdash; Maximum ask/mark spread %. Lower = requires more liquid options.<br>
  <b>max_distance_from_line</b> &mdash; Maximum distance (pts) from the OB mid before options are checked.
</div>
<div class="um-h2">Risk and Profit</div>
<div class="um-box">
  <b>session_pnl_target</b> &mdash; Futures PnL target to close the session (default $600).<br>
  <b>partial_profit_ratio</b> &mdash; Futures PnL &divide; premium at which 50% is booked (default 1.10).<br>
  <b>contract_qty</b> &mdash; BTC contracts per trade (default 1.0 = 1 BTC futures + 1 options contract).
</div>

<div class="um-h1">12. Tips and Warnings</div>
<div class="um-ok">&#10003; The platform is fully automated during the trading window. The System Log on the Overview tab and each trader's inline log show all decisions in real time.</div>
<div class="um-ok">&#10003; The Accounts tab refreshes every 10 seconds. Use it to monitor all 3 traders at once without switching tabs.</div>
<div class="um-ok">&#10003; Settings take effect immediately &mdash; no restart needed. All settings are saved to the database and restored after a server restart.</div>
<div class="um-warn">&#9888; Force Close marks the session as force-closed. It will not appear in the session journal. Only sessions that complete naturally (by hitting the profit target or squareoff time) are recorded.</div>
<div class="um-warn">&#9888; If a trader shows "ANALYZING" status, it is fetching live candle data to detect Order Block zones. Do not change analysis settings during this phase.</div>
<div class="um-warn">&#9888; Options expire at 13:30 IST daily. Any position still open at expiry will expire worthless. Always set force_close time comfortably before 13:30 IST.</div>
<div class="um-warn">&#9888; The session banner shows "EXPIRY IN 13:30" &mdash; this means the current option expiry time, not a countdown. All option positions must be closed before this time.</div>

<div style="margin-top:24px;padding-top:12px;border-top:1px solid #1a2540;font-size:9px;color:#1f2937;text-align:center">
  BTC Hedge Platform &mdash; Paper Trading Only &mdash; No Real Money at Risk
</div>
`;
    }
"""

# Lines 6031-6278 (1-indexed) = indices 6030-6277 (0-indexed, inclusive of 6277)
# Modal comment at line 6538 (1-indexed) = index 6537 (0-indexed)
new_lines = lines[:6030] + [NEW_FUNC + '\n'] + lines[6278:6537]

with open('panel.html', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print(f"Done. New file: {len(new_lines)} lines")
