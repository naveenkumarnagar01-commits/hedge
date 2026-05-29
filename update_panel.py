import re

with open('panel.html', 'r', encoding='utf-8') as f:
    content = f.read()

# A) SESSION BANNER
content = content.replace('<!-- ════ TABS ════ -->', '''<!-- ════ SESSION BANNER ════ -->
    <div id="session-banner" style="background:#1e3a5f;color:#93c5fd;padding:4px 12px;font-size:10px;text-align:center;border-bottom:1px solid #1a2540">
      SESSION | <span id="sb-range">—</span> | EXPIRY IN | <span id="sb-expiry">—</span>
    </div>
    <!-- ════ TABS ════ -->''')

# B) TWO NEW TABS
content = content.replace('<div class="tab" onclick="showTab(5)" id="tab-ob">📦 Order Blocks</div>', '''<div class="tab" onclick="showTab(5)" id="tab-ob">📦 Order Blocks</div>
        <div class="tab" onclick="showTab(6)" id="tab-journal">📓 Journal</div>
        <div class="tab" onclick="showTab(7)" id="tab-settings">⚙ Settings</div>''')

# C) TWO NEW PANELS
content = content.replace('</div><!-- /body -->', '''      <div class="pnl" id="p6"><div id="journal-root" style="padding:8px"></div></div>
      <div class="pnl" id="p7"><div id="settings-root" style="padding:8px;max-width:800px;margin:0 auto;width:100%"></div></div>
    </div><!-- /body -->''')

# D) VOLATILE BUTTONS
vol_btns = '''<button onclick="_volForceClose()" style="font-size:9px;padding:2px 8px;background:#1c0a0a;border:1px solid #7f1d1d;border-radius:3px;color:#f87171;cursor:pointer">⚡ Force Close</button>
            <button onclick="_volClearMemory()" style="font-size:9px;padding:2px 8px;background:#1c0a0a;border:1px solid #7f1d1d;border-radius:3px;color:#fca5a5;cursor:pointer">🧹 Clear Memory</button>
            <button onclick="_volResetTrader()"'''
content = content.replace('<button onclick="_volResetTrader()"', vol_btns)

# E) SESSION-AWARE VALIDATION
old_val_code = '''const toMin = (h, m) => (h ?? 0) * 60 + (m ?? 0);
      const startMin = toMin(v('trade_start_h'), v('trade_start_m'));
      const endMin   = toMin(v('trade_end_h'),   v('trade_end_m'));
      const fcMin    = toMin(v('force_close_h'), v('force_close_m'));

      const errors = [];
      const err  = (fields, msg) => errors.push({ fields, msg, level: 'error' });
      const warn = (fields, msg) => errors.push({ fields, msg, level: 'warn'  });

      // 1. Window must have positive width
      if (endMin <= startMin)
        err(['trade_start_h','trade_start_m','trade_end_h','trade_end_m'],
            `Window Close (${fmt(v('trade_end_h'),v('trade_end_m'))}) must be after Window Open (${fmt(v('trade_start_h'),v('trade_start_m'))})`);

      // 2. Squareoff must be INSIDE the window — most critical rule
      if (fcMin < startMin)
        err(['force_close_h','force_close_m'],
            `Squareoff ${fmt(v('force_close_h'),v('force_close_m'))} is BEFORE window opens at ${fmt(v('trade_start_h'),v('trade_start_m'))} ` +
            `— force-close fires on every eligibility check tick, trader can never run`);
      else if (fcMin > endMin)
        warn(['force_close_h','force_close_m'],
            `Squareoff ${fmt(v('force_close_h'),v('force_close_m'))} is after Window Close ${fmt(v('trade_end_h'),v('trade_end_m'))} ` +
            `— position may stay open after window closes until squareoff fires`);'''

new_val_code = '''const sessionExpiryH = CFG.session_expiry_h ?? 13;
      const sessionExpiryM = CFG.session_expiry_m ?? 30;
      const sessionStartMin = (sessionExpiryH * 60 + sessionExpiryM + 1) % 1440;
      
      const toSesMin = (h, m) => {
        if (h == null || m == null) return 0;
        const tMin = h * 60 + m;
        return tMin >= sessionStartMin ? tMin - sessionStartMin : tMin + (1440 - sessionStartMin);
      };

      const startMin = toSesMin(v('trade_start_h'), v('trade_start_m'));
      const endMin   = toSesMin(v('trade_end_h'),   v('trade_end_m'));
      const fcMin    = toSesMin(v('force_close_h'), v('force_close_m'));

      const errors = [];
      const err  = (fields, msg) => errors.push({ fields, msg, level: 'error' });
      const warn = (fields, msg) => errors.push({ fields, msg, level: 'warn'  });

      if (endMin <= startMin)
        err(['trade_start_h','trade_start_m','trade_end_h','trade_end_m'],
            `Window Close (${fmt(v('trade_end_h'),v('trade_end_m'))}) must be after Window Open (${fmt(v('trade_start_h'),v('trade_start_m'))}) in session context`);

      if (fcMin < endMin)
        err(['force_close_h','force_close_m'],
            `Squareoff ${fmt(v('force_close_h'),v('force_close_m'))} is BEFORE Window Close ${fmt(v('trade_end_h'),v('trade_end_m'))} in session context`);'''

content = content.replace(old_val_code, new_val_code)

# F) CALENDAR IN SETTINGS
injections = """
// ── Calendar injection ─────────────────────────────────────────
    function _toggleBlk(tr, dtStr) {
      let dates = (cfgEdits[tr+'_blackout_dates'] ?? CFG[tr+'_blackout_dates'] ?? '').split(',').map(s=>s.trim()).filter(x=>x);
      if (dates.includes(dtStr)) dates = dates.filter(x => x !== dtStr);
      else dates.push(dtStr);
      _setBlk(tr, dates.join(','));
    }
    function _setBlk(tr, val) {
      cfgEdits[tr+'_blackout_dates'] = val;
      buildSettings();
      el('save-btn').disabled = false;
    }
    function _setWk(tr, val) {
      cfgEdits[tr+'_skip_weekends'] = val;
      buildSettings();
      el('save-btn').disabled = false;
    }
    
    function _buildCalendarSection(tr) {
      const skipWkId = `si-${tr}_skip_weekends`;
      const isSkipWk = (cfgEdits[tr+'_skip_weekends'] ?? CFG[tr+'_skip_weekends']) ? 'checked' : '';
      const blkDates = cfgEdits[tr+'_blackout_dates'] ?? CFG[tr+'_blackout_dates'] ?? '';
      
      let calHtml = `<div style="display:flex;gap:10px;margin:8px 0;overflow-x:auto">`;
      const d = new Date();
      for(let m=0; m<2; m++) {
        const curD = new Date(d.getFullYear(), d.getMonth() + m, 1);
        const monthName = curD.toLocaleString('default', { month: 'long', year: 'numeric' });
        calHtml += `<div style="background:#0a0f1e;border:1px solid #1a2540;padding:6px;border-radius:4px;min-width:180px">
          <div style="text-align:center;font-weight:bold;margin-bottom:4px;color:#93c5fd">${monthName}</div>
          <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:2px;text-align:center;font-size:9px;color:#6b7280;margin-bottom:4px">
            <div>Su</div><div>Mo</div><div>Tu</div><div>We</div><div>Th</div><div>Fr</div><div>Sa</div>
          </div>
          <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:2px;text-align:center;font-size:10px">`;
        const startDay = curD.getDay();
        const daysInMonth = new Date(curD.getFullYear(), curD.getMonth()+1, 0).getDate();
        for(let i=0; i<startDay; i++) calHtml += `<div></div>`;
        for(let i=1; i<=daysInMonth; i++) {
          const dtStr = `${curD.getFullYear()}-${String(curD.getMonth()+1).padStart(2,'0')}-${String(i).padStart(2,'0')}`;
          const isBlk = blkDates.includes(dtStr);
          const bg = isBlk ? '#7f1d1d' : '#1e293b';
          const col = isBlk ? '#fca5a5' : '#e5e7eb';
          calHtml += `<div style="background:${bg};color:${col};padding:2px;cursor:pointer;border-radius:2px" onclick="_toggleBlk('${tr}', '${dtStr}')">${i}</div>`;
        }
        calHtml += `</div></div>`;
      }
      calHtml += `</div>`;

      return `
        <div style="margin-bottom:6px">
          <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
            <input type="checkbox" id="${skipWkId}" ${isSkipWk} onchange="_setWk('${tr}', this.checked)">
            <span>Skip Weekends (Sat/Sun)</span>
          </label>
        </div>
        ${calHtml}
        <div style="margin-bottom:6px">
          <div style="margin-bottom:4px">Raw CSV:</div>
          <input class="inp" type="text" value="${blkDates}" onchange="_setBlk('${tr}', this.value)">
        </div>`;
    }

// ── Render Tabs ─────────────────────────────────────────────
    function renderSettingsTab() {
    }
    
    function renderJournalTab() {
        el('journal-root').innerHTML = `
        <div style="margin-bottom:10px;display:flex;gap:10px">
            <select id="j-trader" class="inp" style="width:auto">
                <option value="all">All Traders</option>
                <option value="bull">Bullish</option>
                <option value="bear">Bearish</option>
                <option value="vol">Volatile</option>
            </select>
            <input type="date" id="j-from" class="inp" style="width:auto">
            <input type="date" id="j-to" class="inp" style="width:auto">
            <button class="btn bb" onclick="_fetchJournal()">Refresh</button>
        </div>
        <div id="j-summary" class="card" style="margin-bottom:10px"></div>
        <div id="j-list" style="display:flex;flex-direction:column;gap:5px"></div>`;
    }
    
    async function _fetchJournal() {
        const tr = el('j-trader').value;
        const from = el('j-from').value;
        const to = el('j-to').value;
        let url = `/api/journal/summary?trader=${tr}&from_date=${from}&to_date=${to}`;
        const sumR = await fetch(url).then(x=>x.json());
        const sum = sumR.summary;
        el('j-summary').innerHTML = `
            <div class="g4">
                <div><div>Total Sessions</div><div class="med">${sum.total_sessions}</div></div>
                <div><div>W/L</div><div class="med">${sum.wins} / ${sum.losses} (${sum.win_rate.toFixed(1)}%)</div></div>
                <div><div>Net PnL</div><div class="med ${sum.net_pnl>=0?'g':'r'}">${sum.net_pnl.toFixed(2)}</div></div>
                <div><div>Avg PnL</div><div class="med ${sum.avg_pnl>=0?'g':'r'}">${sum.avg_pnl.toFixed(2)}</div></div>
            </div>`;
            
        url = `/api/journal/sessions?trader=${tr}&from_date=${from}&to_date=${to}`;
        const sessR = await fetch(url).then(x=>x.json());
        el('j-list').innerHTML = sessR.sessions.map(s => {
            return `<div class="card" style="cursor:pointer" onclick="_expandSess('${s.session_id}', this)">
                <div class="rw">
                    <span>${s.session_date} | ${s.trader_name}</span>
                    <span class="${s.total_pnl>=0?'g':'r'}">${s.total_pnl.toFixed(2)}</span>
                    <span class="gr">${s.close_reason || 'open'}</span>
                </div>
                <div class="sess-trades" style="display:none;margin-top:5px;border-top:1px solid #1a2540;padding-top:5px"></div>
            </div>`;
        }).join('');
    }
    
    async function _expandSess(sid, div) {
        const tDiv = div.querySelector('.sess-trades');
        if(tDiv.style.display === 'block') {
            tDiv.style.display = 'none';
            return;
        }
        const r = await fetch(`/api/journal/sessions/${sid}/trades`).then(x=>x.json());
        tDiv.innerHTML = r.trades.map(t => `<div class="rw">
            <span>${t.ts_ist.substr(11)} | ${t.action} ${t.symbol}</span>
            <span class="${t.pnl>=0?'g':'r'}">${t.pnl.toFixed(2)}</span>
        </div>`).join('');
        tDiv.style.display = 'block';
    }

// ── Volatile buttons ─────────────────────────────────────────
async function _volForceClose() {
  if(confirm("Force close volatile trader?")) {
    await fetch('/api/volatile/force-close', {method: 'POST'});
  }
}
async function _volClearMemory() {
  if(confirm("Clear volatile trader memory?")) {
    await fetch('/api/volatile/clear-memory', {method: 'POST'});
  }
}
async function _updateSessionBanner() {
  try {
    const r = await fetch('/api/session-info').then(x=>x.json());
    if(r.session_start) {
      el('sb-range').innerText = `${r.session_start.substr(5)} to ${r.session_end.substr(5)}`;
      el('sb-expiry').innerText = `${r.expiry_in_h}:${r.expiry_in_m}`;
    }
  } catch(e) {}
}
"""

content = content.replace('// ── Config conflict validation', injections + '\\n// ── Config conflict validation')

content = content.replace('if (i === 5) renderObTab();', '''if (i === 5) renderObTab();
      if (i === 6) { renderJournalTab(); _fetchJournal(); }
      if (i === 7) { 
        if(!el('settings-root').innerHTML) {
          el('settings-root').innerHTML = `
            <div style="display:flex;justify-content:flex-end;margin-bottom:10px;gap:10px">
              <button id="discard-btn" class="btn" onclick="discardSettings()">Discard</button>
              <button id="save-btn" class="btn bg" onclick="saveSettings()" disabled>Save Settings</button>
            </div>
            <div id="sg-wrap"></div>
          `;
        }
        loadSettings(); 
      }''')

content = content.replace(
'''      subs: [
        {
          label: '💵 Virtual Account',''',
'''      subs: [
        { label: '📅 Trading Calendar', calendar: 'bull' },
        {
          label: '💵 Virtual Account','''
)
content = content.replace(
'''      subs: [
        {
          label: '🕒 Analysis & Timeframe',''',
'''      subs: [
        { label: '📅 Trading Calendar', calendar: 'bear' },
        {
          label: '🕒 Analysis & Timeframe','''
)
content = content.replace(
'''      subs: [
        {
          label: '🕒 Execution Window',''',
'''      subs: [
        { label: '📅 Trading Calendar', calendar: 'vol' },
        {
          label: '🕒 Execution Window','''
)

cal_inj = '''
          if (sub.calendar) {
            html += _buildCalendarSection(sub.calendar);
            return;
          }
'''
content = content.replace('if (sub.timeHM) {', cal_inj + '          if (sub.timeHM) {')

old_save = '''      fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfgEdits)
      }).then(r => r.json()).then(d => {'''

new_save = '''      fetch('/api/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cfgEdits)
      }).then(async r => {
        if (r.status === 422) {
            const err = await r.json();
            alert("Validation Errors:\\n" + err.detail.errors.join("\\n"));
            throw new Error("Validation 422");
        }
        return r.json();
      }).then(d => {'''

content = content.replace(old_save, new_save)

boot_inj = '''
  _updateSessionBanner();
  setInterval(_updateSessionBanner, 60000);
'''
content = content.replace('const boot = async () => {', 'const boot = async () => {' + boot_inj)

with open('panel.html', 'w', encoding='utf-8') as f:
    f.write(content)
