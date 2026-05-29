import React, { useState } from "react";

function fmt(v, d = 2) {
  if (v == null) return "—";
  return Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

const STATE_COLORS = {
  SLEEP:              "text-gray-400",
  CHECK_ELIGIBILITY:  "text-blue-300 animate-pulse",
  WAIT_TRIGGER:       "text-blue-400 animate-pulse",
  VERIFY_HEDGE_LOOP:  "text-yellow-300 animate-pulse",
  EXECUTE:            "text-yellow-400 animate-pulse",
  MANAGING_POSITION:  "text-green-400",
  PARTIAL_BOOKING:    "text-cyan-400",
  FORCE_CLOSE:        "text-red-400 animate-pulse",
};

function ForceCloseBtn({ trader }) {
  const [loading, setLoading] = useState(false);
  const [msg, setMsg]         = useState("");

  const doClose = async () => {
    if (!confirm(`Force-close ALL positions for ${trader.toUpperCase()} trader?\nThis will sell open futures + hedge option immediately.`)) return;
    setLoading(true);
    try {
      const r = await fetch(`/api/trader/${trader}/force-close`, { method: "POST" });
      const d = await r.json();
      if (d.status === "no_position") setMsg("No open position");
      else if (d.status === "force_close_initiated") setMsg("Squareoff initiated");
      else setMsg(d.status || "Done");
    } catch {
      setMsg("Request failed");
    }
    setLoading(false);
    setTimeout(() => setMsg(""), 6000);
  };

  return (
    <div className="flex items-center gap-2 mt-2 pt-2 border-t border-gray-700">
      <button onClick={doClose} disabled={loading}
        className="px-3 py-1 text-xs bg-red-900 hover:bg-red-700 border border-red-700 rounded transition-colors disabled:opacity-50">
        {loading ? "Closing…" : "Force Close"}
      </button>
      {msg && <span className="text-xs text-red-300">{msg}</span>}
    </div>
  );
}

function ExecutorCard({ label, trader, pos, color }) {
  const state   = pos?.state || "SLEEP";
  const stCls   = STATE_COLORS[state] || "text-gray-400";
  const unreal  = pos?.unrealized_pnl;
  const pnlColor = !unreal ? "text-gray-400"
                 : unreal > 0 ? "text-green-400" : "text-red-400";

  return (
    <div className={`bg-gray-900 rounded border p-3 ${
      state === "SLEEP" ? "border-gray-700" : `border-${color}-700`}`}>
      <div className="flex justify-between items-center mb-2">
        <span className={`font-bold text-sm text-${color}-300`}>{label}</span>
        <span className={`text-xs font-bold ${stCls}`}>{state}</span>
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-0.5 text-xs">
        <Row label="Mark Price"  value={`$${fmt(pos?.mark_price)}`} />
        <Row label="High Line"   value={pos?.high_line ? `$${fmt(pos.high_line, 0)}` : "—"} />
        <Row label="Low Line"    value={pos?.low_line  ? `$${fmt(pos.low_line,  0)}` : "—"} />
        <Row label="Option"      value={pos?.option_symbol || "—"} />
        <Row label="Hedge Qty"   value={pos?.hedge_qty ? fmt(pos.hedge_qty, 3) : "—"} />
        <Row label="Futures Qty" value={pos?.futures_remaining_qty ? `${fmt(pos.futures_remaining_qty, 3)} BTC` : "—"} />
        <Row label="Entry Price" value={pos?.futures_entry ? `$${fmt(pos.futures_entry)}` : "—"} />
        <div className="col-span-2 flex justify-between">
          <span className="text-gray-500">Unrealized PnL</span>
          <span className={`font-bold ${pnlColor}`}>
            {unreal != null ? `$${fmt(unreal)}` : "—"}
          </span>
        </div>
      </div>

      {pos?.option_symbol && pos?.futures_entry > 0 && (
        <StepTargets pos={pos} />
      )}

      <ForceCloseBtn trader={trader} />
    </div>
  );
}

function StepTargets({ pos }) {
  const m  = pos.hedge_premium_paid || 0;
  const u  = pos.unrealized_pnl || 0;
  const t1 = m * 1.10;
  const t2u = m * 1.50;
  const t2a = m * 2.00;

  const pct = t2a > 0 ? Math.min((u / t2a) * 100, 100) : 0;
  const barColor = u < 0 ? "bg-red-600" : u < t1 ? "bg-blue-600" : u < t2u ? "bg-cyan-500" : "bg-green-500";

  return (
    <div className="mt-2 border-t border-gray-700 pt-2">
      <div className="text-xs text-gray-500 mb-1">Step Targets vs PnL</div>
      <div className="w-full bg-gray-800 rounded h-1.5 relative">
        <div className={`h-1.5 rounded ${barColor} transition-all`}
             style={{ width: `${Math.max(0, pct)}%` }} />
        {[{v:t1,label:"S1"},{v:t2u,label:"S2"},{v:t2a,label:"S2A"}].map(mk => (
          <div key={mk.label}
            style={{ left: `${Math.min((mk.v/t2a)*100,100)}%` }}
            className="absolute top-0 h-1.5 w-0.5 bg-gray-400 opacity-60"
            title={`${mk.label}: $${mk.v.toFixed(0)}`} />
        ))}
      </div>
      <div className="flex justify-between text-xs text-gray-600 mt-0.5">
        <span>S1: ${t1.toFixed(0)}</span>
        <span>S2: ${t2u.toFixed(0)}</span>
        <span>S2A: ${t2a.toFixed(0)}</span>
      </div>
    </div>
  );
}

function Row({ label, value }) {
  return (
    <div className="flex justify-between gap-2">
      <span className="text-gray-500">{label}</span>
      <span className="text-gray-200 font-mono">{value}</span>
    </div>
  );
}

export default function ExecutorSection({ state }) {
  const [activeTab, setActiveTab] = useState(0);

  const positions = state.positions || {};
  const bullPos   = positions["BullishExecutor_Paper"]  || {};
  const bearPos   = positions["BearishExecutor_Paper"]  || {};
  const volPos    = positions["VolatileTrader_Paper"]   || {};

  const mgr = state.managerLogs || [];

  return (
    <div className="space-y-4">
      {/* Sub-tabs */}
      <div className="flex gap-1 bg-gray-800 rounded p-1">
        {["Traders", "Manager Logs"].map((t, i) => (
          <button key={t} onClick={() => setActiveTab(i)}
            className={`px-3 py-1 text-xs rounded transition-colors
              ${activeTab === i ? "bg-gray-600 text-white" : "text-gray-400 hover:text-gray-200"}`}>
            {t}
          </button>
        ))}
      </div>

      {activeTab === 0 && (
        <div className="grid grid-cols-3 gap-4">
          <ExecutorCard label="BULL (Paper)" trader="bull" pos={bullPos} color="green"  />
          <ExecutorCard label="BEAR (Paper)" trader="bear" pos={bearPos} color="red"    />
          <ExecutorCard label="VOL (Paper)"  trader="vol"  pos={volPos}  color="purple" />
        </div>
      )}

      {activeTab === 1 && (
        <div className="bg-gray-900 rounded border border-gray-700 p-3">
          <div className="text-xs text-gray-500 mb-2 uppercase tracking-wider">
            Manager Decisions ({mgr.length} total)
          </div>
          <div className="overflow-auto max-h-96">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-500 border-b border-gray-700">
                  <th className="text-left py-0.5 pr-2">Time (IST)</th>
                  <th className="text-left">Executor</th>
                  <th className="text-left">Request</th>
                  <th className="text-left">Status</th>
                  <th className="text-left">Reason</th>
                </tr>
              </thead>
              <tbody>
                {mgr.length === 0 ? (
                  <tr><td colSpan={5} className="text-center text-gray-600 py-4">
                    No decisions yet
                  </td></tr>
                ) : mgr.map((m, i) => (
                  <tr key={i} className="border-b border-gray-800 hover:bg-gray-800">
                    <td className="py-0.5 pr-2 text-gray-500">{m.ts_ist?.split(" ")[1] || m.ts_ist}</td>
                    <td className={m.executor?.includes("Bull") ? "text-green-400" :
                                   m.executor?.includes("Bear") ? "text-red-400"   : "text-purple-400"}>
                      {m.executor}
                    </td>
                    <td className="text-gray-300">{m.action || m.request_type}</td>
                    <td className={m.status === "APPROVED" ? "text-green-400 font-bold" : "text-red-400 font-bold"}>
                      {m.status}
                    </td>
                    <td className="text-gray-500 max-w-xs truncate">{m.reason}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Live event log */}
      <div className="bg-gray-900 rounded border border-gray-700 p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Executor Log (live)</div>
        <div className="space-y-0.5 max-h-48 overflow-y-auto font-mono text-xs">
          {(state.logs || []).filter(l =>
            l.source?.includes("Executor") || l.source?.includes("Trader") || l.source?.includes("Manager")
          ).slice(0, 50).map((l, i) => (
            <div key={i} className={`flex gap-2
              ${l.level === "WARNING" ? "text-yellow-400"
              : l.level === "ERROR"   ? "text-red-400"
              : "text-gray-400"}`}>
              <span className="text-gray-600 shrink-0">{l.ts_ist?.split(" ")[1] || ""}</span>
              <span className="text-gray-500 shrink-0">[{l.source}]</span>
              <span>{l.message}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
