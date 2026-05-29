import React, { useEffect, useState } from "react";

const PARAM_GROUPS = [
  {
    label: "Analyst",
    params: [
      { key: "candle_count",    label: "Candles (N)",          type: "int",   min: 5,    max: 200 },
      { key: "candle_tf_minutes", label: "Candle TF (min)",    type: "int",   min: 1,    max: 60  },
      { key: "delta_touch",     label: "Δ Touch (points)",     type: "float", min: 1,    max: 500 },
      { key: "min_touches",     label: "Min Touches",          type: "int",   min: 1,    max: 20  },
      { key: "dominance_ratio", label: "Dominance Ratio",      type: "float", min: 0.5,  max: 1.0 },
    ],
  },
  {
    label: "Trading Window",
    params: [
      { key: "window_start_h", label: "Window Start Hour",   type: "int",  min: 0, max: 23 },
      { key: "window_start_m", label: "Window Start Min",    type: "int",  min: 0, max: 59 },
      { key: "window_end_h",   label: "Window End Hour",     type: "int",  min: 0, max: 23 },
      { key: "window_end_m",   label: "Window End Min",      type: "int",  min: 0, max: 59 },
    ],
  },
  {
    label: "Setup Detection",
    params: [
      { key: "distance_threshold", label: "Distance Threshold (USDT)", type: "float", min: 50,  max: 5000 },
      { key: "touch_range",        label: "Touch Range (points)",      type: "float", min: 5,   max: 500  },
    ],
  },
  {
    label: "Options Filters",
    params: [
      { key: "option_max_premium",  label: "Max Premium (USDT)",     type: "float", min: 50,   max: 5000 },
      { key: "option_min_intrinsic",label: "Min Intrinsic (USDT)",   type: "float", min: 50,   max: 5000 },
      { key: "ask_mark_ratio",      label: "Ask/Mark Ratio",         type: "float", min: 0.00, max: 0.50 },
      { key: "fill_timeout_sec",    label: "Fill Timeout (sec)",     type: "int",   min: 1,    max: 60   },
    ],
  },
  {
    label: "Position Management",
    params: [
      { key: "q_max_btc",               label: "Max Qty (BTC)",          type: "float", min: 0.01, max: 5.0  },
      { key: "profit_factor_step1",     label: "Step 1 Factor",          type: "float", min: 1.0,  max: 5.0  },
      { key: "profit_factor_step2_up",  label: "Step 2 Up Factor",       type: "float", min: 1.0,  max: 10.0 },
      { key: "profit_factor_step2_adv", label: "Step 2 Adverse Factor",  type: "float", min: 1.0,  max: 10.0 },
    ],
  },
  {
    label: "Force Close Times (per trader)",
    params: [
      { key: "bull_force_close_h", label: "Bull FC Hour (IST)", type: "int", min: 0, max: 23 },
      { key: "bull_force_close_m", label: "Bull FC Min",        type: "int", min: 0, max: 59 },
      { key: "bear_force_close_h", label: "Bear FC Hour (IST)", type: "int", min: 0, max: 23 },
      { key: "bear_force_close_m", label: "Bear FC Min",        type: "int", min: 0, max: 59 },
      { key: "vol_force_close_h",  label: "Vol FC Hour (IST)",  type: "int", min: 0, max: 23 },
      { key: "vol_force_close_m",  label: "Vol FC Min",         type: "int", min: 0, max: 59 },
    ],
  },
  {
    label: "Forward Testing",
    params: [
      { key: "virtual_balance_usdt", label: "Virtual Balance (USDT)", type: "float", min: 10000, max: 10_000_000 },
    ],
  },
];

export default function SettingsPanel() {
  const [config, setConfig]   = useState({});
  const [edits,  setEdits]    = useState({});
  const [saved,  setSaved]    = useState(false);
  const [changelog, setChangelog] = useState([]);

  useEffect(() => {
    fetch("/api/config").then(r => r.json()).then(c => {
      setConfig(c);
      setEdits({});
    });
  }, []);

  const onChange = (key, val) => setEdits(e => ({ ...e, [key]: val }));

  const save = async () => {
    const payload = {};
    for (const [k, v] of Object.entries(edits)) {
      payload[k] = v;
    }
    const res  = await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (data.changes && Object.keys(data.changes).length > 0) {
      setChangelog(cl => [
        { ts: data.ts_ist, changes: data.changes },
        ...cl,
      ].slice(0, 20));
    }
    fetch("/api/config").then(r => r.json()).then(c => { setConfig(c); setEdits({}); });
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const hasEdits = Object.keys(edits).length > 0;

  return (
    <div className="space-y-4 max-w-4xl">
      {/* Groups */}
      {PARAM_GROUPS.map(group => (
        <div key={group.label} className="bg-gray-900 rounded border border-gray-700 p-4">
          <div className="text-xs text-yellow-400 uppercase tracking-wider font-bold mb-3">
            {group.label}
          </div>
          <div className="grid grid-cols-2 gap-3">
            {group.params.map(p => {
              const currentVal = edits[p.key] !== undefined ? edits[p.key] : (config[p.key] ?? "");
              const changed    = edits[p.key] !== undefined && edits[p.key] != config[p.key];
              return (
                <div key={p.key} className="flex flex-col gap-1">
                  <label className="text-xs text-gray-400">{p.label}</label>
                  <div className="flex items-center gap-2">
                    <input
                      type="number"
                      step={p.type === "float" ? "0.01" : "1"}
                      min={p.min} max={p.max}
                      value={currentVal}
                      onChange={e => onChange(p.key, p.type === "int"
                        ? parseInt(e.target.value)
                        : parseFloat(e.target.value))}
                      className={`w-full bg-gray-800 border rounded px-2 py-1 text-sm text-white
                        focus:outline-none focus:ring-1 focus:ring-yellow-500 transition
                        ${changed ? "border-yellow-500" : "border-gray-600"}`}
                    />
                    {changed && (
                      <span className="text-xs text-gray-500 shrink-0">
                        was: {config[p.key]}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}

      {/* Save button */}
      <div className="flex items-center gap-4">
        <button onClick={save} disabled={!hasEdits}
          className={`px-6 py-2 rounded text-sm font-bold transition-colors
            ${hasEdits
              ? "bg-yellow-600 hover:bg-yellow-500 text-white"
              : "bg-gray-700 text-gray-500 cursor-not-allowed"}`}>
          {saved ? "✓ Saved!" : "Save Changes"}
        </button>
        {hasEdits && (
          <button onClick={() => setEdits({})}
            className="px-4 py-2 text-sm text-gray-400 hover:text-white transition-colors">
            Discard
          </button>
        )}
        {hasEdits && (
          <span className="text-xs text-yellow-400">
            {Object.keys(edits).length} unsaved change(s)
          </span>
        )}
      </div>

      {/* Change log */}
      {changelog.length > 0 && (
        <div className="bg-gray-900 rounded border border-gray-700 p-3">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Change Log</div>
          <div className="space-y-1 max-h-48 overflow-y-auto">
            {changelog.map((entry, i) => (
              <div key={i} className="text-xs border-b border-gray-800 pb-1">
                <div className="text-gray-500">{entry.ts}</div>
                {Object.entries(entry.changes).map(([k, v]) => (
                  <div key={k} className="text-yellow-300 pl-2">
                    {k}: {v}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
