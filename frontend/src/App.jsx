import React, { useState, useReducer, useCallback } from "react";
import { useWebSocket } from "./useWebSocket";
import Overview        from "./components/Overview";
import AnalystRoom     from "./components/AnalystRoom";
import ExecutorSection from "./components/ExecutorSection";
import ForwardTesting  from "./components/ForwardTesting";
import SettingsPanel   from "./components/SettingsPanel";

const TABS = ["Overview", "Analyst Room", "Executors", "Forward Test", "Settings"];

// Centralised state — updated by WS messages
function reducer(state, action) {
  switch (action.type) {
    case "TICK_SPOT":
      return { ...state, spot: action.data };
    case "TICK_FUTURES":
      return { ...state, futures: action.data };
    case "OPTIONS_CHAIN":
      return { ...state, optionsChain: action.data.chain || [] };
    case "ANALYST_LINES":
      return { ...state, analystLines: action.data };
    case "POSITION_UPDATE": {
      const key = action.data.executor;
      return { ...state, positions: { ...state.positions, [key]: action.data } };
    }
    case "MANAGER_DECISION":
      return {
        ...state,
        managerLogs: [action.data, ...state.managerLogs].slice(0, 200),
      };
    case "LOG_EVENT":
      return {
        ...state,
        logs: [action.data, ...state.logs].slice(0, 500),
      };
    case "FEED_STATUS": {
      const feeds = { ...state.feeds, [action.data.feed]: action.data };
      return { ...state, feeds };
    }
    case "SYSTEM_STATUS":
      return { ...state, systemMode: action.data.mode };
    default:
      return state;
  }
}

const initState = {
  spot:        {},
  futures:     {},
  optionsChain:[],
  analystLines:{},
  positions:   {},
  managerLogs: [],
  logs:        [],
  feeds:       {},
  systemMode:  "normal",
};

export default function App() {
  const [tab, setTab]   = useState(0);
  const [state, dispatch] = useReducer(reducer, initState);

  const handlers = {
    "*": useCallback((msg) => dispatch({ type: msg.type, data: msg.data }), []),
  };

  const { connected, latency } = useWebSocket(handlers);

  const latencyColor = !connected ? "bg-red-500"
    : latency == null             ? "bg-gray-400"
    : latency < 100               ? "bg-green-400"
    : latency < 300               ? "bg-yellow-400"
    :                               "bg-red-400";

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 font-mono">
      {/* ── Header ── */}
      <header className="flex items-center justify-between px-4 py-2 bg-gray-900 border-b border-gray-700">
        <div className="flex items-center gap-3">
          <span className="text-yellow-400 text-xl font-bold">₿ HEDGE TRADER</span>
          <span className="text-xs text-gray-500">AI-Managed BTC Platform</span>
        </div>
        <div className="flex items-center gap-4">
          {/* Safe mode badge */}
          {state.systemMode === "safe" && (
            <span className="px-2 py-0.5 text-xs bg-red-700 rounded animate-pulse">
              SAFE MODE
            </span>
          )}
          {/* Feed health dots */}
          <div className="flex gap-1">
            {["spot_feed","futures_feed","options_feed"].map(f => {
              const s = state.feeds[f]?.status || "disconnected";
              const c = s === "ok" ? "bg-green-400" : s === "stale" ? "bg-yellow-400" : "bg-red-500";
              return <span key={f} title={`${f}: ${s}`}
                           className={`w-2 h-2 rounded-full ${c}`} />;
            })}
          </div>
          {/* WS latency */}
          <div className="flex items-center gap-1 text-xs">
            <span className={`w-2 h-2 rounded-full ${latencyColor}`} />
            <span className="text-gray-400">
              {connected ? (latency != null ? `${latency}ms` : "…") : "DISCONNECTED"}
            </span>
          </div>
          {/* Futures price */}
          <span className="text-green-400 font-bold text-sm">
            {state.futures.mark_price
              ? `$${Number(state.futures.mark_price).toLocaleString("en-US", {minimumFractionDigits:2})}`
              : "—"}
          </span>
        </div>
      </header>

      {/* ── Tab bar ── */}
      <nav className="flex bg-gray-900 border-b border-gray-700">
        {TABS.map((t, i) => (
          <button key={t} onClick={() => setTab(i)}
            className={`px-5 py-2 text-sm transition-colors
              ${tab === i
                ? "border-b-2 border-yellow-400 text-yellow-300"
                : "text-gray-400 hover:text-gray-200"}`}>
            {t}
          </button>
        ))}
      </nav>

      {/* ── Content ── */}
      <main className="p-4">
        {tab === 0 && <Overview state={state} />}
        {tab === 1 && <AnalystRoom state={state} />}
        {tab === 2 && <ExecutorSection state={state} />}
        {tab === 3 && <ForwardTesting state={state} />}
        {tab === 4 && <SettingsPanel />}
      </main>
    </div>
  );
}
