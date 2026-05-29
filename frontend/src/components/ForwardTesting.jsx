import React, { useEffect, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, ReferenceLine } from "recharts";

function fmt(v, d = 2) {
  if (v == null) return "—";
  return Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

export default function ForwardTesting() {
  const [data, setData] = useState(null);

  const load = () => {
    fetch("/api/forward").then(r => r.json()).then(setData);
  };

  useEffect(() => {
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, []);

  const reset = async () => {
    if (!confirm("Reset paper account to $100,000 USDT?")) return;
    await fetch("/api/forward/reset", { method: "POST" });
    load();
  };

  if (!data) return <div className="text-gray-500 text-sm">Loading…</div>;

  const initBal = 100_000;
  const pnl     = (data.equity || 0) - initBal;
  const pnlPct  = (pnl / initBal) * 100;
  const pnlCls  = pnl >= 0 ? "text-green-400" : "text-red-400";

  const equityCurve = (data.equity_curve || []).map(e => ({
    ts:     e.ts_ist?.split(" ")[1] || "",
    equity: e.equity,
  }));

  return (
    <div className="space-y-4">
      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-3">
        <Card title="Virtual Balance" value={`$${fmt(data.balance)}`} cls="text-white" />
        <Card title="Unrealized PnL"  value={`$${fmt(data.unrealized_pnl)}`}
              cls={data.unrealized_pnl >= 0 ? "text-green-400" : "text-red-400"} />
        <Card title="Total Equity"    value={`$${fmt(data.equity)}`} cls="text-yellow-300" />
        <Card title="Total PnL"
              value={`${pnl >= 0 ? "+" : ""}$${fmt(pnl)} (${pnlPct.toFixed(2)}%)`}
              cls={pnlCls} />
      </div>

      {/* Equity curve */}
      <div className="bg-gray-900 rounded border border-gray-700 p-3">
        <div className="flex justify-between items-center mb-2">
          <div className="text-xs text-gray-500 uppercase tracking-wider">
            Virtual Equity Curve ({data.trade_count} trades)
          </div>
          <button onClick={reset}
            className="px-3 py-1 text-xs bg-red-800 hover:bg-red-700 rounded transition-colors">
            Reset Account
          </button>
        </div>
        {equityCurve.length > 1 ? (
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={equityCurve}>
              <XAxis dataKey="ts" tick={{ fontSize: 9, fill: "#6b7280" }}
                     tickFormatter={v => v.slice(0, 5)} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 9, fill: "#6b7280" }}
                     tickFormatter={v => `${(v/1000).toFixed(0)}k`}
                     domain={["auto", "auto"]} />
              <Tooltip
                formatter={(v) => [`$${fmt(v)}`, "Equity"]}
                contentStyle={{ background: "#1f2937", border: "1px solid #374151",
                                fontSize: 11, color: "#e5e7eb" }} />
              <ReferenceLine y={initBal} stroke="#4b5563" strokeDasharray="4 2" />
              <Line type="monotone" dataKey="equity"
                    stroke={pnl >= 0 ? "#22c55e" : "#ef4444"}
                    strokeWidth={1.5} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-48 flex items-center justify-center text-gray-600 text-sm">
            No equity history yet — paper trades will appear here
          </div>
        )}
      </div>

      {/* Open positions */}
      {Object.keys(data.open_positions || {}).length > 0 && (
        <div className="bg-gray-900 rounded border border-gray-700 p-3">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Open Paper Positions</div>
          {Object.entries(data.open_positions).map(([sym, pos]) => (
            <div key={sym} className="flex justify-between text-xs py-0.5 border-b border-gray-800">
              <span className="text-gray-300">{sym}</span>
              <span className={pos.side === "BUY" ? "text-green-400" : "text-red-400"}>{pos.side}</span>
              <span className="text-gray-400">{fmt(pos.qty, 4)} BTC @ ${fmt(pos.avg_price)}</span>
            </div>
          ))}
        </div>
      )}

      {/* Trade history */}
      <div className="bg-gray-900 rounded border border-gray-700 p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">
          Recent Paper Trades (last 50)
        </div>
        <div className="overflow-auto max-h-64">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-500 border-b border-gray-700">
                <th className="text-left py-0.5">Time</th>
                <th className="text-left">Symbol</th>
                <th className="text-left">Side</th>
                <th className="text-right">Qty</th>
                <th className="text-right">Fill Price</th>
                <th className="text-right">PnL</th>
              </tr>
            </thead>
            <tbody>
              {(data.recent_trades || []).length === 0 ? (
                <tr><td colSpan={6} className="text-center text-gray-600 py-4">
                  No paper trades yet — system will paper-trade in parallel with live
                </td></tr>
              ) : (data.recent_trades || []).slice().reverse().map((t, i) => (
                <tr key={i} className="border-b border-gray-800 hover:bg-gray-800">
                  <td className="py-0.5 text-gray-500">{t.ts_ist?.split(" ")[1] || t.ts_ist}</td>
                  <td className="text-gray-300">{t.symbol}</td>
                  <td className={t.side?.includes("BUY") ? "text-green-400" : "text-red-400"}>
                    {t.side}
                  </td>
                  <td className="text-right text-gray-300">{fmt(t.qty, 4)}</td>
                  <td className="text-right text-gray-300">${fmt(t.fill_price)}</td>
                  <td className={`text-right font-bold
                    ${t.pnl > 0 ? "text-green-400" : t.pnl < 0 ? "text-red-400" : "text-gray-500"}`}>
                    {t.pnl != null ? `${t.pnl >= 0 ? "+" : ""}$${fmt(t.pnl)}` : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function Card({ title, value, cls = "text-white" }) {
  return (
    <div className="bg-gray-900 rounded border border-gray-700 p-3">
      <div className="text-xs text-gray-500 mb-1 uppercase tracking-wider">{title}</div>
      <div className={`text-lg font-bold ${cls}`}>{value}</div>
    </div>
  );
}
