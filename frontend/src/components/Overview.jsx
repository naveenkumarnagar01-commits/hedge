import React from "react";

function Card({ title, children }) {
  return (
    <div className="bg-gray-900 rounded border border-gray-700 p-3">
      <div className="text-xs text-gray-500 mb-1 uppercase tracking-wider">{title}</div>
      {children}
    </div>
  );
}

function PriceRow({ label, value, cls = "text-white" }) {
  return (
    <div className="flex justify-between py-0.5">
      <span className="text-gray-400 text-xs">{label}</span>
      <span className={`text-xs font-bold ${cls}`}>{value}</span>
    </div>
  );
}

function fmt(v, dec = 2) {
  if (!v && v !== 0) return "—";
  return Number(v).toLocaleString("en-US", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

export default function Overview({ state }) {
  const { spot, futures, optionsChain } = state;

  const puts  = (optionsChain || []).filter(o => o.side === "P").sort((a,b) => a.strike - b.strike);
  const calls = (optionsChain || []).filter(o => o.side === "C").sort((a,b) => a.strike - b.strike);

  // Find ATM strikes (closest to mark price)
  const mark = futures.mark_price || 0;
  const allStrikes = [...new Set((optionsChain || []).map(o => o.strike))].sort((a,b) => a-b);
  const atmStrike  = allStrikes.reduce((prev, curr) =>
    Math.abs(curr - mark) < Math.abs(prev - mark) ? curr : prev, allStrikes[0] || 0);

  const atmPuts  = puts.filter(o => Math.abs(o.strike - atmStrike) < 1000).slice(0, 5);
  const atmCalls = calls.filter(o => Math.abs(o.strike - atmStrike) < 1000).slice(0, 5);
  const nearChain = [...atmPuts, ...atmCalls].sort((a,b) => a.strike - b.strike);

  return (
    <div className="space-y-4">
      {/* Price cards */}
      <div className="grid grid-cols-3 gap-3">
        <Card title="BTC Spot">
          <PriceRow label="Best Bid" value={`$${fmt(spot.bid)}`} cls="text-green-400" />
          <PriceRow label="Best Ask" value={`$${fmt(spot.ask)}`} cls="text-red-400" />
          <PriceRow label="Last"     value={`$${fmt(spot.last_price)}`} />
        </Card>

        <Card title="BTC Futures (Perp)">
          <PriceRow label="Mark Price" value={`$${fmt(futures.mark_price)}`} cls="text-yellow-300" />
          <PriceRow label="Premium"    value={`${fmt(futures.premium, 4)}%`} />
          <PriceRow label="Bid / Ask"  value={`${fmt(futures.bid)} / ${fmt(futures.ask)}`} />
        </Card>

        <Card title="Options Chain">
          <PriceRow label="Active Strikes" value={(optionsChain||[]).length} />
          <PriceRow label="ATM Strike"     value={`$${fmt(atmStrike, 0)}`} />
          <PriceRow label="Mark Price"     value={`$${fmt(mark)}`} />
        </Card>
      </div>

      {/* Options chain table */}
      <div className="bg-gray-900 rounded border border-gray-700 p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">
          Options Chain — Today&apos;s Expiry (near ATM)
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-500 border-b border-gray-700">
                <th className="text-left py-1">Symbol</th>
                <th className="text-right">Strike</th>
                <th className="text-right">Side</th>
                <th className="text-right">Bid</th>
                <th className="text-right">Ask</th>
                <th className="text-right">Mark</th>
                <th className="text-right">Intrinsic</th>
                <th className="text-right">Time Val</th>
                <th className="text-right">IV</th>
                <th className="text-right">Qty@Ask</th>
              </tr>
            </thead>
            <tbody>
              {nearChain.length === 0 ? (
                <tr><td colSpan={10} className="text-center text-gray-600 py-4">
                  Waiting for options data…
                </td></tr>
              ) : nearChain.map(o => {
                const isAtm = Math.abs(o.strike - atmStrike) < 1;
                return (
                  <tr key={o.symbol}
                    className={`border-b border-gray-800 hover:bg-gray-800 transition-colors
                      ${isAtm ? "bg-gray-800" : ""}
                      ${o.side === "C" ? "text-blue-300" : "text-red-300"}`}>
                    <td className="py-0.5 text-gray-300 text-xs">{o.symbol}</td>
                    <td className="text-right">{fmt(o.strike, 0)}</td>
                    <td className="text-right font-bold">{o.side === "C" ? "CALL" : "PUT"}</td>
                    <td className="text-right">{fmt(o.bid)}</td>
                    <td className="text-right">{fmt(o.ask)}</td>
                    <td className="text-right">{fmt(o.mark)}</td>
                    <td className="text-right">{fmt(o.intrinsic)}</td>
                    <td className="text-right">{fmt(o.time_value)}</td>
                    <td className="text-right">{fmt(o.iv, 4)}</td>
                    <td className="text-right">{fmt(o.qty_at_ask, 4)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Live log tail */}
      <div className="bg-gray-900 rounded border border-gray-700 p-3">
        <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">System Log (live)</div>
        <div className="space-y-0.5 max-h-40 overflow-y-auto font-mono text-xs">
          {(state.logs || []).slice(0, 30).map((l, i) => (
            <div key={i} className={`flex gap-2
              ${l.level === "WARNING" ? "text-yellow-400"
              : l.level === "ERROR"   ? "text-red-400"
              : "text-gray-400"}`}>
              <span className="text-gray-600 shrink-0">{l.ts_ist?.split(" ")[1] || ""}</span>
              <span className="text-gray-500 shrink-0">[{l.source}]</span>
              <span>{l.message}</span>
            </div>
          ))}
          {(state.logs || []).length === 0 && (
            <div className="text-gray-600">No log events yet…</div>
          )}
        </div>
      </div>
    </div>
  );
}
