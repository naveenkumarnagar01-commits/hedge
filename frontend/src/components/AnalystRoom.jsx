import React, { useEffect, useRef, useState } from "react";

function fmt(v, d = 2) {
  if (v == null) return "—";
  return Number(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
}

export default function AnalystRoom({ state }) {
  const { analystLines, futures } = state;
  const chartRef  = useRef(null);
  const chartInst = useRef(null);
  const seriesRef = useRef(null);
  const hlRef     = useRef(null);
  const llRef     = useRef(null);
  const [showCandLog, setShowCandLog] = useState(false);
  const [selectedLevel, setSelectedLevel] = useState(null);

  const candidates = analystLines?.candidates || [];
  const touches    = analystLines?.touches    || [];
  const high_line  = analystLines?.high_line;
  const low_line   = analystLines?.low_line;

  // Build chart from live candles + fetch historical
  const [candles, setCandles] = useState([]);

  useEffect(() => {
    fetch("/api/candles?n=60")
      .then(r => r.json())
      .then(d => setCandles(d.candles || []));
  }, []);

  // Init lightweight-charts
  useEffect(() => {
    if (!chartRef.current) return;
    import("lightweight-charts").then(({ createChart, CrosshairMode }) => {
      if (chartInst.current) { chartInst.current.remove(); }
      const chart = createChart(chartRef.current, {
        layout:     { background: { color: "#111827" }, textColor: "#9ca3af" },
        grid:       { vertLines: { color: "#1f2937" }, horzLines: { color: "#1f2937" } },
        crosshair:  { mode: CrosshairMode.Normal },
        rightPriceScale: { borderColor: "#374151" },
        timeScale:       { borderColor: "#374151", timeVisible: true },
        width:  chartRef.current.offsetWidth,
        height: 340,
      });
      const series = chart.addCandlestickSeries({
        upColor: "#00e676", downColor: "#ff5252",
        borderUpColor: "#00e676", borderDownColor: "#ff5252",
        wickUpColor: "#00e676", wickDownColor: "#ff5252",
      });
      chartInst.current = chart;
      seriesRef.current = series;

      window._chart_resize = () => chart.resize(chartRef.current?.offsetWidth || 600, 340);
      window.addEventListener("resize", window._chart_resize);
    });
    return () => {
      window.removeEventListener("resize", window._chart_resize);
      chartInst.current?.remove();
      chartInst.current = null;
    };
  }, []);

  // Update candle data
  useEffect(() => {
    if (!seriesRef.current || !candles.length) return;
    const data = candles
      .filter(c => c.ts && c.open)
      .map(c => ({
        time:  Math.floor(c.ts / 1000),
        open:  c.open, high: c.high, low: c.low, close: c.close,
      }))
      .sort((a, b) => a.time - b.time);
    seriesRef.current.setData(data);
    chartInst.current?.timeScale().fitContent();
  }, [candles]);

  // Draw horizontal lines
  useEffect(() => {
    if (!seriesRef.current) return;
    if (hlRef.current) { try { chartInst.current?.removePriceLine(hlRef.current); } catch{} }
    if (llRef.current) { try { chartInst.current?.removePriceLine(llRef.current); } catch{} }
    if (high_line) {
      hlRef.current = seriesRef.current.createPriceLine({
        price: high_line, color: "#ef4444", lineWidth: 2, lineStyle: 1,
        axisLabelVisible: true, title: `H-Line: ${fmt(high_line, 0)}`,
      });
    }
    if (low_line) {
      llRef.current = seriesRef.current.createPriceLine({
        price: low_line, color: "#22c55e", lineWidth: 2, lineStyle: 1,
        axisLabelVisible: true, title: `L-Line: ${fmt(low_line, 0)}`,
      });
    }
  }, [high_line, low_line]);

  const manualRun = () => {
    fetch("/api/analyst/run", { method: "POST" })
      .then(r => r.json())
      .then(() => {});
  };

  const selectedTouches = selectedLevel
    ? touches.filter(t => Math.abs(t.level - selectedLevel) < 0.5)
    : [];

  return (
    <div className="space-y-4">
      {/* Line summary */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-gray-900 rounded border border-red-800 p-3">
          <div className="text-xs text-red-400 mb-1">HIGH WEIGHTED LINE</div>
          <div className="text-2xl font-bold text-red-300">
            {high_line ? `$${fmt(high_line, 0)}` : "—"}
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {analystLines?.run_time || "Not computed"}
          </div>
        </div>
        <div className="bg-gray-900 rounded border border-green-800 p-3">
          <div className="text-xs text-green-400 mb-1">LOW WEIGHTED LINE</div>
          <div className="text-2xl font-bold text-green-300">
            {low_line ? `$${fmt(low_line, 0)}` : "—"}
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {analystLines?.run_time || "Not computed"}
          </div>
        </div>
        <div className="bg-gray-900 rounded border border-gray-700 p-3 flex flex-col justify-between">
          <div>
            <div className="text-xs text-gray-500 mb-1">LAST RUN</div>
            <div className="text-sm text-gray-300">{analystLines?.run_time || "—"}</div>
            <div className="text-xs text-gray-600 mt-1">
              {analystLines?.candles_n} candles analysed
            </div>
          </div>
          <button onClick={manualRun}
            className="mt-2 px-3 py-1.5 text-xs bg-blue-700 hover:bg-blue-600 rounded transition-colors">
            Re-run Analysis
          </button>
        </div>
      </div>

      {/* Chart */}
      <div className="bg-gray-900 rounded border border-gray-700 p-2">
        <div className="text-xs text-gray-500 mb-1 px-1">
          5-min BTC Futures Candles — Red: High Line | Green: Low Line
        </div>
        <div ref={chartRef} className="w-full" />
      </div>

      {/* Calculation log toggle */}
      <button onClick={() => setShowCandLog(v => !v)}
        className="w-full py-1.5 text-xs text-gray-400 bg-gray-800 hover:bg-gray-700 rounded border border-gray-700 transition-colors">
        {showCandLog ? "▲ Hide" : "▼ Show"} Candidate Lines Log ({candidates.length} evaluated)
      </button>

      {showCandLog && (
        <div className="bg-gray-900 rounded border border-gray-700 p-3">
          <div className="text-xs text-gray-500 mb-2">
            Click a row to see its touch details
          </div>
          <div className="overflow-auto max-h-64">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-500 border-b border-gray-700">
                  <th className="text-right py-0.5 pr-3">Price Level</th>
                  <th className="text-right">Touches</th>
                  <th className="text-right">High%</th>
                  <th className="text-right">Low%</th>
                  <th className="text-right">High Line?</th>
                  <th className="text-right">Low Line?</th>
                </tr>
              </thead>
              <tbody>
                {candidates.map((c, i) => (
                  <tr key={i}
                    onClick={() => setSelectedLevel(c.level)}
                    className={`border-b border-gray-800 cursor-pointer hover:bg-gray-800 transition-colors
                      ${c.accepted_high ? "text-red-300"
                      : c.accepted_low  ? "text-green-300"
                      : "text-gray-500"}`}>
                    <td className="text-right pr-3">{fmt(c.level, 0)}</td>
                    <td className="text-right">{c.t_total}</td>
                    <td className="text-right">{(c.high_dom * 100).toFixed(0)}%</td>
                    <td className="text-right">{(c.low_dom  * 100).toFixed(0)}%</td>
                    <td className="text-right">{c.accepted_high ? "✓" : ""}</td>
                    <td className="text-right">{c.accepted_low  ? "✓" : ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Touch detail for selected line */}
          {selectedLevel != null && selectedTouches.length > 0 && (
            <div className="mt-3 border-t border-gray-700 pt-2">
              <div className="text-xs text-gray-400 mb-1">
                Touches for level {fmt(selectedLevel, 0)} ({selectedTouches.length} touches)
              </div>
              <div className="grid grid-cols-3 gap-1">
                {selectedTouches.map((t, i) => (
                  <div key={i} className="text-xs text-gray-500 bg-gray-800 rounded px-2 py-0.5">
                    Candle #{t.candle_idx} — {t.type} — dist {fmt(t.distance, 1)}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
