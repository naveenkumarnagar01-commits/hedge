/**
 * useWebSocket — connects to the backend WS gateway.
 * Returns { messages, latency, connected }
 * Fires onMessage callback for every incoming message type.
 */
import { useState, useEffect, useRef, useCallback } from "react";

const WS_URL = `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/ws`;

export function useWebSocket(handlers = {}) {
  const [connected, setConnected]   = useState(false);
  const [latency,   setLatency]     = useState(null);
  const wsRef      = useRef(null);
  const pingTs     = useRef(null);
  const handlersRef = useRef(handlers);
  handlersRef.current = handlers;

  const connect = useCallback(() => {
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      // Measure latency
      pingTs.current = Date.now();
      ws.send("ping");
    };

    ws.onmessage = (e) => {
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }

      if (msg.type === "pong" && pingTs.current) {
        setLatency(Date.now() - pingTs.current);
        pingTs.current = null;
      }

      const handler = handlersRef.current[msg.type] || handlersRef.current["*"];
      if (handler) handler(msg);
    };

    ws.onclose = () => {
      setConnected(false);
      setLatency(null);
      setTimeout(connect, 2000);  // auto-reconnect
    };

    ws.onerror = () => ws.close();
  }, []);

  useEffect(() => {
    connect();
    // Ping every 10s to measure latency
    const iv = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        pingTs.current = Date.now();
        wsRef.current.send("ping");
      }
    }, 10_000);
    return () => { clearInterval(iv); wsRef.current?.close(); };
  }, [connect]);

  return { connected, latency };
}
