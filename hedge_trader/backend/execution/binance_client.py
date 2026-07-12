"""
Binance order execution client.
Uses HMAC-SHA256 signed REST calls for all order operations.
API key/secret come from environment — never hardcoded.

NOTE: Your current key is READ-ONLY.
      For live trading, create a key with Futures + Options trading permissions.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import urllib.request
from typing import Optional

from backend.config import cfg

log = logging.getLogger("binance_client")

_FAPI = "https://fapi.binance.com"   # USD-M Futures REST
_EAPI = "https://eapi.binance.com"   # European Options REST


def _sign(params: dict, secret: str) -> str:
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    qs = urllib.parse.urlencode(params)
    sig = hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def _headers() -> dict:
    return {
        "X-MBX-APIKEY": cfg.binance_api_key,
        "Content-Type": "application/x-www-form-urlencoded",
    }


class BinanceClient:
    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def _post(self, base: str, path: str, params: dict) -> Optional[dict]:
        secret = cfg.binance_api_secret
        if not cfg.binance_api_key or not secret:
            log.error("API key/secret not configured — cannot place order.")
            return None

        body = _sign(params, secret).encode()
        url  = f"{base}{path}"
        req  = urllib.request.Request(url, data=body, method="POST", headers=_headers())
        try:
            loop = asyncio.get_event_loop()
            raw  = await loop.run_in_executor(None, lambda: _do_request(req))
            return json.loads(raw)
        except Exception as e:
            log.error(f"POST {path} failed: {e}")
            return None

    async def _delete(self, base: str, path: str, params: dict) -> Optional[dict]:
        secret = cfg.binance_api_secret
        qs     = _sign(params, secret)
        url    = f"{base}{path}?{qs}"
        req    = urllib.request.Request(url, method="DELETE", headers=_headers())
        try:
            loop = asyncio.get_event_loop()
            raw  = await loop.run_in_executor(None, lambda: _do_request(req))
            return json.loads(raw)
        except Exception as e:
            log.error(f"DELETE {path} failed: {e}")
            return None

    async def _get(self, base: str, path: str, params: dict,
                   signed: bool = False) -> Optional[dict]:
        if signed:
            qs = _sign(params, cfg.binance_api_secret)
        else:
            qs = urllib.parse.urlencode(params)
        url = f"{base}{path}?{qs}"
        req = urllib.request.Request(url, headers=_headers())
        try:
            loop = asyncio.get_event_loop()
            raw  = await loop.run_in_executor(None, lambda: _do_request(req))
            return json.loads(raw)
        except Exception as e:
            log.error(f"GET {path} failed: {e}")
            return None

    # ── Futures orders ─────────────────────────────────────────────────────

    async def place_futures_market(self, symbol: str, side: str, qty: float) -> Optional[dict]:
        """Place a futures MARKET order. Returns fill dict or None."""
        params = {
            "symbol":           symbol,
            "side":             side,
            "type":             "MARKET",
            "quantity":         qty,
            "newOrderRespType": "RESULT",
        }
        resp = await self._post(_FAPI, "/fapi/v1/order", params)
        if not resp or "orderId" not in resp:
            log.error(f"Futures market order failed: {resp}")
            return None
        return {
            "order_id":  resp["orderId"],
            "avg_price": float(resp.get("avgPrice", resp.get("price", 0))),
            "qty":       float(resp.get("executedQty", qty)),
            "filled":    True,
        }

    async def place_futures_limit(self, symbol: str, side: str, qty: float,
                                   price: float, tif: str = "GTC") -> Optional[dict]:
        params = {
            "symbol":      symbol,
            "side":        side,
            "type":        "LIMIT",
            "quantity":    qty,
            "price":       price,
            "timeInForce": tif,
        }
        resp = await self._post(_FAPI, "/fapi/v1/order", params)
        if not resp or "orderId" not in resp:
            return None
        return {
            "order_id":  resp["orderId"],
            "avg_price": float(resp.get("avgPrice", price)),
            "qty":       float(resp.get("executedQty", 0)),
            "filled":    float(resp.get("executedQty", 0)) >= qty,
        }

    async def cancel_futures_order(self, symbol: str, order_id: int) -> Optional[dict]:
        return await self._delete(_FAPI, "/fapi/v1/order", {
            "symbol":  symbol,
            "orderId": order_id,
        })

    async def get_futures_position(self, symbol: str) -> Optional[dict]:
        data = await self._get(_FAPI, "/fapi/v2/positionRisk", {"symbol": symbol}, signed=True)
        if data and isinstance(data, list):
            return data[0] if data else None
        return data

    async def set_leverage(self, symbol: str, leverage: int) -> Optional[dict]:
        return await self._post(_FAPI, "/fapi/v1/leverage", {
            "symbol":   symbol,
            "leverage": leverage,
        })

    # ── Options orders ─────────────────────────────────────────────────────

    async def place_option_order(self, symbol: str, side: str, qty: float,
                                  price: float, timeout_sec: int = 2,
                                  order_type: str = "LIMIT") -> Optional[dict]:
        """
        Place an options LIMIT order and wait for fill within timeout_sec.
        If not filled, cancel and return unfilled result.
        """
        params: dict = {
            "symbol":   symbol,
            "side":     side,
            "type":     order_type,
            "quantity": qty,
        }
        if order_type == "LIMIT":
            params["price"] = price
            params["timeInForce"] = "GTC"

        resp = await self._post(_EAPI, "/eapi/v1/order", params)
        if not resp or "orderId" not in resp:
            log.error(f"Options order failed: {resp}")
            return None

        order_id = resp["orderId"]

        # Poll for fill within timeout
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            await asyncio.sleep(0.2)
            status = await self._get(_EAPI, "/eapi/v1/order",
                                      {"symbol": symbol, "orderId": order_id}, signed=True)
            if status and status.get("status") == "FILLED":
                return {
                    "order_id":  order_id,
                    "avg_price": float(status.get("avgPrice", price)),
                    "qty":       float(status.get("executedQty", qty)),
                    "filled":    True,
                }

        # Not filled — cancel
        await self._delete(_EAPI, "/eapi/v1/order",
                           {"symbol": symbol, "orderId": order_id})
        log.warning(f"Options order {order_id} not filled within {timeout_sec}s — cancelled.")
        return {"order_id": order_id, "filled": False, "avg_price": 0.0, "qty": 0.0}

    # ── Account ────────────────────────────────────────────────────────────

    async def get_futures_account(self) -> Optional[dict]:
        return await self._get(_FAPI, "/fapi/v2/account", {}, signed=True)

    async def get_options_account(self) -> Optional[dict]:
        return await self._get(_EAPI, "/eapi/v1/account", {}, signed=True)


def _do_request(req) -> str:
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read().decode()


# Singleton
client = BinanceClient()
