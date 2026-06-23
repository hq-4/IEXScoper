from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from utils.iex_benchmark_core import (
    MAIN_COLUMNS,
    QUOTE_COLUMNS,
    SYSTEM_EVENT_STRINGS,
    TRADING_STATUS_STRINGS,
    TYPE_NAME_MAP,
    decode_sale_flags,
)


def normalize_rob_message(message: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    msg_type = message["type"]
    target = "quote" if msg_type == "quote_update" else "main"
    row = _blank_quote_row() if target == "quote" else _blank_main_row()
    row["type"] = TYPE_NAME_MAP.get(msg_type, msg_type)
    row["timestamp"] = _timestamp_ns(message.get("timestamp"))
    row["symbol"] = _decode_string(message.get("symbol"))

    if msg_type == "system_event":
        event = _decode_string(message.get("event"))
        row["system_event"] = float(ord(event)) if event else None
        row["system_event_str"] = SYSTEM_EVENT_STRINGS.get(event or "")
    elif msg_type == "security_directive":
        row["flags"] = message.get("flags")
        row["round_lot_size"] = message.get("round_lot_size")
        row["adjusted_poc_close"] = message.get("adjusted_poc_close")
        row["luld_tire"] = message.get("luld_tier")
        row["price"] = _decimal_to_float(message.get("adjusted_poc_close"), scale=4)
    elif msg_type == "trading_status":
        row["status"] = _decode_string(message.get("status"))
        row["reason"] = _decode_string(message.get("reason"))
        row["trading_status_message"] = TRADING_STATUS_STRINGS.get(row["status"] or "")
    elif msg_type == "retail_liquidity_indicator":
        row["retail_liquidity_indicator"] = _decode_string(message.get("indicator"))
    elif msg_type == "operational_halt":
        row["halt_status"] = _decode_string(message.get("halt_status"))
    elif msg_type == "short_sale_price_test_status":
        row["short_sale_status"] = (
            float(message["status"]) if message.get("status") is not None else None
        )
        row["detail"] = _decode_string(message.get("detail"))
    elif msg_type == "quote_update":
        row["flags"] = message.get("flags")
        row["bid_size"] = message.get("bid_size")
        row["ask_size"] = message.get("ask_size")
        row["bid_price"] = _decimal_to_float(message.get("bid_price"))
        row["ask_price"] = _decimal_to_float(message.get("ask_price"))
        row["bid_price_int"] = _price_to_int(message.get("bid_price"))
        row["ask_price_int"] = _price_to_int(message.get("ask_price"))
    elif msg_type == "trade_report":
        row["flags"] = message.get("flags")
        row["size"] = message.get("size")
        row["price"] = _decimal_to_float(message.get("price"))
        row["price_int"] = _price_to_int(message.get("price"))
        row["trade_id"] = message.get("trade_id")
        row["sale_flags"] = decode_sale_flags(message.get("flags"))
    elif msg_type == "official_price":
        row["price_type"] = _decode_string(message.get("price_type"))
        row["price"] = _decimal_to_float(message.get("price"))
    elif msg_type == "trade_break":
        row["size"] = message.get("size")
        row["price"] = _decimal_to_float(message.get("price"))
        row["price_int"] = _price_to_int(message.get("price"))
        row["trade_id"] = message.get("trade_id")
        row["sale_flags"] = decode_sale_flags(message.get("flags"))
    elif msg_type == "auction_information":
        row["price"] = _decimal_to_float(message.get("reference_price"))
    return target, row


def normalize_hq4_message(message: Any) -> tuple[str, dict[str, Any]]:
    payload = _hq4_payload(message)
    msg_type = message.__class__.__name__
    target = "quote" if msg_type == "QuoteUpdate" else "main"
    row = _blank_quote_row() if target == "quote" else _blank_main_row()
    row["type"] = msg_type
    row["timestamp"] = payload.get("timestamp")
    row["symbol"] = payload.get("symbol")

    if msg_type == "SystemEvent":
        row["system_event"] = float(payload["system_event"])
        row["system_event_str"] = payload.get("system_event_str")
    elif msg_type == "SecurityDirective":
        row["flags"] = payload.get("flags")
        row["round_lot_size"] = payload.get("round_lot_size")
        row["adjusted_poc_close"] = payload.get("adjusted_poc_close")
        row["luld_tire"] = payload.get("luld_tire")
        row["price"] = payload.get("price")
    elif msg_type == "TradingStatus":
        row["status"] = payload.get("status")
        row["reason"] = payload.get("reason")
        row["trading_status_message"] = payload.get("trading_status_message")
    elif msg_type == "RetailLiquidity":
        row["retail_liquidity_indicator"] = payload.get("retail_liquidity_indicator")
    elif msg_type == "OperationalHalt":
        row["halt_status"] = payload.get("halt_status")
    elif msg_type == "ShortSalePriceSale":
        row["short_sale_status"] = (
            float(payload["short_sale_status"])
            if payload.get("short_sale_status") is not None
            else None
        )
        row["detail"] = payload.get("detail")
    elif msg_type == "QuoteUpdate":
        row["flags"] = payload.get("flags")
        row["bid_size"] = payload.get("bid_size")
        row["bid_price_int"] = payload.get("bid_price_int")
        row["ask_price_int"] = payload.get("ask_price_int")
        row["ask_size"] = payload.get("ask_size")
        row["bid_price"] = payload.get("bid_price")
        row["ask_price"] = payload.get("ask_price")
    elif msg_type == "TradeReport":
        row["flags"] = payload.get("flags")
        row["size"] = payload.get("size")
        row["price_int"] = payload.get("price_int")
        row["trade_id"] = payload.get("trade_id")
        row["price"] = payload.get("price")
        row["sale_flags"] = decode_sale_flags(payload.get("flags"))
    elif msg_type == "OfficialPrice":
        row["price_type"] = payload.get("price_type")
        row["price"] = payload.get("price")
    elif msg_type == "TradeBreak":
        row["size"] = payload.get("size")
        row["price_int"] = payload.get("price_int")
        row["trade_id"] = payload.get("trade_id")
        row["price"] = payload.get("price")
        row["sale_flags"] = decode_sale_flags(payload.get("sale_flags"))
    elif msg_type == "AuctionInformation":
        row["price"] = payload.get("reference_price")
    return target, row


def _blank_main_row() -> dict[str, Any]:
    return dict.fromkeys(MAIN_COLUMNS)


def _blank_quote_row() -> dict[str, Any]:
    return dict.fromkeys(QUOTE_COLUMNS)


def _hq4_payload(message: Any) -> dict[str, Any]:
    payload = asdict(message) if is_dataclass(message) else dict(vars(message))
    for attr in getattr(message, "__slots__", ()):
        if hasattr(message, attr):
            payload[attr] = getattr(message, attr)
    return payload


def _decode_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8").strip() or None
    text = str(value).strip()
    return text or None


def _timestamp_ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(aware.timestamp() * 1_000_000_000)
    raise TypeError(f"unsupported timestamp type {type(value)!r}")


def _price_to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value * 10_000)
    return int(round(float(value) * 10_000))


def _decimal_to_float(value: Any, scale: int | None = None) -> float | None:
    if value is None:
        return None
    if scale is not None and isinstance(value, int):
        return value / (10**scale)
    if isinstance(value, Decimal):
        return float(value)
    return float(value)
