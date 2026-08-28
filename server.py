import os
import gzip
import json
import time
import logging
import threading
import requests

from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse


logging.basicConfig(level=logging.INFO)


# ============================================================
# CONFIGURATION
# ============================================================

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

BASE = "https://api.upstox.com"
INSTR_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

IST = timezone(timedelta(hours=5, minutes=30))

s = requests.Session()

app = FastAPI()

instruments = []
loaded = 0


# ============================================================
# HEADERS
# ============================================================

def upstox_headers():
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }


# ============================================================
# LOAD NSE EQUITY INSTRUMENTS
# ============================================================

def load():
    global instruments, loaded

    if instruments and time.time() - loaded < 3600:
        return

    if not TOKEN:
        raise HTTPException(
            status_code=500,
            detail="UPSTOX_ACCESS_TOKEN is not configured"
        )

    r = s.get(INSTR_URL, timeout=30)
    r.raise_for_status()

    raw = r.content

    try:
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)

        data = json.loads(raw.decode("utf-8"))

    except Exception as e:
        logging.exception("Instrument file error")
        raise HTTPException(
            status_code=500,
            detail=f"Instrument file error: {e}"
        )

    instruments = [
        x for x in data
        if x.get("segment") == "NSE_EQ"
        and x.get("instrument_type") == "EQ"
        and x.get("security_type") == "NORMAL"
        and x.get("instrument_key")
    ]

    loaded = time.time()

    logging.info(
        "Loaded %s NSE EQ instruments",
        len(instruments)
    )


# ============================================================
# CHUNK HELPER
# ============================================================

def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ============================================================
# GET LIVE 1-MINUTE OHLC
# ============================================================

def quotes():
    out = []

    for batch in chunks(instruments, 500):

        keys = ",".join(
            x["instrument_key"]
            for x in batch
        )

        try:

            r = s.get(
                BASE + "/v3/market-quote/ohlc",
                headers=upstox_headers(),
                params={
                    "instrument_key": keys,
                    "interval": "I1"
                },
                timeout=30
            )

            r.raise_for_status()

            payload = r.json()

            data = payload.get("data", {})

            if isinstance(data, dict):
                out.extend(data.values())

        except Exception as e:

            logging.warning(
                "Quote batch error: %s",
                e
            )

    return out


# ============================================================
# CANDLE TIME
# ============================================================

def candle_time(ts):

    try:

        if not ts:
            return ""

        return datetime.fromtimestamp(
            int(ts) / 1000,
            tz=timezone.utc
        ).astimezone(IST).strftime("%H:%M:%S")

    except Exception:

        return ""


# ============================================================
# SYMBOL MAP
# ============================================================

def make_symbol_map():

    result = {}

    for x in instruments:

        key = x.get("instrument_key")

        if key:
            result[key] = x

        trading_symbol = x.get("trading_symbol")

        if trading_symbol:
            result[trading_symbol] = x

    return result


# ============================================================
# SCANNER
# ============================================================

def run_scan(rows=10, mult=3):

    if not TOKEN:
        raise HTTPException(
            status_code=500,
            detail="UPSTOX_ACCESS_TOKEN is not configured"
        )

    rows = max(1, min(100, int(rows)))
    mult = max(0.1, min(1000, float(mult)))

    load()

    qs = quotes()

    by_key = make_symbol_map()

    results = []

    for q in qs:

        try:

            price = float(
                q.get("last_price") or 0
            )

            # Only shares priced at ₹100 or above
            if price < 100:
                continue

            live = q.get("live_ohlc") or {}
            prev = q.get("prev_ohlc") or {}

            current_volume = float(
                live.get("volume") or 0
            )

            previous_volume = float(
                prev.get("volume") or 0
            )

            if current_volume <= 0:
                continue

            if previous_volume <= 0:
                continue

            volume_multiplier = (
                current_volume / previous_volume
            )

            if volume_multiplier < mult:
                continue

            key = q.get("instrument_token")

            info = by_key.get(key, {})

            symbol = (
                info.get("trading_symbol")
                or q.get("symbol")
                or str(key or "")
            )

            ts = (
                live.get("ts")
                or prev.get("ts")
            )

            results.append({
                "symbol": symbol,
                "price": price,

                "volume": current_volume,
                "avg5": previous_volume,

                "multiplier": round(
                    volume_multiplier,
                    2
                ),

                "time": candle_time(ts),

                "current_1min_volume":
                    current_volume,

                "previous_1min_volume":
                    previous_volume,

                "volume_jump":
                    round(volume_multiplier, 2)
            })

        except Exception as e:

            logging.warning(
                "Scan item error: %s",
                e
            )

    results.sort(
        key=lambda x: x["multiplier"],
        reverse=True
    )

    return {
        "connected": True,

        "mode":
            "Live 1-minute vs previous 1-minute volume",

        "scanned":
            len(instruments),

        "updated_at":
            datetime.now(IST).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "results":
            results[:rows]
    }


# ============================================================
# TELEGRAM SEND MESSAGE
# ============================================================

def telegram_send(chat_id, text):

    if not TELEGRAM_TOKEN:
        logging.warning(
            "TELEGRAM_BOT_TOKEN is not configured"
        )
        return False

    try:

        r = s.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text
            },
            timeout=15
        )

        r.raise_for_status()

        return True

    except Exception as e:

        logging.warning(
            "Telegram send error: %s",
            e
        )

        return False


# ============================================================
# TELEGRAM FORMAT
# ============================================================

def format_scan_message(data):

    results = data.get("results", [])

    if not results:

        return (
            "ParulScanner Bot\n\n"
            "Abhi koi share nahi mila.\n"
            "Volume condition ko satisfy karne wala "
            "share nahi hai."
        )

    lines = [
        "🚀 ParulScanner",
        "",
        "Live 1-minute Volume Jump",
        "Price: ₹100+",
        ""
    ]

    for i, x in enumerate(results, 1):

        lines.append(
            f"{i}. {x['symbol']}\n"
            f"Price: ₹{x['price']:.2f}\n"
            f"Current Vol: {x['current_1min_volume']:.0f}\n"
            f"Previous Vol: {x['previous_1min_volume']:.0f}\n"
            f"Jump: {x['volume_jump']:.2f}x\n"
            f"Time: {x['time']}"
        )

        lines.append("")

    return "\n".join(lines)


# ============================================================
# TELEGRAM /start AND /scan POLLING
# ============================================================

telegram_running = False


def telegram_polling():

    global telegram_running

    if not TELEGRAM_TOKEN:
        logging.warning(
            "Telegram polling disabled: token missing"
        )
        return

    telegram_running = True

    offset = 0

    # Remove any old webhook so polling can work.
    try:

        s.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook",
            timeout=15
        )

    except Exception as e:

        logging.warning(
            "Telegram webhook cleanup error: %s",
            e
        )

    logging.info("Telegram polling started")

    while True:

        try:

            r = s.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params={
                    "timeout": 25,
                    "offset": offset
                },
                timeout=35
            )

            r.raise_for_status()

            payload = r.json()

            updates = payload.get(
                "result",
                []
            )

            for update in updates:

                offset = update["update_id"] + 1

                message = update.get(
                    "message"
                )

                if not message:
                    continue

                chat = message.get(
                    "chat",
                    {}
                )

                chat_id = chat.get(
                    "id"
                )

                text = (
                    message.get("text")
                    or ""
                ).strip()

                if not chat_id:
                    continue

                if text.startswith("/start"):

                    telegram_send(
                        chat_id,
                        "✅ ParulScanner Bot Active\n\n"
                        "Commands:\n"
                        "/scan - Live volume scanner\n"
                        "/start - Bot status\n\n"
                        "Scanner NSE equity shares ko "
                        "check karta hai aur ₹100+ price "
                        "wale shares mein live 1-minute "
                        "volume jump dikhata hai."
                    )

                elif text.startswith("/scan"):

                    try:

                        data = run_scan(
                            rows=10,
                            mult=3
                        )

                        telegram_send(
                            chat_id,
                            format_scan_message(data)
                        )

                    except Exception as e:

                        telegram_send(
                            chat_id,
                            "❌ Scanner error:\n"
                            + str(e)
                        )

                else:

                    telegram_send(
                        chat_id,
                        "Command samajh nahi aaya.\n\n"
                        "/scan bhejiye scanner chalane ke liye."
                    )

        except Exception as e:

            logging.warning(
                "Telegram polling error: %s",
                e
            )

            time.sleep(5)


# ============================================================
# START TELEGRAM THREAD
# ============================================================

@app.on_event("startup")
def startup_event():

    if TELEGRAM_TOKEN:

        thread = threading.Thread(
            target=telegram_polling,
            daemon=True
        )

        thread.start()

    else:

        logging.warning(
            "TELEGRAM_BOT_TOKEN missing"
        )


# ============================================================
# HOME PAGE
# ============================================================

@app.get("/")
def root():

    return FileResponse(
        "index.html"
    )


# ============================================================
# SCANNER API
# ============================================================

@app.get("/api/scan")
def scan(
    rows: int = 10,
    mult: float = 3
):

    return run_scan(
        rows=rows,
        mult=mult
    )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
def health():

    return {
        "status": "ok",
        "upstox": bool(TOKEN),
        "telegram": bool(TELEGRAM_TOKEN)
    }
