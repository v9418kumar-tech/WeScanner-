import os, gzip, json, time, logging
from datetime import datetime, timezone, timedelta
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
BASE = "https://api.upstox.com"
INSTR_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"

# One reusable HTTP session. The scanner now uses only the V3 OHLC endpoint
# and does not make one historical request per stock.
s = requests.Session()
app = FastAPI()

instruments = []
loaded = 0

IST = timezone(timedelta(hours=5, minutes=30))


def H():
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }


def load():
    global instruments, loaded

    if instruments and time.time() - loaded < 21600:
        return

    r = s.get(INSTR_URL, timeout=30)
    r.raise_for_status()

    data = json.loads(gzip.decompress(r.content))

    instruments = [
        x for x in data
        if x.get("segment") == "NSE_EQ"
        and x.get("instrument_type") == "EQ"
        and x.get("security_type") == "NORMAL"
        and x.get("instrument_key")
    ]

    loaded = time.time()
    logging.info("Loaded %s NSE EQ instruments", len(instruments))


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def quotes():
    """
    V3 OHLC gives:
      live_ohlc  = current 1-minute candle
      prev_ohlc  = previous 1-minute candle

    Therefore we can compare consecutive 1-minute volumes directly,
    without calling the historical-candle API for every stock.
    """
    out = []

    for batch in chunks(instruments, 500):
        keys = ",".join(x["instrument_key"] for x in batch)

        r = s.get(
            BASE + "/v3/market-quote/ohlc",
            headers=H(),
            params={"instrument_key": keys, "interval": "I1"},
            timeout=30,
        )
        r.raise_for_status()

        out.extend(list(r.json().get("data", {}).values()))

    return out


def candle_time(ts):
    try:
        # Upstox returns milliseconds since Unix epoch.
        return datetime.fromtimestamp(
            int(ts) / 1000,
            tz=timezone.utc
        ).astimezone(IST).strftime("%H:%M:%S")
    except Exception:
        return ""


@app.get("/")
def root():
    return FileResponse("index.html")


@app.get("/api/scan")
def scan(rows: int = 10, mult: float = 3):
    if not TOKEN:
        raise HTTPException(
            500,
            "UPSTOX_ACCESS_TOKEN is not configured"
        )

    rows = max(1, min(100, rows))
    mult = max(0.1, min(1000, mult))

    load()
    qs = quotes()

    by_key = {
        x["instrument_key"]: x
        for x in instruments
    }

    results = []

    for q in qs:
        try:
            price = float(q.get("last_price") or 0)

            # Only NSE equity shares priced at Rs 100 or above.
            if price < 100:
                continue

            live = q.get("live_ohlc") or {}
            prev = q.get("prev_ohlc") or {}

            current_volume = float(live.get("volume") or 0)
            previous_volume = float(prev.get("volume") or 0)

            if current_volume <= 0 or previous_volume <= 0:
                continue

            volume_multiplier = current_volume / previous_volume

            if volume_multiplier < mult:
                continue

            key = q.get("instrument_token")

            info = by_key.get(key, {})
            symbol = (
                info.get("trading_symbol")
                or q.get("symbol")
                or str(key or "")
            )

            ts = live.get("ts") or prev.get("ts")

            results.append({
                "symbol": symbol,
                "price": price,

                # Keep the old field names so the existing index.html
                # continues to display the results.
                "volume": current_volume,
                "avg5": previous_volume,

                "multiplier": volume_multiplier,
                "time": candle_time(ts),

                # New explicit fields for the 1-minute comparison.
                "current_1min_volume": current_volume,
                "previous_1min_volume": previous_volume,
                "volume_jump": volume_multiplier,
            })

        except Exception as e:
            logging.warning("scan item error: %s", e)

    results.sort(
        key=lambda x: x["multiplier"],
        reverse=True
    )

    return {
        "connected": True,
        "mode": "Live 1-minute vs previous 1-minute volume",
        "scanned": len(instruments),
        "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        "results": results[:rows],
    }
