import os, json, time, threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
import upstox_client
from flask import Flask, jsonify, send_from_directory

PORT = int(os.environ.get("PORT", "10000"))
TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
IST = ZoneInfo("Asia/Kolkata")

app = Flask(__name__, static_folder=".", static_url_path="")

# instrument_key -> metadata
INSTRUMENTS = {}
# instrument_key -> state
STATE = {}
LOCK = threading.Lock()
CONNECTED = False
LAST_ERROR = ""
LAST_TICK = None

def load_instruments():
    global INSTRUMENTS
    r = requests.get(NSE_URL, timeout=30)
    r.raise_for_status()
    # Upstox publishes the NSE file as gzip-compressed JSON.
    import gzip
    data = gzip.decompress(r.content)
    arr = json.loads(data.decode("utf-8"))
    out = {}
    for x in arr:
        if x.get("segment") != "NSE_EQ":
            continue
        if x.get("instrument_type") != "EQ":
            continue
        if x.get("security_type") not in (None, "", "NORMAL"):
            continue
        key = x.get("instrument_key")
        symbol = x.get("trading_symbol")
        if key and symbol:
            out[key] = symbol
    INSTRUMENTS = out
    print(f"Loaded {len(INSTRUMENTS)} NSE equity instruments")

def minute_key(ts_ms=None):
    if ts_ms is None:
        dt = datetime.now(IST)
    else:
        dt = datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).astimezone(IST)
    return dt.strftime("%Y-%m-%d %H:%M")

def update_from_tick(message):
    global LAST_TICK
    feeds = message.get("feeds", {}) if isinstance(message, dict) else {}
    now = time.time()
    with LOCK:
        for key, feed in feeds.items():
            symbol = INSTRUMENTS.get(key)
            if not symbol:
                continue
            ltpc = feed.get("ltpc") or {}
            price = ltpc.get("ltp")
            ltq = ltpc.get("ltq")
            ltt = ltpc.get("ltt")
            if price is None or ltt is None:
                continue

            # Deduplicate the same trade update.
            s = STATE.setdefault(key, {
                "symbol": symbol, "price": 0.0, "current_min": None,
                "current_volume": 0, "completed": deque(maxlen=5),
                "last_ltt": None, "last_seen": 0
            })
            if s["last_ltt"] == str(ltt):
                continue
            s["last_ltt"] = str(ltt)
            s["price"] = float(price)

            mk = minute_key(ltt)
            if s["current_min"] is None:
                s["current_min"] = mk
            elif mk != s["current_min"]:
                # Move the completed minute into the 5-minute history.
                s["completed"].append(int(s["current_volume"]))
                s["current_min"] = mk
                s["current_volume"] = 0

            try:
                qty = int(float(ltq or 0))
            except Exception:
                qty = 0
            s["current_volume"] += max(qty, 0)
            s["last_seen"] = now
            LAST_TICK = now

def feed_thread():
    global CONNECTED, LAST_ERROR
    if not TOKEN:
        LAST_ERROR = "UPSTOX_ACCESS_TOKEN environment variable is missing."
        return

    try:
        configuration = upstox_client.Configuration()
        configuration.access_token = TOKEN
        streamer = upstox_client.MarketDataStreamerV3(
            upstox_client.ApiClient(configuration)
        )

        def on_open():
            global CONNECTED, LAST_ERROR
            CONNECTED = True
            LAST_ERROR = ""
            keys = list(INSTRUMENTS.keys())
            # V3 LTPC supports up to 5000 keys per subscription, subject to
            # Upstox account/feed limits.
            for i in range(0, len(keys), 4500):
                streamer.subscribe(keys[i:i+4500], "ltpc")
                time.sleep(0.5)
            print(f"Subscribed to {len(keys)} NSE EQ instruments")

        def on_message(message):
            update_from_tick(message)

        def on_close(*args):
            global CONNECTED
            CONNECTED = False
            print("Upstox stream closed", args)

        def on_error(err):
            global LAST_ERROR
            LAST_ERROR = str(err)
            print("Upstox stream error:", err)

        streamer.on("open", on_open)
        streamer.on("message", on_message)
        streamer.on("close", on_close)
        streamer.on("error", on_error)
        streamer.auto_reconnect(True, 10, 100)
        streamer.connect()

    except Exception as e:
        CONNECTED = False
        LAST_ERROR = repr(e)
        print("Feed startup error:", repr(e))

@app.get("/")
def home():
    return send_from_directory(".", "index.html")

@app.get("/api/scanner")
def scanner():
    try:
        min_spike = float(__import__("flask").request.args.get("min_spike", "3"))
        limit = min(int(__import__("flask").request.args.get("limit", "30")), 100)
    except Exception:
        min_spike, limit = 3.0, 30

    rows = []
    with LOCK:
        for s in STATE.values():
            if s["price"] < 100:
                continue
            if len(s["completed"]) < 5:
                continue
            avg5 = sum(s["completed"]) / 5.0
            if avg5 <= 0:
                continue
            spike = s["current_volume"] / avg5
            if spike < min_spike:
                continue
            rows.append({
                "symbol": s["symbol"],
                "price": s["price"],
                "current_volume": s["current_volume"],
                "avg5": avg5,
                "spike": spike,
                "minute": s["current_min"]
            })

    rows.sort(key=lambda x: x["spike"], reverse=True)

    if not TOKEN:
        return jsonify({
            "live": False, "warmup": False, "rows": [],
            "message": "Upstox access token अभी Render Environment Variables में नहीं डाला गया है।"
        })

    if not CONNECTED:
        return jsonify({
            "live": False, "warmup": False, "rows": rows,
            "message": LAST_ERROR or "Upstox live feed अभी connect हो रहा है..."
        })

    if not rows:
        with LOCK:
            count = len(STATE)
        msg = "Live feed connected है। Scanner पिछले 5 completed minutes का baseline बना रहा है।"
        if count > 0:
            msg += f" अभी {count} instruments से ticks मिले हैं।"
        return jsonify({"live": True, "warmup": True, "rows": [], "message": msg})

    return jsonify({
        "live": True, "warmup": False, "rows": rows[:limit],
        "message": "Live Upstox market data"
    })

if __name__ == "__main__":
    try:
        load_instruments()
    except Exception as e:
        LAST_ERROR = f"Instrument file error: {e!r}"
        print(LAST_ERROR)
    if TOKEN and INSTRUMENTS:
        threading.Thread(target=feed_thread, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)
