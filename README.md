# ParulScanner

Live NSE equity 1-minute volume scanner using Upstox Market Data Feed V3.

Environment variable required:
- `UPSTOX_ACCESS_TOKEN`

The app does not place orders. It only reads live market data and calculates:
Current 1-minute volume / average volume of previous 5 completed 1-minute candles.

Minimum price: ₹100.
