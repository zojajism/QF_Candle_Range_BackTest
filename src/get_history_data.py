import oandapyV20
import oandapyV20.endpoints.instruments as instruments

access_token = "989e75aba5470f8edbe455502a86eec4-fec454888a1a6aa1e16720596ce09c2f"
client = oandapyV20.API(access_token=access_token)

instrument = "EUR_USD"   # currency pair
params = {
    "granularity": "M1",      # Hourly candles
    "from": "2026-02-23T70:00:00Z",
    "to": "2026-02-23T70:30:00Z",
    "price": "M"              # mid prices
}

req = instruments.InstrumentsCandles(instrument=instrument, params=params)
response = client.request(req)

candles = response.get("candles", [])
for c in candles:
    print(c["time"], c["mid"])