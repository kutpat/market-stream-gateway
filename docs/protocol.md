# WebSocket protocol v1

Connect to `/v1/stream`. The server first sends a `hello` control message. A client can then add or
remove explicit subscriptions without opening another connection.

```json
{
  "op": "subscribe",
  "request_id": "worker-1",
  "subscriptions": [
    {
      "provider": "okx",
      "market": "linear_perpetual",
      "symbol": "BTC-USDT-SWAP",
      "channels": ["ticker", "candle_1m"]
    }
  ]
}
```

An accepted command receives an `ack`. Events use the same schema on every provider:

```json
{
  "schema_version": 1,
  "stream_epoch": "0198fc9f-bba2-7f31-a567-001122334455",
  "delivery_sequence": 42,
  "connection_epoch": "0198fca0-159e-7281-b215-aabbccddeeff",
  "instrument_id": "okx:linear_perpetual:BTC-USDT-SWAP",
  "provider": "okx",
  "market": "linear_perpetual",
  "symbol": "BTC-USDT-SWAP",
  "exchange_time_ms": 1754000000123,
  "gateway_received_time_ms": 1754000000130,
  "type": "ticker",
  "data": {
    "last": {"value": "115432.1", "observed_at_ms": 1754000000123},
    "mark": {"value": "115430.8", "observed_at_ms": 1754000000119}
  }
}
```

Decimal values are always strings. Ticker fields are optional and must not be substituted for one
another. The separate `observed_at_ms` values expose staleness when a provider splits ticker state
over several upstream feeds.

Delivery is live and best-effort. `delivery_sequence` is scoped to one `stream_epoch` and lets a
client detect local loss; it does not order events between providers. If a bounded client queue
lags, the gateway emits a `gap` message when possible and closes the connection. The client must
reconnect and repair closed-candle history before resuming authoritative processing.

