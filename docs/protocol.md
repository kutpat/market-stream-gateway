# WebSocket protocol v1

Connect to `WS /v1/stream`. The server sends a `hello` frame before accepting client commands:

```json
{
  "type": "hello",
  "schema_version": 1,
  "stream_epoch": "0198fc9f-bba2-7f31-a567-001122334455",
  "max_subscriptions": 512,
  "max_provider_subscriptions": 100
}
```

`max_subscriptions` bounds one client across all providers. `max_provider_subscriptions` is the
minimum ceiling safe to apply uniformly to every enabled provider. It can understate the capacity
of more permissive venues.

## Client commands

Commands are strict JSON objects. Unknown fields are rejected. Every command carries a client
chosen `request_id` used to correlate the response.

```json
{
  "op": "subscribe",
  "request_id": "client-1",
  "subscriptions": [{
    "provider": "bybit",
    "market": "linear_perpetual",
    "symbol": "BTCUSDT",
    "channels": ["ticker", "candle_1m"]
  }]
}
```

The supported operations are `subscribe`, `unsubscribe`, and `ping`. Subscriptions always use an
exact provider, market, venue symbol, and channel. The gateway checks them against the latest
successful catalog snapshot before changing demand.

An `ack` means the command was accepted and recorded locally. Provider acknowledgement is reflected
by readiness; the client command response does not claim the upstream route is live yet.

```json
{
  "type": "ack",
  "schema_version": 1,
  "request_id": "client-1",
  "operation": "subscribe",
  "subscriptions": [{
    "provider": "bybit",
    "market": "linear_perpetual",
    "symbol": "BTCUSDT",
    "channel": "ticker"
  }]
}
```

Errors use stable codes: `invalid_json`, `invalid_command`, `limit_exceeded`,
`unsupported_subscription`, `lagged`, and `internal`.

## Market events

Every provider emits the same event envelope while retaining its exact venue identity:

```json
{
  "schema_version": 1,
  "stream_epoch": "0198fc9f-bba2-7f31-a567-001122334455",
  "delivery_sequence": 42,
  "connection_epoch": "0198fca0-159e-7281-b215-aabbccddeeff",
  "instrument_id": "bybit:linear_perpetual:BTCUSDT",
  "provider": "bybit",
  "market": "linear_perpetual",
  "symbol": "BTCUSDT",
  "exchange_time_ms": 1754000000123,
  "gateway_received_time_ms": 1754000000130,
  "type": "ticker",
  "data": {
    "last": {"value": "115432.1", "observed_at_ms": 1754000000123},
    "mark": {"value": "115430.8", "observed_at_ms": 1754000000119}
  }
}
```

Decimal values are strings. Ticker fields are optional and independently timestamped because some
venues publish last, mark, index, funding, and best prices on different feeds. An omitted field
remains unavailable; adapters do not substitute another price for it.

## Delivery semantics

Delivery is live and best effort:

- `stream_epoch` changes when the gateway process starts.
- `delivery_sequence` detects local loss only within one stream epoch.
- `connection_epoch` changes whenever a provider endpoint reconnects.
- Duplicate observations are possible around reconnects.
- Provider source sequences are never compared across connection epochs.

Each client has a bounded queue. If that queue overruns, the gateway sends a `gap` frame when
possible and closes the connection. The client must reconnect and repair the missing candle window
before resuming authoritative processing.

## Candle semantics

Candle end timestamps are exclusive. A one-minute candle starting at `12:00:00Z` ends at
`12:01:00Z`. Finality is `open`, `closed`, or `unknown`; consumers must not treat `unknown` as a
confirmed close.

Volume dimensions remain distinct:

- `base_volume` is base-asset quantity.
- `quote_volume` is quote-asset notional.
- `contract_volume` is contract count.

Unavailable or untrusted values are omitted rather than estimated.

## Discovery and repair

`GET /v1/instruments` returns currently live linear perpetuals and supports exact provider,
market, symbol, and asset filters. `GET /v1/catalog/status` exposes refresh state for every enabled
provider.

`GET /v1/candles` accepts `provider`, `symbol`, minute-aligned `start_time_ms`, exclusive
`end_time_ms`, and an optional `limit`. Requests, provider pagination, concurrent work, and output
size are bounded. Results are returned in ascending order and conflicting duplicate candles are
rejected.
