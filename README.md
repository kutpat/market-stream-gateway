# Market Stream Gateway

A demand-driven Rust service that exposes public derivatives market data from Bybit, Binance,
OKX, KuCoin, MEXC, and BingX through one versioned contract. The wire format is provider-neutral
rather than a copy of Bybit's protocol: provider and exact venue symbol remain explicit, decimals
remain lossless strings, and no adapter invents missing prices, volume, finality, or sequence
numbers.

This repository is local-only for its initial release. Axion Trading Core has a local feature
branch that consumes this contract, but nothing here deploys or changes production.

## V1 capabilities

- Linear perpetual tickers and one-minute candles from all six providers.
- Exact live-instrument discovery with lifecycle, tick/quantity, contract, and capability metadata.
- Bounded historical one-minute candle retrieval for bootstrap and gap repair.
- Shared upstream demand, dynamic native subscriptions, acknowledgement correlation, heartbeats,
  reconnect backoff, proactive connection rotation, and sparse ticker accumulation.
- Bounded per-client routing with explicit gap notification and disconnect on overrun.
- Readiness details, catalog status, Prometheus metrics, structured logs, and graceful shutdown.
- A locked multi-stage container running as UID/GID 10001 with no required credentials.

V1 intentionally does not choose a venue for a trade, synthesize a cross-venue price, place orders,
or provide durable delivery. Those policy and ledger concerns remain in Trading Core.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /health/live` | Process liveness |
| `GET /health/ready` | Stream demand and catalog readiness |
| `GET /metrics` | Prometheus text exposition |
| `GET /v1/providers` | Configured providers and V1 channels |
| `GET /v1/instruments` | Filterable normalized live-instrument catalog |
| `GET /v1/catalog/status` | Per-provider refresh state and last error |
| `GET /v1/candles` | Bounded normalized one-minute history |
| `WS /v1/stream` | Dynamic normalized ticker/candle stream |

Catalog filters are exact `provider`, `market`, `symbol`, `base_asset`, `quote_asset`, and
`settle_asset` query parameters. Candle history requires `provider`, `symbol`, `start_time_ms`, and
`end_time_ms`; bounds are aligned UTC minutes in a half-open `[start,end)` range. `limit` defaults
to 1000 and is capped at 10,000.

## Run locally

Rust 1.97.1 is pinned by `rust-toolchain.toml`.

```console
cargo run --locked -- --bind 127.0.0.1:8080
curl http://127.0.0.1:8080/health/ready
curl "http://127.0.0.1:8080/v1/instruments?provider=okx&symbol=BTC-USDT-SWAP"
```

Use `MSG_PROVIDERS=none` for an offline process/container smoke, or a comma-separated subset such
as `bybit,mexc,bingx`. Public provider endpoints need no API credentials.

```console
docker compose up --build
curl http://127.0.0.1:18070/health/ready
```

The Compose service binds only to host loopback and runs with a read-only root filesystem, all
Linux capabilities dropped, and `no-new-privileges` enabled.

## Stream contract

The server sends `hello` first. A client then requests exact venue instruments:

```json
{
  "op": "subscribe",
  "request_id": "worker-1",
  "subscriptions": [{
    "provider": "okx",
    "market": "linear_perpetual",
    "symbol": "BTC-USDT-SWAP",
    "channels": ["ticker", "candle_1m"]
  }]
}
```

An `ack` means the catalog accepted the exact instrument and the gateway recorded the desired
subscription. Upstream-native acknowledgement is reflected by endpoint readiness; it is not
claimed by the client command ACK. Consumers must wait for the route to become ready and process
`gap` or reconnect events by repairing candle history.

The default per-provider limit is 60 channel subscriptions. A Worker route uses ticker plus
one-minute candle channels, so the default safely admits 30 active routes per provider; the `hello`
frame advertises the configured limits before any subscription command is sent.

Every provider emits the same event envelope:

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

Decimals are always strings. Ticker fields are optional and independently timestamped because
some venues split last, mark, index, funding, and best prices across feeds. Candle end timestamps
are exclusive, finality is `open`, `closed`, or `unknown`, and base/quote/contract volume units stay
separate. Delivery is best-effort and can duplicate after reconnect; `delivery_sequence` detects
loss only within one `stream_epoch`.

## Trading Core migration boundary

The local Trading Core integration uses provider+market+venue-symbol route keys, accumulates sparse
ticker fields by their own `observed_at_ms`, waits for genuine last and mark prices, converts candle
end from exclusive to Core's inclusive end, and accepts only `closed` authoritative candles. Venue
choice, catalog evidence, and resolver version are frozen on each trade so an active trade never
silently moves exchanges.

KuCoin, MEXC, and BingX V1 stream candles remain informational because their public futures feeds
do not provide a reliable explicit close flag; KuCoin also lacks reliable futures volume. Their
REST history uses public server time to classify completed intervals. The Core resolver therefore
requires ticker support plus authoritative one-minute history and at least one trustworthy history
volume dimension. MEXC and BingX qualify for fallback through REST reconciliation, while KuCoin
remains ineligible for authoritative persistence. No production worker or VPS service should use
this gateway until the local integration is reviewed and a production rollout is explicitly
approved.

## Validation

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
docker compose config --quiet
docker build --tag market-stream-gateway:local .
```

The opt-in public catalog and WebSocket smokes are ignored by the deterministic suite:

```console
cargo test --locked catalog::tests::live_public_catalogs_parse -- --ignored --nocapture
cargo test --locked --test live_providers -- --ignored --nocapture
```
