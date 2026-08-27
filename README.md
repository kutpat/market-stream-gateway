# Market Stream Gateway

**Live today:** Market Stream Gateway supplies normalized, real-time derivatives data that
simplifies trade management for [Axion](https://axioncrypto.net/), a crypto trading community of
more than 100,000 people.

Market Stream Gateway is a demand-driven Rust service that presents public futures market data
from multiple exchanges through one provider-neutral contract. Provider identity and exact venue
symbols remain explicit, decimal values stay lossless strings, and missing market data is never
invented.

## Capabilities

- Linear-perpetual instrument discovery, tickers, and one-minute candles.
- Bounded historical candle retrieval for bootstrap and gap repair.
- Shared upstream subscriptions with provider-native acknowledgements and heartbeats.
- Reconnect backoff, proactive connection rotation, and sparse ticker accumulation.
- Bounded per-client routing with explicit gap notification on overrun.
- Readiness, catalog status, Prometheus metrics, structured logs, and graceful shutdown.
- No exchange account or API credentials required.

The gateway provides market observations only. It does not select venues, make trading decisions,
place orders, or provide durable event storage.

## Providers

| Provider | Catalog | Tickers | 1m candles | REST history |
| --- | --- | --- | --- | --- |
| Bybit | Yes | Yes | Yes | Yes |
| Binance USD-M | Yes | Yes | Yes | Yes |
| OKX | Yes | Yes | Yes | Yes |
| KuCoin Futures | Yes | Yes | Yes | Yes |
| MEXC Futures | Yes | Yes | Yes | Yes |
| BingX Swap | Yes | Yes | Yes | Yes |

Provider feeds differ in finality, volume units, connection limits, and contract representation.
The gateway preserves those differences instead of forcing every venue into a misleading common
shape. See [Provider behavior](docs/providers.md) for the important qualifications.

## Run locally

Rust 1.97.1 is pinned by `rust-toolchain.toml`.

```console
cargo run --locked
curl http://127.0.0.1:8080/health/ready
curl "http://127.0.0.1:8080/v1/instruments?provider=bybit&symbol=BTCUSDT"
```

Use Docker Compose to build and expose the gateway on host loopback:

```console
docker compose up --build
curl http://127.0.0.1:18070/health/ready
```

Release images are published to GHCR:

```console
docker pull ghcr.io/kutpat/market-stream-gateway:v0.3.2
```

Set `MSG_PROVIDERS=none` for an offline process or container smoke test. Set it to a
comma-separated subset such as `bybit,binance` to enable only selected providers. The complete
configuration reference is in [`config/example.env`](config/example.env).

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /health/live` | Process liveness |
| `GET /health/ready` | Catalog and demanded-stream readiness |
| `GET /metrics` | Prometheus text exposition |
| `GET /v1/providers` | Enabled providers and channels |
| `GET /v1/instruments` | Filterable live-instrument catalog |
| `GET /v1/catalog/status` | Per-provider catalog refresh state |
| `GET /v1/candles` | Bounded normalized one-minute history |
| `WS /v1/stream` | Dynamic normalized ticker and candle stream |

A WebSocket client subscribes to exact venue instruments:

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

An `ack` confirms local acceptance. Consumers should wait for route readiness before treating the
stream as live, and must repair candle history after a `gap` or reconnect. Decimal values are JSON
strings, candle end timestamps are exclusive, and delivery can duplicate around reconnects.

See the [WebSocket protocol](docs/protocol.md) for message shapes and delivery semantics.

## Network safety

The gateway implements no end-user authentication or TLS. It binds to `127.0.0.1:8080` by default,
Docker Compose publishes only to host loopback, and browser WebSocket origins are denied unless
explicitly allowlisted. Do not expose the service directly to the public internet; place it behind
an authenticated private-network or TLS edge when remote access is required.

## Development

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
docker compose config --quiet
docker build --tag market-stream-gateway:local .
```

Live provider tests are intentionally ignored by the deterministic suite because they call public
exchange endpoints:

```console
cargo test --locked catalog::tests::live_public_catalogs_parse -- --ignored --nocapture
cargo test --locked --test live_providers -- --ignored --nocapture
```

## License

Released under the [MIT License](LICENSE).
