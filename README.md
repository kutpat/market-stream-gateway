# Market Stream Gateway

A demand-driven Rust service that exposes public derivatives market data from Bybit, Binance,
OKX, KuCoin, MEXC, and BingX through one versioned contract. The wire format is provider-neutral
rather than a copy of Bybit's protocol: provider and exact venue symbol remain explicit, decimals
remain lossless strings, and no adapter invents missing prices, volume, finality, or sequence
numbers.

Nothing consumes this service in production yet. Axion Trading Core has a local feature branch that
reads this contract; until that lands, the gateway can be deployed and observed without affecting
anything.

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

Subscription ceilings are per provider, because the venues differ: some document a per-connection
topic limit and some do not, so one global number is either too low for the permissive venues or
unsafe for the strict ones. Each provider contributes its own ceiling, and the `hello` frame
advertises them in `provider_subscription_limits` before any subscription command is sent. The
scalar `max_provider_subscriptions` is retained as the minimum across enabled providers, so a client
that reads only that value still cannot oversubscribe the strictest venue.

`MSG_MAX_PROVIDER_SUBSCRIPTIONS` is an optional ceiling applied on top. It only ever tightens a
limit: it cannot raise one past what a provider declared, since that declaration may be a venue
rule.

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

## Deployment

Pushing to `main` validates the change and publishes an image. Publishing a GitHub Release deploys
it. A push never deploys.

| Event | Workflow | Effect |
| --- | --- | --- |
| Pull request | `ci.yml` | Format, clippy, tests, docs, image build plus the hardened container smoke. Nothing is pushed or deployed. |
| Push to `main` | `ci.yml` | Same checks, then pushes `ghcr.io/kutpat/market-stream-gateway:sha-<full-sha>` and `:main`. **No deploy.** |
| Release published | `release.yml` | Retags the already-tested `sha-<sha>` image as `:<tag>` and deploys it. |
| Manual `workflow_dispatch` | `release.yml` | Same deploy for any other published release tag: redeploys or rolls back. |

A release never rebuilds. It promotes the exact image CI smoke tested, so the bytes in production are
the bytes that passed the gates. Naming a commit CI never built on `main` fails with that reason
rather than building something unverified.

```console
gh release create v0.1.0 --target main --generate-notes
gh workflow run release.yml -f tag=v0.1.0   # redeploy or roll back
```

The deploy directory holds no source checkout and, uniquely among the Axion services, **no secrets**:

```
/root/axion/gateway/
  compose.prod.yaml   uploaded by the release workflow
  .env                MARKET_GATEWAY_IMAGE pin, written by the release workflow
```

Every provider feed is public, so there is no credential to place on the host and no env file to
maintain. Configuration lives in `compose.prod.yaml` where it can be reviewed.

The container joins the external `axion-trading` Docker network and is reached by consumers as
`http://axion-market-gateway:8080`. Port `18070` is published on host loopback only, for diagnostics:
the gateway implements no authentication and no TLS, so it must never be exposed beyond the host.

`MSG_PROVIDERS` is set to `bybit` alone for now. `/health/ready` requires every *enabled* provider's
catalogue to have refreshed successfully, so enabling a venue before anything consumes it only adds
ways for the container to report unhealthy.

### One-time host preparation

Required once per host, and already satisfied on a host running Trading Core:

- Docker with the Compose v2 plugin.
- The `axion-trading` network. Core's deployment creates it; elsewhere run
  `docker network create axion-trading`.
- Repository secrets `HOST`, `USERNAME`, and `SSH_PRIVATE_KEY`, plus optional
  `DISCORD_DEPLOY_WEBHOOK_URL`. The host stores no registry credential: the deploy logs in to GHCR
  with the workflow's own job token and logs out on the way out.

Inspect production with:

```console
docker compose -p axion-gateway -f /root/axion/gateway/compose.prod.yaml ps
docker logs --tail 100 axion-market-gateway
curl -s http://127.0.0.1:18070/health/ready
```

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
