# Provider behavior

Market Stream Gateway exposes one contract across six public derivatives venues while preserving
provider-specific facts that affect downstream correctness.

| Provider | Market | Streamed ticker | Streamed 1m candle | Historical 1m candle |
| --- | --- | --- | --- | --- |
| Bybit | Linear perpetual | Yes | Explicit finality | Yes |
| Binance USD-M | Linear perpetual | Yes | Explicit finality | Yes |
| OKX | Linear swap | Yes | Explicit finality | Yes |
| KuCoin Futures | Linear perpetual | Yes | Unknown finality | Yes |
| MEXC Futures | Linear perpetual | Yes | Unknown finality | Yes |
| BingX Swap | Linear perpetual | Yes | Unknown finality | Yes |

All provider URLs are configurable for regional deployments and deterministic tests. No exchange
credentials are required.

## Normalization rules

- Provider, market, exact venue symbol, settlement asset, and contract metadata remain explicit.
- Financial values are parsed as decimals and serialized as strings.
- Ticker fields keep independent observation times when a venue splits state across feeds.
- Missing last, mark, index, funding, best-price, volume, sequence, or finality fields stay missing.
- Candle base, quote, and contract volumes are separate dimensions.
- Provider sequence values are opaque and scoped to one upstream connection.
- Catalog refreshes are atomic; a failed refresh retains the last complete provider snapshot.
- Native subscription commands are bounded, paced, correlated to acknowledgements, and timed out.

## Important qualifications

### Bybit

Ticker deltas are accumulated over the latest snapshot. An omitted value means unchanged, not
zero. Linear candle base and quote volume retain their documented units.

### Binance USD-M

Last/BBO and mark/index/funding updates arrive on separate streams and keep separate timestamps.
The gateway preserves exact venue symbols, including valid non-ASCII symbols, and rotates
connections before the venue's documented lifetime limit.

### OKX

Ticker, mark-price, and funding data use the public WebSocket endpoint; one-minute candles use the
business endpoint. Swap candle base, quote, and contract volumes remain distinct. Contract size is
reported instead of being folded silently into quantity.

### KuCoin Futures

The public bullet endpoint supplies a temporary WebSocket URL and heartbeat parameters. Futures
candles do not provide a dependable explicit close flag, so streamed finality is `unknown`.
Provider-documented unreliable candle volume is omitted rather than exposed as authoritative data.

### MEXC Futures

Ticker, funding, and candle updates are accumulated without inventing absent fields. Streamed
candles have no trustworthy explicit close flag; bounded REST history uses public server time to
classify completed intervals.

### BingX Swap

Ticker and mark updates retain their independent timestamps. Streamed candles use `unknown`
finality where the venue does not provide a reliable close flag; REST history is normalized
separately for completed-interval repair.

## Provider selection

Enable all providers or an explicit comma-separated subset through `MSG_PROVIDERS`:

```dotenv
MSG_PROVIDERS=bybit,binance,okx,kucoin,mexc,bingx
```

Readiness requires an initial successful catalog for every enabled provider. Stream endpoints stay
idle without demand and become ready only after their current desired subscription set is live.
