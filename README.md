# Market Stream Gateway

Market Stream Gateway is a Rust service that multiplexes public derivatives market data from
Bybit, Binance, OKX, and KuCoin into one versioned, provider-neutral WebSocket contract.

The service is under active local development. It is not yet connected to Axion Trading Core or
deployed to production.

## Development

```console
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

