use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures_util::future::join_all;
use market_stream_gateway::config::Settings;
use market_stream_gateway::domain::{Channel, MarketKind, Provider, SubscriptionKey};
use market_stream_gateway::gateway::{GatewayHub, SubscriptionRegistry};
use market_stream_gateway::health::HealthRegistry;
use market_stream_gateway::metrics::Metrics;
use market_stream_gateway::providers::ProviderAdapter;
use market_stream_gateway::providers::binance::BinanceAdapter;
use market_stream_gateway::providers::bingx::BingxAdapter;
use market_stream_gateway::providers::bybit::BybitAdapter;
use market_stream_gateway::providers::kucoin::KucoinAdapter;
use market_stream_gateway::providers::mexc::MexcAdapter;
use market_stream_gateway::providers::okx::OkxAdapter;
use market_stream_gateway::runtime::{RuntimeContext, spawn_provider_supervisors};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit live smoke test against public exchange websocket feeds"]
async fn all_provider_tickers_and_candles_stream_live() {
    let settings = Arc::new(Settings::try_parse_from(["gateway"]).unwrap());
    let registry = Arc::new(SubscriptionRegistry::new(32));
    let hub = Arc::new(GatewayHub::new(128));
    let health = Arc::new(HealthRegistry::default());
    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let context = RuntimeContext {
        registry: Arc::clone(&registry),
        hub: Arc::clone(&hub),
        health,
        metrics,
        http,
        shutdown: shutdown.clone(),
        settings,
    };
    let adapters: Vec<Arc<dyn ProviderAdapter>> = vec![
        Arc::new(BybitAdapter::default()),
        Arc::new(BinanceAdapter::default()),
        Arc::new(OkxAdapter::default()),
        Arc::new(KucoinAdapter::default()),
        Arc::new(MexcAdapter::default()),
        Arc::new(BingxAdapter::default()),
    ];
    let tasks = spawn_provider_supervisors(adapters, context);
    let expected = subscriptions();
    let mut events = hub.subscribe();
    registry
        .add(Uuid::new_v4(), expected.iter().cloned())
        .await
        .unwrap();

    let observed = tokio::time::timeout(Duration::from_mins(2), async {
        let mut observed = BTreeSet::new();
        while observed.len() < expected.len() {
            let event = events.recv().await.unwrap();
            event.payload.validate().unwrap();
            if observed.insert(event.subscription_key()) {
                eprintln!(
                    "live {} {} {} sequence={}",
                    event.provider,
                    event.symbol,
                    event.payload.channel(),
                    event.delivery_sequence
                );
            }
        }
        observed
    })
    .await
    .expect("all public providers should produce ticker and candle data within 120 seconds");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), join_all(tasks))
        .await
        .expect("provider supervisors should stop gracefully");
    assert_eq!(observed, expected);
}

fn subscriptions() -> BTreeSet<SubscriptionKey> {
    [
        (Provider::Bybit, "BTCUSDT"),
        (Provider::Binance, "BTCUSDT"),
        (Provider::Okx, "BTC-USDT-SWAP"),
        (Provider::Kucoin, "XBTUSDTM"),
        (Provider::Mexc, "FET_USDT"),
        (Provider::Bingx, "FET-USDT"),
    ]
    .into_iter()
    .flat_map(|(provider, symbol)| {
        [Channel::Ticker, Channel::Candle1m]
            .into_iter()
            .map(move |channel| {
                SubscriptionKey::new(provider, MarketKind::LinearPerpetual, symbol, channel)
                    .unwrap()
            })
    })
    .collect()
}
