use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use market_stream_gateway::config::Settings;
use market_stream_gateway::domain::{
    Channel, MarketKind, MarketPayload, Provider, SubscriptionKey,
};
use market_stream_gateway::gateway::{GatewayHub, SubscriptionRegistry};
use market_stream_gateway::health::HealthRegistry;
use market_stream_gateway::metrics::Metrics;
use market_stream_gateway::providers::ProviderAdapter;
use market_stream_gateway::providers::bybit::BybitAdapter;
use market_stream_gateway::runtime::{RuntimeContext, spawn_provider_supervisors};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn supervisor_subscribes_and_publishes_normalized_provider_data() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let fake_provider = tokio::spawn(async move {
        let (tcp, _) = upstream.accept().await.unwrap();
        let mut websocket = accept_async(tcp).await.unwrap();
        let subscribe = websocket.next().await.unwrap().unwrap();
        let Message::Text(subscribe) = subscribe else {
            panic!("expected subscription text");
        };
        let command: Value = serde_json::from_str(subscribe.as_str()).unwrap();
        assert_eq!(command["op"], "subscribe");
        assert_eq!(command["args"], json!(["tickers.BTCUSDT"]));

        websocket
            .send(Message::Text(
                json!({"success": true, "op": "subscribe", "req_id": "gateway-1"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        websocket
            .send(Message::Text(
                json!({
                    "topic": "tickers.BTCUSDT",
                    "type": "snapshot",
                    "ts": 1_750_000_000_100_u64,
                    "cs": 10,
                    "data": {
                        "symbol": "BTCUSDT",
                        "lastPrice": "100001.25",
                        "markPrice": "100000.75",
                        "indexPrice": "100000.50",
                        "bid1Price": "100001.00",
                        "ask1Price": "100001.50"
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();
        while let Some(message) = websocket.next().await {
            if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    });

    let mut settings = Settings::try_parse_from(["gateway"]).unwrap();
    settings.upstream_backoff_min_ms = 10;
    settings.upstream_backoff_max_ms = 100;
    let settings = Arc::new(settings);
    let registry = Arc::new(SubscriptionRegistry::new(8));
    let hub = Arc::new(GatewayHub::new(8));
    let mut events = hub.subscribe();
    let shutdown = CancellationToken::new();
    let context = RuntimeContext {
        registry: Arc::clone(&registry),
        hub,
        health: Arc::new(HealthRegistry::default()),
        metrics: Arc::new(Metrics::new()),
        http: reqwest::Client::new(),
        shutdown: shutdown.clone(),
        settings,
    };
    let subscription = SubscriptionKey::new(
        Provider::Bybit,
        MarketKind::LinearPerpetual,
        "BTCUSDT",
        Channel::Ticker,
    )
    .unwrap();
    registry.add(Uuid::new_v4(), [subscription]).await.unwrap();
    let adapters: Vec<Arc<dyn ProviderAdapter>> = vec![Arc::new(BybitAdapter::with_url(
        Url::parse(&format!("ws://{upstream_address}")).unwrap(),
    ))];
    let tasks = spawn_provider_supervisors(adapters, context);

    let event = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("normalized event timed out")
        .unwrap();
    assert_eq!(event.provider, Provider::Bybit);
    assert_eq!(event.symbol, "BTCUSDT");
    let MarketPayload::Ticker(ticker) = &event.payload else {
        panic!("expected ticker event");
    };
    assert_eq!(ticker.last.as_ref().unwrap().value.as_str(), "100001.25");
    assert_eq!(ticker.mark.as_ref().unwrap().value.as_str(), "100000.75");

    shutdown.cancel();
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
    fake_provider.await.unwrap();
}
