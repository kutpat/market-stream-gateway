use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use market_stream_gateway::api::{AppState, router};
use market_stream_gateway::catalog::{Catalog, CatalogSources};
use market_stream_gateway::domain::{
    MarketKind, MarketPayload, ObservedDecimal, Provider, ProviderEvent, Ticker,
};
use market_stream_gateway::gateway::{GatewayHub, SubscriptionRegistry};
use market_stream_gateway::health::HealthRegistry;
use market_stream_gateway::history::{HistoryClient, HistorySources};
use market_stream_gateway::metrics::Metrics;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[tokio::test]
async fn websocket_subscriptions_receive_only_matching_normalized_events() {
    let hub = Arc::new(GatewayHub::new(16));
    let subscriptions = Arc::new(SubscriptionRegistry::new(8));
    let catalog = test_catalog().await;
    let http = reqwest::Client::new();
    let state = AppState {
        hub: Arc::clone(&hub),
        subscriptions: Arc::clone(&subscriptions),
        health: Arc::new(HealthRegistry::default()),
        metrics: Arc::new(Metrics::new()),
        catalog,
        history: Arc::new(HistoryClient::with_client(http, HistorySources::default())),
        history_slots: Arc::new(Semaphore::new(2)),
        enabled_providers: Arc::new(BTreeSet::from([Provider::Bybit])),
        allowed_origins: Arc::new(BTreeSet::new()),
        max_command_bytes: 4096,
        catalog_on_demand_cooldown: Duration::from_mins(1),
        shutdown: tokio_util::sync::CancellationToken::new(),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });

    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{address}/v1/stream"))
        .await
        .unwrap();
    let hello = next_json(&mut client).await;
    assert_eq!(hello["type"], "hello");

    client
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "request_id": "integration-1",
                "subscriptions": [{
                    "provider": "bybit",
                    "market": "linear_perpetual",
                    "symbol": "BTCUSDT",
                    "channels": ["ticker"]
                }]
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let acknowledgement = next_json(&mut client).await;
    assert_eq!(acknowledgement["type"], "ack");
    assert_eq!(subscriptions.desired_snapshot().len(), 1);

    hub.publish(ProviderEvent {
        connection_epoch: Uuid::new_v4(),
        provider: Provider::Bybit,
        market: MarketKind::LinearPerpetual,
        symbol: "ETHUSDT".to_owned(),
        exchange_time_ms: Some(1_750_000_000_000),
        gateway_received_time_ms: 1_750_000_000_001,
        source_sequence: None,
        payload: MarketPayload::Ticker(Ticker {
            last: Some(ObservedDecimal::new("2000", 1_750_000_000_000).unwrap()),
            ..Ticker::default()
        }),
    })
    .unwrap();
    hub.publish(ProviderEvent {
        connection_epoch: Uuid::new_v4(),
        provider: Provider::Bybit,
        market: MarketKind::LinearPerpetual,
        symbol: "BTCUSDT".to_owned(),
        exchange_time_ms: Some(1_750_000_000_002),
        gateway_received_time_ms: 1_750_000_000_003,
        source_sequence: None,
        payload: MarketPayload::Ticker(Ticker {
            last: Some(ObservedDecimal::new("100000.5", 1_750_000_000_002).unwrap()),
            ..Ticker::default()
        }),
    })
    .unwrap();

    let event = next_json(&mut client).await;
    assert_eq!(event["type"], "ticker");
    assert_eq!(event["symbol"], "BTCUSDT");
    assert_eq!(event["data"]["last"]["value"], "100000.5");

    client.close(None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if subscriptions.desired_snapshot().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

async fn test_catalog() -> Arc<Catalog> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/catalog",
        axum::routing::get(|| async {
            axum::Json(json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "category": "linear",
                    "nextPageCursor": "",
                    "list": [{
                        "symbol": "BTCUSDT",
                        "contractType": "LinearPerpetual",
                        "status": "Trading",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "settleCoin": "USDT",
                        "launchTime": "1584230400000",
                        "deliveryTime": "0",
                        "priceFilter": {"tickSize": "0.10"},
                        "lotSizeFilter": {"qtyStep": "0.001"},
                        "leverageFilter": {"maxLeverage": "100.00"}
                    }]
                }
            }))
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let endpoint = url::Url::parse(&format!("http://{address}/catalog")).unwrap();
    let catalog = Arc::new(Catalog::new(
        reqwest::Client::new(),
        CatalogSources {
            bybit: endpoint.clone(),
            binance: endpoint.clone(),
            okx: endpoint.clone(),
            kucoin: endpoint.clone(),
            mexc: endpoint.clone(),
            bingx: endpoint,
            enabled_providers: BTreeSet::from([Provider::Bybit]),
        },
    ));
    catalog.refresh_provider(Provider::Bybit).await.unwrap();
    server.abort();
    catalog
}

async fn next_json(
    client: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("websocket response timed out")
        .expect("websocket closed")
        .expect("websocket read failed");
    let Message::Text(text) = message else {
        panic!("expected a text websocket message");
    };
    serde_json::from_str(text.as_str()).unwrap()
}
