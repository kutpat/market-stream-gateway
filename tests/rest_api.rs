use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use market_stream_gateway::api::{AppState, router};
use market_stream_gateway::catalog::{Catalog, CatalogSources};
use market_stream_gateway::domain::Provider;
use market_stream_gateway::gateway::{GatewayHub, SubscriptionRegistry};
use market_stream_gateway::health::HealthRegistry;
use market_stream_gateway::history::{HistoryClient, HistorySources};
use market_stream_gateway::metrics::Metrics;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const START: u64 = 1_700_000_040_000;

#[tokio::test]
async fn rest_api_exposes_catalog_and_normalized_history() {
    let (root, mock_task) = spawn_mock_provider().await;
    let http = reqwest::Client::new();
    let catalog = Arc::new(Catalog::new(
        http.clone(),
        CatalogSources {
            bybit: root.join("v5/market/instruments-info").unwrap(),
            binance: root.clone(),
            okx: root.clone(),
            kucoin: root.clone(),
            mexc: root.clone(),
            bingx: root.clone(),
            enabled_providers: BTreeSet::from([Provider::Bybit]),
        },
    ));
    catalog.refresh_provider(Provider::Bybit).await.unwrap();
    let state = AppState {
        hub: Arc::new(GatewayHub::new(8)),
        subscriptions: Arc::new(SubscriptionRegistry::new(8)),
        health: Arc::new(HealthRegistry::default()),
        metrics: Arc::new(Metrics::new()),
        catalog,
        history: Arc::new(HistoryClient::with_client(
            http,
            HistorySources {
                bybit: root.clone(),
                binance: root.clone(),
                okx: root.clone(),
                kucoin: root.clone(),
                mexc: root.clone(),
                bingx: root,
            },
        )),
        history_slots: Arc::new(Semaphore::new(2)),
        enabled_providers: Arc::new(BTreeSet::from([Provider::Bybit])),
        allowed_origins: Arc::new(BTreeSet::new()),
        max_command_bytes: 4096,
        catalog_on_demand_cooldown: Duration::from_mins(1),
        shutdown: CancellationToken::new(),
    };
    let app = router(state);

    let ready = request(&app, "/health/ready").await;
    assert_eq!(ready.0, StatusCode::OK);
    assert_eq!(ready.1["ready"], true);

    let instruments = request(&app, "/v1/instruments?provider=bybit&symbol=BTCUSDT").await;
    assert_eq!(instruments.0, StatusCode::OK);
    assert_eq!(instruments.1.as_array().unwrap().len(), 1);
    assert_eq!(
        instruments.1[0]["instrument_id"],
        "bybit:linear_perpetual:BTCUSDT"
    );
    assert_eq!(instruments.1[0]["tick_size"], "0.10");

    let candles = request(
        &app,
        &format!(
            "/v1/candles?provider=bybit&symbol=BTCUSDT&start_time_ms={START}&end_time_ms={}",
            START + 120_000
        ),
    )
    .await;
    assert_eq!(candles.0, StatusCode::OK);
    assert_eq!(candles.1["candles"].as_array().unwrap().len(), 2);
    assert_eq!(candles.1["candles"][0]["start_time_ms"], START);
    assert_eq!(candles.1["candles"][0]["end_time_ms"], START + 60_000);
    assert_eq!(candles.1["candles"][0]["base_volume"], "10");
    assert_eq!(candles.1["candles"][0]["quote_volume"], "20");
    assert_eq!(candles.1["candles"][0]["finality"], "closed");

    let unknown = request(
        &app,
        &format!(
            "/v1/candles?provider=bybit&symbol=UNKNOWNUSDT&start_time_ms={START}&end_time_ms={}",
            START + 60_000
        ),
    )
    .await;
    assert_eq!(unknown.0, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(unknown.1["code"], "unsupported_instrument");

    mock_task.abort();
}

#[tokio::test]
async fn instrument_lookup_reads_through_for_a_symbol_listed_after_the_last_refresh() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (root, mock_task) = spawn_growing_mock_provider(Arc::clone(&calls)).await;
    let http = reqwest::Client::new();
    let catalog = Arc::new(Catalog::new(
        http.clone(),
        CatalogSources {
            bybit: root.join("v5/market/instruments-info").unwrap(),
            enabled_providers: BTreeSet::from([Provider::Bybit]),
            ..CatalogSources::default()
        },
    ));
    catalog.refresh_provider(Provider::Bybit).await.unwrap();
    let app = router(app_state(
        Arc::clone(&catalog),
        http,
        root,
        Duration::from_millis(1),
    ));

    // The snapshot was taken before the listing existed.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(5)).await;

    let found = request(&app, "/v1/instruments?provider=bybit&symbol=NEWUSDT").await;

    assert_eq!(found.0, StatusCode::OK);
    assert_eq!(found.1.as_array().unwrap().len(), 1);
    assert_eq!(
        found.1[0]["instrument_id"],
        "bybit:linear_perpetual:NEWUSDT"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    mock_task.abort();
}

#[tokio::test]
async fn an_unfiltered_catalog_listing_never_reads_through() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (root, mock_task) = spawn_growing_mock_provider(Arc::clone(&calls)).await;
    let http = reqwest::Client::new();
    let catalog = Arc::new(Catalog::new(
        http.clone(),
        CatalogSources {
            bybit: root.join("v5/market/instruments-info").unwrap(),
            enabled_providers: BTreeSet::from([Provider::Bybit]),
            ..CatalogSources::default()
        },
    ));
    let app = router(app_state(
        Arc::clone(&catalog),
        http,
        root,
        Duration::from_millis(1),
    ));
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Empty because nothing has been refreshed yet, but no symbol was named, so a bulk
    // sweep must not be provoked by an ordinary catalog listing.
    let listing = request(&app, "/v1/instruments").await;

    assert_eq!(listing.0, StatusCode::OK);
    assert!(listing.1.as_array().unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    mock_task.abort();
}

fn app_state(
    catalog: Arc<Catalog>,
    http: reqwest::Client,
    root: url::Url,
    catalog_on_demand_cooldown: Duration,
) -> AppState {
    AppState {
        hub: Arc::new(GatewayHub::new(8)),
        subscriptions: Arc::new(SubscriptionRegistry::new(8)),
        health: Arc::new(HealthRegistry::default()),
        metrics: Arc::new(Metrics::new()),
        catalog,
        history: Arc::new(HistoryClient::with_client(
            http,
            HistorySources {
                bybit: root.clone(),
                binance: root.clone(),
                okx: root.clone(),
                kucoin: root.clone(),
                mexc: root.clone(),
                bingx: root,
            },
        )),
        history_slots: Arc::new(Semaphore::new(2)),
        enabled_providers: Arc::new(BTreeSet::from([Provider::Bybit])),
        allowed_origins: Arc::new(BTreeSet::new()),
        max_command_bytes: 4096,
        catalog_on_demand_cooldown,
        shutdown: CancellationToken::new(),
    }
}

fn bybit_instrument(symbol: &str, base: &str) -> Value {
    json!({
        "symbol": symbol,
        "contractType": "LinearPerpetual",
        "status": "Trading",
        "baseCoin": base,
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "launchTime": "1584230400000",
        "deliveryTime": "0",
        "priceFilter": {"tickSize": "0.10"},
        "lotSizeFilter": {"qtyStep": "0.001"},
        "leverageFilter": {"maxLeverage": "100.00"}
    })
}

async fn spawn_growing_mock_provider(
    calls: Arc<AtomicUsize>,
) -> (url::Url, tokio::task::JoinHandle<()>) {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let mock = Router::new()
        .route(
            "/v5/market/instruments-info",
            get(|State(calls): State<Arc<AtomicUsize>>| async move {
                // The first sweep predates the listing; later reads include it.
                let list = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    vec![bybit_instrument("BTCUSDT", "BTC")]
                } else {
                    vec![
                        bybit_instrument("BTCUSDT", "BTC"),
                        bybit_instrument("NEWUSDT", "NEW"),
                    ]
                };
                Json(json!({
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {
                        "category": "linear",
                        "nextPageCursor": "",
                        "list": list
                    }
                }))
            }),
        )
        .with_state(calls);
    let task = tokio::spawn(async move {
        axum::serve(upstream, mock).await.unwrap();
    });
    let root = url::Url::parse(&format!("http://{upstream_address}/")).unwrap();
    (root, task)
}

async fn spawn_mock_provider() -> (url::Url, tokio::task::JoinHandle<()>) {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream.local_addr().unwrap();
    let mock = Router::new()
        .route(
            "/v5/market/instruments-info",
            get(|| async {
                Json(json!({
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
        )
        .route(
            "/v5/market/kline",
            get(|| async {
                Json(json!({
                    "retCode": 0,
                    "retMsg": "OK",
                    "time": START + 180_000,
                    "result": {
                        "category": "linear",
                        "symbol": "BTCUSDT",
                        "list": [
                            [(START + 60_000).to_string(), "2", "4", "1.5", "3", "20", "60"],
                            [START.to_string(), "1", "3", "0.5", "2", "10", "20"]
                        ]
                    }
                }))
            }),
        );
    let task = tokio::spawn(async move {
        axum::serve(upstream, mock).await.unwrap();
    });
    let root = url::Url::parse(&format!("http://{upstream_address}/")).unwrap();
    (root, task)
}

async fn request(app: &Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
