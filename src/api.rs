use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::catalog::{Catalog, CatalogFilter, Instrument, ProviderCatalogStatus};
use crate::domain::{Channel, DecimalValue, MarketKind, Provider, Subscription, SubscriptionKey};
use crate::gateway::{ClientId, GatewayHub, RegistryError, SubscriptionRegistry};
use crate::health::{EndpointHealth, HealthRegistry};
use crate::history::{HistoryClient, HistoryError, HistoryRequest, HistoryResult};
use crate::metrics::{Metrics, ProviderLabels};
use crate::protocol::{ClientCommand, ControlMessage, ErrorCode};

const CLIENT_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REQUEST_ID_BYTES: usize = 128;

#[derive(Clone)]
pub struct AppState {
    pub hub: Arc<GatewayHub>,
    pub subscriptions: Arc<SubscriptionRegistry>,
    pub health: Arc<HealthRegistry>,
    pub metrics: Arc<Metrics>,
    pub catalog: Arc<Catalog>,
    pub history: Arc<HistoryClient>,
    pub history_slots: Arc<Semaphore>,
    pub enabled_providers: Arc<BTreeSet<Provider>>,
    pub allowed_origins: Arc<BTreeSet<String>>,
    pub max_command_bytes: usize,
    pub catalog_on_demand_cooldown: Duration,
    pub shutdown: CancellationToken,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/metrics", get(prometheus_metrics))
        .route("/v1/providers", get(providers))
        .route("/v1/instruments", get(instruments))
        .route("/v1/catalog/status", get(catalog_status))
        .route("/v1/candles", get(candles))
        .route("/v1/stream", get(stream))
        .with_state(state)
}

async fn liveness() -> Json<ServiceStatus> {
    Json(ServiceStatus { status: "live" })
}

async fn readiness(State(state): State<AppState>) -> Response {
    let stream = state.health.readiness();
    let catalogs = state.catalog.statuses();
    let catalogs_ready = catalogs
        .iter()
        .filter(|catalog| catalog.enabled)
        .all(|catalog| catalog.last_success_at_ms.is_some());
    let ready = stream.ready && catalogs_ready;
    let snapshot = ServiceReadiness {
        ready,
        status: if ready { "ready" } else { "degraded" },
        endpoints: stream.endpoints,
        catalogs,
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(snapshot)).into_response()
}

async fn prometheus_metrics(State(state): State<AppState>) -> Response {
    match state.metrics.encode() {
        Ok(metrics) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            metrics,
        )
            .into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "metrics encoding failed").into_response(),
    }
}

async fn providers(State(state): State<AppState>) -> Json<Vec<ProviderDescriptor>> {
    let providers = [
        Provider::Bybit,
        Provider::Binance,
        Provider::Okx,
        Provider::Kucoin,
        Provider::Mexc,
        Provider::Bingx,
    ]
    .into_iter()
    .map(|provider| ProviderDescriptor {
        provider,
        enabled: state.enabled_providers.contains(&provider),
        markets: vec![MarketKind::LinearPerpetual],
        channels: vec![Channel::Ticker, Channel::Candle1m],
    })
    .collect();
    Json(providers)
}

async fn instruments(
    State(state): State<AppState>,
    Query(query): Query<InstrumentQuery>,
) -> Json<Vec<crate::catalog::Instrument>> {
    let filter = CatalogFilter {
        provider: query.provider,
        market: query.market,
        symbol: query.symbol,
        base_asset: query.base_asset,
        quote_asset: query.quote_asset,
        settle_asset: query.settle_asset,
    };
    let instruments = state.catalog.filter(&filter);
    if !instruments.is_empty() || filter.symbol.is_none() {
        return Json(instruments);
    }
    // A named symbol that no snapshot knows is most often a listing newer than the last
    // scheduled refresh. Read through to the upstream once, under a cooldown, rather than
    // reporting it absent until the next sweep hours later.
    if state
        .catalog
        .fill_missing_symbol(filter.provider, state.catalog_on_demand_cooldown)
        .await
    {
        return Json(state.catalog.filter(&filter));
    }
    Json(instruments)
}

async fn catalog_status(State(state): State<AppState>) -> Json<Vec<ProviderCatalogStatus>> {
    Json(state.catalog.statuses())
}

async fn candles(State(state): State<AppState>, Query(query): Query<CandleQuery>) -> Response {
    let key = match SubscriptionKey::new(
        query.provider,
        MarketKind::LinearPerpetual,
        &query.symbol,
        Channel::Candle1m,
    ) {
        Ok(key) => key,
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &error.to_string(),
            );
        }
    };
    let instrument = match state.catalog.validate_subscription(&key) {
        Ok(instrument) => instrument,
        Err(error) => {
            return api_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported_instrument",
                &error.to_string(),
            );
        }
    };
    let request = match HistoryRequest::new(
        query.provider,
        &key.symbol,
        query.start_time_ms,
        query.end_time_ms,
        query.limit.unwrap_or(1_000),
    ) {
        Ok(request) => request,
        Err(error) => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &error.to_string(),
            );
        }
    };
    let Ok(_permit) = Arc::clone(&state.history_slots).try_acquire_owned() else {
        return api_error(
            StatusCode::TOO_MANY_REQUESTS,
            "history_busy",
            "too many concurrent history requests",
        );
    };
    let labels = ProviderLabels::new(query.provider);
    state.metrics.history_requests.get_or_create(&labels).inc();
    match state.history.fetch(&request).await {
        Ok(mut result) => {
            if let Err(error) = enrich_history_volumes(&mut result, &instrument) {
                state.metrics.history_failures.get_or_create(&labels).inc();
                warn!(provider = %query.provider, %error, "history_volume_enrichment_failed");
                return api_error(
                    StatusCode::BAD_GATEWAY,
                    "provider_unavailable",
                    &format!("{} history request failed", query.provider),
                );
            }
            Json(result).into_response()
        }
        Err(HistoryError::InvalidRequest(message)) => {
            state.metrics.history_failures.get_or_create(&labels).inc();
            api_error(StatusCode::BAD_REQUEST, "invalid_request", &message)
        }
        Err(error) => {
            state.metrics.history_failures.get_or_create(&labels).inc();
            warn!(provider = %query.provider, %error, "history_request_failed");
            api_error(
                StatusCode::BAD_GATEWAY,
                "provider_unavailable",
                &format!("{} history request failed", query.provider),
            )
        }
    }
}

fn enrich_history_volumes(
    result: &mut HistoryResult,
    instrument: &Instrument,
) -> Result<(), String> {
    if result.provider != Provider::Mexc {
        return Ok(());
    }
    let contract_size = instrument
        .contract_size
        .as_ref()
        .ok_or_else(|| "MEXC instrument is missing contract size".to_owned())?;
    if instrument.contract_size_asset.as_deref() != Some(instrument.base_asset.as_str()) {
        return Err("MEXC contract size is not denominated in the base asset".to_owned());
    }
    let contract_size = Decimal::from_str(contract_size.as_str())
        .map_err(|_| "MEXC contract size is not a decimal".to_owned())?;
    for row in &mut result.candles {
        if row.candle.base_volume.is_some() {
            continue;
        }
        let contracts = row
            .candle
            .contract_volume
            .as_ref()
            .ok_or_else(|| "MEXC history candle is missing contract volume".to_owned())?;
        let contracts = Decimal::from_str(contracts.as_str())
            .map_err(|_| "MEXC contract volume is not a decimal".to_owned())?;
        let base_volume = contracts
            .checked_mul(contract_size)
            .ok_or_else(|| "MEXC base volume overflowed".to_owned())?;
        row.candle.base_volume = Some(
            DecimalValue::new(base_volume.normalize().to_string())
                .map_err(|error| error.to_string())?,
        );
        row.candle.validate().map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn stream(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if let Some(origin) = headers.get(header::ORIGIN) {
        let allowed = origin.to_str().ok().is_some_and(|origin| {
            state.allowed_origins.contains("*") || state.allowed_origins.contains(origin)
        });
        if !allowed {
            return (StatusCode::FORBIDDEN, "websocket origin is not allowed").into_response();
        }
    }
    ws.max_message_size(state.max_command_bytes)
        .max_frame_size(state.max_command_bytes)
        .on_upgrade(move |socket| client_session(socket, state))
        .into_response()
}

async fn client_session(mut socket: WebSocket, state: AppState) {
    let client_id = Uuid::new_v4();
    let Some(mut feed) = state.hub.try_register_client(client_id) else {
        reject_client_at_capacity(&mut socket).await;
        return;
    };
    state.metrics.increment_connections();

    let mut subscriptions = BTreeSet::new();
    let hello = ControlMessage::hello(
        state.hub.stream_epoch().to_string(),
        state.subscriptions.max_subscriptions_per_client(),
        state.subscriptions.provider_limits(),
    );
    if !send_json(&mut socket, &hello).await {
        cleanup_session(&state, client_id).await;
        return;
    }

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(Ok(message)) = incoming else {
                    break;
                };
                if !handle_client_message(
                    &mut socket,
                    &state,
                    client_id,
                    message,
                    &mut subscriptions,
                )
                .await
                {
                    break;
                }
            }
            event = feed.events.recv() => {
                match event {
                    Some(event) => {
                        if !send_json(&mut socket, event.as_ref()).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
            lagged = feed.lagged.changed() => {
                if lagged.is_err() {
                    break;
                }
                let dropped = *feed.lagged.borrow_and_update();
                state.metrics.downstream_lagged.inc();
                let gap = ControlMessage::Gap {
                    schema_version: crate::domain::SCHEMA_VERSION,
                    dropped_messages: dropped,
                    message: "downstream buffer overrun; reconnect and repair gaps".to_owned(),
                };
                let _sent = send_json(&mut socket, &gap).await;
                break;
            }
            () = state.shutdown.cancelled() => {
                let _closed = send_message(&mut socket, Message::Close(None)).await;
                break;
            }
        }
    }

    cleanup_session(&state, client_id).await;
}

async fn reject_client_at_capacity(socket: &mut WebSocket) {
    let error = ControlMessage::error(
        None,
        ErrorCode::LimitExceeded,
        "downstream client limit exceeded",
    );
    let _sent = send_json(socket, &error).await;
    let _closed = send_message(socket, Message::Close(None)).await;
}

async fn handle_client_message(
    socket: &mut WebSocket,
    state: &AppState,
    client_id: ClientId,
    message: Message,
    subscriptions: &mut BTreeSet<SubscriptionKey>,
) -> bool {
    match message {
        Message::Text(text) => {
            handle_text_message(socket, state, client_id, text.as_str(), subscriptions).await
        }
        Message::Ping(payload) => send_message(socket, Message::Pong(payload)).await,
        Message::Pong(_) => true,
        Message::Binary(_) => {
            let error = ControlMessage::error(
                None,
                ErrorCode::InvalidCommand,
                "commands must be UTF-8 JSON text",
            );
            reject_client_command(socket, state, &error).await
        }
        Message::Close(_) => false,
    }
}

async fn handle_text_message(
    socket: &mut WebSocket,
    state: &AppState,
    client_id: ClientId,
    text: &str,
    subscriptions: &mut BTreeSet<SubscriptionKey>,
) -> bool {
    if text.len() > state.max_command_bytes {
        let error = ControlMessage::error(
            None,
            ErrorCode::LimitExceeded,
            "command exceeds maximum size",
        );
        return reject_client_command(socket, state, &error).await;
    }

    let command = match parse_command(text) {
        Ok(command) => command,
        Err(error) => return reject_client_command(socket, state, &error).await,
    };
    let CommandResult::Reply(reply) = apply_command(state, client_id, command, subscriptions).await;
    send_json(socket, &reply).await
}

async fn reject_client_command(
    socket: &mut WebSocket,
    state: &AppState,
    error: &ControlMessage,
) -> bool {
    state.metrics.downstream_commands_rejected.inc();
    send_json(socket, error).await
}

async fn cleanup_session(state: &AppState, client_id: ClientId) {
    state.hub.remove_client(client_id);
    state.subscriptions.cleanup_client(client_id).await;
    state.metrics.decrement_connections();
}

fn parse_command(text: &str) -> Result<ClientCommand, ControlMessage> {
    serde_json::from_str(text).map_err(|error| {
        let code = if error.is_syntax() || error.is_eof() {
            ErrorCode::InvalidJson
        } else {
            ErrorCode::InvalidCommand
        };
        ControlMessage::error(None, code, "invalid client command")
    })
}

async fn apply_command(
    state: &AppState,
    client_id: ClientId,
    command: ClientCommand,
    subscriptions: &mut BTreeSet<SubscriptionKey>,
) -> CommandResult {
    let request_id = command.request_id().to_owned();
    if request_id.is_empty() || request_id.len() > MAX_REQUEST_ID_BYTES {
        return rejected_reply(
            state,
            Some(request_id),
            ErrorCode::InvalidCommand,
            "request_id must contain between 1 and 128 bytes",
        );
    }

    match command {
        ClientCommand::Ping { request_id } => CommandResult::Reply(ControlMessage::Pong {
            schema_version: crate::domain::SCHEMA_VERSION,
            request_id,
        }),
        ClientCommand::Subscribe {
            request_id,
            subscriptions: requested,
        } => subscribe(state, client_id, request_id, requested, subscriptions).await,
        ClientCommand::Unsubscribe {
            request_id,
            subscriptions: requested,
        } => unsubscribe(state, client_id, request_id, requested, subscriptions).await,
    }
}

async fn subscribe(
    state: &AppState,
    client_id: ClientId,
    request_id: String,
    requested: Vec<Subscription>,
    subscriptions: &mut BTreeSet<SubscriptionKey>,
) -> CommandResult {
    let keys = match validated_subscription_keys(state, &request_id, requested) {
        Ok(keys) => keys,
        Err(reply) => return reply,
    };
    let change = match state.subscriptions.add(client_id, keys.clone()).await {
        Ok(change) => change,
        Err(error) => return registry_error_reply(state, request_id, &error),
    };

    subscriptions.clone_from(&change.client_subscriptions);
    state
        .hub
        .update_client_subscriptions(client_id, subscriptions);
    CommandResult::Reply(ControlMessage::Ack {
        schema_version: crate::domain::SCHEMA_VERSION,
        request_id,
        operation: "subscribe".to_owned(),
        subscriptions: keys,
    })
}

async fn unsubscribe(
    state: &AppState,
    client_id: ClientId,
    request_id: String,
    requested: Vec<Subscription>,
    subscriptions: &mut BTreeSet<SubscriptionKey>,
) -> CommandResult {
    let keys = match expand_subscriptions(requested) {
        Ok(keys) => keys,
        Err(message) => {
            return rejected_reply(state, Some(request_id), ErrorCode::InvalidCommand, message);
        }
    };
    let change = state.subscriptions.remove(client_id, keys.clone()).await;
    subscriptions.clone_from(&change.client_subscriptions);
    state
        .hub
        .update_client_subscriptions(client_id, subscriptions);
    CommandResult::Reply(ControlMessage::Ack {
        schema_version: crate::domain::SCHEMA_VERSION,
        request_id,
        operation: "unsubscribe".to_owned(),
        subscriptions: keys,
    })
}

fn validated_subscription_keys(
    state: &AppState,
    request_id: &str,
    requested: Vec<Subscription>,
) -> Result<Vec<SubscriptionKey>, CommandResult> {
    let keys = expand_subscriptions(requested).map_err(|message| {
        rejected_reply(
            state,
            Some(request_id.to_owned()),
            ErrorCode::InvalidCommand,
            message,
        )
    })?;

    if let Some(key) = keys
        .iter()
        .find(|key| !state.enabled_providers.contains(&key.provider))
    {
        return Err(rejected_reply(
            state,
            Some(request_id.to_owned()),
            ErrorCode::UnsupportedSubscription,
            format!("{} is disabled", key.provider),
        ));
    }
    if let Some(error) = keys
        .iter()
        .find_map(|key| state.catalog.validate_subscription(key).err())
    {
        return Err(rejected_reply(
            state,
            Some(request_id.to_owned()),
            ErrorCode::UnsupportedSubscription,
            error.to_string(),
        ));
    }
    Ok(keys)
}

fn registry_error_reply(
    state: &AppState,
    request_id: String,
    error: &RegistryError,
) -> CommandResult {
    let message = match error {
        RegistryError::ClientLimitExceeded { limit, requested } => {
            format!("subscription limit exceeded: requested {requested}, maximum {limit}")
        }
        RegistryError::ProviderLimitExceeded {
            provider,
            limit,
            requested,
        } => format!(
            "{provider} subscription limit exceeded: requested {requested}, maximum {limit}"
        ),
    };
    rejected_reply(state, Some(request_id), ErrorCode::LimitExceeded, message)
}

fn rejected_reply(
    state: &AppState,
    request_id: Option<String>,
    code: ErrorCode,
    message: impl Into<String>,
) -> CommandResult {
    state.metrics.downstream_commands_rejected.inc();
    CommandResult::Reply(ControlMessage::error(request_id, code, message))
}

fn expand_subscriptions(requested: Vec<Subscription>) -> Result<Vec<SubscriptionKey>, String> {
    if requested.is_empty() {
        return Err("subscriptions must not be empty".to_owned());
    }
    let mut keys = BTreeSet::new();
    for subscription in requested {
        let expanded = subscription
            .into_keys()
            .map_err(|error| error.to_string())?;
        keys.extend(expanded);
    }
    Ok(keys.into_iter().collect())
}

async fn send_json<T>(socket: &mut WebSocket, value: &T) -> bool
where
    T: Serialize + ?Sized,
{
    let Ok(text) = serde_json::to_string(value) else {
        return false;
    };
    send_message(socket, Message::Text(text.into())).await
}

async fn send_message(socket: &mut WebSocket, message: Message) -> bool {
    matches!(
        tokio::time::timeout(CLIENT_SEND_TIMEOUT, socket.send(message)).await,
        Ok(Ok(()))
    )
}

enum CommandResult {
    Reply(ControlMessage),
}

#[derive(Debug, Serialize)]
struct ServiceStatus {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ProviderDescriptor {
    provider: Provider,
    enabled: bool,
    markets: Vec<MarketKind>,
    channels: Vec<Channel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentQuery {
    provider: Option<Provider>,
    market: Option<MarketKind>,
    symbol: Option<String>,
    base_asset: Option<String>,
    quote_asset: Option<String>,
    settle_asset: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CandleQuery {
    provider: Provider,
    symbol: String,
    start_time_ms: u64,
    end_time_ms: u64,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ServiceReadiness {
    ready: bool,
    status: &'static str,
    endpoints: Vec<EndpointHealth>,
    catalogs: Vec<ProviderCatalogStatus>,
}

#[derive(Debug, Serialize)]
struct ApiError {
    code: &'static str,
    message: String,
}

fn api_error(status: StatusCode, code: &'static str, message: &str) -> Response {
    (
        status,
        Json(ApiError {
            code,
            message: message.chars().take(500).collect(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::domain::SCHEMA_VERSION;

    fn state() -> AppState {
        let catalog_sources = crate::catalog::CatalogSources {
            enabled_providers: BTreeSet::new(),
            ..crate::catalog::CatalogSources::default()
        };
        let http = reqwest::Client::new();
        AppState {
            hub: Arc::new(GatewayHub::new(8)),
            subscriptions: Arc::new(SubscriptionRegistry::new(4)),
            health: Arc::new(HealthRegistry::default()),
            metrics: Arc::new(Metrics::new()),
            catalog: Arc::new(Catalog::new(http.clone(), catalog_sources)),
            history: Arc::new(HistoryClient::with_client(
                http,
                crate::history::HistorySources::default(),
            )),
            history_slots: Arc::new(Semaphore::new(2)),
            enabled_providers: Arc::new(BTreeSet::from([
                Provider::Bybit,
                Provider::Binance,
                Provider::Okx,
                Provider::Kucoin,
                Provider::Mexc,
                Provider::Bingx,
            ])),
            allowed_origins: Arc::new(BTreeSet::new()),
            max_command_bytes: 1024,
            catalog_on_demand_cooldown: Duration::from_mins(1),
            shutdown: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn health_and_metrics_routes_are_available() {
        let app = router(state());
        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let metrics = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
    }

    #[test]
    fn client_subscriptions_expand_to_channel_keys() {
        let keys = expand_subscriptions(vec![Subscription {
            provider: Provider::Bybit,
            market: MarketKind::LinearPerpetual,
            symbol: "btcusdt".to_owned(),
            channels: vec![Channel::Ticker, Channel::Candle1m],
        }])
        .unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key.symbol == "BTCUSDT"));
    }

    #[test]
    fn malformed_json_has_stable_public_error() {
        let error = parse_command("{").unwrap_err();
        let json = serde_json::to_value(error).unwrap();
        assert_eq!(json["type"], "error");
        assert_eq!(json["schema_version"], SCHEMA_VERSION);
        assert_eq!(json["code"], "invalid_json");
        assert_eq!(json["message"], "invalid client command");
    }

    #[test]
    fn mexc_history_contract_volume_is_converted_to_exact_base_volume() {
        let capabilities = crate::catalog::InstrumentCapabilities {
            ticker: true,
            candle_1m: true,
            candle_1m_finality: crate::catalog::CandleFinalitySupport::Unknown,
            candle_1m_volume: crate::catalog::CandleVolumeSupport::Partial,
            history_1m: true,
            history_1m_finality: crate::catalog::CandleFinalitySupport::Authoritative,
            history_1m_volume: crate::catalog::CandleVolumeSupport::Available,
        };
        let instrument = Instrument {
            instrument_id: "mexc:linear_perpetual:FET_USDT".to_owned(),
            symbol: "FET_USDT".to_owned(),
            provider: Provider::Mexc,
            market: MarketKind::LinearPerpetual,
            base_asset: "FET".to_owned(),
            quote_asset: "USDT".to_owned(),
            settle_asset: "USDT".to_owned(),
            status: crate::catalog::InstrumentStatus::Live,
            venue_status: "state=0,type=1".to_owned(),
            tick_size: DecimalValue::new("0.0001").unwrap(),
            quantity_step: DecimalValue::new("1").unwrap(),
            min_order_quantity: Some(DecimalValue::new("1").unwrap()),
            min_notional: None,
            contract_size: Some(DecimalValue::new("10").unwrap()),
            contract_size_asset: Some("FET".to_owned()),
            max_leverage: Some(DecimalValue::new("200").unwrap()),
            listing_time_ms: None,
            expiry_time_ms: None,
            capabilities,
        };
        let mut result = HistoryResult {
            provider: Provider::Mexc,
            market: MarketKind::LinearPerpetual,
            symbol: "FET_USDT".to_owned(),
            start_time_ms: 60_000,
            end_time_ms: 120_000,
            candles: vec![crate::history::HistoryCandle {
                provider: Provider::Mexc,
                market: MarketKind::LinearPerpetual,
                symbol: "FET_USDT".to_owned(),
                candle: crate::domain::Candle {
                    interval: "1m".to_owned(),
                    start_time_ms: 60_000,
                    end_time_ms: 120_000,
                    open: DecimalValue::new("0.14").unwrap(),
                    high: DecimalValue::new("0.15").unwrap(),
                    low: DecimalValue::new("0.13").unwrap(),
                    close: DecimalValue::new("0.14").unwrap(),
                    base_volume: None,
                    quote_volume: Some(DecimalValue::new("1051.19").unwrap()),
                    contract_volume: Some(DecimalValue::new("751").unwrap()),
                    finality: crate::domain::CandleFinality::Closed,
                    data_quality: Vec::new(),
                },
            }],
        };

        enrich_history_volumes(&mut result, &instrument).unwrap();

        assert_eq!(
            result.candles[0]
                .candle
                .base_volume
                .as_ref()
                .unwrap()
                .as_str(),
            "7510"
        );
    }
}
