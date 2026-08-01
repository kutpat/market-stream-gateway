use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::future::pending;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::Settings;
use crate::domain::{Provider, SubscriptionKey};
use crate::gateway::{GatewayHub, SubscriptionRegistry};
use crate::health::{ConnectionState, HealthRegistry};
use crate::metrics::{EndpointLabels, Metrics};
use crate::providers::{
    AdapterError, EndpointKind, Heartbeat, OutboundCommand, ParsedFrame, ProviderAdapter,
    SubscriptionAction,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct RuntimeContext {
    pub registry: Arc<SubscriptionRegistry>,
    pub hub: Arc<GatewayHub>,
    pub health: Arc<HealthRegistry>,
    pub metrics: Arc<Metrics>,
    pub http: reqwest::Client,
    pub shutdown: CancellationToken,
    pub settings: Arc<Settings>,
}

#[allow(clippy::needless_pass_by_value)]
pub fn spawn_provider_supervisors(
    adapters: Vec<Arc<dyn ProviderAdapter>>,
    context: RuntimeContext,
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    for adapter in adapters {
        for &endpoint in adapter.endpoints() {
            context.health.register(adapter.provider(), endpoint);
            let adapter = Arc::clone(&adapter);
            let context = context.clone();
            tasks.push(tokio::spawn(async move {
                supervise_endpoint(adapter, endpoint, context).await;
            }));
        }
    }
    tasks
}

async fn supervise_endpoint(
    adapter: Arc<dyn ProviderAdapter>,
    endpoint: EndpointKind,
    context: RuntimeContext,
) {
    let provider = adapter.provider();
    let labels = EndpointLabels::new(provider, endpoint);
    let mut desired_rx = context.registry.subscribe_desired();
    let mut backoff = context.settings.backoff_min();

    loop {
        let Some(desired) =
            wait_for_desired(&adapter, endpoint, &mut desired_rx, &context, &labels).await
        else {
            break;
        };

        context.health.update(provider, endpoint, |health| {
            health.state = ConnectionState::Connecting;
            health.connection_epoch = None;
            health.connected_at_ms = None;
            health.last_event_at_ms = None;
            health.last_error = None;
        });

        let started = Instant::now();
        let result = run_connection(
            Arc::clone(&adapter),
            endpoint,
            desired,
            &mut desired_rx,
            &context,
            &labels,
        )
        .await;
        context
            .metrics
            .upstream_connections
            .get_or_create(&labels)
            .set(0);

        if context.shutdown.is_cancelled() {
            break;
        }
        if matches!(result, Ok(ConnectionExit::Idle)) {
            backoff = context.settings.backoff_min();
            continue;
        }

        let stable = started.elapsed() >= context.settings.stable_connection_duration();
        if stable {
            backoff = context.settings.backoff_min();
        }

        let error = match result {
            Ok(ConnectionExit::Rotate) => "planned connection rotation".to_owned(),
            Ok(ConnectionExit::Idle) => unreachable!(),
            Err(error) => error.to_string(),
        };
        context
            .metrics
            .upstream_reconnects
            .get_or_create(&labels)
            .inc();
        context.health.update(provider, endpoint, |health| {
            health.state = ConnectionState::Backoff;
            health.reconnects = health.reconnects.saturating_add(1);
            health.connection_epoch = None;
            health.connected_at_ms = None;
            health.last_event_at_ms = None;
            health.last_error = Some(error.clone());
        });
        warn!(
            provider = %provider,
            endpoint = %endpoint,
            error = %error,
            backoff_ms = backoff.as_millis(),
            "upstream_disconnected"
        );

        let jitter = {
            let mut rng = rand::rng();
            Duration::from_secs_f64(rng.random_range(0.0..=backoff.as_secs_f64()))
        };
        tokio::select! {
            () = context.shutdown.cancelled() => break,
            () = tokio::time::sleep(jitter) => {}
        }
        backoff = backoff
            .saturating_mul(2)
            .min(context.settings.backoff_max());
    }

    context
        .metrics
        .upstream_connections
        .get_or_create(&labels)
        .set(0);
    context.health.update(provider, endpoint, |health| {
        health.state = ConnectionState::Stopped;
        health.connection_epoch = None;
        health.connected_at_ms = None;
        health.last_event_at_ms = None;
    });
}

async fn wait_for_desired(
    adapter: &Arc<dyn ProviderAdapter>,
    endpoint: EndpointKind,
    desired_rx: &mut watch::Receiver<Arc<BTreeSet<SubscriptionKey>>>,
    context: &RuntimeContext,
    labels: &EndpointLabels,
) -> Option<BTreeSet<SubscriptionKey>> {
    loop {
        let desired = filter_desired(adapter.as_ref(), endpoint, &desired_rx.borrow());
        update_desired_metrics(adapter.provider(), endpoint, desired.len(), context, labels);
        if !desired.is_empty() {
            return Some(desired);
        }
        context
            .health
            .update(adapter.provider(), endpoint, |health| {
                health.state = ConnectionState::Idle;
                health.connection_epoch = None;
                health.connected_at_ms = None;
                health.last_event_at_ms = None;
                health.last_error = None;
            });
        tokio::select! {
            () = context.shutdown.cancelled() => return None,
            changed = desired_rx.changed() => {
                if changed.is_err() {
                    return None;
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_connection(
    adapter: Arc<dyn ProviderAdapter>,
    endpoint: EndpointKind,
    mut desired: BTreeSet<SubscriptionKey>,
    desired_rx: &mut watch::Receiver<Arc<BTreeSet<SubscriptionKey>>>,
    context: &RuntimeContext,
    labels: &EndpointLabels,
) -> Result<ConnectionExit, RuntimeError> {
    let provider = adapter.provider();
    let target = adapter.connection_target(endpoint, &context.http).await?;
    let websocket_config = WebSocketConfig::default()
        .max_message_size(Some(target.max_message_bytes))
        .max_frame_size(Some(target.max_message_bytes));
    let (websocket, _) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        connect_async_with_config(target.url.as_str(), Some(websocket_config), true),
    )
    .await
    .map_err(|_| RuntimeError::ConnectTimeout(CONNECT_TIMEOUT))??;
    let connection_epoch = Uuid::new_v4();
    let mut session = adapter.session(endpoint, connection_epoch);
    let (mut writer, mut reader) = websocket.split();
    let mut commands = CommandState::default();
    let initial_subscriptions = desired.iter().cloned().collect::<Vec<_>>();
    queue_subscription_diff(
        &mut commands,
        session.as_mut(),
        SubscriptionAction::Subscribe,
        &initial_subscriptions,
    )?;

    let now = unix_time_ms();
    context
        .metrics
        .upstream_connections
        .get_or_create(labels)
        .set(1);
    context.health.update(provider, endpoint, |health| {
        health.state = ConnectionState::Connecting;
        health.connection_epoch = Some(connection_epoch);
        health.connected_at_ms = Some(now);
        health.last_event_at_ms = None;
        health.last_error = None;
    });

    let mut heartbeat = tokio::time::interval(target.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut command_pacer = tokio::time::interval(target.command_interval);
    command_pacer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_market_event = Instant::now();
    let mut acknowledgement_deadline = None;
    let mut live = false;
    let mut idle_after_commands = false;
    let rotate = rotation_timer(target.rotate_after);
    tokio::pin!(rotate);

    if commands.readiness_complete() {
        mark_live(provider, endpoint, connection_epoch, desired.len(), context);
        live = true;
    }

    loop {
        let acknowledgement_timeout = deadline_timer(acknowledgement_deadline);
        tokio::pin!(acknowledgement_timeout);
        tokio::select! {
            () = context.shutdown.cancelled() => {
                let _ = send_message(&mut writer, Message::Close(None)).await;
                return Ok(ConnectionExit::Idle);
            }
            () = &mut rotate => {
                let _ = send_message(&mut writer, Message::Close(None)).await;
                return Ok(ConnectionExit::Rotate);
            }
            () = &mut acknowledgement_timeout => {
                return Err(RuntimeError::SubscriptionAcknowledgementTimeout(
                    target.subscription_ack_timeout,
                ));
            }
            _ = command_pacer.tick(), if commands.has_queued() => {
                let command = commands
                    .pop_front()
                    .expect("a guarded command queue cannot be empty");
                let QueuedCommand {
                    request_id,
                    text,
                    expected_acknowledgements,
                } = command;
                send_message(&mut writer, Message::Text(text.into())).await?;
                commands.mark_sent(request_id, expected_acknowledgements)?;
                acknowledgement_deadline = Some(
                    Instant::now() + target.subscription_ack_timeout,
                );
                if idle_after_commands && !commands.has_queued() {
                    let _ = send_message(&mut writer, Message::Close(None)).await;
                    return Ok(ConnectionExit::Idle);
                }
            }
            _ = heartbeat.tick() => {
                if last_market_event.elapsed() >= target.stale_after {
                    return Err(RuntimeError::Stale(target.stale_after));
                }
                send_heartbeat(&mut writer, session.heartbeat()).await?;
            }
            changed = desired_rx.changed() => {
                changed.map_err(|_| RuntimeError::RegistryClosed)?;
                let next = filter_desired(adapter.as_ref(), endpoint, &desired_rx.borrow());
                update_desired_metrics(provider, endpoint, next.len(), context, labels);
                let removed = desired.difference(&next).cloned().collect::<Vec<_>>();
                let added = next.difference(&desired).cloned().collect::<Vec<_>>();
                queue_subscription_diff(
                    &mut commands,
                    session.as_mut(),
                    SubscriptionAction::Unsubscribe,
                    &removed,
                )?;
                queue_subscription_diff(
                    &mut commands,
                    session.as_mut(),
                    SubscriptionAction::Subscribe,
                    &added,
                )?;
                if !removed.is_empty() || !added.is_empty() {
                    live = false;
                    context.health.update(provider, endpoint, |health| {
                        health.state = ConnectionState::Connecting;
                        health.last_error = None;
                    });
                }
                desired = next;
                idle_after_commands = desired.is_empty();
                if idle_after_commands && !commands.has_queued() {
                    let _ = send_message(&mut writer, Message::Close(None)).await;
                    return Ok(ConnectionExit::Idle);
                }
            }
            frame = reader.next() => {
                let frame = frame.ok_or(RuntimeError::Closed)??;
                context.metrics.upstream_frames.get_or_create(labels).inc();
                match frame {
                    Message::Text(text) => {
                        let processed = process_text(
                            session.as_mut(),
                            text.as_str(),
                            &desired,
                            context,
                            labels,
                            provider,
                            endpoint,
                        )?;
                        if handle_processed_frame(
                            processed,
                            &mut commands,
                            &mut live,
                            &mut acknowledgement_deadline,
                            target.subscription_ack_timeout,
                            provider,
                            endpoint,
                            connection_epoch,
                            desired.len(),
                            context,
                        )? {
                            last_market_event = Instant::now();
                        }
                    }
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes)
                            .map_err(|_| RuntimeError::NonUtf8Binary)?;
                        let processed = process_text(
                            session.as_mut(),
                            text,
                            &desired,
                            context,
                            labels,
                            provider,
                            endpoint,
                        )?;
                        if handle_processed_frame(
                            processed,
                            &mut commands,
                            &mut live,
                            &mut acknowledgement_deadline,
                            target.subscription_ack_timeout,
                            provider,
                            endpoint,
                            connection_epoch,
                            desired.len(),
                            context,
                        )? {
                            last_market_event = Instant::now();
                        }
                    }
                    Message::Ping(payload) => {
                        send_message(&mut writer, Message::Pong(payload)).await?;
                    }
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(frame) => return Err(RuntimeError::ProviderClosed(format!("{frame:?}"))),
                }
            }
        }
    }
}

fn process_text(
    session: &mut dyn crate::providers::AdapterSession,
    text: &str,
    desired: &BTreeSet<SubscriptionKey>,
    context: &RuntimeContext,
    labels: &EndpointLabels,
    provider: Provider,
    endpoint: EndpointKind,
) -> Result<ProcessedFrame, RuntimeError> {
    let received_at_ms = unix_time_ms();
    match session.parse(text, received_at_ms) {
        Ok(ParsedFrame::Events(events)) => {
            let mut demanded_event = false;
            for event in events {
                if let Err(error) = event.validate() {
                    context.metrics.invalid_frames.get_or_create(labels).inc();
                    warn!(
                        provider = %provider,
                        endpoint = %endpoint,
                        error = %error,
                        "normalized_event_rejected"
                    );
                    continue;
                }
                demanded_event |= desired.contains(&SubscriptionKey {
                    provider: event.provider,
                    market: event.market,
                    symbol: event.symbol.clone(),
                    channel: event.payload.channel(),
                });
                context
                    .metrics
                    .normalized_events
                    .get_or_create(labels)
                    .inc();
                let _ = context.hub.publish(event);
            }
            if demanded_event {
                context.health.update(provider, endpoint, |health| {
                    health.last_event_at_ms = Some(received_at_ms);
                });
            }
            Ok(if demanded_event {
                ProcessedFrame::DemandedMarketEvent
            } else {
                ProcessedFrame::Other
            })
        }
        Ok(ParsedFrame::Acknowledgement { request_id }) => {
            Ok(ProcessedFrame::Acknowledgement(request_id))
        }
        Ok(ParsedFrame::Pong | ParsedFrame::Ignored) => Ok(ProcessedFrame::Other),
        Err(error @ AdapterError::CommandRejected { .. }) => Err(error.into()),
        Err(error) => {
            context.metrics.invalid_frames.get_or_create(labels).inc();
            warn!(provider = %provider, endpoint = %endpoint, error = %error, "provider_frame_rejected");
            Ok(ProcessedFrame::Other)
        }
    }
}

fn queue_subscription_diff(
    commands: &mut CommandState,
    session: &mut dyn crate::providers::AdapterSession,
    action: SubscriptionAction,
    subscriptions: &[SubscriptionKey],
) -> Result<(), RuntimeError> {
    if subscriptions.is_empty() {
        return Ok(());
    }
    commands.enqueue(session.subscription_messages(action, subscriptions)?)
}

async fn send_heartbeat<S>(writer: &mut S, heartbeat: Heartbeat) -> Result<(), RuntimeError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match heartbeat {
        Heartbeat::Text(text) => send_message(writer, Message::Text(text.into())).await?,
        Heartbeat::WebSocketPing(payload) => {
            send_message(writer, Message::Ping(payload.into())).await?;
        }
    }
    Ok(())
}

async fn send_message<S>(writer: &mut S, message: Message) -> Result<(), RuntimeError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::time::timeout(WRITE_TIMEOUT, writer.send(message))
        .await
        .map_err(|_| RuntimeError::WriteTimeout(WRITE_TIMEOUT))??;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProcessedFrame {
    Acknowledgement(String),
    DemandedMarketEvent,
    Other,
}

#[derive(Debug)]
struct QueuedCommand {
    request_id: String,
    text: String,
    expected_acknowledgements: usize,
}

#[derive(Debug)]
struct PendingAcknowledgements {
    remaining: usize,
}

#[derive(Debug, Default)]
struct CommandState {
    queued: VecDeque<QueuedCommand>,
    pending: HashMap<String, PendingAcknowledgements>,
    known_request_ids: HashSet<String>,
}

impl CommandState {
    fn enqueue(&mut self, commands: Vec<OutboundCommand>) -> Result<(), RuntimeError> {
        for command in commands {
            if command.request_id.is_empty() || command.expected_acknowledgements == 0 {
                return Err(RuntimeError::InvalidOutboundCommand);
            }
            if !self.known_request_ids.insert(command.request_id.clone()) {
                return Err(RuntimeError::DuplicateOutboundRequestId(command.request_id));
            }
            self.queued.push_back(QueuedCommand {
                request_id: command.request_id,
                text: command.text,
                expected_acknowledgements: command.expected_acknowledgements,
            });
        }
        Ok(())
    }

    fn has_queued(&self) -> bool {
        !self.queued.is_empty()
    }

    fn pop_front(&mut self) -> Option<QueuedCommand> {
        self.queued.pop_front()
    }

    fn mark_sent(
        &mut self,
        request_id: String,
        expected_acknowledgements: usize,
    ) -> Result<(), RuntimeError> {
        let replaced = self.pending.insert(
            request_id.clone(),
            PendingAcknowledgements {
                remaining: expected_acknowledgements,
            },
        );
        if replaced.is_some() {
            return Err(RuntimeError::DuplicateOutboundRequestId(request_id));
        }
        Ok(())
    }

    fn acknowledge(&mut self, request_id: &str) -> Result<(), RuntimeError> {
        let pending = self
            .pending
            .get_mut(request_id)
            .ok_or_else(|| RuntimeError::UnexpectedAcknowledgement(request_id.to_owned()))?;
        pending.remaining -= 1;
        if pending.remaining == 0 {
            self.pending.remove(request_id);
            self.known_request_ids.remove(request_id);
        }
        Ok(())
    }

    fn readiness_complete(&self) -> bool {
        self.queued.is_empty() && self.pending.is_empty()
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_processed_frame(
    processed: ProcessedFrame,
    commands: &mut CommandState,
    live: &mut bool,
    acknowledgement_deadline: &mut Option<Instant>,
    acknowledgement_timeout: Duration,
    provider: Provider,
    endpoint: EndpointKind,
    connection_epoch: Uuid,
    desired_subscriptions: usize,
    context: &RuntimeContext,
) -> Result<bool, RuntimeError> {
    match processed {
        ProcessedFrame::Acknowledgement(request_id) => {
            commands.acknowledge(&request_id)?;
            if commands.readiness_complete() {
                *acknowledgement_deadline = None;
                if !*live {
                    mark_live(
                        provider,
                        endpoint,
                        connection_epoch,
                        desired_subscriptions,
                        context,
                    );
                    *live = true;
                }
            } else {
                *acknowledgement_deadline = Some(Instant::now() + acknowledgement_timeout);
            }
            Ok(false)
        }
        ProcessedFrame::DemandedMarketEvent => Ok(true),
        ProcessedFrame::Other => Ok(false),
    }
}

fn mark_live(
    provider: Provider,
    endpoint: EndpointKind,
    connection_epoch: Uuid,
    subscriptions: usize,
    context: &RuntimeContext,
) {
    context.health.update(provider, endpoint, |health| {
        health.state = ConnectionState::Live;
        health.last_error = None;
    });
    info!(
        provider = %provider,
        endpoint = %endpoint,
        connection_epoch = %connection_epoch,
        subscriptions,
        "upstream_connected"
    );
}

fn filter_desired(
    adapter: &dyn ProviderAdapter,
    endpoint: EndpointKind,
    snapshot: &BTreeSet<SubscriptionKey>,
) -> BTreeSet<SubscriptionKey> {
    snapshot
        .iter()
        .filter(|subscription| {
            subscription.provider == adapter.provider()
                && adapter.endpoint_for(subscription.channel) == endpoint
        })
        .cloned()
        .collect()
}

fn update_desired_metrics(
    provider: Provider,
    endpoint: EndpointKind,
    desired: usize,
    context: &RuntimeContext,
    labels: &EndpointLabels,
) {
    let desired_i64 = i64::try_from(desired).unwrap_or(i64::MAX);
    context
        .metrics
        .desired_subscriptions
        .get_or_create(labels)
        .set(desired_i64);
    context.health.update(provider, endpoint, |health| {
        health.desired_subscriptions = desired;
    });
}

async fn rotation_timer(rotate_after: Option<Duration>) {
    match rotate_after {
        Some(duration) => tokio::time::sleep(duration).await,
        None => pending::<()>().await,
    }
}

async fn deadline_timer(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

pub fn unix_time_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionExit {
    Idle,
    Rotate,
}

#[derive(Debug, thiserror::Error)]
enum RuntimeError {
    #[error(transparent)]
    Adapter(#[from] AdapterError),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("upstream websocket connect timed out after {0:?}")]
    ConnectTimeout(Duration),
    #[error("upstream websocket write timed out after {0:?}")]
    WriteTimeout(Duration),
    #[error("upstream websocket closed")]
    Closed,
    #[error("upstream websocket was stale for {0:?}")]
    Stale(Duration),
    #[error("provider closed the websocket: {0}")]
    ProviderClosed(String),
    #[error("subscription registry closed")]
    RegistryClosed,
    #[error("provider sent a non-UTF-8 binary frame")]
    NonUtf8Binary,
    #[error("provider generated an invalid outbound subscription command")]
    InvalidOutboundCommand,
    #[error("provider reused outbound request id {0}")]
    DuplicateOutboundRequestId(String),
    #[error("provider acknowledged unknown or already completed request id {0}")]
    UnexpectedAcknowledgement(String),
    #[error("initial subscription acknowledgement timed out after {0:?}")]
    SubscriptionAcknowledgementTimeout(Duration),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Channel, MarketKind};

    fn outbound(request_id: &str, expected_acknowledgements: usize) -> OutboundCommand {
        OutboundCommand {
            request_id: request_id.to_owned(),
            text: format!(r#"{{"id":"{request_id}"}}"#),
            expected_acknowledgements,
        }
    }

    fn send_next(commands: &mut CommandState) {
        let command = commands.pop_front().expect("expected a queued command");
        commands
            .mark_sent(command.request_id, command.expected_acknowledgements)
            .unwrap();
    }

    struct EndpointAdapter;

    #[async_trait::async_trait]
    impl ProviderAdapter for EndpointAdapter {
        fn provider(&self) -> Provider {
            Provider::Okx
        }

        fn endpoints(&self) -> &'static [EndpointKind] {
            &[EndpointKind::Public, EndpointKind::Business]
        }

        fn endpoint_for(&self, channel: Channel) -> EndpointKind {
            match channel {
                Channel::Ticker => EndpointKind::Public,
                Channel::Candle1m => EndpointKind::Business,
            }
        }

        async fn connection_target(
            &self,
            _endpoint: EndpointKind,
            _http: &reqwest::Client,
        ) -> Result<crate::providers::ConnectionTarget, AdapterError> {
            unreachable!()
        }

        fn session(
            &self,
            _endpoint: EndpointKind,
            _connection_epoch: Uuid,
        ) -> Box<dyn crate::providers::AdapterSession> {
            unreachable!()
        }
    }

    #[test]
    fn desired_subscriptions_are_partitioned_by_provider_and_endpoint() {
        let adapter = EndpointAdapter;
        let snapshot = [
            SubscriptionKey::new(
                Provider::Okx,
                MarketKind::LinearPerpetual,
                "BTC-USDT-SWAP",
                Channel::Ticker,
            )
            .unwrap(),
            SubscriptionKey::new(
                Provider::Okx,
                MarketKind::LinearPerpetual,
                "BTC-USDT-SWAP",
                Channel::Candle1m,
            )
            .unwrap(),
            SubscriptionKey::new(
                Provider::Bybit,
                MarketKind::LinearPerpetual,
                "BTCUSDT",
                Channel::Ticker,
            )
            .unwrap(),
        ]
        .into_iter()
        .collect();

        let public = filter_desired(&adapter, EndpointKind::Public, &snapshot);
        assert_eq!(public.len(), 1);
        assert_eq!(public.iter().next().unwrap().channel, Channel::Ticker);
    }

    #[test]
    fn wall_clock_is_in_milliseconds() {
        assert!(unix_time_ms() > 1_700_000_000_000);
    }

    #[test]
    fn readiness_waits_for_every_command_send_and_native_acknowledgement() {
        let mut commands = CommandState::default();
        commands
            .enqueue(vec![outbound("okx-1", 3), outbound("okx-2", 1)])
            .unwrap();
        assert!(!commands.readiness_complete());

        send_next(&mut commands);
        assert!(!commands.readiness_complete());
        for _ in 0..3 {
            commands.acknowledge("okx-1").unwrap();
        }
        // The first request is complete, but the second command has not even been sent yet.
        assert!(!commands.readiness_complete());

        send_next(&mut commands);
        assert!(!commands.readiness_complete());
        commands.acknowledge("okx-2").unwrap();
        assert!(commands.readiness_complete());
    }

    #[test]
    fn unknown_and_duplicate_acknowledgements_are_rejected() {
        let mut commands = CommandState::default();
        commands.enqueue(vec![outbound("1", 1)]).unwrap();
        send_next(&mut commands);

        assert!(commands.acknowledge("1").is_ok());
        assert!(matches!(
            commands.acknowledge("1"),
            Err(RuntimeError::UnexpectedAcknowledgement(request_id)) if request_id == "1"
        ));
        assert!(matches!(
            commands.acknowledge("missing"),
            Err(RuntimeError::UnexpectedAcknowledgement(request_id)) if request_id == "missing"
        ));
    }

    #[test]
    fn dynamic_commands_regress_readiness_until_their_acknowledgement() {
        let mut commands = CommandState::default();
        assert!(commands.readiness_complete());
        commands.enqueue(vec![outbound("dynamic", 1)]).unwrap();
        assert!(!commands.readiness_complete());
        send_next(&mut commands);
        assert!(!commands.readiness_complete());
        commands.acknowledge("dynamic").unwrap();
        assert!(commands.readiness_complete());
    }
}
