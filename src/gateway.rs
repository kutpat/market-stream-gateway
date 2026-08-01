use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use thiserror::Error;
use tokio::sync::{Mutex, broadcast, mpsc, watch};
use uuid::Uuid;

use crate::domain::{MarketEvent, ProviderEvent, SubscriptionKey};

/// A stable identity for one connected downstream client.
pub type ClientId = Uuid;

/// An immutable, deterministically ordered view of every currently desired subscription.
pub type DesiredSubscriptions = Arc<BTreeSet<SubscriptionKey>>;

/// Fans normalized provider events out to all current downstream consumers.
///
/// The Tokio broadcast receiver is intentionally exposed directly. A slow receiver therefore
/// observes [`broadcast::error::RecvError::Lagged`] and can report or recover from the gap using
/// its own transport semantics.
#[derive(Debug)]
pub struct GatewayHub {
    stream_epoch: Uuid,
    next_delivery_sequence: AtomicU64,
    publish_order: StdMutex<()>,
    client_capacity: usize,
    max_clients: usize,
    routes: StdMutex<RouteState>,
    // A diagnostics-only stream used by integration tests and internal observers. Downstream
    // clients use the key-routed bounded queues below, so unrelated instruments cannot lag them.
    events: broadcast::Sender<Arc<MarketEvent>>,
}

#[derive(Debug, Default)]
struct RouteState {
    clients: HashMap<ClientId, ClientRoute>,
    clients_by_key: HashMap<SubscriptionKey, BTreeSet<ClientId>>,
}

#[derive(Debug)]
struct ClientRoute {
    events: mpsc::Sender<Arc<MarketEvent>>,
    lagged: watch::Sender<u64>,
    subscriptions: BTreeSet<SubscriptionKey>,
}

/// The bounded event and lag-notification channels for one downstream client.
#[derive(Debug)]
pub struct ClientFeed {
    pub events: mpsc::Receiver<Arc<MarketEvent>>,
    pub lagged: watch::Receiver<u64>,
}

impl GatewayHub {
    /// Create a hub backed by a bounded broadcast ring.
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero.
    pub fn new(capacity: usize) -> Self {
        Self::with_client_limit(capacity, usize::MAX)
    }

    /// Create a hub with bounded per-client queues and a global client limit.
    ///
    /// # Panics
    ///
    /// Panics when either limit is zero.
    pub fn with_client_limit(capacity: usize, max_clients: usize) -> Self {
        assert!(capacity > 0, "broadcast capacity must be greater than zero");
        assert!(max_clients > 0, "client limit must be greater than zero");
        let (events, _) = broadcast::channel(capacity);
        Self {
            stream_epoch: Uuid::new_v4(),
            next_delivery_sequence: AtomicU64::new(1),
            publish_order: StdMutex::new(()),
            client_capacity: capacity,
            max_clients,
            routes: StdMutex::new(RouteState::default()),
            events,
        }
    }

    /// The epoch shared by every event emitted during this process lifetime.
    pub fn stream_epoch(&self) -> Uuid {
        self.stream_epoch
    }

    /// Subscribe to every normalized event for diagnostics and tests.
    ///
    /// Downstream client sessions should use [`Self::register_client`] so unrelated instruments
    /// do not consume their bounded queue capacity.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<MarketEvent>> {
        self.events.subscribe()
    }

    /// Number of active routed downstream clients.
    pub fn receiver_count(&self) -> usize {
        self.with_routes(|routes| routes.clients.len())
    }

    /// Register a downstream client and return its bounded, initially unsubscribed feed.
    ///
    /// # Panics
    ///
    /// Panics when the configured downstream client limit has already been reached. Runtime
    /// callers that accept untrusted connections should use [`Self::try_register_client`].
    pub fn register_client(&self, client_id: ClientId) -> ClientFeed {
        self.try_register_client(client_id)
            .expect("downstream client limit exceeded")
    }

    /// Register a downstream client unless the configured global limit is reached.
    pub fn try_register_client(&self, client_id: ClientId) -> Option<ClientFeed> {
        let (events_tx, events_rx) = mpsc::channel(self.client_capacity);
        let (lagged_tx, lagged_rx) = watch::channel(0_u64);
        let registered = self.with_routes_mut(|routes| {
            if !routes.clients.contains_key(&client_id) && routes.clients.len() >= self.max_clients
            {
                return false;
            }
            remove_route(routes, client_id, true);
            routes.clients.insert(
                client_id,
                ClientRoute {
                    events: events_tx,
                    lagged: lagged_tx,
                    subscriptions: BTreeSet::new(),
                },
            );
            true
        });
        registered.then_some(ClientFeed {
            events: events_rx,
            lagged: lagged_rx,
        })
    }

    /// Atomically replace the keys routed to one registered client.
    ///
    /// Returns `false` when the client has already disconnected.
    pub fn update_client_subscriptions(
        &self,
        client_id: ClientId,
        subscriptions: &BTreeSet<SubscriptionKey>,
    ) -> bool {
        self.with_routes_mut(|routes| {
            let Some(current) = routes
                .clients
                .get(&client_id)
                .map(|client| client.subscriptions.clone())
            else {
                return false;
            };
            for key in current.difference(subscriptions) {
                remove_key_route(routes, key, client_id);
            }
            for key in subscriptions.difference(&current) {
                routes
                    .clients_by_key
                    .entry(key.clone())
                    .or_default()
                    .insert(client_id);
            }
            if let Some(client) = routes.clients.get_mut(&client_id) {
                client.subscriptions.clone_from(subscriptions);
            }
            true
        })
    }

    /// Remove a downstream route and close its event channels.
    pub fn remove_client(&self, client_id: ClientId) {
        self.with_routes_mut(|routes| remove_route(routes, client_id, true));
    }

    /// Normalize and publish one provider event.
    ///
    /// Publishing succeeds even when there are no receivers. The returned event is useful for
    /// metrics and tests and carries the exact sequence assigned by the hub.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError::SequenceExhausted`] instead of allowing the sequence to wrap.
    pub fn publish(&self, event: ProviderEvent) -> Result<Arc<MarketEvent>, PublishError> {
        // Serializing this small critical section makes receiver delivery order agree with the
        // delivery sequence even when multiple provider tasks publish concurrently. No await is
        // performed while the standard mutex is held.
        let _publish_guard = self
            .publish_order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let delivery_sequence = self
            .next_delivery_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map_err(|_| PublishError::SequenceExhausted)?;
        let event = Arc::new(event.into_market_event(self.stream_epoch, delivery_sequence));

        self.route_event(&event);
        // Diagnostics observers are optional. Tokio reports no observers as SendError, so it is
        // intentionally not a publish failure.
        drop(self.events.send(Arc::clone(&event)));
        Ok(event)
    }

    fn route_event(&self, event: &Arc<MarketEvent>) {
        let key = event.subscription_key();
        self.with_routes_mut(|routes| {
            let client_ids = routes.clients_by_key.get(&key).cloned().unwrap_or_default();
            let mut lagged = Vec::new();
            let mut closed = Vec::new();
            for client_id in client_ids {
                let Some(client) = routes.clients.get(&client_id) else {
                    continue;
                };
                match client.events.try_send(Arc::clone(event)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        client
                            .lagged
                            .send_modify(|count| *count = count.saturating_add(1));
                        lagged.push(client_id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => closed.push(client_id),
                }
            }
            for client_id in lagged {
                // Stop routing immediately. Retain the lag watch sender until the API session
                // observes it and performs normal cleanup.
                remove_route(routes, client_id, false);
            }
            for client_id in closed {
                remove_route(routes, client_id, true);
            }
        });
    }

    fn with_routes<T>(&self, read: impl FnOnce(&RouteState) -> T) -> T {
        match self.routes.lock() {
            Ok(routes) => read(&routes),
            Err(poisoned) => read(&poisoned.into_inner()),
        }
    }

    fn with_routes_mut<T>(&self, update: impl FnOnce(&mut RouteState) -> T) -> T {
        match self.routes.lock() {
            Ok(mut routes) => update(&mut routes),
            Err(poisoned) => update(&mut poisoned.into_inner()),
        }
    }
}

fn remove_route(routes: &mut RouteState, client_id: ClientId, drop_client: bool) {
    let subscriptions = routes
        .clients
        .get_mut(&client_id)
        .map(|client| std::mem::take(&mut client.subscriptions))
        .unwrap_or_default();
    for key in subscriptions {
        remove_key_route(routes, &key, client_id);
    }
    if drop_client {
        routes.clients.remove(&client_id);
    }
}

fn remove_key_route(routes: &mut RouteState, key: &SubscriptionKey, client_id: ClientId) {
    let remove_key = if let Some(client_ids) = routes.clients_by_key.get_mut(key) {
        client_ids.remove(&client_id);
        client_ids.is_empty()
    } else {
        false
    };
    if remove_key {
        routes.clients_by_key.remove(key);
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum PublishError {
    #[error("delivery sequence exhausted")]
    SequenceExhausted,
}

/// The observable result of one client's registry mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionChange {
    /// Keys newly owned by this client during this operation.
    pub added: Vec<SubscriptionKey>,
    /// Keys no longer owned by this client after this operation.
    pub removed: Vec<SubscriptionKey>,
    /// Keys whose global reference count changed from zero to one.
    pub activated: Vec<SubscriptionKey>,
    /// Keys whose global reference count changed from one to zero.
    pub deactivated: Vec<SubscriptionKey>,
    /// The client's complete subscription set after the operation.
    pub client_subscriptions: DesiredSubscriptions,
}

impl SubscriptionChange {
    fn unchanged(client_subscriptions: BTreeSet<SubscriptionKey>) -> Self {
        Self {
            added: Vec::new(),
            removed: Vec::new(),
            activated: Vec::new(),
            deactivated: Vec::new(),
            client_subscriptions: Arc::new(client_subscriptions),
        }
    }

    pub fn desired_state_changed(&self) -> bool {
        !self.activated.is_empty() || !self.deactivated.is_empty()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RegistryError {
    #[error("client subscription limit exceeded: requested {requested}, maximum is {limit}")]
    ClientLimitExceeded { limit: usize, requested: usize },
    #[error(
        "provider subscription limit exceeded for {provider}: requested {requested}, maximum is {limit}"
    )]
    ProviderLimitExceeded {
        provider: crate::domain::Provider,
        limit: usize,
        requested: usize,
    },
}

#[derive(Debug, Default)]
struct RegistryState {
    clients: HashMap<ClientId, BTreeSet<SubscriptionKey>>,
    reference_counts: HashMap<SubscriptionKey, usize>,
    desired: BTreeSet<SubscriptionKey>,
}

/// Ref-counts downstream demand and publishes bounded, immutable desired-state snapshots.
#[derive(Debug)]
pub struct SubscriptionRegistry {
    max_subscriptions_per_client: usize,
    max_subscriptions_per_provider: usize,
    state: Mutex<RegistryState>,
    desired: watch::Sender<DesiredSubscriptions>,
}

impl SubscriptionRegistry {
    pub fn new(max_subscriptions_per_client: usize) -> Self {
        Self::with_provider_limit(max_subscriptions_per_client, usize::MAX)
    }

    pub fn with_provider_limit(
        max_subscriptions_per_client: usize,
        max_subscriptions_per_provider: usize,
    ) -> Self {
        let (desired, _) = watch::channel(Arc::new(BTreeSet::new()));
        Self {
            max_subscriptions_per_client,
            max_subscriptions_per_provider,
            state: Mutex::new(RegistryState::default()),
            desired,
        }
    }

    pub fn max_subscriptions_per_client(&self) -> usize {
        self.max_subscriptions_per_client
    }

    pub fn max_subscriptions_per_provider(&self) -> usize {
        self.max_subscriptions_per_provider
    }

    /// Subscribe to the latest complete desired-state snapshot.
    ///
    /// The returned watch channel is bounded to one current value. It changes only when a key's
    /// global reference count crosses zero.
    pub fn subscribe_desired(&self) -> watch::Receiver<DesiredSubscriptions> {
        self.desired.subscribe()
    }

    /// Return the latest complete desired-state snapshot without waiting.
    pub fn desired_snapshot(&self) -> DesiredSubscriptions {
        self.desired.borrow().clone()
    }

    /// Add subscriptions for a client atomically.
    ///
    /// Duplicate keys, including keys already owned by the client, are idempotent. If the unique
    /// resulting set exceeds the per-client limit, nothing is changed.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::ClientLimitExceeded`] when the resulting unique client set would
    /// exceed the configured maximum.
    pub async fn add<I>(
        &self,
        client_id: ClientId,
        keys: I,
    ) -> Result<SubscriptionChange, RegistryError>
    where
        I: IntoIterator<Item = SubscriptionKey>,
    {
        let requested = keys.into_iter().collect::<BTreeSet<_>>();
        let mut state = self.state.lock().await;
        let existing = state.clients.get(&client_id).cloned().unwrap_or_default();
        let added = requested.difference(&existing).cloned().collect::<Vec<_>>();
        let requested_total = existing.len() + added.len();

        if requested_total > self.max_subscriptions_per_client {
            return Err(RegistryError::ClientLimitExceeded {
                limit: self.max_subscriptions_per_client,
                requested: requested_total,
            });
        }
        if added.is_empty() {
            return Ok(SubscriptionChange::unchanged(existing));
        }

        let mut provider_totals = HashMap::new();
        for key in &state.desired {
            *provider_totals.entry(key.provider).or_insert(0_usize) += 1;
        }
        for key in &added {
            if !state.reference_counts.contains_key(key) {
                let total = provider_totals.entry(key.provider).or_insert(0);
                *total = total.saturating_add(1);
            }
        }
        if let Some((provider, requested)) = provider_totals
            .into_iter()
            .find(|(_, total)| *total > self.max_subscriptions_per_provider)
        {
            return Err(RegistryError::ProviderLimitExceeded {
                provider,
                limit: self.max_subscriptions_per_provider,
                requested,
            });
        }

        state
            .clients
            .entry(client_id)
            .or_default()
            .extend(added.iter().cloned());

        let mut activated = Vec::new();
        for key in &added {
            let reference_count = state.reference_counts.entry(key.clone()).or_default();
            // The count cannot reach usize::MAX before the client map itself exhausts the
            // address space. Saturation still prevents a wrap if that invariant is ever broken.
            *reference_count = reference_count.saturating_add(1);
            if *reference_count == 1 {
                state.desired.insert(key.clone());
                activated.push(key.clone());
            }
        }

        let client_subscriptions =
            Arc::new(state.clients.get(&client_id).cloned().unwrap_or_default());
        let previous_snapshot = if activated.is_empty() {
            None
        } else {
            Some(self.desired.send_replace(Arc::new(state.desired.clone())))
        };
        drop(state);
        drop(previous_snapshot);

        Ok(SubscriptionChange {
            added,
            removed: Vec::new(),
            activated,
            deactivated: Vec::new(),
            client_subscriptions,
        })
    }

    /// Remove keys owned by a client. Unknown keys are ignored.
    pub async fn remove<I>(&self, client_id: ClientId, keys: I) -> SubscriptionChange
    where
        I: IntoIterator<Item = SubscriptionKey>,
    {
        let requested = keys.into_iter().collect::<BTreeSet<_>>();
        let mut state = self.state.lock().await;
        let existing = state.clients.get(&client_id).cloned().unwrap_or_default();
        let removed = requested
            .intersection(&existing)
            .cloned()
            .collect::<Vec<_>>();

        if removed.is_empty() {
            return SubscriptionChange::unchanged(existing);
        }

        let client_is_empty = {
            // `existing` was cloned under this same lock, so the entry must be present. Using
            // entry here keeps a violated internal invariant recoverable instead of panicking.
            let client_keys = state.clients.entry(client_id).or_default();
            for key in &removed {
                client_keys.remove(key);
            }
            client_keys.is_empty()
        };
        if client_is_empty {
            state.clients.remove(&client_id);
        }

        let deactivated = decrement_references(&mut state, &removed);
        let client_subscriptions =
            Arc::new(state.clients.get(&client_id).cloned().unwrap_or_default());
        let previous_snapshot = publish_snapshot_if_changed(&self.desired, &state, &deactivated);
        drop(state);
        drop(previous_snapshot);

        SubscriptionChange {
            added: Vec::new(),
            removed,
            activated: Vec::new(),
            deactivated,
            client_subscriptions,
        }
    }

    /// Remove every key owned by a disconnected client in one atomic operation.
    pub async fn cleanup_client(&self, client_id: ClientId) -> SubscriptionChange {
        let mut state = self.state.lock().await;
        let Some(client_keys) = state.clients.remove(&client_id) else {
            return SubscriptionChange::unchanged(BTreeSet::new());
        };
        let removed = client_keys.into_iter().collect::<Vec<_>>();
        let deactivated = decrement_references(&mut state, &removed);
        let previous_snapshot = publish_snapshot_if_changed(&self.desired, &state, &deactivated);
        drop(state);
        drop(previous_snapshot);

        SubscriptionChange {
            added: Vec::new(),
            removed,
            activated: Vec::new(),
            deactivated,
            client_subscriptions: Arc::new(BTreeSet::new()),
        }
    }

    /// Return one client's current subscriptions as an immutable snapshot.
    pub async fn client_snapshot(&self, client_id: ClientId) -> DesiredSubscriptions {
        let state = self.state.lock().await;
        Arc::new(state.clients.get(&client_id).cloned().unwrap_or_default())
    }

    /// Return the number of clients currently demanding a key.
    pub async fn reference_count(&self, key: &SubscriptionKey) -> usize {
        let state = self.state.lock().await;
        state.reference_counts.get(key).copied().unwrap_or(0)
    }
}

fn decrement_references(
    state: &mut RegistryState,
    removed: &[SubscriptionKey],
) -> Vec<SubscriptionKey> {
    let mut deactivated = Vec::new();
    for key in removed {
        let remove_reference = match state.reference_counts.get_mut(key) {
            Some(reference_count) if *reference_count > 1 => {
                *reference_count -= 1;
                false
            }
            Some(_) => true,
            None => {
                debug_assert!(false, "client key must have a reference count");
                false
            }
        };
        if remove_reference {
            state.reference_counts.remove(key);
            state.desired.remove(key);
            deactivated.push(key.clone());
        }
    }
    deactivated
}

fn publish_snapshot_if_changed(
    sender: &watch::Sender<DesiredSubscriptions>,
    state: &RegistryState,
    deactivated: &[SubscriptionKey],
) -> Option<DesiredSubscriptions> {
    if deactivated.is_empty() {
        None
    } else {
        Some(sender.send_replace(Arc::new(state.desired.clone())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Barrier;

    use super::*;
    use crate::domain::{Channel, MarketKind, MarketPayload, ObservedDecimal, Provider, Ticker};

    fn key(provider: Provider, symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(provider, MarketKind::LinearPerpetual, symbol, channel).unwrap()
    }

    fn provider_event(symbol: &str, received_at_ms: u64) -> ProviderEvent {
        ProviderEvent {
            connection_epoch: Uuid::nil(),
            provider: Provider::Bybit,
            market: MarketKind::LinearPerpetual,
            symbol: symbol.to_owned(),
            exchange_time_ms: Some(received_at_ms),
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload: MarketPayload::Ticker(Ticker {
                last: Some(ObservedDecimal::new("1.0", received_at_ms).unwrap()),
                ..Ticker::default()
            }),
        }
    }

    #[test]
    fn one_stream_epoch_is_used_for_every_event() {
        let hub = GatewayHub::new(4);
        let first = hub.publish(provider_event("BTCUSDT", 1)).unwrap();
        let second = hub.publish(provider_event("ETHUSDT", 2)).unwrap();

        assert_eq!(first.stream_epoch, hub.stream_epoch());
        assert_eq!(second.stream_epoch, first.stream_epoch);
        assert_eq!(first.delivery_sequence, 1);
        assert_eq!(second.delivery_sequence, 2);
    }

    #[tokio::test]
    async fn concurrent_publish_sequences_match_delivery_order() {
        let event_count = 64_usize;
        let hub = Arc::new(GatewayHub::new(event_count));
        let mut receiver = hub.subscribe();
        let barrier = Arc::new(Barrier::new(event_count));
        let mut tasks = Vec::new();

        for index in 0..event_count {
            let hub = Arc::clone(&hub);
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                hub.publish(provider_event("BTCUSDT", u64::try_from(index).unwrap()))
                    .unwrap()
            }));
        }

        let mut assigned = Vec::new();
        for task in tasks {
            assigned.push(task.await.unwrap().delivery_sequence);
        }
        assigned.sort_unstable();
        assert_eq!(
            assigned,
            (1..=u64::try_from(event_count).unwrap()).collect::<Vec<_>>()
        );

        for expected in 1..=u64::try_from(event_count).unwrap() {
            assert_eq!(receiver.recv().await.unwrap().delivery_sequence, expected);
        }
    }

    #[tokio::test]
    async fn slow_broadcast_receiver_observes_lag() {
        let hub = GatewayHub::new(2);
        let mut receiver = hub.subscribe();
        for index in 0..6 {
            hub.publish(provider_event("BTCUSDT", index)).unwrap();
        }

        let error = receiver.recv().await.unwrap_err();
        assert!(matches!(error, broadcast::error::RecvError::Lagged(dropped) if dropped > 0));
        assert_eq!(receiver.recv().await.unwrap().delivery_sequence, 5);
    }

    #[tokio::test]
    async fn routed_clients_only_receive_matching_subscriptions() {
        let hub = GatewayHub::new(2);
        let client = Uuid::new_v4();
        let mut feed = hub.register_client(client);
        let subscribed = key(Provider::Bybit, "BTCUSDT", Channel::Ticker);
        hub.update_client_subscriptions(client, &BTreeSet::from([subscribed]));

        hub.publish(provider_event("ETHUSDT", 1)).unwrap();
        hub.publish(provider_event("BTCUSDT", 2)).unwrap();

        let event = feed.events.recv().await.unwrap();
        assert_eq!(event.symbol, "BTCUSDT");
        assert!(feed.events.try_recv().is_err());
        assert_eq!(*feed.lagged.borrow(), 0);
    }

    #[tokio::test]
    async fn unrelated_volume_cannot_lag_a_routed_client() {
        let hub = GatewayHub::new(1);
        let client = Uuid::new_v4();
        let mut feed = hub.register_client(client);
        let subscribed = key(Provider::Bybit, "BTCUSDT", Channel::Ticker);
        hub.update_client_subscriptions(client, &BTreeSet::from([subscribed]));

        for sequence in 0..100 {
            hub.publish(provider_event("ETHUSDT", sequence)).unwrap();
        }
        hub.publish(provider_event("BTCUSDT", 101)).unwrap();

        assert_eq!(feed.events.recv().await.unwrap().symbol, "BTCUSDT");
        assert_eq!(*feed.lagged.borrow(), 0);
    }

    #[tokio::test]
    async fn matching_queue_overrun_notifies_and_deactivates_client() {
        let hub = GatewayHub::new(1);
        let client = Uuid::new_v4();
        let mut feed = hub.register_client(client);
        let subscribed = key(Provider::Bybit, "BTCUSDT", Channel::Ticker);
        hub.update_client_subscriptions(client, &BTreeSet::from([subscribed]));

        hub.publish(provider_event("BTCUSDT", 1)).unwrap();
        hub.publish(provider_event("BTCUSDT", 2)).unwrap();
        feed.lagged.changed().await.unwrap();

        assert_eq!(*feed.lagged.borrow(), 1);
        assert_eq!(feed.events.recv().await.unwrap().exchange_time_ms, Some(1));
        hub.publish(provider_event("BTCUSDT", 3)).unwrap();
        assert!(feed.events.try_recv().is_err());
    }

    #[test]
    fn routed_client_limit_is_atomic() {
        let hub = GatewayHub::with_client_limit(2, 1);
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        assert!(hub.try_register_client(first).is_some());
        assert!(hub.try_register_client(second).is_none());
        hub.remove_client(first);
        assert!(hub.try_register_client(second).is_some());
    }

    #[tokio::test]
    async fn registry_ref_counts_shared_subscriptions() {
        let registry = SubscriptionRegistry::new(8);
        let mut desired = registry.subscribe_desired();
        let subscription = key(Provider::Binance, "BTCUSDT", Channel::Ticker);
        let first_client = Uuid::new_v4();
        let second_client = Uuid::new_v4();

        let first = registry
            .add(first_client, [subscription.clone()])
            .await
            .unwrap();
        assert_eq!(first.activated, vec![subscription.clone()]);
        desired.changed().await.unwrap();

        let second = registry
            .add(second_client, [subscription.clone()])
            .await
            .unwrap();
        assert!(second.activated.is_empty());
        assert!(!desired.has_changed().unwrap());
        assert_eq!(registry.reference_count(&subscription).await, 2);
    }

    #[tokio::test]
    async fn one_client_cannot_unsubscribe_another_clients_demand() {
        let registry = SubscriptionRegistry::new(8);
        let subscription = key(Provider::Okx, "BTC-USDT-SWAP", Channel::Candle1m);
        let first_client = Uuid::new_v4();
        let second_client = Uuid::new_v4();
        registry
            .add(first_client, [subscription.clone()])
            .await
            .unwrap();
        registry
            .add(second_client, [subscription.clone()])
            .await
            .unwrap();

        let change = registry.remove(first_client, [subscription.clone()]).await;

        assert!(change.deactivated.is_empty());
        assert_eq!(registry.reference_count(&subscription).await, 1);
        assert!(registry.desired_snapshot().contains(&subscription));
    }

    #[tokio::test]
    async fn cleanup_removes_all_client_references() {
        let registry = SubscriptionRegistry::new(8);
        let ticker = key(Provider::Kucoin, "XBTUSDTM", Channel::Ticker);
        let candle = key(Provider::Kucoin, "XBTUSDTM", Channel::Candle1m);
        let shared_client = Uuid::new_v4();
        let disconnected_client = Uuid::new_v4();
        registry.add(shared_client, [ticker.clone()]).await.unwrap();
        registry
            .add(disconnected_client, [ticker.clone(), candle.clone()])
            .await
            .unwrap();

        let change = registry.cleanup_client(disconnected_client).await;

        assert_eq!(change.removed, vec![ticker.clone(), candle.clone()]);
        assert_eq!(change.deactivated, vec![candle.clone()]);
        assert_eq!(registry.reference_count(&ticker).await, 1);
        assert_eq!(registry.reference_count(&candle).await, 0);
        assert_eq!(
            registry.desired_snapshot().as_ref(),
            &BTreeSet::from([ticker])
        );
    }

    #[tokio::test]
    async fn add_limit_is_atomic_and_counts_unique_keys() {
        let registry = SubscriptionRegistry::new(1);
        let client = Uuid::new_v4();
        let ticker = key(Provider::Bybit, "BTCUSDT", Channel::Ticker);
        let candle = key(Provider::Bybit, "BTCUSDT", Channel::Candle1m);
        registry
            .add(client, [ticker.clone(), ticker.clone()])
            .await
            .unwrap();

        let error = registry.add(client, [candle.clone()]).await.unwrap_err();

        assert_eq!(
            error,
            RegistryError::ClientLimitExceeded {
                limit: 1,
                requested: 2
            }
        );
        assert_eq!(
            registry.client_snapshot(client).await.as_ref(),
            &BTreeSet::from([ticker.clone()])
        );
        assert!(!registry.desired_snapshot().contains(&candle));
    }

    #[tokio::test]
    async fn provider_limit_is_global_and_atomic_across_clients() {
        let registry = SubscriptionRegistry::with_provider_limit(8, 1);
        let first_client = Uuid::new_v4();
        let second_client = Uuid::new_v4();
        let ticker = key(Provider::Binance, "BTCUSDT", Channel::Ticker);
        let candle = key(Provider::Binance, "BTCUSDT", Channel::Candle1m);
        registry.add(first_client, [ticker.clone()]).await.unwrap();

        let error = registry
            .add(second_client, [candle.clone()])
            .await
            .unwrap_err();

        assert_eq!(
            error,
            RegistryError::ProviderLimitExceeded {
                provider: Provider::Binance,
                limit: 1,
                requested: 2,
            }
        );
        assert_eq!(
            registry.desired_snapshot().as_ref(),
            &BTreeSet::from([ticker])
        );
        assert_eq!(registry.reference_count(&candle).await, 0);
    }

    #[tokio::test]
    async fn concurrent_clients_produce_consistent_snapshots() {
        let client_count = 64_usize;
        let registry = Arc::new(SubscriptionRegistry::new(2));
        let shared = key(Provider::Binance, "ETHUSDT", Channel::Ticker);
        let mut tasks = Vec::new();

        for _ in 0..client_count {
            let registry = Arc::clone(&registry);
            let shared = shared.clone();
            tasks.push(tokio::spawn(async move {
                let client = Uuid::new_v4();
                registry.add(client, [shared]).await.unwrap();
                client
            }));
        }

        let mut clients = Vec::new();
        for task in tasks {
            clients.push(task.await.unwrap());
        }
        assert_eq!(registry.reference_count(&shared).await, client_count);
        assert_eq!(
            registry.desired_snapshot().as_ref(),
            &BTreeSet::from([shared.clone()])
        );

        let mut cleanup_tasks = Vec::new();
        for client in clients {
            let registry = Arc::clone(&registry);
            cleanup_tasks.push(tokio::spawn(async move {
                registry.cleanup_client(client).await
            }));
        }
        let mut deactivation_count = 0;
        for task in cleanup_tasks {
            deactivation_count += task.await.unwrap().deactivated.len();
        }

        assert_eq!(deactivation_count, 1);
        assert_eq!(registry.reference_count(&shared).await, 0);
        assert!(registry.desired_snapshot().is_empty());
    }
}
