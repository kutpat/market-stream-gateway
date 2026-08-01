use std::collections::BTreeMap;
use std::sync::RwLock;

use serde::Serialize;
use uuid::Uuid;

use crate::domain::Provider;
use crate::providers::EndpointKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Idle,
    Connecting,
    Live,
    Backoff,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct EndpointHealth {
    pub provider: Provider,
    pub endpoint: String,
    pub state: ConnectionState,
    pub desired_subscriptions: usize,
    pub reconnects: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_epoch: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_event_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl EndpointHealth {
    fn new(provider: Provider, endpoint: EndpointKind) -> Self {
        Self {
            provider,
            endpoint: endpoint.to_string(),
            state: ConnectionState::Idle,
            desired_subscriptions: 0,
            reconnects: 0,
            connection_epoch: None,
            connected_at_ms: None,
            last_event_at_ms: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ReadinessSnapshot {
    pub ready: bool,
    pub status: &'static str,
    pub endpoints: Vec<EndpointHealth>,
}

#[derive(Debug, Default)]
pub struct HealthRegistry {
    endpoints: RwLock<BTreeMap<(Provider, EndpointKind), EndpointHealth>>,
}

impl HealthRegistry {
    pub fn register(&self, provider: Provider, endpoint: EndpointKind) {
        self.with_write(|endpoints| {
            endpoints
                .entry((provider, endpoint))
                .or_insert_with(|| EndpointHealth::new(provider, endpoint));
        });
    }

    pub fn update<F>(&self, provider: Provider, endpoint: EndpointKind, update: F)
    where
        F: FnOnce(&mut EndpointHealth),
    {
        self.with_write(|endpoints| {
            let health = endpoints
                .entry((provider, endpoint))
                .or_insert_with(|| EndpointHealth::new(provider, endpoint));
            update(health);
            if let Some(error) = &mut health.last_error {
                error.truncate(500);
            }
        });
    }

    pub fn readiness(&self) -> ReadinessSnapshot {
        let endpoints = self.with_read(|values| values.values().cloned().collect::<Vec<_>>());
        let ready = endpoints.iter().all(|endpoint| {
            endpoint.desired_subscriptions == 0 || endpoint.state == ConnectionState::Live
        });
        ReadinessSnapshot {
            ready,
            status: if ready { "ready" } else { "degraded" },
            endpoints,
        }
    }

    fn with_read<T>(
        &self,
        read: impl FnOnce(&BTreeMap<(Provider, EndpointKind), EndpointHealth>) -> T,
    ) -> T {
        match self.endpoints.read() {
            Ok(guard) => read(&guard),
            Err(poisoned) => read(&poisoned.into_inner()),
        }
    }

    fn with_write<T>(
        &self,
        write: impl FnOnce(&mut BTreeMap<(Provider, EndpointKind), EndpointHealth>) -> T,
    ) -> T {
        match self.endpoints.write() {
            Ok(mut guard) => write(&mut guard),
            Err(poisoned) => write(&mut poisoned.into_inner()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_endpoints_are_ready_but_demand_requires_live_connection() {
        let health = HealthRegistry::default();
        health.register(Provider::Bybit, EndpointKind::Primary);
        assert!(health.readiness().ready);

        health.update(Provider::Bybit, EndpointKind::Primary, |entry| {
            entry.desired_subscriptions = 1;
            entry.state = ConnectionState::Connecting;
        });
        assert!(!health.readiness().ready);

        health.update(Provider::Bybit, EndpointKind::Primary, |entry| {
            entry.state = ConnectionState::Live;
        });
        assert!(health.readiness().ready);
    }

    #[test]
    fn errors_are_bounded() {
        let health = HealthRegistry::default();
        health.update(Provider::Okx, EndpointKind::Public, |entry| {
            entry.last_error = Some("x".repeat(1000));
        });
        assert_eq!(
            health.readiness().endpoints[0]
                .last_error
                .as_ref()
                .unwrap()
                .len(),
            500
        );
    }
}
