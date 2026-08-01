use std::sync::atomic::AtomicI64;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use prometheus_client::registry::Unit;

use crate::domain::Provider;
use crate::providers::EndpointKind;

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EndpointLabels {
    provider: String,
    endpoint: String,
}

impl EndpointLabels {
    pub fn new(provider: Provider, endpoint: EndpointKind) -> Self {
        Self {
            provider: provider.to_string(),
            endpoint: endpoint.to_string(),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ProviderLabels {
    provider: String,
}

impl ProviderLabels {
    pub fn new(provider: Provider) -> Self {
        Self {
            provider: provider.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Metrics {
    registry: Registry,
    pub upstream_connections: Family<EndpointLabels, Gauge>,
    pub upstream_reconnects: Family<EndpointLabels, Counter>,
    pub upstream_frames: Family<EndpointLabels, Counter>,
    pub normalized_events: Family<EndpointLabels, Counter>,
    pub invalid_frames: Family<EndpointLabels, Counter>,
    pub desired_subscriptions: Family<EndpointLabels, Gauge>,
    pub catalog_refresh_successes: Family<ProviderLabels, Counter>,
    pub catalog_refresh_failures: Family<ProviderLabels, Counter>,
    pub catalog_instruments: Family<ProviderLabels, Gauge>,
    pub history_requests: Family<ProviderLabels, Counter>,
    pub history_failures: Family<ProviderLabels, Counter>,
    pub downstream_connections: Gauge,
    pub downstream_lagged: Counter,
    pub downstream_commands_rejected: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        let mut metrics = Self {
            registry: Registry::default(),
            upstream_connections: Family::default(),
            upstream_reconnects: Family::default(),
            upstream_frames: Family::default(),
            normalized_events: Family::default(),
            invalid_frames: Family::default(),
            desired_subscriptions: Family::default(),
            catalog_refresh_successes: Family::default(),
            catalog_refresh_failures: Family::default(),
            catalog_instruments: Family::default(),
            history_requests: Family::default(),
            history_failures: Family::default(),
            downstream_connections: Gauge::<i64, AtomicI64>::default(),
            downstream_lagged: Counter::default(),
            downstream_commands_rejected: Counter::default(),
        };
        metrics.register_upstream_metrics();
        metrics.register_provider_rest_metrics();
        metrics.register_downstream_metrics();
        metrics
    }

    fn register_upstream_metrics(&mut self) {
        self.registry.register(
            "msg_upstream_connections",
            "Current live upstream websocket connections.",
            self.upstream_connections.clone(),
        );
        self.registry.register(
            "msg_upstream_reconnects_total",
            "Upstream websocket reconnect attempts.",
            self.upstream_reconnects.clone(),
        );
        self.registry.register(
            "msg_upstream_frames_total",
            "Upstream websocket frames received.",
            self.upstream_frames.clone(),
        );
        self.registry.register(
            "msg_normalized_events_total",
            "Normalized events accepted from providers.",
            self.normalized_events.clone(),
        );
        self.registry.register(
            "msg_invalid_frames_total",
            "Provider frames rejected during normalization.",
            self.invalid_frames.clone(),
        );
        self.registry.register(
            "msg_desired_subscriptions",
            "Current globally desired upstream subscriptions.",
            self.desired_subscriptions.clone(),
        );
    }

    fn register_provider_rest_metrics(&mut self) {
        self.registry.register(
            "msg_catalog_refresh_successes_total",
            "Successful provider instrument catalog refreshes.",
            self.catalog_refresh_successes.clone(),
        );
        self.registry.register(
            "msg_catalog_refresh_failures_total",
            "Failed provider instrument catalog refreshes.",
            self.catalog_refresh_failures.clone(),
        );
        self.registry.register(
            "msg_catalog_instruments",
            "Current live instruments in each provider catalog snapshot.",
            self.catalog_instruments.clone(),
        );
        self.registry.register(
            "msg_history_requests_total",
            "Accepted provider candle history requests.",
            self.history_requests.clone(),
        );
        self.registry.register(
            "msg_history_failures_total",
            "Provider candle history requests that failed.",
            self.history_failures.clone(),
        );
    }

    fn register_downstream_metrics(&mut self) {
        self.registry.register(
            "msg_downstream_connections",
            "Current downstream websocket clients.",
            self.downstream_connections.clone(),
        );
        self.registry.register_with_unit(
            "msg_downstream_lagged_total",
            "Downstream clients closed after bounded-buffer lag.",
            Unit::Other("connections".to_owned()),
            self.downstream_lagged.clone(),
        );
        self.registry.register(
            "msg_downstream_commands_rejected_total",
            "Invalid or over-limit downstream commands.",
            self.downstream_commands_rejected.clone(),
        );
    }

    /// Encode all metrics in the Prometheus text exposition format.
    ///
    /// # Errors
    ///
    /// Returns an encoding error if a registered collector produces invalid output.
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut output = String::new();
        encode(&mut output, &self.registry)?;
        Ok(output)
    }

    pub fn increment_connections(&self) {
        self.downstream_connections.inc();
    }

    pub fn decrement_connections(&self) {
        self.downstream_connections.dec();
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_encode_with_bounded_labels() {
        let metrics = Metrics::new();
        let labels = EndpointLabels::new(Provider::Bybit, EndpointKind::Primary);
        metrics.normalized_events.get_or_create(&labels).inc();
        metrics
            .catalog_instruments
            .get_or_create(&ProviderLabels::new(Provider::Bybit))
            .set(42);
        let output = metrics.encode().unwrap();
        assert!(output.contains("msg_normalized_events_total"));
        assert!(output.contains("provider=\"bybit\""));
        assert!(output.contains("endpoint=\"primary\""));
        assert!(output.contains("msg_catalog_instruments"));
    }
}
