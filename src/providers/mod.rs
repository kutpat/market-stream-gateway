use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use url::Url;
use uuid::Uuid;

use crate::domain::{Channel, Provider, ProviderEvent, SubscriptionKey};

pub mod binance;
pub mod bingx;
pub mod bybit;
pub mod kucoin;
pub mod mexc;
pub mod okx;

/// Default channel-subscription capacity for one provider.
///
/// This is a local resource guard, not a venue rule. Individual commands are
/// already bounded by each adapter's per-command topic/argument limits and by
/// `ConnectionTarget::max_message_bytes`; this bounds how much total demand one
/// provider may accumulate. Providers whose venue documents a per-connection
/// ceiling override [`ProviderAdapter::max_subscriptions`] with that ceiling.
pub const DEFAULT_MAX_PROVIDER_SUBSCRIPTIONS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EndpointKind {
    Primary,
    Public,
    Business,
}

impl fmt::Display for EndpointKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Primary => "primary",
            Self::Public => "public",
            Self::Business => "business",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionTarget {
    pub url: Url,
    pub command_interval: Duration,
    pub subscription_ack_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub stale_after: Duration,
    pub max_message_bytes: usize,
    pub rotate_after: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Heartbeat {
    Text(String),
    WebSocketPing(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundCommand {
    pub request_id: String,
    pub text: String,
    pub expected_acknowledgements: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedFrame {
    Events(Vec<ProviderEvent>),
    Acknowledgement { request_id: String },
    Reply(Heartbeat),
    Pong,
    Ignored,
}

#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    fn provider(&self) -> Provider;

    fn endpoints(&self) -> &'static [EndpointKind];

    fn endpoint_for(&self, channel: Channel) -> EndpointKind;

    /// Maximum channel subscriptions the gateway will hold for this provider.
    ///
    /// Override this where the venue documents a per-connection ceiling, so the
    /// gateway refuses demand it could not honour rather than letting the venue
    /// silently drop topics. Otherwise the default resource guard applies. A
    /// provider that spreads channels over several endpoints, as OKX does, is
    /// counted conservatively: the limit applies to the provider total rather
    /// than to each of its connections.
    fn max_subscriptions(&self) -> usize {
        DEFAULT_MAX_PROVIDER_SUBSCRIPTIONS
    }

    async fn connection_target(
        &self,
        endpoint: EndpointKind,
        http: &reqwest::Client,
    ) -> Result<ConnectionTarget, AdapterError>;

    fn session(&self, endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession>;
}

pub trait AdapterSession: Send {
    /// Build provider-native subscription commands for one desired-state diff.
    ///
    /// # Errors
    ///
    /// Returns an error when a subscription is invalid for this provider or command encoding
    /// fails.
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError>;

    fn heartbeat(&mut self) -> Heartbeat;

    /// Parse one provider text frame into normalized market data or correlated control state.
    ///
    /// # Errors
    ///
    /// Returns an error when the frame is malformed or the provider rejects a command.
    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError>;
}

#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("unsupported {provider} endpoint {endpoint}")]
    UnsupportedEndpoint {
        provider: Provider,
        endpoint: EndpointKind,
    },
    #[error("invalid subscription for {provider}: {message}")]
    InvalidSubscription { provider: Provider, message: String },
    #[error("{provider} returned invalid data: {message}")]
    InvalidPayload { provider: Provider, message: String },
    #[error("{provider} rejected a command: {message}")]
    CommandRejected { provider: Provider, message: String },
    #[error("could not discover {provider} websocket endpoint: {message}")]
    Discovery { provider: Provider, message: String },
}

pub(crate) fn validate_subscriptions(
    provider: Provider,
    subscriptions: &[SubscriptionKey],
) -> Result<(), AdapterError> {
    if let Some(subscription) = subscriptions
        .iter()
        .find(|subscription| subscription.provider != provider)
    {
        return Err(AdapterError::InvalidSubscription {
            provider,
            message: format!(
                "received {} subscription for {}",
                subscription.provider, subscription.symbol
            ),
        });
    }
    Ok(())
}
