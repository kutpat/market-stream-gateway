use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use url::Url;

use crate::domain::Provider;

#[derive(Debug, Clone, Parser)]
#[command(name = "market-stream-gateway", version, about)]
pub struct Settings {
    #[arg(long, env = "MSG_BIND", default_value = "127.0.0.1:8080")]
    pub bind: SocketAddr,

    #[arg(long, env = "MSG_LOG_FORMAT", value_enum, default_value = "json")]
    pub log_format: LogFormat,

    #[arg(long, env = "MSG_DOWNSTREAM_BUFFER", default_value_t = 4096)]
    pub downstream_buffer: usize,

    #[arg(long, env = "MSG_MAX_DOWNSTREAM_CLIENTS", default_value_t = 64)]
    pub max_downstream_clients: usize,

    #[arg(long, env = "MSG_MAX_CLIENT_SUBSCRIPTIONS", default_value_t = 512)]
    pub max_client_subscriptions: usize,

    /// Optional ceiling applied on top of every provider's declared capacity.
    ///
    /// Unset means each provider uses its own declared capacity. A value only
    /// ever tightens a limit: it cannot raise one past what the provider
    /// declared, because that declaration can be a venue rule.
    #[arg(long, env = "MSG_MAX_PROVIDER_SUBSCRIPTIONS")]
    pub max_provider_subscriptions: Option<usize>,

    #[arg(long, env = "MSG_MAX_COMMAND_BYTES", default_value_t = 65_536)]
    pub max_command_bytes: usize,

    #[arg(long, env = "MSG_MAX_HISTORY_REQUESTS", default_value_t = 8)]
    pub max_history_requests: usize,

    #[arg(
        long,
        env = "MSG_ALLOWED_ORIGINS",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub allowed_origins: Vec<String>,

    #[arg(long, env = "MSG_UPSTREAM_BACKOFF_MIN_MS", default_value_t = 500)]
    pub upstream_backoff_min_ms: u64,

    #[arg(long, env = "MSG_UPSTREAM_BACKOFF_MAX_MS", default_value_t = 30_000)]
    pub upstream_backoff_max_ms: u64,

    #[arg(long, env = "MSG_STABLE_CONNECTION_SECONDS", default_value_t = 60)]
    pub stable_connection_seconds: u64,

    #[arg(long, env = "MSG_SHUTDOWN_GRACE_SECONDS", default_value_t = 10)]
    pub shutdown_grace_seconds: u64,

    #[arg(long, env = "MSG_CATALOG_REFRESH_SECONDS", default_value_t = 21_600)]
    pub catalog_refresh_seconds: u64,

    #[arg(
        long,
        env = "MSG_BYBIT_WS_URL",
        default_value = "wss://stream.bybit.com/v5/public/linear"
    )]
    pub bybit_ws_url: Url,

    #[arg(
        long,
        env = "MSG_BINANCE_WS_URL",
        default_value = "wss://fstream.binance.com/market/ws"
    )]
    pub binance_ws_url: Url,

    #[arg(
        long,
        env = "MSG_OKX_PUBLIC_WS_URL",
        default_value = "wss://ws.okx.com:8443/ws/v5/public"
    )]
    pub okx_public_ws_url: Url,

    #[arg(
        long,
        env = "MSG_OKX_BUSINESS_WS_URL",
        default_value = "wss://ws.okx.com:8443/ws/v5/business"
    )]
    pub okx_business_ws_url: Url,

    #[arg(
        long,
        env = "MSG_MEXC_WS_URL",
        default_value = "wss://contract.mexc.com/edge"
    )]
    pub mexc_ws_url: Url,

    #[arg(
        long,
        env = "MSG_BINGX_WS_URL",
        default_value = "wss://open-api-swap.bingx.com/swap-market"
    )]
    pub bingx_ws_url: Url,

    #[arg(
        long,
        env = "MSG_BYBIT_REST_URL",
        default_value = "https://api.bybit.com/"
    )]
    pub bybit_rest_url: Url,

    #[arg(
        long,
        env = "MSG_BINANCE_FUTURES_REST_URL",
        default_value = "https://fapi.binance.com/"
    )]
    pub binance_futures_rest_url: Url,

    #[arg(long, env = "MSG_OKX_REST_URL", default_value = "https://www.okx.com/")]
    pub okx_rest_url: Url,

    #[arg(
        long,
        env = "MSG_KUCOIN_FUTURES_REST_URL",
        default_value = "https://api-futures.kucoin.com/"
    )]
    pub kucoin_futures_rest_url: Url,

    #[arg(
        long,
        env = "MSG_MEXC_FUTURES_REST_URL",
        default_value = "https://api.mexc.com/"
    )]
    pub mexc_futures_rest_url: Url,

    #[arg(
        long,
        env = "MSG_BINGX_SWAP_REST_URL",
        default_value = "https://open-api.bingx.com/"
    )]
    pub bingx_swap_rest_url: Url,

    #[arg(
        long,
        env = "MSG_PROVIDERS",
        value_delimiter = ',',
        default_value = "all"
    )]
    pub providers: Vec<ProviderSelection>,
}

impl Settings {
    /// Validate limits that Clap cannot express as scalar parsers.
    ///
    /// # Errors
    ///
    /// Returns a message when a buffer or limit is zero, or when the reconnect range is inverted.
    pub fn validate(&self) -> Result<(), String> {
        if self.downstream_buffer == 0 {
            return Err("downstream buffer must be greater than zero".to_owned());
        }
        if self.max_downstream_clients == 0 {
            return Err("max downstream clients must be greater than zero".to_owned());
        }
        if self.max_client_subscriptions == 0 {
            return Err("max client subscriptions must be greater than zero".to_owned());
        }
        if self.max_provider_subscriptions == Some(0) {
            return Err("max provider subscriptions must be greater than zero".to_owned());
        }
        if self.max_command_bytes == 0 {
            return Err("max command bytes must be greater than zero".to_owned());
        }
        if self.max_history_requests == 0 {
            return Err("max history requests must be greater than zero".to_owned());
        }
        if self.upstream_backoff_min_ms == 0
            || self.upstream_backoff_max_ms < self.upstream_backoff_min_ms
        {
            return Err("upstream reconnect backoff range is invalid".to_owned());
        }
        if self.stable_connection_seconds == 0
            || self.shutdown_grace_seconds == 0
            || self.catalog_refresh_seconds == 0
        {
            return Err(
                "connection stability, shutdown, and catalog refresh durations must be positive"
                    .to_owned(),
            );
        }
        for (name, url) in [
            ("Bybit websocket", &self.bybit_ws_url),
            ("Binance websocket", &self.binance_ws_url),
            ("OKX public websocket", &self.okx_public_ws_url),
            ("OKX business websocket", &self.okx_business_ws_url),
            ("MEXC websocket", &self.mexc_ws_url),
            ("BingX websocket", &self.bingx_ws_url),
        ] {
            if !matches!(url.scheme(), "ws" | "wss") {
                return Err(format!("{name} URL must use ws or wss"));
            }
        }
        for (name, url) in [
            ("Bybit REST", &self.bybit_rest_url),
            ("Binance Futures REST", &self.binance_futures_rest_url),
            ("OKX REST", &self.okx_rest_url),
            ("KuCoin Futures REST", &self.kucoin_futures_rest_url),
            ("MEXC Futures REST", &self.mexc_futures_rest_url),
            ("BingX Swap REST", &self.bingx_swap_rest_url),
        ] {
            if !matches!(url.scheme(), "http" | "https") {
                return Err(format!("{name} URL must use http or https"));
            }
            if url.query().is_some() || url.fragment().is_some() || !url.path().ends_with('/') {
                return Err(format!(
                    "{name} URL must be a root URL ending in / without a query or fragment"
                ));
            }
        }
        if self.providers.is_empty() {
            return Err("providers must contain at least one selection".to_owned());
        }
        let has_all = self.providers.contains(&ProviderSelection::All);
        let has_none = self.providers.contains(&ProviderSelection::None);
        if (has_all || has_none) && self.providers.len() != 1 {
            return Err("all and none provider selections cannot be combined".to_owned());
        }
        if self.allowed_origins.iter().any(String::is_empty) {
            return Err("allowed origins must not contain an empty value".to_owned());
        }
        Ok(())
    }

    pub fn backoff_min(&self) -> Duration {
        Duration::from_millis(self.upstream_backoff_min_ms)
    }

    pub fn backoff_max(&self) -> Duration {
        Duration::from_millis(self.upstream_backoff_max_ms)
    }

    pub fn stable_connection_duration(&self) -> Duration {
        Duration::from_secs(self.stable_connection_seconds)
    }

    pub fn shutdown_grace(&self) -> Duration {
        Duration::from_secs(self.shutdown_grace_seconds)
    }

    pub fn catalog_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.catalog_refresh_seconds)
    }

    pub fn enabled_providers(&self) -> BTreeSet<Provider> {
        if self.providers.contains(&ProviderSelection::None) {
            return BTreeSet::new();
        }
        if self.providers.contains(&ProviderSelection::All) {
            return [
                Provider::Bybit,
                Provider::Binance,
                Provider::Okx,
                Provider::Kucoin,
                Provider::Mexc,
                Provider::Bingx,
            ]
            .into_iter()
            .collect();
        }
        self.providers
            .iter()
            .filter_map(|selection| selection.provider())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProviderSelection {
    All,
    None,
    Bybit,
    Binance,
    Okx,
    Kucoin,
    Mexc,
    Bingx,
}

impl ProviderSelection {
    const fn provider(self) -> Option<Provider> {
        match self {
            Self::All | Self::None => None,
            Self::Bybit => Some(Provider::Bybit),
            Self::Binance => Some(Provider::Binance),
            Self::Okx => Some(Provider::Okx),
            Self::Kucoin => Some(Provider::Kucoin),
            Self::Mexc => Some(Provider::Mexc),
            Self::Bingx => Some(Provider::Bingx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe_for_local_use() {
        let settings = Settings::try_parse_from(["gateway"]).unwrap();
        assert_eq!(settings.bind, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(settings.downstream_buffer, 4096);
        // Unset by default: each provider contributes its own declared capacity
        // instead of one global ceiling.
        assert_eq!(settings.max_provider_subscriptions, None);
        assert!(settings.validate().is_ok());
        assert_eq!(settings.binance_ws_url.path(), "/market/ws");
        assert_eq!(settings.enabled_providers().len(), 6);
    }

    #[test]
    fn invalid_limits_are_rejected() {
        let settings = Settings::try_parse_from(["gateway", "--downstream-buffer", "0"]).unwrap();
        assert!(settings.validate().is_err());
        let zero_provider_limit =
            Settings::try_parse_from(["gateway", "--max-provider-subscriptions", "0"]).unwrap();
        assert!(zero_provider_limit.validate().is_err());
    }

    #[test]
    fn provider_subscription_override_is_accepted_above_the_previous_hard_ceiling() {
        // The limit used to be validated against a compile-time constant of 60,
        // which no configuration could exceed. A single Trading Core worker needs
        // more than that on one provider.
        let settings =
            Settings::try_parse_from(["gateway", "--max-provider-subscriptions", "400"]).unwrap();
        assert_eq!(settings.max_provider_subscriptions, Some(400));
        assert!(settings.validate().is_ok());
    }

    #[test]
    fn provider_selection_supports_subsets_and_explicit_none() {
        let subset = Settings::try_parse_from(["gateway", "--providers", "bybit,okx"]).unwrap();
        assert_eq!(
            subset.enabled_providers(),
            BTreeSet::from([Provider::Bybit, Provider::Okx])
        );

        let none = Settings::try_parse_from(["gateway", "--providers", "none"]).unwrap();
        assert!(none.enabled_providers().is_empty());

        let invalid = Settings::try_parse_from(["gateway", "--providers", "all,bybit"]).unwrap();
        assert!(invalid.validate().is_err());
    }
}
