use std::fmt;
use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Bybit,
    Binance,
    Okx,
    Kucoin,
    Mexc,
    Bingx,
}

impl fmt::Display for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Bybit => "bybit",
            Self::Binance => "binance",
            Self::Okx => "okx",
            Self::Kucoin => "kucoin",
            Self::Mexc => "mexc",
            Self::Bingx => "bingx",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    LinearPerpetual,
}

impl fmt::Display for MarketKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("linear_perpetual")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Ticker,
    #[serde(rename = "candle_1m")]
    Candle1m,
}

impl fmt::Display for Channel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Ticker => "ticker",
            Self::Candle1m => "candle_1m",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionKey {
    pub provider: Provider,
    pub market: MarketKind,
    pub symbol: String,
    pub channel: Channel,
}

impl SubscriptionKey {
    /// Build a key after normalizing and validating the venue symbol.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidSymbol`] when the symbol is empty, too long, or contains
    /// unsupported characters.
    pub fn new(
        provider: Provider,
        market: MarketKind,
        symbol: impl AsRef<str>,
        channel: Channel,
    ) -> Result<Self, DomainError> {
        Ok(Self {
            provider,
            market,
            symbol: normalize_symbol(symbol.as_ref())?,
            channel,
        })
    }

    pub fn instrument_id(&self) -> String {
        format!("{}:{}:{}", self.provider, self.market, self.symbol)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub provider: Provider,
    pub market: MarketKind,
    pub symbol: String,
    pub channels: Vec<Channel>,
}

impl Subscription {
    /// Expand a client subscription into normalized, unique channel keys.
    ///
    /// # Errors
    ///
    /// Returns an error when no channels were requested or the venue symbol is invalid.
    pub fn into_keys(self) -> Result<Vec<SubscriptionKey>, DomainError> {
        if self.channels.is_empty() {
            return Err(DomainError::NoChannels);
        }
        let symbol = normalize_symbol(&self.symbol)?;
        let mut channels = self.channels;
        channels.sort_unstable();
        channels.dedup();
        Ok(channels
            .into_iter()
            .map(|channel| SubscriptionKey {
                provider: self.provider,
                market: self.market,
                symbol: symbol.clone(),
                channel,
            })
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DecimalValue(String);

impl DecimalValue {
    /// Validate a decimal and retain its original lossless string representation.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidDecimal`] when the value is not a finite decimal.
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        Decimal::from_str(&value).map_err(|_| DomainError::InvalidDecimal(value.clone()))?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DecimalValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<&str> for DecimalValue {
    type Error = DomainError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedDecimal {
    pub value: DecimalValue,
    pub observed_at_ms: u64,
}

impl ObservedDecimal {
    /// Build a timestamped decimal observation.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidDecimal`] when the value is not a finite decimal.
    pub fn new(value: impl Into<String>, observed_at_ms: u64) -> Result<Self, DomainError> {
        Ok(Self {
            value: DecimalValue::new(value)?,
            observed_at_ms,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ticker {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<ObservedDecimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mark: Option<ObservedDecimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<ObservedDecimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bid: Option<ObservedDecimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ask: Option<ObservedDecimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub funding_rate: Option<ObservedDecimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_funding_time_ms: Option<u64>,
}

impl Ticker {
    pub fn has_price(&self) -> bool {
        self.last.is_some()
            || self.mark.is_some()
            || self.index.is_some()
            || self.bid.is_some()
            || self.ask.is_some()
    }

    /// Validate portable price invariants after provider normalization.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive prices, zero observation times, or a crossed best bid
    /// and ask. Funding rates are intentionally allowed to be negative.
    pub fn validate(&self) -> Result<(), DomainError> {
        for (name, observation) in [
            ("last", self.last.as_ref()),
            ("mark", self.mark.as_ref()),
            ("index", self.index.as_ref()),
            ("bid", self.bid.as_ref()),
            ("ask", self.ask.as_ref()),
        ] {
            if let Some(observation) = observation {
                if observation.observed_at_ms == 0 {
                    return Err(DomainError::InvalidMarketData(format!(
                        "{name} observation timestamp must be positive"
                    )));
                }
                if decimal(&observation.value)? <= Decimal::ZERO {
                    return Err(DomainError::InvalidMarketData(format!(
                        "{name} price must be positive"
                    )));
                }
            }
        }
        if let Some(funding) = &self.funding_rate
            && funding.observed_at_ms == 0
        {
            return Err(DomainError::InvalidMarketData(
                "funding observation timestamp must be positive".to_owned(),
            ));
        }
        if let (Some(bid), Some(ask)) = (&self.bid, &self.ask)
            && decimal(&bid.value)? > decimal(&ask.value)?
        {
            return Err(DomainError::InvalidMarketData(
                "best bid must not exceed best ask".to_owned(),
            ));
        }
        if !self.has_price() {
            return Err(DomainError::InvalidMarketData(
                "ticker must contain at least one price".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleFinality {
    Open,
    Closed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataQuality {
    ProviderVolumeUntrusted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candle {
    pub interval: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub open: DecimalValue,
    pub high: DecimalValue,
    pub low: DecimalValue,
    pub close: DecimalValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_volume: Option<DecimalValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_volume: Option<DecimalValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_volume: Option<DecimalValue>,
    pub finality: CandleFinality,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data_quality: Vec<DataQuality>,
}

impl Candle {
    /// Validate one-minute candle price, duration, and quantity invariants.
    ///
    /// # Errors
    ///
    /// Returns an error when prices are non-positive, OHLC bounds conflict, the interval is not
    /// exactly one minute, or a reported volume is negative.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.interval != "1m" || self.end_time_ms.checked_sub(self.start_time_ms) != Some(60_000)
        {
            return Err(DomainError::InvalidMarketData(
                "candle must cover exactly one minute".to_owned(),
            ));
        }
        let open = decimal(&self.open)?;
        let high = decimal(&self.high)?;
        let low = decimal(&self.low)?;
        let close = decimal(&self.close)?;
        if [open, high, low, close]
            .into_iter()
            .any(|value| value <= Decimal::ZERO)
        {
            return Err(DomainError::InvalidMarketData(
                "candle prices must be positive".to_owned(),
            ));
        }
        if high < open || high < close || high < low || low > open || low > close {
            return Err(DomainError::InvalidMarketData(
                "candle OHLC bounds are inconsistent".to_owned(),
            ));
        }
        for volume in [
            self.base_volume.as_ref(),
            self.quote_volume.as_ref(),
            self.contract_volume.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if decimal(volume)? < Decimal::ZERO {
                return Err(DomainError::InvalidMarketData(
                    "candle volume must not be negative".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSequence {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum MarketPayload {
    Ticker(Ticker),
    Candle(Candle),
}

impl MarketPayload {
    pub fn channel(&self) -> Channel {
        match self {
            Self::Ticker(_) => Channel::Ticker,
            Self::Candle(_) => Channel::Candle1m,
        }
    }

    /// Validate common invariants that every provider adapter must preserve.
    ///
    /// # Errors
    ///
    /// Returns an error when the normalized ticker or candle is economically invalid.
    pub fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Ticker(ticker) => ticker.validate(),
            Self::Candle(candle) => candle.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketEvent {
    pub schema_version: u16,
    pub stream_epoch: Uuid,
    pub delivery_sequence: u64,
    pub connection_epoch: Uuid,
    pub instrument_id: String,
    pub provider: Provider,
    pub market: MarketKind,
    pub symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange_time_ms: Option<u64>,
    pub gateway_received_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_sequence: Option<SourceSequence>,
    #[serde(flatten)]
    pub payload: MarketPayload,
}

impl MarketEvent {
    pub fn subscription_key(&self) -> SubscriptionKey {
        SubscriptionKey {
            provider: self.provider,
            market: self.market,
            symbol: self.symbol.clone(),
            channel: self.payload.channel(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderEvent {
    pub connection_epoch: Uuid,
    pub provider: Provider,
    pub market: MarketKind,
    pub symbol: String,
    pub exchange_time_ms: Option<u64>,
    pub gateway_received_time_ms: u64,
    pub source_sequence: Option<SourceSequence>,
    pub payload: MarketPayload,
}

impl ProviderEvent {
    pub fn into_market_event(self, stream_epoch: Uuid, delivery_sequence: u64) -> MarketEvent {
        let instrument_id = format!("{}:{}:{}", self.provider, self.market, self.symbol);
        MarketEvent {
            schema_version: SCHEMA_VERSION,
            stream_epoch,
            delivery_sequence,
            connection_epoch: self.connection_epoch,
            instrument_id,
            provider: self.provider,
            market: self.market,
            symbol: self.symbol,
            exchange_time_ms: self.exchange_time_ms,
            gateway_received_time_ms: self.gateway_received_time_ms,
            source_sequence: self.source_sequence,
            payload: self.payload,
        }
    }

    /// Validate normalized payload and timestamp invariants before fan-out.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider event has no receive time or its payload is invalid.
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.gateway_received_time_ms == 0 {
            return Err(DomainError::InvalidMarketData(
                "gateway receive timestamp must be positive".to_owned(),
            ));
        }
        self.payload.validate()
    }
}

fn decimal(value: &DecimalValue) -> Result<Decimal, DomainError> {
    Decimal::from_str(value.as_str())
        .map_err(|_| DomainError::InvalidDecimal(value.as_str().to_owned()))
}

/// Normalize a venue symbol for identity and subscription matching.
///
/// # Errors
///
/// Returns [`DomainError::InvalidSymbol`] when the symbol is empty, longer than 64 UTF-8 bytes,
/// or contains characters outside the supported venue-symbol alphabet. Unicode letters and
/// numbers are accepted because some venues list contracts with non-ASCII base assets.
pub fn normalize_symbol(symbol: &str) -> Result<String, DomainError> {
    let symbol = symbol.trim();
    if symbol.is_empty() || symbol.len() > 64 {
        return Err(DomainError::InvalidSymbol(symbol.to_owned()));
    }
    if !symbol
        .chars()
        .all(|character| character.is_alphanumeric() || matches!(character, '-' | '_' | '.'))
    {
        return Err(DomainError::InvalidSymbol(symbol.to_owned()));
    }
    Ok(symbol.to_ascii_uppercase())
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid venue symbol: {0}")]
    InvalidSymbol(String),
    #[error("subscription must contain at least one channel")]
    NoChannels,
    #[error("invalid decimal value: {0}")]
    InvalidDecimal(String),
    #[error("invalid normalized market data: {0}")]
    InvalidMarketData(String),
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn subscription_keys_are_normalized_and_deduplicated() {
        let keys = Subscription {
            provider: Provider::Okx,
            market: MarketKind::LinearPerpetual,
            symbol: " btc-usdt-swap ".to_owned(),
            channels: vec![Channel::Ticker, Channel::Ticker, Channel::Candle1m],
        }
        .into_keys()
        .unwrap();

        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].symbol, "BTC-USDT-SWAP");
        assert_eq!(
            keys[0].instrument_id(),
            "okx:linear_perpetual:BTC-USDT-SWAP"
        );
    }

    #[test]
    fn symbols_are_strictly_validated() {
        assert_eq!(normalize_symbol("ethusdt").unwrap(), "ETHUSDT");
        assert_eq!(normalize_symbol("币安人生usdt").unwrap(), "币安人生USDT");
        assert!(matches!(
            normalize_symbol("BTC/USDT"),
            Err(DomainError::InvalidSymbol(_))
        ));
    }

    #[test]
    fn one_minute_candle_channel_has_stable_wire_name() {
        assert_eq!(
            serde_json::to_value(Channel::Candle1m).unwrap(),
            json!("candle_1m")
        );
        assert_eq!(
            serde_json::from_value::<Channel>(json!("candle_1m")).unwrap(),
            Channel::Candle1m
        );
        assert!(serde_json::from_value::<Channel>(json!("candle1m")).is_err());
    }

    #[test]
    fn decimal_values_remain_json_strings() {
        let value = DecimalValue::new("0.0000000100").unwrap();
        assert_eq!(serde_json::to_value(value).unwrap(), json!("0.0000000100"));
        assert!(DecimalValue::new("NaN").is_err());
    }

    #[test]
    fn event_contract_is_provider_neutral() {
        let event = MarketEvent {
            schema_version: SCHEMA_VERSION,
            stream_epoch: Uuid::nil(),
            delivery_sequence: 7,
            connection_epoch: Uuid::nil(),
            instrument_id: "binance:linear_perpetual:BTCUSDT".to_owned(),
            provider: Provider::Binance,
            market: MarketKind::LinearPerpetual,
            symbol: "BTCUSDT".to_owned(),
            exchange_time_ms: Some(1_700_000_000_001),
            gateway_received_time_ms: 1_700_000_000_002,
            source_sequence: None,
            payload: MarketPayload::Ticker(Ticker {
                last: Some(ObservedDecimal::new("42000.10", 1_700_000_000_001).unwrap()),
                mark: Some(ObservedDecimal::new("41999.90", 1_700_000_000_000).unwrap()),
                ..Ticker::default()
            }),
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["type"], "ticker");
        assert_eq!(json["provider"], "binance");
        assert_eq!(json["data"]["last"]["value"], "42000.10");
        assert!(json["data"].get("bid").is_none());
    }

    #[test]
    fn normalized_market_invariants_reject_bad_prices_and_candles() {
        let crossed = Ticker {
            bid: Some(ObservedDecimal::new("2", 1).unwrap()),
            ask: Some(ObservedDecimal::new("1", 1).unwrap()),
            ..Ticker::default()
        };
        assert!(crossed.validate().is_err());

        let candle = Candle {
            interval: "1m".to_owned(),
            start_time_ms: 60_000,
            end_time_ms: 120_000,
            open: DecimalValue::new("10").unwrap(),
            high: DecimalValue::new("9").unwrap(),
            low: DecimalValue::new("8").unwrap(),
            close: DecimalValue::new("8.5").unwrap(),
            base_volume: None,
            quote_volume: None,
            contract_volume: None,
            finality: CandleFinality::Closed,
            data_quality: Vec::new(),
        };
        assert!(candle.validate().is_err());
    }
}
