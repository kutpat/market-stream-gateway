use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use url::Url;
use uuid::Uuid;

use super::{
    AdapterError, AdapterSession, ConnectionTarget, EndpointKind, Heartbeat, OutboundCommand,
    ParsedFrame, ProviderAdapter, SubscriptionAction, validate_subscriptions,
};
use crate::domain::{
    Candle, CandleFinality, Channel, DecimalValue, MarketKind, MarketPayload, ObservedDecimal,
    Provider, ProviderEvent, SubscriptionKey, Ticker, normalize_symbol,
};

const PRODUCTION_URL: &str = "wss://contract.mexc.com/edge";
// MEXC does not publish a Futures WebSocket command-rate limit. Four commands per second is a
// deliberately conservative default and remains configurable through an explicit adapter URL.
const COMMAND_INTERVAL: Duration = Duration::from_millis(250);
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const STALE_AFTER: Duration = Duration::from_secs(45);
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const ENDPOINTS: &[EndpointKind] = &[EndpointKind::Primary];

/// Adapter for MEXC public USDT-margined perpetual market streams.
#[derive(Debug, Clone)]
pub struct MexcAdapter {
    url: Url,
}

impl Default for MexcAdapter {
    fn default() -> Self {
        Self {
            url: Url::parse(PRODUCTION_URL).expect("hard-coded MEXC URL must be valid"),
        }
    }
}

impl MexcAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_url(url: Url) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ProviderAdapter for MexcAdapter {
    fn provider(&self) -> Provider {
        Provider::Mexc
    }

    fn endpoints(&self) -> &'static [EndpointKind] {
        ENDPOINTS
    }

    fn endpoint_for(&self, _channel: Channel) -> EndpointKind {
        EndpointKind::Primary
    }

    async fn connection_target(
        &self,
        endpoint: EndpointKind,
        _http: &reqwest::Client,
    ) -> Result<ConnectionTarget, AdapterError> {
        if endpoint != EndpointKind::Primary {
            return Err(AdapterError::UnsupportedEndpoint {
                provider: Provider::Mexc,
                endpoint,
            });
        }
        Ok(ConnectionTarget {
            url: self.url.clone(),
            command_interval: COMMAND_INTERVAL,
            subscription_ack_timeout: SUBSCRIPTION_ACK_TIMEOUT,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            stale_after: STALE_AFTER,
            max_message_bytes: MAX_MESSAGE_BYTES,
            // The current MEXC Futures documentation specifies idle timeout but no maximum
            // connection lifetime.
            rotate_after: None,
        })
    }

    fn session(&self, endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession> {
        Box::new(MexcSession {
            endpoint,
            connection_epoch,
            next_request_id: 1,
            pending_acknowledgements: VecDeque::new(),
            tickers: HashMap::new(),
        })
    }
}

#[derive(Debug)]
struct MexcSession {
    endpoint: EndpointKind,
    connection_epoch: Uuid,
    next_request_id: u64,
    // MEXC acknowledgement frames carry neither a client id nor the symbol. Commands are emitted
    // and acknowledged in order, so retain the expected response channel alongside our internal
    // request id and reject any sequence mismatch.
    pending_acknowledgements: VecDeque<PendingAcknowledgement>,
    tickers: HashMap<String, Ticker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingAcknowledgement {
    response_channel: String,
    request_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NativeSubscription {
    method: &'static str,
    symbol: String,
    interval: Option<&'static str>,
}

impl AdapterSession for MexcSession {
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        validate_subscriptions(Provider::Mexc, subscriptions)?;
        if self.endpoint != EndpointKind::Primary {
            return Err(AdapterError::InvalidSubscription {
                provider: Provider::Mexc,
                message: format!(
                    "subscriptions do not belong on the {} endpoint",
                    self.endpoint
                ),
            });
        }

        let mut seen = HashSet::new();
        let mut native_subscriptions = Vec::new();
        for subscription in subscriptions {
            validate_mexc_symbol(&subscription.symbol).map_err(|message| {
                AdapterError::InvalidSubscription {
                    provider: Provider::Mexc,
                    message,
                }
            })?;
            let methods: &[(&str, Option<&str>)] = match subscription.channel {
                // The ticker includes last/mark/index/BBO/funding. The separate funding stream is
                // also needed because it is the only public stream carrying nextSettleTime.
                Channel::Ticker => &[("ticker", None), ("funding.rate", None)],
                Channel::Candle1m => &[("kline", Some("Min1"))],
            };
            for &(method, interval) in methods {
                let native = NativeSubscription {
                    method,
                    symbol: subscription.symbol.clone(),
                    interval,
                };
                if seen.insert(native.clone()) {
                    native_subscriptions.push(native);
                }
            }
            if action == SubscriptionAction::Unsubscribe && subscription.channel == Channel::Ticker
            {
                self.tickers.remove(&subscription.symbol);
            }
        }

        native_subscriptions
            .into_iter()
            .map(|subscription| self.command(action, subscription))
            .collect()
    }

    fn heartbeat(&mut self) -> Heartbeat {
        Heartbeat::Text(json!({ "method": "ping" }).to_string())
    }

    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| invalid(format!("invalid JSON: {error}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("message must be a JSON object"))?;
        let channel = required_string(object, "channel")?;

        if channel == "pong" {
            return Ok(ParsedFrame::Pong);
        }
        if channel == "rs.error" {
            self.pending_acknowledgements.pop_front();
            return Err(AdapterError::CommandRejected {
                provider: Provider::Mexc,
                message: object
                    .get("data")
                    .map_or_else(|| "unknown command error".to_owned(), display_value),
            });
        }
        if channel.starts_with("rs.sub.") {
            return self.parse_acknowledgement(channel, object);
        }
        if channel.starts_with("rs.unsub.") {
            // The public API does not document unsubscribe acknowledgements and the live service
            // currently emits none. Ignore one if a future server version starts sending it.
            return Ok(ParsedFrame::Ignored);
        }

        match channel {
            "push.ticker" => self.parse_ticker(object, text, received_at_ms),
            "push.funding.rate" => self.parse_funding(object, text, received_at_ms),
            "push.kline" => self.parse_candle(object, text, received_at_ms),
            _ => Ok(ParsedFrame::Ignored),
        }
    }
}

impl MexcSession {
    fn take_request_id(&mut self) -> String {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        format!("mexc-{id}")
    }

    fn command(
        &mut self,
        action: SubscriptionAction,
        subscription: NativeSubscription,
    ) -> Result<OutboundCommand, AdapterError> {
        let operation = match action {
            SubscriptionAction::Subscribe => "sub",
            SubscriptionAction::Unsubscribe => "unsub",
        };
        let method = format!("{operation}.{}", subscription.method);
        let response_channel = format!("rs.{method}");
        let request_id = self.take_request_id();
        let mut parameter = Map::new();
        parameter.insert("symbol".to_owned(), Value::String(subscription.symbol));
        if let Some(interval) = subscription.interval
            && action == SubscriptionAction::Subscribe
        {
            parameter.insert("interval".to_owned(), Value::String(interval.to_owned()));
        }
        let text = serde_json::to_string(&json!({
            "method": &method,
            "param": parameter,
            "gzip": false,
        }))
        .map_err(|error| invalid(format!("could not encode command: {error}")))?;

        let expected_acknowledgements = usize::from(action == SubscriptionAction::Subscribe);
        if expected_acknowledgements == 1 {
            self.pending_acknowledgements
                .push_back(PendingAcknowledgement {
                    response_channel,
                    request_id: request_id.clone(),
                });
        }
        Ok(OutboundCommand {
            request_id,
            text,
            expected_acknowledgements,
        })
    }

    fn parse_acknowledgement(
        &mut self,
        channel: &str,
        object: &Map<String, Value>,
    ) -> Result<ParsedFrame, AdapterError> {
        let result = required_string(object, "data")?;
        if result != "success" {
            self.pending_acknowledgements.pop_front();
            return Err(AdapterError::CommandRejected {
                provider: Provider::Mexc,
                message: format!("{channel}: {result}"),
            });
        }
        let pending = self
            .pending_acknowledgements
            .pop_front()
            .ok_or_else(|| invalid(format!("unexpected acknowledgement {channel}")))?;
        if pending.response_channel != channel {
            return Err(invalid(format!(
                "expected acknowledgement {}, got {channel}",
                pending.response_channel
            )));
        }
        Ok(ParsedFrame::Acknowledgement {
            request_id: pending.request_id,
        })
    }

    fn parse_ticker(
        &mut self,
        object: &Map<String, Value>,
        original_json: &str,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let outer_symbol = required_string(object, "symbol")?;
        let data = required_object(object, "data")?;
        let symbol = checked_payload_symbol(data, outer_symbol)?;
        let observed_at_ms = required_u64(data, "timestamp")?;
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();

        update_observation(
            &mut ticker.last,
            data,
            "lastPrice",
            observed_at_ms,
            original_json,
        )?;
        update_observation(
            &mut ticker.mark,
            data,
            "fairPrice",
            observed_at_ms,
            original_json,
        )?;
        update_observation(
            &mut ticker.index,
            data,
            "indexPrice",
            observed_at_ms,
            original_json,
        )?;
        update_observation(&mut ticker.bid, data, "bid1", observed_at_ms, original_json)?;
        update_observation(&mut ticker.ask, data, "ask1", observed_at_ms, original_json)?;
        update_observation(
            &mut ticker.funding_rate,
            data,
            "fundingRate",
            observed_at_ms,
            original_json,
        )?;
        if !ticker.has_price() {
            return Err(invalid("ticker contains no usable price"));
        }
        self.tickers.insert(symbol.clone(), ticker.clone());
        Ok(ParsedFrame::Events(vec![self.event(
            symbol,
            Some(observed_at_ms),
            received_at_ms,
            MarketPayload::Ticker(ticker),
        )]))
    }

    fn parse_funding(
        &mut self,
        object: &Map<String, Value>,
        original_json: &str,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let outer_symbol = required_string(object, "symbol")?;
        let data = required_object(object, "data")?;
        let symbol = checked_payload_symbol(data, outer_symbol)?;
        let observed_at_ms = required_u64(object, "ts")?;
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        // The current field table says fundingRate while its sample and the live service use
        // rate. Accept both spellings, preferring the observed wire contract.
        let (rate_field, rate) = data
            .get("rate")
            .map(|value| ("rate", value))
            .or_else(|| data.get("fundingRate").map(|value| ("fundingRate", value)))
            .ok_or_else(|| invalid("missing rate or fundingRate"))?;
        ticker.funding_rate = Some(observation(
            rate,
            rate_field,
            observed_at_ms,
            original_json,
        )?);
        ticker.next_funding_time_ms = Some(required_u64(data, "nextSettleTime")?);
        self.tickers.insert(symbol.clone(), ticker.clone());

        // A funding update may arrive before the first price-bearing ticker. Cache it but do not
        // emit an invalid normalized ticker with no price.
        if !ticker.has_price() {
            return Ok(ParsedFrame::Events(Vec::new()));
        }
        Ok(ParsedFrame::Events(vec![self.event(
            symbol,
            Some(observed_at_ms),
            received_at_ms,
            MarketPayload::Ticker(ticker),
        )]))
    }

    fn parse_candle(
        &self,
        object: &Map<String, Value>,
        original_json: &str,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let outer_symbol = required_string(object, "symbol")?;
        let data = required_object(object, "data")?;
        let symbol = checked_payload_symbol(data, outer_symbol)?;
        if required_string(data, "interval")? != "Min1" {
            return Err(invalid("expected Min1 candle interval"));
        }
        let start_time_ms = required_u64(data, "t")?
            .checked_mul(1_000)
            .ok_or_else(|| invalid("candle start time overflow"))?;
        let end_time_ms = start_time_ms
            .checked_add(60_000)
            .ok_or_else(|| invalid("candle end time overflow"))?;
        let exchange_time_ms = optional_u64(object, "ts")?;
        let candle = Candle {
            interval: "1m".to_owned(),
            start_time_ms,
            end_time_ms,
            open: decimal(required_value(data, "o")?, "o", original_json)?,
            high: decimal(required_value(data, "h")?, "h", original_json)?,
            low: decimal(required_value(data, "l")?, "l", original_json)?,
            close: decimal(required_value(data, "c")?, "c", original_json)?,
            // q is contract count and a is quote turnover. Base volume requires the catalog's
            // per-instrument contractSize and cannot be derived safely inside this WS session.
            base_volume: None,
            quote_volume: optional_decimal(data, "a", original_json)?,
            contract_volume: optional_decimal(data, "q", original_json)?,
            // MEXC carries no candle-close flag. Consumers must treat stream finality as unknown
            // and use time/history reconciliation before close-only calculations.
            finality: CandleFinality::Unknown,
            data_quality: Vec::new(),
        };
        Ok(ParsedFrame::Events(vec![self.event(
            symbol,
            exchange_time_ms,
            received_at_ms,
            MarketPayload::Candle(candle),
        )]))
    }

    fn event(
        &self,
        symbol: String,
        exchange_time_ms: Option<u64>,
        received_at_ms: u64,
        payload: MarketPayload,
    ) -> ProviderEvent {
        ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Mexc,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms,
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload,
        }
    }
}

fn checked_payload_symbol(
    data: &Map<String, Value>,
    expected_symbol: &str,
) -> Result<String, AdapterError> {
    validate_mexc_symbol(expected_symbol).map_err(invalid)?;
    let symbol = required_string(data, "symbol")?;
    if symbol != expected_symbol {
        return Err(invalid(format!(
            "payload symbol {symbol} does not match envelope symbol {expected_symbol}"
        )));
    }
    normalize_symbol(symbol).map_err(|error| invalid(error.to_string()))
}

fn update_observation(
    target: &mut Option<ObservedDecimal>,
    object: &Map<String, Value>,
    field: &str,
    observed_at_ms: u64,
    original_json: &str,
) -> Result<(), AdapterError> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    if value.is_null() || value.as_str() == Some("") {
        return Ok(());
    }
    *target = Some(observation(value, field, observed_at_ms, original_json)?);
    Ok(())
}

fn observation(
    value: &Value,
    field: &str,
    observed_at_ms: u64,
    original_json: &str,
) -> Result<ObservedDecimal, AdapterError> {
    ObservedDecimal::new(decimal_string(value, field, original_json)?, observed_at_ms)
        .map_err(|error| invalid(error.to_string()))
}

fn optional_decimal(
    object: &Map<String, Value>,
    field: &str,
    original_json: &str,
) -> Result<Option<DecimalValue>, AdapterError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(value) => decimal(value, field, original_json).map(Some),
    }
}

fn decimal(value: &Value, field: &str, original_json: &str) -> Result<DecimalValue, AdapterError> {
    let raw = decimal_string(value, field, original_json)?;
    let normalized = if raw.contains(['e', 'E']) {
        Decimal::from_scientific(&raw)
            .map(|parsed| parsed.to_string())
            .map_err(|_| invalid(format!("{field} is not a decimal")))?
    } else {
        raw
    };
    DecimalValue::new(normalized).map_err(|error| invalid(error.to_string()))
}

fn decimal_string(value: &Value, field: &str, original_json: &str) -> Result<String, AdapterError> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(value.clone()),
        Value::Number(_) => raw_number_field(original_json, field)
            .map(str::to_owned)
            .or_else(|| Some(value.to_string()))
            .ok_or_else(|| invalid(format!("could not read {field}"))),
        _ => Err(invalid(format!(
            "{field} must be a decimal string or number"
        ))),
    }
}

fn required_object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Map<String, Value>, AdapterError> {
    object
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("{field} must be an object")))
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, AdapterError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid(format!("{field} must be a non-empty string")))
}

fn required_value<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a Value, AdapterError> {
    object
        .get(field)
        .ok_or_else(|| invalid(format!("missing {field}")))
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, AdapterError> {
    let value = required_value(object, field)?;
    match value {
        Value::Number(value) => value
            .as_u64()
            .ok_or_else(|| invalid(format!("{field} must be an unsigned integer"))),
        Value::String(value)
            if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            value
                .parse()
                .map_err(|_| invalid(format!("{field} must be an unsigned integer")))
        }
        _ => Err(invalid(format!("{field} must be an unsigned integer"))),
    }
}

fn optional_u64(object: &Map<String, Value>, field: &str) -> Result<Option<u64>, AdapterError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => required_u64(object, field).map(Some),
    }
}

fn raw_number_field<'a>(text: &'a str, field: &str) -> Option<&'a str> {
    let bytes = text.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b'"' {
            cursor += 1;
            continue;
        }
        let key_start = cursor + 1;
        cursor = key_start;
        let mut escaped = false;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'\\' => {
                    escaped = true;
                    cursor = cursor.saturating_add(2);
                }
                b'"' => break,
                _ => cursor += 1,
            }
        }
        if cursor >= bytes.len() {
            return None;
        }
        let is_field = !escaped && text.get(key_start..cursor) == Some(field);
        cursor += 1;
        if !is_field {
            continue;
        }
        cursor = skip_ascii_whitespace(bytes, cursor);
        if bytes.get(cursor) != Some(&b':') {
            continue;
        }
        cursor = skip_ascii_whitespace(bytes, cursor + 1);
        let number_start = cursor;
        while bytes.get(cursor).is_some_and(|byte| {
            byte.is_ascii_digit() || matches!(*byte, b'-' | b'+' | b'.' | b'e' | b'E')
        }) {
            cursor += 1;
        }
        if cursor > number_start {
            return text.get(number_start..cursor);
        }
    }
    None
}

fn skip_ascii_whitespace(bytes: &[u8], mut cursor: usize) -> usize {
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor += 1;
    }
    cursor
}

fn display_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}

fn validate_mexc_symbol(symbol: &str) -> Result<(), String> {
    let Some(base) = symbol.strip_suffix("_USDT") else {
        return Err(format!(
            "{symbol} is not a MEXC USDT-margined perpetual symbol"
        ));
    };
    if base.is_empty() || !base.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(format!("{symbol} has invalid MEXC symbol syntax"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidPayload {
        provider: Provider::Mexc,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn session() -> MexcSession {
        MexcSession {
            endpoint: EndpointKind::Primary,
            connection_epoch: Uuid::nil(),
            next_request_id: 1,
            pending_acknowledgements: VecDeque::new(),
            tickers: HashMap::new(),
        }
    }

    fn subscription(symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(Provider::Mexc, MarketKind::LinearPerpetual, symbol, channel).unwrap()
    }

    #[tokio::test]
    async fn target_and_heartbeat_match_mexc_contract() {
        let adapter = MexcAdapter::new();
        let target = adapter
            .connection_target(EndpointKind::Primary, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(target.url.as_str(), PRODUCTION_URL);
        assert_eq!(target.command_interval, Duration::from_millis(250));
        assert_eq!(target.subscription_ack_timeout, Duration::from_secs(15));
        assert_eq!(target.heartbeat_interval, Duration::from_secs(15));
        assert_eq!(target.stale_after, Duration::from_secs(45));
        assert_eq!(target.rotate_after, None);
        assert_eq!(
            session().heartbeat(),
            Heartbeat::Text("{\"method\":\"ping\"}".to_owned())
        );
    }

    #[test]
    fn subscriptions_expand_deduplicate_and_model_asymmetric_acks() {
        let mut session = session();
        let subscriptions = [
            subscription("FET_USDT", Channel::Ticker),
            subscription("FET_USDT", Channel::Ticker),
            subscription("FET_USDT", Channel::Candle1m),
        ];
        let commands = session
            .subscription_messages(SubscriptionAction::Subscribe, &subscriptions)
            .unwrap();
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0].request_id, "mexc-1");
        assert_eq!(commands[0].expected_acknowledgements, 1);
        assert_eq!(
            serde_json::from_str::<Value>(&commands[0].text).unwrap(),
            json!({
                "method": "sub.ticker",
                "param": {"symbol": "FET_USDT"},
                "gzip": false
            })
        );
        assert_eq!(
            serde_json::from_str::<Value>(&commands[1].text).unwrap()["method"],
            "sub.funding.rate"
        );
        assert_eq!(
            serde_json::from_str::<Value>(&commands[2].text).unwrap(),
            json!({
                "method": "sub.kline",
                "param": {"symbol": "FET_USDT", "interval": "Min1"},
                "gzip": false
            })
        );

        let commands = session
            .subscription_messages(SubscriptionAction::Unsubscribe, &subscriptions)
            .unwrap();
        assert_eq!(commands.len(), 3);
        assert!(
            commands
                .iter()
                .all(|command| command.expected_acknowledgements == 0)
        );
        let kline = serde_json::from_str::<Value>(&commands[2].text).unwrap();
        assert_eq!(kline["method"], "unsub.kline");
        assert!(kline["param"].get("interval").is_none());
    }

    #[test]
    fn acknowledgement_fifo_restores_internal_request_ids() {
        let mut adapter_session = session();
        adapter_session
            .subscription_messages(
                SubscriptionAction::Subscribe,
                &[subscription("FET_USDT", Channel::Ticker)],
            )
            .unwrap();
        assert_eq!(
            adapter_session
                .parse(
                    r#"{"channel":"rs.sub.ticker","data":"success","ts":1785558226831}"#,
                    1
                )
                .unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "mexc-1".to_owned()
            }
        );
        assert_eq!(
            adapter_session
                .parse(
                    r#"{"channel":"rs.sub.funding.rate","data":"success","ts":1785558227344}"#,
                    1
                )
                .unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "mexc-2".to_owned()
            }
        );

        let mut mismatched = session();
        mismatched
            .subscription_messages(
                SubscriptionAction::Subscribe,
                &[subscription("FET_USDT", Channel::Candle1m)],
            )
            .unwrap();
        assert!(matches!(
            mismatched.parse(r#"{"channel":"rs.sub.ticker","data":"success","ts":1}"#, 1),
            Err(AdapterError::InvalidPayload { .. })
        ));
    }

    #[test]
    fn live_ticker_shape_normalizes_all_price_fields() {
        let frame = r#"{
            "symbol":"FET_USDT",
            "data":{"symbol":"FET_USDT","lastPrice":0.1419,"fairPrice":0.1418,
            "indexPrice":0.1420,"timestamp":1785558225941,"bid1":0.1418,
            "ask1":0.1419,"fundingRate":0.000062},
            "channel":"push.ticker","ts":1785558225941
        }"#;
        let ParsedFrame::Events(events) = session().parse(frame, 1_785_558_225_950).unwrap() else {
            panic!("expected ticker event");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].provider, Provider::Mexc);
        assert_eq!(events[0].symbol, "FET_USDT");
        assert_eq!(events[0].exchange_time_ms, Some(1_785_558_225_941));
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(ticker.last.as_ref().unwrap().value.as_str(), "0.1419");
        assert_eq!(ticker.mark.as_ref().unwrap().value.as_str(), "0.1418");
        assert_eq!(ticker.index.as_ref().unwrap().value.as_str(), "0.1420");
        assert_eq!(ticker.bid.as_ref().unwrap().value.as_str(), "0.1418");
        assert_eq!(ticker.ask.as_ref().unwrap().value.as_str(), "0.1419");
        assert_eq!(
            ticker.funding_rate.as_ref().unwrap().value.as_str(),
            "0.000062"
        );
        assert_eq!(
            ticker.mark.as_ref().unwrap().observed_at_ms,
            1_785_558_225_941
        );
    }

    #[test]
    fn funding_updates_merge_with_ticker_and_preserve_next_settlement() {
        let mut session = session();
        let funding = r#"{"symbol":"FET_USDT","data":{"symbol":"FET_USDT",
            "rate":0.000064,"nextSettleTime":1785571200000},
            "channel":"push.funding.rate","ts":1785558240772}"#;
        let ParsedFrame::Events(events) = session.parse(funding, 1).unwrap() else {
            panic!("expected an empty event batch");
        };
        assert!(events.is_empty());

        let ticker = r#"{"symbol":"FET_USDT","data":{"symbol":"FET_USDT",
            "lastPrice":0.1420,"fairPrice":0.1419,"indexPrice":0.1420,
            "timestamp":1785558241000,"bid1":0.1419,"ask1":0.1420},
            "channel":"push.ticker","ts":1785558241000}"#;
        let ParsedFrame::Events(events) = session.parse(ticker, 2).unwrap() else {
            panic!("expected ticker event");
        };
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(ticker.next_funding_time_ms, Some(1_785_571_200_000));
        assert_eq!(
            ticker.funding_rate.as_ref().unwrap().value.as_str(),
            "0.000064"
        );
        assert_eq!(
            ticker.funding_rate.as_ref().unwrap().observed_at_ms,
            1_785_558_240_772
        );

        let documented_alias = r#"{"symbol":"FET_USDT","data":{"symbol":"FET_USDT",
            "fundingRate":0.000075,"nextSettleTime":1785571200000},
            "channel":"push.funding.rate","ts":1785558242000}"#;
        let ParsedFrame::Events(events) = session.parse(documented_alias, 3).unwrap() else {
            panic!("expected ticker event");
        };
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(
            ticker.funding_rate.as_ref().unwrap().value.as_str(),
            "0.000075"
        );
    }

    #[test]
    fn candle_preserves_documented_contract_and_quote_units() {
        let frame = r#"{
            "symbol":"FET_USDT",
            "data":{"symbol":"FET_USDT","interval":"Min1","t":1785558180,
            "o":0.1420,"c":0.1420,"h":0.1420,"l":0.1419,
            "a":1.066397E3,"q":751,"ro":0.1419,"rc":0.1420,"rh":0.1420,"rl":0.1419},
            "channel":"push.kline"
        }"#;
        let ParsedFrame::Events(events) = session().parse(frame, 1_785_558_231_250).unwrap() else {
            panic!("expected candle event");
        };
        let MarketPayload::Candle(candle) = &events[0].payload else {
            panic!("expected candle");
        };
        assert_eq!(candle.start_time_ms, 1_785_558_180_000);
        assert_eq!(candle.end_time_ms, 1_785_558_240_000);
        assert_eq!(candle.open.as_str(), "0.1420");
        assert_eq!(candle.contract_volume.as_ref().unwrap().as_str(), "751");
        assert_eq!(candle.quote_volume.as_ref().unwrap().as_str(), "1066.397");
        assert_eq!(candle.base_volume, None);
        assert_eq!(candle.finality, CandleFinality::Unknown);
        assert_eq!(events[0].exchange_time_ms, None);
    }

    #[test]
    fn errors_pongs_and_invalid_contracts_are_explicit() {
        let mut session = session();
        assert_eq!(
            session
                .parse(
                    r#"{"channel":"pong","data":1785558227616,"ts":1785558227616}"#,
                    1
                )
                .unwrap(),
            ParsedFrame::Pong
        );
        assert!(matches!(
            session.parse(
                r#"{"channel":"rs.error","data":"Contract [NOPE_USDT] not exists","ts":1}"#,
                1
            ),
            Err(AdapterError::CommandRejected { .. })
        ));
        assert!(matches!(
            session.subscription_messages(
                SubscriptionAction::Subscribe,
                &[subscription("FETUSDT", Channel::Ticker)]
            ),
            Err(AdapterError::InvalidSubscription { .. })
        ));
        assert!(matches!(
            session.parse(
                r#"{"symbol":"FET_USDT","data":{"symbol":"BTC_USDT","lastPrice":1,
                "timestamp":2},"channel":"push.ticker","ts":2}"#,
                3
            ),
            Err(AdapterError::InvalidPayload { .. })
        ));
        assert!(matches!(
            session.parse("not-json", 1),
            Err(AdapterError::InvalidPayload { .. })
        ));
    }
}
