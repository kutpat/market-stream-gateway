use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use super::{
    AdapterError, AdapterSession, ConnectionTarget, EndpointKind, Heartbeat, OutboundCommand,
    ParsedFrame, ProviderAdapter, SubscriptionAction, validate_subscriptions,
};
use crate::domain::{
    Candle, CandleFinality, Channel, DecimalValue, MarketKind, MarketPayload, ObservedDecimal,
    Provider, ProviderEvent, SubscriptionKey, Ticker,
};

const PRODUCTION_PUBLIC_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const PRODUCTION_BUSINESS_URL: &str = "wss://ws.okx.com:8443/ws/v5/business";
const COMMAND_INTERVAL: Duration = Duration::from_millis(400);
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);
const COMMAND_ARGS_LIMIT: usize = 100;
const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const ENDPOINTS: &[EndpointKind] = &[EndpointKind::Public, EndpointKind::Business];

/// Adapter for OKX V5 public linear-perpetual market streams.
#[derive(Debug, Clone)]
pub struct OkxAdapter {
    public_url: Url,
    business_url: Url,
}

impl OkxAdapter {
    /// Create an adapter using explicit V5 public and business WebSocket endpoints.
    pub fn new(public_url: Url, business_url: Url) -> Self {
        Self {
            public_url,
            business_url,
        }
    }
}

impl Default for OkxAdapter {
    fn default() -> Self {
        Self::new(
            Url::parse(PRODUCTION_PUBLIC_URL).expect("hard-coded OKX public URL must be valid"),
            Url::parse(PRODUCTION_BUSINESS_URL).expect("hard-coded OKX business URL must be valid"),
        )
    }
}

#[async_trait]
impl ProviderAdapter for OkxAdapter {
    fn provider(&self) -> Provider {
        Provider::Okx
    }

    fn endpoints(&self) -> &'static [EndpointKind] {
        ENDPOINTS
    }

    fn endpoint_for(&self, channel: Channel) -> EndpointKind {
        match channel {
            Channel::Ticker => EndpointKind::Public,
            Channel::Candle1m => EndpointKind::Business,
        }
    }

    async fn connection_target(
        &self,
        endpoint: EndpointKind,
        _http: &reqwest::Client,
    ) -> Result<ConnectionTarget, AdapterError> {
        let url = match endpoint {
            EndpointKind::Public => self.public_url.clone(),
            EndpointKind::Business => self.business_url.clone(),
            EndpointKind::Primary => {
                return Err(AdapterError::UnsupportedEndpoint {
                    provider: Provider::Okx,
                    endpoint,
                });
            }
        };
        Ok(ConnectionTarget {
            url,
            command_interval: COMMAND_INTERVAL,
            subscription_ack_timeout: SUBSCRIPTION_ACK_TIMEOUT,
            heartbeat_interval: Duration::from_secs(20),
            stale_after: Duration::from_secs(45),
            max_message_bytes: MAX_MESSAGE_BYTES,
            rotate_after: None,
        })
    }

    fn session(&self, endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession> {
        Box::new(OkxSession {
            endpoint,
            connection_epoch,
            next_command_id: 1,
            tickers: HashMap::new(),
        })
    }
}

#[derive(Debug)]
struct OkxSession {
    endpoint: EndpointKind,
    connection_epoch: Uuid,
    next_command_id: u64,
    tickers: HashMap<String, Ticker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
struct OkxSubscriptionArg {
    channel: &'static str,
    inst_id: String,
}

#[derive(Debug, Serialize)]
struct OkxCommand<'a> {
    id: String,
    op: &'a str,
    args: &'a [OkxSubscriptionArg],
}

impl AdapterSession for OkxSession {
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        validate_subscriptions(Provider::Okx, subscriptions)?;
        let args = self.subscription_args(subscriptions)?;
        if action == SubscriptionAction::Unsubscribe {
            for subscription in subscriptions {
                if subscription.channel == Channel::Ticker {
                    self.tickers.remove(&subscription.symbol);
                }
            }
        }
        let operation = match action {
            SubscriptionAction::Subscribe => "subscribe",
            SubscriptionAction::Unsubscribe => "unsubscribe",
        };
        args.chunks(COMMAND_ARGS_LIMIT)
            .map(|chunk| {
                let request_id = self.take_command_id();
                let command = OkxCommand {
                    id: request_id.clone(),
                    op: operation,
                    args: chunk,
                };
                let text =
                    serde_json::to_string(&command).map_err(|error| invalid(error.to_string()))?;
                Ok(OutboundCommand {
                    request_id,
                    text,
                    // OKX emits one subscribe/unsubscribe event for each argument in a command.
                    expected_acknowledgements: chunk.len(),
                })
            })
            .collect()
    }

    fn heartbeat(&mut self) -> Heartbeat {
        Heartbeat::Text("ping".to_owned())
    }

    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        if text.trim() == "pong" {
            return Ok(ParsedFrame::Pong);
        }
        let value: Value = serde_json::from_str(text)
            .map_err(|error| invalid(format!("invalid JSON: {error}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("message must be a JSON object"))?;
        if let Some(event) = object.get("event") {
            return parse_command_response(event, object);
        }
        self.parse_data_message(object, received_at_ms)
    }
}

impl OkxSession {
    fn take_command_id(&mut self) -> String {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.wrapping_add(1).max(1);
        id.to_string()
    }

    fn subscription_args(
        &self,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OkxSubscriptionArg>, AdapterError> {
        let mut seen = HashSet::new();
        let mut args = Vec::new();
        for subscription in subscriptions {
            validate_okx_symbol(&subscription.symbol).map_err(|message| {
                AdapterError::InvalidSubscription {
                    provider: Provider::Okx,
                    message,
                }
            })?;
            let channels: &[&str] = match (self.endpoint, subscription.channel) {
                (EndpointKind::Public, Channel::Ticker) => {
                    &["tickers", "mark-price", "funding-rate"]
                }
                (EndpointKind::Business, Channel::Candle1m) => &["candle1m"],
                (endpoint, channel) => {
                    return Err(AdapterError::InvalidSubscription {
                        provider: Provider::Okx,
                        message: format!("{channel} does not belong on the {endpoint} endpoint"),
                    });
                }
            };
            for &channel in channels {
                let arg = OkxSubscriptionArg {
                    channel,
                    inst_id: subscription.symbol.clone(),
                };
                if seen.insert(arg.clone()) {
                    args.push(arg);
                }
            }
        }
        Ok(args)
    }

    fn parse_data_message(
        &mut self,
        object: &serde_json::Map<String, Value>,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let argument = required_object(object, "arg")?;
        let channel = required_string(argument, "channel")?;
        let symbol = required_string(argument, "instId")?;
        validate_okx_symbol(symbol).map_err(invalid)?;
        let data = object
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("data must be an array"))?;
        if data.is_empty() {
            return Ok(ParsedFrame::Events(Vec::new()));
        }
        let events = match channel {
            "tickers" => self.parse_tickers(data, symbol, received_at_ms)?,
            "mark-price" => self.parse_mark_prices(data, symbol, received_at_ms)?,
            "funding-rate" => self.parse_funding_rates(data, symbol, received_at_ms)?,
            "candle1m" => self.parse_candles(data, symbol, received_at_ms)?,
            _ => return Ok(ParsedFrame::Ignored),
        };
        Ok(ParsedFrame::Events(events))
    }

    fn parse_tickers(
        &mut self,
        data: &[Value],
        expected_symbol: &str,
        received_at_ms: u64,
    ) -> Result<Vec<ProviderEvent>, AdapterError> {
        let mut events = Vec::with_capacity(data.len());
        for item in data {
            let item = item
                .as_object()
                .ok_or_else(|| invalid("ticker item must be an object"))?;
            let (symbol, observed_at_ms) = parse_item_identity(item, expected_symbol)?;
            let last = optional_observation(item, "last", observed_at_ms)?;
            let bid = optional_observation(item, "bidPx", observed_at_ms)?;
            let ask = optional_observation(item, "askPx", observed_at_ms)?;
            if last.is_none() && bid.is_none() && ask.is_none() {
                return Err(invalid("ticker item contains no usable price"));
            }
            let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
            if last.is_some() {
                ticker.last = last;
            }
            if bid.is_some() {
                ticker.bid = bid;
            }
            if ask.is_some() {
                ticker.ask = ask;
            }
            self.tickers.insert(symbol.clone(), ticker.clone());
            events.push(self.event(
                symbol,
                Some(observed_at_ms),
                received_at_ms,
                MarketPayload::Ticker(ticker),
            ));
        }
        Ok(events)
    }

    fn parse_mark_prices(
        &mut self,
        data: &[Value],
        expected_symbol: &str,
        received_at_ms: u64,
    ) -> Result<Vec<ProviderEvent>, AdapterError> {
        let mut events = Vec::with_capacity(data.len());
        for item in data {
            let item = item
                .as_object()
                .ok_or_else(|| invalid("mark-price item must be an object"))?;
            let (symbol, observed_at_ms) = parse_item_identity(item, expected_symbol)?;
            let mark = required_observation(item, "markPx", observed_at_ms)?;
            let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
            ticker.mark = Some(mark);
            self.tickers.insert(symbol.clone(), ticker.clone());
            events.push(self.event(
                symbol,
                Some(observed_at_ms),
                received_at_ms,
                MarketPayload::Ticker(ticker),
            ));
        }
        Ok(events)
    }

    fn parse_funding_rates(
        &mut self,
        data: &[Value],
        expected_symbol: &str,
        received_at_ms: u64,
    ) -> Result<Vec<ProviderEvent>, AdapterError> {
        let mut events = Vec::with_capacity(data.len());
        for item in data {
            let item = item
                .as_object()
                .ok_or_else(|| invalid("funding-rate item must be an object"))?;
            let (symbol, observed_at_ms) = parse_item_identity(item, expected_symbol)?;
            let funding_rate = required_observation(item, "fundingRate", observed_at_ms)?;
            let funding_time = optional_timestamp(item, "fundingTime")?
                .or(optional_timestamp(item, "nextFundingTime")?);
            let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
            ticker.funding_rate = Some(funding_rate);
            ticker.next_funding_time_ms = funding_time;
            self.tickers.insert(symbol.clone(), ticker.clone());
            events.push(self.event(
                symbol,
                Some(observed_at_ms),
                received_at_ms,
                MarketPayload::Ticker(ticker),
            ));
        }
        Ok(events)
    }

    fn parse_candles(
        &self,
        data: &[Value],
        symbol: &str,
        received_at_ms: u64,
    ) -> Result<Vec<ProviderEvent>, AdapterError> {
        data.iter()
            .map(|item| {
                let values = item
                    .as_array()
                    .ok_or_else(|| invalid("candle item must be an array"))?;
                if values.len() != 9 {
                    return Err(invalid(format!(
                        "candle item must contain 9 fields, got {}",
                        values.len()
                    )));
                }
                let start_time_ms = parse_timestamp(&values[0], "candle start time")?;
                let end_time_ms = start_time_ms
                    .checked_add(60_000)
                    .ok_or_else(|| invalid("candle end time overflow"))?;
                let finality = match required_scalar(&values[8], "candle confirmation")? {
                    "0" => CandleFinality::Open,
                    "1" => CandleFinality::Closed,
                    value => return Err(invalid(format!("invalid candle confirmation: {value}"))),
                };
                let candle = Candle {
                    interval: "1m".to_owned(),
                    start_time_ms,
                    end_time_ms,
                    open: decimal(&values[1], "candle open")?,
                    high: decimal(&values[2], "candle high")?,
                    low: decimal(&values[3], "candle low")?,
                    close: decimal(&values[4], "candle close")?,
                    contract_volume: optional_decimal_value(&values[5], "contract volume")?,
                    base_volume: optional_decimal_value(&values[6], "base volume")?,
                    quote_volume: optional_decimal_value(&values[7], "quote volume")?,
                    finality,
                    data_quality: Vec::new(),
                };
                Ok(self.event(
                    symbol.to_owned(),
                    Some(start_time_ms),
                    received_at_ms,
                    MarketPayload::Candle(candle),
                ))
            })
            .collect()
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
            provider: Provider::Okx,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms,
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload,
        }
    }
}

fn parse_command_response(
    event: &Value,
    object: &serde_json::Map<String, Value>,
) -> Result<ParsedFrame, AdapterError> {
    let event = event
        .as_str()
        .ok_or_else(|| invalid("event must be a string"))?;
    match event {
        "subscribe" | "unsubscribe" => {
            let request_id = object
                .get("id")
                .and_then(Value::as_str)
                .filter(|request_id| !request_id.is_empty() && request_id.len() <= 32)
                .ok_or_else(|| invalid("command response is missing a valid id"))?;
            Ok(ParsedFrame::Acknowledgement {
                request_id: request_id.to_owned(),
            })
        }
        "error" => {
            let code = object
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let message = object
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            Err(AdapterError::CommandRejected {
                provider: Provider::Okx,
                message: format!("code {code}: {message}"),
            })
        }
        _ => Ok(ParsedFrame::Ignored),
    }
}

fn parse_item_identity(
    item: &serde_json::Map<String, Value>,
    expected_symbol: &str,
) -> Result<(String, u64), AdapterError> {
    let symbol = required_string(item, "instId")?;
    if symbol != expected_symbol {
        return Err(invalid(format!(
            "payload symbol {symbol} does not match topic symbol {expected_symbol}"
        )));
    }
    if let Some(instrument_type) = item.get("instType").and_then(Value::as_str)
        && instrument_type != "SWAP"
    {
        return Err(invalid(format!(
            "expected SWAP instrument, got {instrument_type}"
        )));
    }
    let observed_at_ms = required_timestamp(item, "ts")?;
    Ok((symbol.to_owned(), observed_at_ms))
}

fn required_object<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a serde_json::Map<String, Value>, AdapterError> {
    object
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("{field} must be an object")))
}

fn required_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, AdapterError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid(format!("{field} must be a non-empty string")))
}

fn required_timestamp(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<u64, AdapterError> {
    let value = object
        .get(field)
        .ok_or_else(|| invalid(format!("missing {field}")))?;
    parse_timestamp(value, field)
}

fn optional_timestamp(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, AdapterError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(value) => parse_timestamp(value, field).map(Some),
    }
}

fn parse_timestamp(value: &Value, field: &str) -> Result<u64, AdapterError> {
    required_scalar(value, field)?
        .parse::<u64>()
        .map_err(|_| invalid(format!("{field} must be an unsigned integer")))
}

fn required_observation(
    object: &serde_json::Map<String, Value>,
    field: &str,
    observed_at_ms: u64,
) -> Result<ObservedDecimal, AdapterError> {
    let value = object
        .get(field)
        .ok_or_else(|| invalid(format!("missing {field}")))?;
    let value = required_scalar(value, field)?;
    ObservedDecimal::new(value.to_owned(), observed_at_ms)
        .map_err(|error| invalid(error.to_string()))
}

fn optional_observation(
    object: &serde_json::Map<String, Value>,
    field: &str,
    observed_at_ms: u64,
) -> Result<Option<ObservedDecimal>, AdapterError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(value) => {
            let value = required_scalar(value, field)?;
            ObservedDecimal::new(value.to_owned(), observed_at_ms)
                .map(Some)
                .map_err(|error| invalid(error.to_string()))
        }
    }
}

fn decimal(value: &Value, field: &str) -> Result<DecimalValue, AdapterError> {
    DecimalValue::new(required_scalar(value, field)?.to_owned())
        .map_err(|error| invalid(error.to_string()))
}

fn optional_decimal_value(
    value: &Value,
    field: &str,
) -> Result<Option<DecimalValue>, AdapterError> {
    if value.is_null() || value.as_str() == Some("") {
        return Ok(None);
    }
    decimal(value, field).map(Some)
}

fn required_scalar<'a>(value: &'a Value, field: &str) -> Result<&'a str, AdapterError> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid(format!("{field} must be a non-empty string")))
}

fn validate_okx_symbol(symbol: &str) -> Result<(), String> {
    if symbol.ends_with("-USDT-SWAP") || symbol.ends_with("-USDC-SWAP") {
        Ok(())
    } else {
        Err(format!(
            "{symbol} is not an OKX USDT/USDC linear perpetual symbol"
        ))
    }
}

fn invalid(message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidPayload {
        provider: Provider::Okx,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::Value;

    use super::*;

    fn subscription(symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(Provider::Okx, MarketKind::LinearPerpetual, symbol, channel).unwrap()
    }

    fn session(endpoint: EndpointKind) -> Box<dyn AdapterSession> {
        OkxAdapter::default().session(endpoint, Uuid::nil())
    }

    #[test]
    fn routes_tickers_and_candles_to_separate_endpoints() {
        let adapter = OkxAdapter::default();
        assert_eq!(adapter.endpoint_for(Channel::Ticker), EndpointKind::Public);
        assert_eq!(
            adapter.endpoint_for(Channel::Candle1m),
            EndpointKind::Business
        );
    }

    #[tokio::test]
    async fn target_paces_commands_and_bounds_subscription_ack_waits() {
        let target = OkxAdapter::default()
            .connection_target(EndpointKind::Public, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(target.command_interval, Duration::from_millis(400));
        assert_eq!(target.subscription_ack_timeout, Duration::from_secs(15));
    }

    #[test]
    fn expands_ticker_subscriptions_and_unsubscribes_symmetrically() {
        let mut session = session(EndpointKind::Public);
        let key = subscription("btc-usdt-swap", Channel::Ticker);
        let subscribe = session
            .subscription_messages(SubscriptionAction::Subscribe, std::slice::from_ref(&key))
            .unwrap();
        let unsubscribe = session
            .subscription_messages(SubscriptionAction::Unsubscribe, &[key])
            .unwrap();

        assert_eq!(subscribe.len(), 1);
        assert_eq!(subscribe[0].request_id, "1");
        assert_eq!(subscribe[0].expected_acknowledgements, 3);
        let subscribe: Value = serde_json::from_str(&subscribe[0].text).unwrap();
        let unsubscribe: Value = serde_json::from_str(&unsubscribe[0].text).unwrap();
        assert_eq!(subscribe["op"], "subscribe");
        assert_eq!(unsubscribe["op"], "unsubscribe");
        assert_eq!(subscribe["args"].as_array().unwrap().len(), 3);
        assert_eq!(subscribe["args"], unsubscribe["args"]);
        assert_eq!(subscribe["args"][1]["channel"], "mark-price");
    }

    #[test]
    fn rejects_wrong_endpoint_and_inverse_contract() {
        let mut public = session(EndpointKind::Public);
        assert!(
            public
                .subscription_messages(
                    SubscriptionAction::Subscribe,
                    &[subscription("BTC-USDT-SWAP", Channel::Candle1m)]
                )
                .is_err()
        );
        assert!(
            public
                .subscription_messages(
                    SubscriptionAction::Subscribe,
                    &[subscription("BTC-USD-SWAP", Channel::Ticker)]
                )
                .is_err()
        );
    }

    #[test]
    fn parses_ticker_mark_and_funding_updates_losslessly() {
        let mut session = session(EndpointKind::Public);
        let ticker = r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","last":"9999.9900","askPx":"10000.01","bidPx":"9999.98","ts":"1597026383085"}]}"#;
        let mark = r#"{"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","markPx":"9999.00000001","ts":"1597026383090"}]}"#;
        let funding = r#"{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","fundingRate":"0.0001875391284828","fundingTime":"1700726400000","nextFundingTime":"1700755200000","ts":"1700724675402"}]}"#;

        let ParsedFrame::Events(events) = session.parse(ticker, 10).unwrap() else {
            panic!("expected ticker event");
        };
        let MarketPayload::Ticker(value) = &events[0].payload else {
            panic!("expected ticker payload");
        };
        assert_eq!(value.last.as_ref().unwrap().value.as_str(), "9999.9900");

        let ParsedFrame::Events(events) = session.parse(mark, 11).unwrap() else {
            panic!("expected mark event");
        };
        let MarketPayload::Ticker(value) = &events[0].payload else {
            panic!("expected ticker payload");
        };
        assert_eq!(value.mark.as_ref().unwrap().value.as_str(), "9999.00000001");
        assert_eq!(value.last.as_ref().unwrap().value.as_str(), "9999.9900");

        let ParsedFrame::Events(events) = session.parse(funding, 12).unwrap() else {
            panic!("expected funding event");
        };
        let MarketPayload::Ticker(value) = &events[0].payload else {
            panic!("expected ticker payload");
        };
        assert_eq!(value.next_funding_time_ms, Some(1_700_726_400_000));
        assert!(value.mark.is_some());
    }

    #[test]
    fn parses_derivative_candle_volume_and_finality() {
        let mut session = session(EndpointKind::Business);
        let message = r#"{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1597026360000","8533.0200","8553.74","8527.17","8548.26","45247","529.5858061","4529585.22","1"]]}"#;
        let ParsedFrame::Events(events) = session.parse(message, 1_597_026_383_100).unwrap() else {
            panic!("expected candle event");
        };
        let MarketPayload::Candle(candle) = &events[0].payload else {
            panic!("expected candle payload");
        };
        assert_eq!(candle.start_time_ms, 1_597_026_360_000);
        assert_eq!(candle.end_time_ms, 1_597_026_420_000);
        assert_eq!(candle.open.as_str(), "8533.0200");
        assert_eq!(candle.contract_volume.as_ref().unwrap().as_str(), "45247");
        assert_eq!(candle.base_volume.as_ref().unwrap().as_str(), "529.5858061");
        assert_eq!(candle.finality, CandleFinality::Closed);
    }

    #[test]
    fn handles_acks_errors_pongs_and_malformed_payloads() {
        let mut session = session(EndpointKind::Public);
        assert_eq!(session.parse("pong", 1).unwrap(), ParsedFrame::Pong);
        assert_eq!(
            session
                .parse(
                    r#"{"id":"1","event":"subscribe","arg":{"channel":"tickers"}}"#,
                    1,
                )
                .unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "1".to_owned()
            }
        );
        assert!(matches!(
            session.parse(
                r#"{"event":"error","code":"60012","msg":"invalid request"}"#,
                1
            ),
            Err(AdapterError::CommandRejected { .. })
        ));
        assert!(session.parse("{", 1).is_err());
        assert!(
            session
                .parse(
                    r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","last":"not-a-price","ts":"1"}]}"#,
                    1
                )
                .is_err()
        );
    }

    #[test]
    fn empty_data_is_valid_and_unsubscribe_clears_ticker_state() {
        let mut session = session(EndpointKind::Public);
        let ticker = r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","last":"1.00","ts":"10"}]}"#;
        let mark = r#"{"arg":{"channel":"mark-price","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","markPx":"0.99","ts":"11"}]}"#;
        session.parse(ticker, 1).unwrap();
        session
            .subscription_messages(
                SubscriptionAction::Unsubscribe,
                &[subscription("BTC-USDT-SWAP", Channel::Ticker)],
            )
            .unwrap();
        let ParsedFrame::Events(events) = session.parse(mark, 2).unwrap() else {
            panic!("expected mark event");
        };
        let MarketPayload::Ticker(value) = &events[0].payload else {
            panic!("expected ticker payload");
        };
        assert!(value.last.is_none());
        assert!(value.mark.is_some());
        assert_eq!(
            session
                .parse(
                    r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[]}"#,
                    3,
                )
                .unwrap(),
            ParsedFrame::Events(Vec::new())
        );
    }
}
