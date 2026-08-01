use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use crate::domain::{
    Candle, CandleFinality, Channel, DecimalValue, MarketKind, MarketPayload, ObservedDecimal,
    Provider, ProviderEvent, SourceSequence, SubscriptionKey, Ticker, normalize_symbol,
};

use super::{
    AdapterError, AdapterSession, ConnectionTarget, EndpointKind, Heartbeat, OutboundCommand,
    ParsedFrame, ProviderAdapter, SubscriptionAction, validate_subscriptions,
};

const BYBIT_LINEAR_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const BYBIT_COMMAND_INTERVAL: Duration = Duration::from_millis(150);
const BYBIT_SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);
const BYBIT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
const BYBIT_STALE_AFTER: Duration = Duration::from_secs(45);
const BYBIT_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
// Bybit caps the combined character count of public subscription args at 21,000 per
// connection. Keeping commands under 20,000 leaves room for the JSON command envelope.
const BYBIT_MAX_TOPIC_CHARS_PER_COMMAND: usize = 20_000;
const BYBIT_MAX_TOPICS_PER_COMMAND: usize = 100;
const ENDPOINTS: &[EndpointKind] = &[EndpointKind::Primary];

#[derive(Debug, Clone)]
pub struct BybitAdapter {
    url: Url,
}

impl Default for BybitAdapter {
    fn default() -> Self {
        Self {
            url: Url::parse(BYBIT_LINEAR_URL).expect("the official Bybit URL must be valid"),
        }
    }
}

impl BybitAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_url(url: Url) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ProviderAdapter for BybitAdapter {
    fn provider(&self) -> Provider {
        Provider::Bybit
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
                provider: Provider::Bybit,
                endpoint,
            });
        }

        Ok(ConnectionTarget {
            url: self.url.clone(),
            command_interval: BYBIT_COMMAND_INTERVAL,
            subscription_ack_timeout: BYBIT_SUBSCRIPTION_ACK_TIMEOUT,
            heartbeat_interval: BYBIT_HEARTBEAT_INTERVAL,
            stale_after: BYBIT_STALE_AFTER,
            max_message_bytes: BYBIT_MAX_MESSAGE_BYTES,
            rotate_after: None,
        })
    }

    fn session(&self, _endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession> {
        Box::new(BybitSession::new(connection_epoch))
    }
}

struct BybitSession {
    connection_epoch: Uuid,
    next_request_id: u64,
    tickers: HashMap<String, Ticker>,
}

impl BybitSession {
    fn new(connection_epoch: Uuid) -> Self {
        Self {
            connection_epoch,
            next_request_id: 1,
            tickers: HashMap::new(),
        }
    }

    fn request_id(&mut self) -> String {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).unwrap_or(1);
        format!("gateway-{request_id}")
    }

    fn command_messages(
        &mut self,
        action: SubscriptionAction,
        topics: &[String],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        let operation = match action {
            SubscriptionAction::Subscribe => "subscribe",
            SubscriptionAction::Unsubscribe => "unsubscribe",
        };
        let mut messages = Vec::new();
        let mut batch = Vec::new();
        let mut batch_chars = 0;

        for topic in topics {
            let extra_chars = topic.len();
            if !batch.is_empty()
                && (batch.len() == BYBIT_MAX_TOPICS_PER_COMMAND
                    || batch_chars + extra_chars > BYBIT_MAX_TOPIC_CHARS_PER_COMMAND)
            {
                messages.push(self.command(operation, &batch)?);
                batch.clear();
                batch_chars = 0;
            }
            batch.push(topic.clone());
            batch_chars += extra_chars;
        }

        if !batch.is_empty() {
            messages.push(self.command(operation, &batch)?);
        }
        Ok(messages)
    }

    fn command(
        &mut self,
        operation: &str,
        topics: &[String],
    ) -> Result<OutboundCommand, AdapterError> {
        let request_id = self.request_id();
        let text = serde_json::to_string(&json!({
            "req_id": request_id,
            "op": operation,
            "args": topics,
        }))
        .map_err(|error| invalid_payload(format!("could not encode command: {error}")))?;
        Ok(OutboundCommand {
            request_id,
            text,
            expected_acknowledgements: 1,
        })
    }

    fn parse_control(value: &Value) -> Option<Result<ParsedFrame, AdapterError>> {
        let object = value.as_object()?;

        if object.get("type").and_then(Value::as_str) == Some("COMMAND_RESP") {
            let failed = object
                .get("data")
                .and_then(Value::as_object)
                .and_then(|data| data.get("failTopics"))
                .and_then(Value::as_array);
            return Some(match failed {
                Some(topics) if !topics.is_empty() => Err(command_rejected(format!(
                    "topics rejected: {}",
                    Value::Array(topics.clone())
                ))),
                Some(_) => control_acknowledgement(object),
                None => Err(invalid_payload("COMMAND_RESP is missing data.failTopics")),
            });
        }

        let operation = match object.get("op") {
            Some(Value::String(operation)) => operation.as_str(),
            Some(_) => {
                return Some(Err(invalid_payload("control field op must be a string")));
            }
            None => {
                if object.get("success").and_then(Value::as_bool) == Some(false) {
                    return Some(Err(command_rejected(control_error_message(value))));
                }
                return None;
            }
        };

        if object.get("success").and_then(Value::as_bool) == Some(false) {
            return Some(Err(command_rejected(control_error_message(value))));
        }

        match operation {
            "ping" | "pong" => Some(Ok(ParsedFrame::Pong)),
            "subscribe" | "unsubscribe" => Some(match object.get("success") {
                Some(Value::Bool(true)) => control_acknowledgement(object),
                Some(_) => Err(invalid_payload("control field success must be a boolean")),
                None => Err(invalid_payload(format!(
                    "{operation} response is missing success"
                ))),
            }),
            _ => Some(Ok(ParsedFrame::Ignored)),
        }
    }

    fn parse_tickers(
        &mut self,
        value: Value,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let envelope: BybitTickerEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid ticker message: {error}")))?;
        let topic_symbol = ticker_topic_symbol(&envelope.topic)?;
        let is_snapshot = match envelope.message_type.as_str() {
            "snapshot" => true,
            "delta" => false,
            other => {
                return Err(invalid_payload(format!(
                    "unsupported ticker message type {other}"
                )));
            }
        };
        let updates = one_or_many::<BybitTickerData>(envelope.data, "ticker data")?;
        if updates.is_empty() {
            return Err(invalid_payload("ticker data must not be empty"));
        }

        let mut pending = HashMap::<String, Ticker>::new();
        let mut events = Vec::with_capacity(updates.len());
        for update in updates {
            let symbol = normalized_symbol(&update.symbol)?;
            if symbol != topic_symbol {
                return Err(invalid_payload(format!(
                    "ticker topic symbol {topic_symbol} does not match payload symbol {symbol}"
                )));
            }
            let base = if is_snapshot {
                Ticker::default()
            } else {
                pending
                    .get(&symbol)
                    .or_else(|| self.tickers.get(&symbol))
                    .cloned()
                    .unwrap_or_default()
            };
            let ticker = apply_ticker_update(base, update, envelope.ts)?;
            pending.insert(symbol.clone(), ticker.clone());
            events.push(ProviderEvent {
                connection_epoch: self.connection_epoch,
                provider: Provider::Bybit,
                market: MarketKind::LinearPerpetual,
                symbol,
                exchange_time_ms: Some(envelope.ts),
                gateway_received_time_ms: received_at_ms,
                source_sequence: Some(SourceSequence {
                    first: None,
                    last: Some(envelope.cs.to_string()),
                    previous: None,
                }),
                payload: MarketPayload::Ticker(ticker),
            });
        }
        self.tickers.extend(pending);
        Ok(ParsedFrame::Events(events))
    }

    fn parse_klines(&self, value: Value, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        let envelope: BybitKlineEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid kline message: {error}")))?;
        if envelope.message_type != "snapshot" {
            return Err(invalid_payload(format!(
                "unsupported kline message type {}",
                envelope.message_type
            )));
        }
        let symbol = kline_topic_symbol(&envelope.topic)?;
        let events = envelope
            .data
            .into_iter()
            .map(|item| {
                let candle = normalize_kline(&item)?;
                Ok(ProviderEvent {
                    connection_epoch: self.connection_epoch,
                    provider: Provider::Bybit,
                    market: MarketKind::LinearPerpetual,
                    symbol: symbol.clone(),
                    exchange_time_ms: Some(envelope.ts),
                    gateway_received_time_ms: received_at_ms,
                    source_sequence: None,
                    payload: MarketPayload::Candle(candle),
                })
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        Ok(ParsedFrame::Events(events))
    }
}

impl AdapterSession for BybitSession {
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        validate_subscriptions(Provider::Bybit, subscriptions)?;

        let mut seen = HashSet::new();
        let mut topics = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            let topic = match subscription.channel {
                Channel::Ticker => format!("tickers.{}", subscription.symbol),
                Channel::Candle1m => format!("kline.1.{}", subscription.symbol),
            };
            if seen.insert(topic.clone()) {
                topics.push(topic);
            }
            if action == SubscriptionAction::Unsubscribe && subscription.channel == Channel::Ticker
            {
                self.tickers.remove(&subscription.symbol);
            }
        }
        self.command_messages(action, &topics)
    }

    fn heartbeat(&mut self) -> Heartbeat {
        Heartbeat::Text(json!({ "op": "ping" }).to_string())
    }

    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| invalid_payload(format!("invalid JSON: {error}")))?;
        if !value.is_object() {
            return Err(invalid_payload("top-level payload must be an object"));
        }
        if let Some(result) = Self::parse_control(&value) {
            return result;
        }

        let topic = match value.get("topic") {
            Some(Value::String(topic)) => topic.as_str(),
            Some(_) => return Err(invalid_payload("event field topic must be a string")),
            None => return Ok(ParsedFrame::Ignored),
        };
        if topic.starts_with("tickers.") {
            self.parse_tickers(value, received_at_ms)
        } else if topic.starts_with("kline.") {
            self.parse_klines(value, received_at_ms)
        } else {
            Ok(ParsedFrame::Ignored)
        }
    }
}

#[derive(Debug, Deserialize)]
struct BybitTickerEnvelope {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    ts: u64,
    cs: u64,
    data: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitTickerData {
    symbol: String,
    last_price: Option<String>,
    mark_price: Option<String>,
    index_price: Option<String>,
    bid1_price: Option<String>,
    ask1_price: Option<String>,
    funding_rate: Option<String>,
    next_funding_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BybitKlineEnvelope {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    ts: u64,
    data: Vec<BybitKlineData>,
}

#[derive(Debug, Deserialize)]
struct BybitKlineData {
    start: u64,
    end: u64,
    interval: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    turnover: String,
    confirm: bool,
    #[serde(rename = "timestamp")]
    _timestamp: u64,
}

fn apply_ticker_update(
    mut ticker: Ticker,
    update: BybitTickerData,
    observed_at_ms: u64,
) -> Result<Ticker, AdapterError> {
    set_observation(
        &mut ticker.last,
        update.last_price,
        observed_at_ms,
        "lastPrice",
    )?;
    set_observation(
        &mut ticker.mark,
        update.mark_price,
        observed_at_ms,
        "markPrice",
    )?;
    set_observation(
        &mut ticker.index,
        update.index_price,
        observed_at_ms,
        "indexPrice",
    )?;
    set_observation(
        &mut ticker.bid,
        update.bid1_price,
        observed_at_ms,
        "bid1Price",
    )?;
    set_observation(
        &mut ticker.ask,
        update.ask1_price,
        observed_at_ms,
        "ask1Price",
    )?;
    set_observation(
        &mut ticker.funding_rate,
        update.funding_rate,
        observed_at_ms,
        "fundingRate",
    )?;
    if let Some(value) = update.next_funding_time.filter(|value| !value.is_empty()) {
        ticker.next_funding_time_ms = Some(value.parse::<u64>().map_err(|error| {
            invalid_payload(format!("invalid nextFundingTime {value:?}: {error}"))
        })?);
    }
    Ok(ticker)
}

fn set_observation(
    target: &mut Option<ObservedDecimal>,
    value: Option<String>,
    observed_at_ms: u64,
    field: &str,
) -> Result<(), AdapterError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    *target = Some(
        ObservedDecimal::new(value.clone(), observed_at_ms)
            .map_err(|error| invalid_payload(format!("invalid {field} {value:?}: {error}")))?,
    );
    Ok(())
}

fn normalize_kline(item: &BybitKlineData) -> Result<Candle, AdapterError> {
    if item.interval != "1" {
        return Err(invalid_payload(format!(
            "expected 1m kline interval, got {}",
            item.interval
        )));
    }
    let end_time_ms = item
        .end
        .checked_add(1)
        .ok_or_else(|| invalid_payload("kline end time cannot be normalized to exclusive"))?;
    Ok(Candle {
        interval: "1m".to_owned(),
        start_time_ms: item.start,
        end_time_ms,
        open: decimal(&item.open, "open")?,
        high: decimal(&item.high, "high")?,
        low: decimal(&item.low, "low")?,
        close: decimal(&item.close, "close")?,
        // Linear USDT/USDC contract volume is base-coin volume; turnover is quote-coin volume.
        base_volume: Some(decimal(&item.volume, "volume")?),
        quote_volume: Some(decimal(&item.turnover, "turnover")?),
        contract_volume: None,
        finality: if item.confirm {
            CandleFinality::Closed
        } else {
            CandleFinality::Open
        },
        data_quality: Vec::new(),
    })
}

fn decimal(value: &str, field: &str) -> Result<DecimalValue, AdapterError> {
    DecimalValue::new(value.to_owned())
        .map_err(|error| invalid_payload(format!("invalid {field} {value:?}: {error}")))
}

fn one_or_many<T>(value: Value, name: &str) -> Result<Vec<T>, AdapterError>
where
    T: for<'de> Deserialize<'de>,
{
    match value {
        Value::Object(_) => serde_json::from_value(value)
            .map(|item| vec![item])
            .map_err(|error| invalid_payload(format!("invalid {name}: {error}"))),
        Value::Array(_) => serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid {name}: {error}"))),
        _ => Err(invalid_payload(format!(
            "{name} must be an object or array"
        ))),
    }
}

fn ticker_topic_symbol(topic: &str) -> Result<String, AdapterError> {
    let symbol = topic
        .strip_prefix("tickers.")
        .ok_or_else(|| invalid_payload(format!("invalid ticker topic {topic:?}")))?;
    normalized_symbol(symbol)
}

fn kline_topic_symbol(topic: &str) -> Result<String, AdapterError> {
    let symbol = topic
        .strip_prefix("kline.1.")
        .ok_or_else(|| invalid_payload(format!("invalid 1m kline topic {topic:?}")))?;
    normalized_symbol(symbol)
}

fn normalized_symbol(symbol: &str) -> Result<String, AdapterError> {
    normalize_symbol(symbol)
        .map_err(|error| invalid_payload(format!("invalid symbol {symbol:?}: {error}")))
}

fn control_error_message(value: &Value) -> String {
    let message = value
        .get("ret_msg")
        .or_else(|| value.get("retMsg"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown command error");
    let code = value
        .get("ret_code")
        .or_else(|| value.get("retCode"))
        .and_then(Value::as_i64);
    code.map_or_else(
        || message.to_owned(),
        |code| format!("code {code}: {message}"),
    )
}

fn control_acknowledgement(
    object: &serde_json::Map<String, Value>,
) -> Result<ParsedFrame, AdapterError> {
    let request_id = object
        .get("req_id")
        .or_else(|| object.get("reqId"))
        .and_then(Value::as_str)
        .filter(|request_id| !request_id.is_empty() && request_id.len() <= 36)
        .ok_or_else(|| invalid_payload("command response is missing a valid request id"))?;
    Ok(ParsedFrame::Acknowledgement {
        request_id: request_id.to_owned(),
    })
}

fn invalid_payload(message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidPayload {
        provider: Provider::Bybit,
        message: message.into(),
    }
}

fn command_rejected(message: impl Into<String>) -> AdapterError {
    AdapterError::CommandRejected {
        provider: Provider::Bybit,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::Value;

    use super::*;

    fn session() -> BybitSession {
        BybitSession::new(Uuid::nil())
    }

    fn subscription(symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(
            Provider::Bybit,
            MarketKind::LinearPerpetual,
            symbol,
            channel,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn target_and_heartbeat_match_bybit_contract() {
        let adapter = BybitAdapter::new();
        let target = adapter
            .connection_target(EndpointKind::Primary, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(target.url.as_str(), BYBIT_LINEAR_URL);
        assert_eq!(target.command_interval, Duration::from_millis(150));
        assert_eq!(target.subscription_ack_timeout, Duration::from_secs(15));
        assert_eq!(target.heartbeat_interval, Duration::from_secs(20));
        assert_eq!(
            session().heartbeat(),
            Heartbeat::Text("{\"op\":\"ping\"}".to_owned())
        );
    }

    #[test]
    fn subscriptions_are_dynamic_deduplicated_and_batched() {
        let mut session = session();
        let mut subscriptions = Vec::new();
        for index in 0..101 {
            subscriptions.push(subscription(&format!("COIN{index}USDT"), Channel::Ticker));
        }
        subscriptions.push(subscriptions[0].clone());

        let messages = session
            .subscription_messages(SubscriptionAction::Subscribe, &subscriptions)
            .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].expected_acknowledgements, 1);
        assert_eq!(messages[0].request_id, "gateway-1");
        let first: Value = serde_json::from_str(&messages[0].text).unwrap();
        let second: Value = serde_json::from_str(&messages[1].text).unwrap();
        assert_eq!(first["op"], "subscribe");
        assert_eq!(first["args"].as_array().unwrap().len(), 100);
        assert_eq!(second["args"].as_array().unwrap().len(), 1);

        let unsubscribe = session
            .subscription_messages(
                SubscriptionAction::Unsubscribe,
                &[subscription("BTCUSDT", Channel::Candle1m)],
            )
            .unwrap();
        let unsubscribe: Value = serde_json::from_str(&unsubscribe[0].text).unwrap();
        assert_eq!(unsubscribe["op"], "unsubscribe");
        assert_eq!(unsubscribe["args"][0], "kline.1.BTCUSDT");
    }

    #[test]
    fn sparse_ticker_deltas_materialize_cached_state() {
        let mut session = session();
        let snapshot = r#"{
            "topic":"tickers.BTCUSDT","type":"snapshot","ts":1760325052630,"cs":9532239429,
            "data":{"symbol":"BTCUSDT","lastPrice":"66666.60","markPrice":"66665.50",
                    "indexPrice":"66664.40","bid1Price":"66666.50","ask1Price":"66666.60",
                    "fundingRate":"-0.005","nextFundingTime":"1760342400000"}}
        "#;
        let delta = r#"{
            "topic":"tickers.BTCUSDT","type":"delta","ts":1760325052730,"cs":9532239430,
            "data":{"symbol":"BTCUSDT","lastPrice":"66667.10","bid1Price":"66667.00"}}
        "#;
        session.parse(snapshot, 1_760_325_052_637).unwrap();
        let ParsedFrame::Events(events) = session.parse(delta, 1_760_325_052_735).unwrap() else {
            panic!("expected events");
        };
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(ticker.last.as_ref().unwrap().value.as_str(), "66667.10");
        assert_eq!(
            ticker.last.as_ref().unwrap().observed_at_ms,
            1_760_325_052_730
        );
        assert_eq!(ticker.mark.as_ref().unwrap().value.as_str(), "66665.50");
        assert_eq!(
            ticker.mark.as_ref().unwrap().observed_at_ms,
            1_760_325_052_630
        );
        assert_eq!(ticker.ask.as_ref().unwrap().value.as_str(), "66666.60");
        assert_eq!(ticker.next_funding_time_ms, Some(1_760_342_400_000));
        assert_eq!(
            events[0].source_sequence.as_ref().unwrap().last.as_deref(),
            Some("9532239430")
        );
    }

    #[test]
    fn kline_table_preserves_units_and_finality() {
        for (confirm, finality) in [
            (false, CandleFinality::Open),
            (true, CandleFinality::Closed),
        ] {
            let frame = format!(
                r#"{{
                    "topic":"kline.1.ETHUSDT","type":"snapshot","ts":1672324988882,
                    "data":[{{"start":1672324800000,"end":1672324859999,"interval":"1",
                    "open":"16649.5","close":"16677","high":"16677","low":"16608",
                    "volume":"2.081","turnover":"34666.4005","confirm":{confirm},
                    "timestamp":1672324988881}}]
                }}"#
            );
            let ParsedFrame::Events(events) = session().parse(&frame, 1_672_324_988_888).unwrap()
            else {
                panic!("expected events");
            };
            let MarketPayload::Candle(candle) = &events[0].payload else {
                panic!("expected candle");
            };
            assert_eq!(candle.interval, "1m");
            assert_eq!(candle.end_time_ms, 1_672_324_860_000);
            assert_eq!(candle.base_volume.as_ref().unwrap().as_str(), "2.081");
            assert_eq!(candle.quote_volume.as_ref().unwrap().as_str(), "34666.4005");
            assert_eq!(candle.contract_volume, None);
            assert_eq!(candle.finality, finality);
        }
    }

    #[test]
    fn acknowledgements_errors_and_malformed_payloads_are_explicit() {
        let mut session = session();
        assert_eq!(
            session
                .parse(
                    r#"{"success":true,"ret_msg":"","op":"subscribe","req_id":"gateway-1"}"#,
                    1
                )
                .unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "gateway-1".to_owned()
            }
        );
        assert_eq!(
            session
                .parse(r#"{"success":true,"ret_msg":"pong","op":"ping"}"#, 1)
                .unwrap(),
            ParsedFrame::Pong
        );
        assert!(matches!(
            session.parse(
                r#"{"success":false,"ret_msg":"invalid topic","op":"subscribe"}"#,
                1
            ),
            Err(AdapterError::CommandRejected { .. })
        ));
        assert!(matches!(
            session.parse(
                r#"{"topic":"tickers.BTCUSDT","type":"delta","ts":1,"cs":2,"data":{"symbol":"BTCUSDT","lastPrice":12.3}}"#,
                1
            ),
            Err(AdapterError::InvalidPayload { .. })
        ));
        assert!(matches!(
            session.parse("not-json", 1),
            Err(AdapterError::InvalidPayload { .. })
        ));
    }
}
