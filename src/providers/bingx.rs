use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value, json};
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

const BINGX_SWAP_URL: &str = "wss://open-api-swap.bingx.com/swap-market";
// BingX documents the per-connection topic ceiling, but not a command-rate ceiling. Keep
// subscription changes deliberately paced while allowing a full 200-topic connection to become
// ready in a reasonable amount of time.
const BINGX_COMMAND_INTERVAL: Duration = Duration::from_millis(100);
const BINGX_SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);
// The server currently sends its application-level Ping about every five seconds. The scheduled
// Pong is a fallback if a Ping is lost; received Pings are also answered immediately by the
// runtime through ParsedFrame::Reply.
const BINGX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(4);
const BINGX_STALE_AFTER: Duration = Duration::from_secs(20);
const BINGX_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const BINGX_MAX_TOPICS_PER_CONNECTION: usize = 200;
const ENDPOINTS: &[EndpointKind] = &[EndpointKind::Primary];

#[derive(Debug, Clone)]
pub struct BingxAdapter {
    url: Url,
}

impl Default for BingxAdapter {
    fn default() -> Self {
        Self {
            url: Url::parse(BINGX_SWAP_URL).expect("the official BingX URL must be valid"),
        }
    }
}

impl BingxAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_url(url: Url) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ProviderAdapter for BingxAdapter {
    fn provider(&self) -> Provider {
        Provider::Bingx
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
                provider: Provider::Bingx,
                endpoint,
            });
        }

        Ok(ConnectionTarget {
            url: self.url.clone(),
            command_interval: BINGX_COMMAND_INTERVAL,
            subscription_ack_timeout: BINGX_SUBSCRIPTION_ACK_TIMEOUT,
            heartbeat_interval: BINGX_HEARTBEAT_INTERVAL,
            stale_after: BINGX_STALE_AFTER,
            max_message_bytes: BINGX_MAX_MESSAGE_BYTES,
            // BingX does not document a forced connection lifetime. Reconnects remain driven by
            // stale detection, provider closes, and transport failures.
            rotate_after: None,
        })
    }

    fn session(&self, _endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession> {
        Box::new(BingxSession::new(connection_epoch))
    }
}

struct BingxSession {
    connection_epoch: Uuid,
    next_request_id: u64,
    active_topics: HashSet<String>,
    tickers: HashMap<String, Ticker>,
}

impl BingxSession {
    fn new(connection_epoch: Uuid) -> Self {
        Self {
            connection_epoch,
            next_request_id: 1,
            active_topics: HashSet::new(),
            tickers: HashMap::new(),
        }
    }

    fn request_id(&mut self) -> String {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).unwrap_or(1);
        format!("gateway-{request_id}")
    }

    fn command(
        &mut self,
        action: SubscriptionAction,
        topic: &str,
    ) -> Result<OutboundCommand, AdapterError> {
        let request_id = self.request_id();
        let request_type = match action {
            SubscriptionAction::Subscribe => "sub",
            SubscriptionAction::Unsubscribe => "unsub",
        };
        let text = serde_json::to_string(&json!({
            "id": request_id,
            "reqType": request_type,
            "dataType": topic,
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
        let request_id = object.get("id")?;
        let request_id = match request_id {
            Value::String(request_id) if !request_id.is_empty() && request_id.len() <= 64 => {
                request_id
            }
            _ => return Some(Err(invalid_payload("invalid command response id"))),
        };
        let code = match object.get("code") {
            Some(Value::Number(code)) => match code.as_i64() {
                Some(code) => code,
                None => return Some(Err(invalid_payload("control code must be an integer"))),
            },
            Some(_) => return Some(Err(invalid_payload("control code must be an integer"))),
            None => return Some(Err(invalid_payload("command response is missing code"))),
        };
        if code != 0 {
            return Some(Err(command_rejected(control_error_message(object, code))));
        }
        Some(Ok(ParsedFrame::Acknowledgement {
            request_id: request_id.clone(),
        }))
    }

    fn parse_ticker(
        &mut self,
        value: Value,
        received_at_ms: u64,
        topic: &str,
    ) -> Result<ParsedFrame, AdapterError> {
        let envelope: BingxTickerEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid ticker message: {error}")))?;
        ensure_success(envelope.code, envelope.message.as_deref())?;
        let topic_symbol = topic_symbol(topic, "@ticker")?;
        let symbol = normalized_symbol(&envelope.data.symbol)?;
        ensure_topic_symbol(&topic_symbol, &symbol)?;

        let observed_at_ms = envelope
            .data
            .event_time
            .or(envelope.data.timestamp)
            .unwrap_or(received_at_ms);
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.last = Some(observation(
            &envelope.data.last,
            observed_at_ms,
            "last price",
        )?);
        self.tickers.insert(symbol.clone(), ticker.clone());

        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Bingx,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(observed_at_ms),
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload: MarketPayload::Ticker(ticker),
        }]))
    }

    fn parse_book_ticker(
        &mut self,
        value: Value,
        received_at_ms: u64,
        topic: &str,
    ) -> Result<ParsedFrame, AdapterError> {
        let envelope: BingxBookTickerEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid book-ticker message: {error}")))?;
        ensure_success(envelope.code, envelope.message.as_deref())?;
        let topic_symbol = topic_symbol(topic, "@bookTicker")?;
        let symbol = normalized_symbol(&envelope.data.symbol)?;
        ensure_topic_symbol(&topic_symbol, &symbol)?;
        let observed_at_ms = envelope.data.match_time;

        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.bid = Some(observation(&envelope.data.bid, observed_at_ms, "best bid")?);
        ticker.ask = Some(observation(&envelope.data.ask, observed_at_ms, "best ask")?);
        self.tickers.insert(symbol.clone(), ticker.clone());

        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Bingx,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(observed_at_ms),
            gateway_received_time_ms: received_at_ms,
            source_sequence: envelope.data.update_id.map(|update_id| SourceSequence {
                first: None,
                last: Some(update_id.to_string()),
                previous: None,
            }),
            payload: MarketPayload::Ticker(ticker),
        }]))
    }

    fn parse_mark_price(
        &mut self,
        value: Value,
        received_at_ms: u64,
        topic: &str,
    ) -> Result<ParsedFrame, AdapterError> {
        let envelope: BingxMarkPriceEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid mark-price message: {error}")))?;
        ensure_success(envelope.code, envelope.message.as_deref())?;
        let topic_symbol = topic_symbol(topic, "@markPrice")?;
        let symbol = normalized_symbol(&envelope.data.symbol)?;
        ensure_topic_symbol(&topic_symbol, &symbol)?;
        let observed_at_ms = envelope
            .data
            .event_time
            .or(envelope.data.timestamp)
            .ok_or_else(|| invalid_payload("mark-price message is missing event time"))?;

        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.mark = Some(observation(
            &envelope.data.mark_price,
            observed_at_ms,
            "mark price",
        )?);
        self.tickers.insert(symbol.clone(), ticker.clone());

        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Bingx,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(observed_at_ms),
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload: MarketPayload::Ticker(ticker),
        }]))
    }

    fn parse_kline(
        &self,
        value: Value,
        received_at_ms: u64,
        topic: &str,
    ) -> Result<ParsedFrame, AdapterError> {
        let envelope: BingxKlineEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid kline message: {error}")))?;
        ensure_success(envelope.code, envelope.message.as_deref())?;
        let topic_symbol = topic_symbol(topic, "@kline_1m")?;
        let (payload_symbol, items) = envelope.data.into_items(envelope.symbol)?;
        let symbol = normalized_symbol(&payload_symbol)?;
        ensure_topic_symbol(&topic_symbol, &symbol)?;
        if items.is_empty() {
            return Err(invalid_payload("kline data must not be empty"));
        }

        let events = items
            .into_iter()
            .map(|item| {
                if item.start_time % 60_000 != 0 {
                    return Err(invalid_payload(format!(
                        "1m kline start time {} is not minute aligned",
                        item.start_time
                    )));
                }
                let end_time_ms = item
                    .start_time
                    .checked_add(60_000)
                    .ok_or_else(|| invalid_payload("kline end time overflow"))?;
                if let Some(close_time) = item.close_time
                    && close_time != end_time_ms
                    && close_time != end_time_ms - 1
                {
                    return Err(invalid_payload(format!(
                        "1m kline close time {close_time} does not match start time {}",
                        item.start_time
                    )));
                }
                Ok(ProviderEvent {
                    connection_epoch: self.connection_epoch,
                    provider: Provider::Bingx,
                    market: MarketKind::LinearPerpetual,
                    symbol: symbol.clone(),
                    // BingX includes the candle start but no event-generation timestamp.
                    exchange_time_ms: None,
                    gateway_received_time_ms: received_at_ms,
                    source_sequence: None,
                    payload: MarketPayload::Candle(Candle {
                        interval: "1m".to_owned(),
                        start_time_ms: item.start_time,
                        end_time_ms,
                        open: decimal(&item.open, "open")?,
                        high: decimal(&item.high, "high")?,
                        low: decimal(&item.low, "low")?,
                        close: decimal(&item.close, "close")?,
                        base_volume: Some(decimal(&item.volume, "base volume")?),
                        quote_volume: None,
                        contract_volume: None,
                        // The stream does not expose a close/confirm flag. Consumers may only
                        // promote finality after seeing the next interval or reconciling REST.
                        finality: CandleFinality::Unknown,
                        data_quality: Vec::new(),
                    }),
                })
            })
            .collect::<Result<Vec<_>, AdapterError>>()?;
        Ok(ParsedFrame::Events(events))
    }
}

impl AdapterSession for BingxSession {
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        validate_subscriptions(Provider::Bingx, subscriptions)?;

        let mut requested_topics = Vec::with_capacity(subscriptions.len() * 2);
        let mut seen = HashSet::new();
        for subscription in subscriptions {
            validate_bingx_symbol(&subscription.symbol)?;
            let topics = match subscription.channel {
                // Use the dedicated 200 ms book stream for BBO. BingX's rolling ticker also
                // carries bid/ask fields, but live observations show they can lag bookTicker.
                Channel::Ticker => vec![
                    format!("{}@ticker", subscription.symbol),
                    format!("{}@markPrice", subscription.symbol),
                    format!("{}@bookTicker", subscription.symbol),
                ],
                Channel::Candle1m => vec![format!("{}@kline_1m", subscription.symbol)],
            };
            for topic in topics {
                if seen.insert(topic.clone()) {
                    requested_topics.push(topic);
                }
            }
            if action == SubscriptionAction::Unsubscribe && subscription.channel == Channel::Ticker
            {
                self.tickers.remove(&subscription.symbol);
            }
        }

        let topics = match action {
            SubscriptionAction::Subscribe => {
                let new_topics = requested_topics
                    .into_iter()
                    .filter(|topic| !self.active_topics.contains(topic))
                    .collect::<Vec<_>>();
                if self.active_topics.len() + new_topics.len() > BINGX_MAX_TOPICS_PER_CONNECTION {
                    return Err(AdapterError::InvalidSubscription {
                        provider: Provider::Bingx,
                        message: format!(
                            "subscription would exceed BingX's {BINGX_MAX_TOPICS_PER_CONNECTION}-topic connection limit"
                        ),
                    });
                }
                self.active_topics.extend(new_topics.iter().cloned());
                new_topics
            }
            SubscriptionAction::Unsubscribe => requested_topics
                .into_iter()
                .filter(|topic| self.active_topics.remove(topic))
                .collect(),
        };

        topics
            .iter()
            .map(|topic| self.command(action, topic))
            .collect()
    }

    fn heartbeat(&mut self) -> Heartbeat {
        Heartbeat::Text("Pong".to_owned())
    }

    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        match text {
            "Ping" => {
                return Ok(ParsedFrame::Reply(Heartbeat::Text("Pong".to_owned())));
            }
            "Pong" => return Ok(ParsedFrame::Pong),
            _ => {}
        }

        let value: Value = serde_json::from_str(text)
            .map_err(|error| invalid_payload(format!("invalid JSON: {error}")))?;
        if !value.is_object() {
            return Err(invalid_payload("top-level payload must be an object"));
        }
        if let Some(result) = Self::parse_control(&value) {
            return result;
        }

        let topic = match value.get("dataType") {
            Some(Value::String(topic)) if !topic.is_empty() => topic.clone(),
            Some(Value::String(_)) | None => return Ok(ParsedFrame::Ignored),
            Some(_) => return Err(invalid_payload("event field dataType must be a string")),
        };
        if topic.ends_with("@ticker") {
            self.parse_ticker(value, received_at_ms, &topic)
        } else if topic.ends_with("@markPrice") {
            self.parse_mark_price(value, received_at_ms, &topic)
        } else if topic.ends_with("@bookTicker") {
            self.parse_book_ticker(value, received_at_ms, &topic)
        } else if topic.ends_with("@kline_1m") {
            self.parse_kline(value, received_at_ms, &topic)
        } else {
            Ok(ParsedFrame::Ignored)
        }
    }
}

#[derive(Debug, Deserialize)]
struct BingxTickerEnvelope {
    code: i64,
    #[serde(default, rename = "msg")]
    message: Option<String>,
    data: BingxTickerData,
}

#[derive(Debug, Deserialize)]
struct BingxTickerData {
    #[serde(default, rename = "E")]
    event_time: Option<u64>,
    #[serde(default, rename = "T")]
    timestamp: Option<u64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    last: String,
}

#[derive(Debug, Deserialize)]
struct BingxMarkPriceEnvelope {
    code: i64,
    #[serde(default, rename = "msg")]
    message: Option<String>,
    data: BingxMarkPriceData,
}

#[derive(Debug, Deserialize)]
struct BingxMarkPriceData {
    #[serde(default, rename = "E")]
    event_time: Option<u64>,
    #[serde(default, rename = "T")]
    timestamp: Option<u64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    mark_price: String,
}

#[derive(Debug, Deserialize)]
struct BingxBookTickerEnvelope {
    code: i64,
    #[serde(default, rename = "msg")]
    message: Option<String>,
    data: BingxBookTickerData,
}

#[derive(Debug, Deserialize)]
struct BingxBookTickerData {
    #[serde(default, rename = "u")]
    update_id: Option<u64>,
    #[serde(rename = "T")]
    match_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
}

#[derive(Debug, Deserialize)]
struct BingxKlineEnvelope {
    code: i64,
    #[serde(default, rename = "msg")]
    message: Option<String>,
    #[serde(default, rename = "s")]
    symbol: Option<String>,
    data: BingxKlineData,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BingxKlineData {
    Legacy(Vec<BingxLegacyKline>),
    Documented(BingxDocumentedKlineData),
}

impl BingxKlineData {
    fn into_items(
        self,
        legacy_symbol: Option<String>,
    ) -> Result<(String, Vec<BingxKline>), AdapterError> {
        match self {
            Self::Legacy(items) => {
                let symbol = legacy_symbol
                    .ok_or_else(|| invalid_payload("legacy kline message is missing symbol"))?;
                Ok((symbol, items.into_iter().map(BingxKline::from).collect()))
            }
            Self::Documented(data) => Ok((data.symbol, vec![BingxKline::from(data.kline)])),
        }
    }
}

#[derive(Debug, Deserialize)]
struct BingxLegacyKline {
    #[serde(rename = "T")]
    start_time: u64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
}

#[derive(Debug, Deserialize)]
struct BingxDocumentedKlineData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "K")]
    kline: BingxDocumentedKline,
}

#[derive(Debug, Deserialize)]
struct BingxDocumentedKline {
    #[serde(rename = "t")]
    start_time: u64,
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    volume: String,
}

struct BingxKline {
    start_time: u64,
    close_time: Option<u64>,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
}

impl From<BingxLegacyKline> for BingxKline {
    fn from(item: BingxLegacyKline) -> Self {
        Self {
            start_time: item.start_time,
            close_time: None,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
            volume: item.volume,
        }
    }
}

impl From<BingxDocumentedKline> for BingxKline {
    fn from(item: BingxDocumentedKline) -> Self {
        Self {
            start_time: item.start_time,
            close_time: Some(item.close_time),
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
            volume: item.volume,
        }
    }
}

fn validate_bingx_symbol(symbol: &str) -> Result<(), AdapterError> {
    let normalized = normalized_symbol(symbol)?;
    if normalized != symbol || normalized.matches('-').count() != 1 {
        return Err(AdapterError::InvalidSubscription {
            provider: Provider::Bingx,
            message: format!("symbol {symbol:?} must use uppercase BASE-QUOTE grammar"),
        });
    }
    let (base, quote) =
        normalized
            .split_once('-')
            .ok_or_else(|| AdapterError::InvalidSubscription {
                provider: Provider::Bingx,
                message: format!("symbol {symbol:?} must use BASE-QUOTE grammar"),
            })?;
    if base.is_empty()
        || quote.is_empty()
        || !base
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
        || !quote
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
    {
        return Err(AdapterError::InvalidSubscription {
            provider: Provider::Bingx,
            message: format!("symbol {symbol:?} must use ASCII BASE-QUOTE grammar"),
        });
    }
    Ok(())
}

fn topic_symbol(topic: &str, suffix: &str) -> Result<String, AdapterError> {
    let symbol = topic
        .strip_suffix(suffix)
        .ok_or_else(|| invalid_payload(format!("invalid BingX topic {topic:?}")))?;
    normalized_symbol(symbol)
}

fn ensure_topic_symbol(topic_symbol: &str, payload_symbol: &str) -> Result<(), AdapterError> {
    if topic_symbol == payload_symbol {
        Ok(())
    } else {
        Err(invalid_payload(format!(
            "topic symbol {topic_symbol} does not match payload symbol {payload_symbol}"
        )))
    }
}

fn ensure_success(code: i64, message: Option<&str>) -> Result<(), AdapterError> {
    if code == 0 {
        Ok(())
    } else {
        Err(command_rejected(format!(
            "code {code}: {}",
            message.unwrap_or("unknown provider error")
        )))
    }
}

fn control_error_message(object: &Map<String, Value>, code: i64) -> String {
    let message = object
        .get("msg")
        .and_then(Value::as_str)
        .unwrap_or("unknown command error");
    format!("code {code}: {message}")
}

fn observation(
    value: &str,
    observed_at_ms: u64,
    field: &str,
) -> Result<ObservedDecimal, AdapterError> {
    ObservedDecimal::new(value.to_owned(), observed_at_ms)
        .map_err(|error| invalid_payload(format!("invalid {field} {value:?}: {error}")))
}

fn decimal(value: &str, field: &str) -> Result<DecimalValue, AdapterError> {
    DecimalValue::new(value.to_owned())
        .map_err(|error| invalid_payload(format!("invalid {field} {value:?}: {error}")))
}

fn normalized_symbol(symbol: &str) -> Result<String, AdapterError> {
    normalize_symbol(symbol)
        .map_err(|error| invalid_payload(format!("invalid symbol {symbol:?}: {error}")))
}

fn invalid_payload(message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidPayload {
        provider: Provider::Bingx,
        message: message.into(),
    }
}

fn command_rejected(message: impl Into<String>) -> AdapterError {
    AdapterError::CommandRejected {
        provider: Provider::Bingx,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn session() -> BingxSession {
        BingxSession::new(Uuid::nil())
    }

    fn subscription(symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(
            Provider::Bingx,
            MarketKind::LinearPerpetual,
            symbol,
            channel,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn target_and_heartbeat_match_bingx_contract() {
        let target = BingxAdapter::new()
            .connection_target(EndpointKind::Primary, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(target.url.as_str(), BINGX_SWAP_URL);
        assert_eq!(target.command_interval, Duration::from_millis(100));
        assert_eq!(target.subscription_ack_timeout, Duration::from_secs(15));
        assert_eq!(target.heartbeat_interval, Duration::from_secs(4));
        assert_eq!(target.rotate_after, None);
        assert_eq!(session().heartbeat(), Heartbeat::Text("Pong".to_owned()));
    }

    #[test]
    fn subscriptions_expand_deduplicate_and_enforce_topic_limit() {
        let mut session = session();
        let messages = session
            .subscription_messages(
                SubscriptionAction::Subscribe,
                &[
                    subscription("FET-USDT", Channel::Ticker),
                    subscription("FET-USDT", Channel::Candle1m),
                    subscription("FET-USDT", Channel::Ticker),
                ],
            )
            .unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].request_id, "gateway-1");
        assert_eq!(messages[0].expected_acknowledgements, 1);
        let commands = messages
            .iter()
            .map(|message| serde_json::from_str::<Value>(&message.text).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(commands[0]["reqType"], "sub");
        assert_eq!(commands[0]["dataType"], "FET-USDT@ticker");
        assert_eq!(commands[1]["dataType"], "FET-USDT@markPrice");
        assert_eq!(commands[2]["dataType"], "FET-USDT@bookTicker");
        assert_eq!(commands[3]["dataType"], "FET-USDT@kline_1m");

        assert!(
            session
                .subscription_messages(
                    SubscriptionAction::Subscribe,
                    &(0..100)
                        .map(|index| subscription(&format!("COIN{index}-USDT"), Channel::Ticker))
                        .collect::<Vec<_>>(),
                )
                .is_err()
        );
    }

    #[test]
    fn unsubscribe_only_emits_active_topics_and_clears_cached_ticker() {
        let mut session = session();
        let key = subscription("FET-USDT", Channel::Ticker);
        session
            .subscription_messages(SubscriptionAction::Subscribe, std::slice::from_ref(&key))
            .unwrap();
        session
            .tickers
            .insert("FET-USDT".to_owned(), Ticker::default());

        let messages = session
            .subscription_messages(SubscriptionAction::Unsubscribe, std::slice::from_ref(&key))
            .unwrap();
        assert_eq!(messages.len(), 3);
        assert!(
            messages
                .iter()
                .all(|message| message.text.contains("unsub"))
        );
        assert!(!session.tickers.contains_key("FET-USDT"));
        assert!(
            session
                .subscription_messages(SubscriptionAction::Unsubscribe, &[key])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ticker_and_mark_updates_materialize_one_ticker() {
        let mut session = session();
        let ticker = r#"{
            "code":0,"dataType":"FET-USDT@ticker","data":{
              "e":"24hTicker","E":1785558332089,"s":"FET-USDT","p":"0.0001",
              "P":"0.07","c":"0.1420","L":"56","h":"0.1437","l":"0.1402",
              "v":"18854367","q":"2672510.54","o":"0.1419","O":1785557986213,
              "C":1785558331217,"A":"0.1444","a":"242870","B":"0.1417","b":"10116617"
            }}"#;
        let mark = r#"{
            "code":0,"dataType":"FET-USDT@markPrice","data":{
              "e":"markPriceUpdate","E":1785558332239,"s":"FET-USDT","p":"0.1420"
            }}"#;
        let book = r#"{
            "code":0,"dataType":"FET-USDT@bookTicker","data":{
              "e":"bookTicker","u":267107655,"E":1785558331765,"T":1785558331777,
              "s":"FET-USDT","b":"0.1417","B":"10116617","a":"0.1422","A":"7223530"
            }}"#;

        session.parse(ticker, 1_785_558_332_100).unwrap();
        session.parse(mark, 1_785_558_332_250).unwrap();
        let ParsedFrame::Events(events) = session.parse(book, 1_785_558_332_260).unwrap() else {
            panic!("expected events");
        };
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(ticker.last.as_ref().unwrap().value.as_str(), "0.1420");
        assert_eq!(
            ticker.last.as_ref().unwrap().observed_at_ms,
            1_785_558_332_089
        );
        assert_eq!(ticker.bid.as_ref().unwrap().value.as_str(), "0.1417");
        assert_eq!(ticker.ask.as_ref().unwrap().value.as_str(), "0.1422");
        assert_eq!(
            ticker.ask.as_ref().unwrap().observed_at_ms,
            1_785_558_331_777
        );
        assert_eq!(ticker.mark.as_ref().unwrap().value.as_str(), "0.1420");
        assert_eq!(
            ticker.mark.as_ref().unwrap().observed_at_ms,
            1_785_558_332_239
        );
        assert_eq!(ticker.index, None);
        assert_eq!(ticker.funding_rate, None);
        assert_eq!(
            events[0].source_sequence.as_ref().unwrap().last.as_deref(),
            Some("267107655")
        );
    }

    #[test]
    fn kline_preserves_base_volume_and_unknown_finality() {
        let frame = r#"{
          "code":0,"dataType":"FET-USDT@kline_1m","s":"FET-USDT",
          "data":[{"c":"0.1420","o":"0.1419","h":"0.1420","l":"0.1419",
                   "v":"6835","T":1785558300000}]
        }"#;
        let ParsedFrame::Events(events) = session().parse(frame, 1_785_558_332_300).unwrap() else {
            panic!("expected events");
        };
        assert_eq!(events[0].exchange_time_ms, None);
        let MarketPayload::Candle(candle) = &events[0].payload else {
            panic!("expected candle");
        };
        assert_eq!(candle.start_time_ms, 1_785_558_300_000);
        assert_eq!(candle.end_time_ms, 1_785_558_360_000);
        assert_eq!(candle.base_volume.as_ref().unwrap().as_str(), "6835");
        assert_eq!(candle.quote_volume, None);
        assert_eq!(candle.finality, CandleFinality::Unknown);
    }

    #[test]
    fn documented_and_timestamp_sparse_shapes_remain_compatible() {
        let mut session = session();
        let ticker = r#"{
          "code":0,"dataType":"FET-USDT@ticker","data":{
            "s":"FET-USDT","c":"0.1420"
          }
        }"#;
        let ParsedFrame::Events(events) = session.parse(ticker, 1_785_558_332_100).unwrap() else {
            panic!("expected ticker event");
        };
        assert_eq!(events[0].exchange_time_ms, Some(1_785_558_332_100));
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(
            ticker.last.as_ref().unwrap().observed_at_ms,
            1_785_558_332_100
        );

        let book = r#"{
          "code":0,"dataType":"FET-USDT@bookTicker","data":{
            "T":1785558332177,"s":"FET-USDT","b":"0.1417","a":"0.1422"
          }
        }"#;
        let ParsedFrame::Events(events) = session.parse(book, 1_785_558_332_200).unwrap() else {
            panic!("expected book-ticker event");
        };
        assert_eq!(events[0].source_sequence, None);

        let documented_kline = r#"{
          "code":0,"dataType":"FET-USDT@kline_1m","data":{
            "s":"FET-USDT","K":{"t":1785558300000,"T":1785558359999,
              "o":"0.1419","h":"0.1420","l":"0.1419","c":"0.1420","v":"6835"}
          }
        }"#;
        let ParsedFrame::Events(events) =
            session.parse(documented_kline, 1_785_558_332_300).unwrap()
        else {
            panic!("expected documented kline event");
        };
        assert_eq!(events[0].symbol, "FET-USDT");
        let MarketPayload::Candle(candle) = &events[0].payload else {
            panic!("expected candle");
        };
        assert_eq!(candle.start_time_ms, 1_785_558_300_000);
        assert_eq!(candle.end_time_ms, 1_785_558_360_000);
        assert_eq!(candle.base_volume.as_ref().unwrap().as_str(), "6835");
        assert_eq!(candle.finality, CandleFinality::Unknown);
    }

    #[test]
    fn acknowledgements_ping_errors_and_malformed_frames_are_explicit() {
        let mut session = session();
        assert_eq!(
            session
                .parse(
                    r#"{"id":"gateway-1","code":0,"msg":"","dataType":"","data":null}"#,
                    1
                )
                .unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "gateway-1".to_owned()
            }
        );
        assert_eq!(
            session.parse("Ping", 1).unwrap(),
            ParsedFrame::Reply(Heartbeat::Text("Pong".to_owned()))
        );
        assert!(matches!(
            session.parse(
                r#"{"id":"gateway-2","code":80403,"msg":"too many topics"}"#,
                1
            ),
            Err(AdapterError::CommandRejected { .. })
        ));
        assert!(matches!(
            session.parse(
                r#"{"code":0,"dataType":"BTC-USDT@markPrice","data":{"E":10,"s":"ETH-USDT","p":"1"}}"#,
                11
            ),
            Err(AdapterError::InvalidPayload { .. })
        ));
        assert!(matches!(
            session.parse(
                r#"{"code":0,"dataType":"FET-USDT@ticker","data":{"E":10,"s":"FET-USDT","c":0.142}}"#,
                11
            ),
            Err(AdapterError::InvalidPayload { .. })
        ));
        assert!(matches!(
            session.parse("not-json", 1),
            Err(AdapterError::InvalidPayload { .. })
        ));
    }
}
