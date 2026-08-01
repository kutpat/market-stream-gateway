use std::collections::{HashMap, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use super::{
    AdapterError, AdapterSession, ConnectionTarget, EndpointKind, Heartbeat, OutboundCommand,
    ParsedFrame, ProviderAdapter, SubscriptionAction, validate_subscriptions,
};
use crate::domain::{
    Candle, CandleFinality, Channel, DataQuality, DecimalValue, MarketKind, MarketPayload,
    ObservedDecimal, Provider, ProviderEvent, SourceSequence, SubscriptionKey, Ticker,
};

const PRODUCTION_FUTURES_REST_URL: &str = "https://api-futures.kucoin.com";
const BULLET_PUBLIC_PATH: &str = "/api/v1/bullet-public";
// Eight commands per second stays below KuCoin's 100 messages per ten seconds while
// preserving capacity for the token-advertised heartbeat.
const COMMAND_INTERVAL: Duration = Duration::from_millis(125);
const SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);
const TOPICS_PER_COMMAND: usize = 100;
const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const ROTATE_AFTER: Duration = Duration::from_mins(1_410);
const ENDPOINTS: &[EndpointKind] = &[EndpointKind::Primary];

/// Adapter for `KuCoin` Classic Futures public linear-perpetual streams.
#[derive(Debug, Clone)]
pub struct KucoinAdapter {
    futures_rest_url: Url,
}

impl KucoinAdapter {
    /// Create an adapter using an explicit `KuCoin` Classic Futures REST base URL.
    pub fn new(futures_rest_url: Url) -> Self {
        Self { futures_rest_url }
    }
}

impl Default for KucoinAdapter {
    fn default() -> Self {
        Self::new(
            Url::parse(PRODUCTION_FUTURES_REST_URL)
                .expect("hard-coded KuCoin Futures REST URL must be valid"),
        )
    }
}

#[async_trait]
impl ProviderAdapter for KucoinAdapter {
    fn provider(&self) -> Provider {
        Provider::Kucoin
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
        http: &reqwest::Client,
    ) -> Result<ConnectionTarget, AdapterError> {
        if endpoint != EndpointKind::Primary {
            return Err(AdapterError::UnsupportedEndpoint {
                provider: Provider::Kucoin,
                endpoint,
            });
        }
        let bullet_url = self
            .futures_rest_url
            .join(BULLET_PUBLIC_PATH)
            .map_err(|error| discovery(format!("invalid Futures REST base URL: {error}")))?;
        let response = http
            .post(bullet_url)
            .send()
            .await
            .map_err(|error| discovery(format!("bullet-public request failed: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(discovery(format!("bullet-public returned HTTP {status}")));
        }
        let response: BulletResponse = response
            .json()
            .await
            .map_err(|error| discovery(format!("invalid bullet-public response: {error}")))?;
        target_from_discovery(response, Uuid::new_v4())
    }

    fn session(&self, endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession> {
        Box::new(KucoinSession {
            endpoint,
            connection_epoch,
            next_request_id: 1,
            tickers: HashMap::new(),
        })
    }
}

#[derive(Debug, Deserialize)]
struct BulletResponse {
    code: String,
    data: Option<BulletData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulletData {
    token: String,
    instance_servers: Vec<InstanceServer>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstanceServer {
    endpoint: String,
    encrypt: bool,
    protocol: String,
    ping_interval: u64,
    ping_timeout: u64,
}

fn target_from_discovery(
    response: BulletResponse,
    connect_id: Uuid,
) -> Result<ConnectionTarget, AdapterError> {
    if response.code != "200000" {
        return Err(discovery(format!(
            "bullet-public returned code {}",
            response.code
        )));
    }
    let data = response
        .data
        .ok_or_else(|| discovery("bullet-public response is missing data"))?;
    if data.token.is_empty() {
        return Err(discovery("bullet-public returned an empty token"));
    }
    let server = data
        .instance_servers
        .iter()
        .find(|server| server.encrypt && server.protocol.eq_ignore_ascii_case("websocket"))
        .ok_or_else(|| discovery("bullet-public returned no encrypted websocket server"))?;
    if server.ping_interval == 0 || server.ping_timeout == 0 {
        return Err(discovery(
            "bullet-public returned invalid heartbeat timings",
        ));
    }
    let mut url = Url::parse(&server.endpoint)
        .map_err(|error| discovery(format!("invalid websocket endpoint: {error}")))?;
    if url.scheme() != "wss" {
        return Err(discovery("encrypted websocket endpoint must use wss"));
    }
    url.query_pairs_mut()
        .append_pair("token", &data.token)
        .append_pair("connectId", &connect_id.simple().to_string());

    // Sending halfway through the advertised interval leaves headroom for scheduler and network
    // jitter while still deriving the cadence from the server-provided configuration.
    let heartbeat_interval = Duration::from_millis((server.ping_interval / 2).max(1));
    let stale_after = Duration::from_millis(
        server
            .ping_interval
            .checked_add(server.ping_timeout)
            .ok_or_else(|| discovery("heartbeat timing overflow"))?,
    );
    Ok(ConnectionTarget {
        url,
        command_interval: COMMAND_INTERVAL,
        subscription_ack_timeout: SUBSCRIPTION_ACK_TIMEOUT,
        heartbeat_interval,
        stale_after,
        max_message_bytes: MAX_MESSAGE_BYTES,
        rotate_after: Some(ROTATE_AFTER),
    })
}

#[derive(Debug)]
struct KucoinSession {
    endpoint: EndpointKind,
    connection_epoch: Uuid,
    next_request_id: u64,
    tickers: HashMap<String, Ticker>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KucoinCommand<'a> {
    id: String,
    #[serde(rename = "type")]
    command_type: &'a str,
    topic: String,
    private_channel: bool,
    response: bool,
}

impl AdapterSession for KucoinSession {
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        validate_subscriptions(Provider::Kucoin, subscriptions)?;
        if self.endpoint != EndpointKind::Primary {
            return Err(AdapterError::InvalidSubscription {
                provider: Provider::Kucoin,
                message: format!(
                    "subscriptions do not belong on the {} endpoint",
                    self.endpoint
                ),
            });
        }
        let (ticker_symbols, candle_symbols) = collect_symbols(subscriptions)?;
        if action == SubscriptionAction::Unsubscribe {
            for symbol in &ticker_symbols {
                self.tickers.remove(symbol);
            }
        }
        let topics = build_topics(&ticker_symbols, &candle_symbols);
        let command_type = match action {
            SubscriptionAction::Subscribe => "subscribe",
            SubscriptionAction::Unsubscribe => "unsubscribe",
        };
        topics
            .into_iter()
            .map(|topic| {
                let request_id = self.take_request_id();
                let command = KucoinCommand {
                    id: request_id.clone(),
                    command_type,
                    topic,
                    private_channel: false,
                    response: true,
                };
                let text =
                    serde_json::to_string(&command).map_err(|error| invalid(error.to_string()))?;
                Ok(OutboundCommand {
                    request_id,
                    text,
                    expected_acknowledgements: 1,
                })
            })
            .collect()
    }

    fn heartbeat(&mut self) -> Heartbeat {
        let id = self.take_request_id();
        Heartbeat::Text(format!(r#"{{"id":"{id}","type":"ping"}}"#))
    }

    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        let value: Value = serde_json::from_str(text)
            .map_err(|error| invalid(format!("invalid JSON: {error}")))?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("message must be a JSON object"))?;
        match object.get("type") {
            Some(Value::String(message_type)) => match message_type.as_str() {
                "ack" => {
                    let request_id = object
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|request_id| !request_id.is_empty() && request_id.len() <= 64)
                        .ok_or_else(|| invalid("ack is missing a valid request id"))?;
                    Ok(ParsedFrame::Acknowledgement {
                        request_id: request_id.to_owned(),
                    })
                }
                "pong" => Ok(ParsedFrame::Pong),
                "error" => Err(command_error(object)),
                "message" => self.parse_market_message(object, text, received_at_ms),
                // The initial `welcome` frame and unknown control extensions are not command ACKs.
                _ => Ok(ParsedFrame::Ignored),
            },
            Some(_) => Err(invalid("type must be a string")),
            None if object.contains_key("topic") => {
                self.parse_market_message(object, text, received_at_ms)
            }
            None => Ok(ParsedFrame::Ignored),
        }
    }
}

impl KucoinSession {
    fn take_request_id(&mut self) -> String {
        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        id.to_string()
    }

    fn parse_market_message(
        &mut self,
        object: &serde_json::Map<String, Value>,
        text: &str,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let topic = required_string(object, "topic")?;
        let subject = object.get("subject").and_then(Value::as_str);
        if subject == Some("tickerV2") || topic.starts_with("/contractMarket/tickerV2:") {
            self.parse_ticker_v2(object, received_at_ms)
        } else if subject == Some("match") || topic.starts_with("/contractMarket/execution:") {
            self.parse_execution(object, received_at_ms)
        } else if subject == Some("mark.index.price") {
            self.parse_mark_index(object, topic, text, received_at_ms)
        } else if subject == Some("funding.rate") {
            self.parse_funding(object, topic, text, received_at_ms)
        } else if subject == Some("candle.stick")
            || topic.starts_with("/contractMarket/limitCandle:")
        {
            self.parse_candle(object, received_at_ms)
        } else {
            Ok(ParsedFrame::Ignored)
        }
    }

    fn parse_ticker_v2(
        &mut self,
        object: &serde_json::Map<String, Value>,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let topic = required_string(object, "topic")?;
        let data = required_object(object, "data")?;
        let symbol = required_linear_symbol(data, "symbol")?.to_owned();
        validate_topic_symbol(topic, "/contractMarket/tickerV2:", &symbol, "")?;
        let observed_at_ms = nanoseconds_to_milliseconds(required_value(data, "ts")?, "ts")?;
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.bid = Some(observation_from_field(
            data,
            "bestBidPrice",
            observed_at_ms,
            None,
        )?);
        ticker.ask = Some(observation_from_field(
            data,
            "bestAskPrice",
            observed_at_ms,
            None,
        )?);
        self.tickers.insert(symbol.clone(), ticker.clone());
        let sequence = parse_sequence(object, data)?;
        Ok(ParsedFrame::Events(vec![self.ticker_event(
            symbol,
            observed_at_ms,
            received_at_ms,
            sequence,
            ticker,
        )]))
    }

    fn parse_execution(
        &mut self,
        object: &serde_json::Map<String, Value>,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let topic = required_string(object, "topic")?;
        let data = required_object(object, "data")?;
        let symbol = required_linear_symbol(data, "symbol")?.to_owned();
        validate_topic_symbol(topic, "/contractMarket/execution:", &symbol, "")?;
        let observed_at_ms = nanoseconds_to_milliseconds(required_value(data, "ts")?, "ts")?;
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.last = Some(observation_from_field(data, "price", observed_at_ms, None)?);
        self.tickers.insert(symbol.clone(), ticker.clone());
        let sequence = parse_sequence(object, data)?;
        Ok(ParsedFrame::Events(vec![self.ticker_event(
            symbol,
            observed_at_ms,
            received_at_ms,
            sequence,
            ticker,
        )]))
    }

    fn parse_mark_index(
        &mut self,
        object: &serde_json::Map<String, Value>,
        topic: &str,
        text: &str,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let symbol = instrument_topic_symbol(topic)?;
        let data = required_object(object, "data")?;
        let observed_at_ms = parse_u64(required_value(data, "timestamp")?, "timestamp")?;
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.mark = Some(observation_from_field(
            data,
            "markPrice",
            observed_at_ms,
            Some(text),
        )?);
        ticker.index = Some(observation_from_field(
            data,
            "indexPrice",
            observed_at_ms,
            Some(text),
        )?);
        self.tickers.insert(symbol.clone(), ticker.clone());
        Ok(ParsedFrame::Events(vec![self.ticker_event(
            symbol,
            observed_at_ms,
            received_at_ms,
            None,
            ticker,
        )]))
    }

    fn parse_funding(
        &mut self,
        object: &serde_json::Map<String, Value>,
        topic: &str,
        text: &str,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let symbol = instrument_topic_symbol(topic)?;
        let data = required_object(object, "data")?;
        let observed_at_ms = parse_u64(required_value(data, "timestamp")?, "timestamp")?;
        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.funding_rate = Some(observation_from_field(
            data,
            "fundingRate",
            observed_at_ms,
            Some(text),
        )?);
        self.tickers.insert(symbol.clone(), ticker.clone());
        Ok(ParsedFrame::Events(vec![self.ticker_event(
            symbol,
            observed_at_ms,
            received_at_ms,
            None,
            ticker,
        )]))
    }

    fn parse_candle(
        &self,
        object: &serde_json::Map<String, Value>,
        received_at_ms: u64,
    ) -> Result<ParsedFrame, AdapterError> {
        let topic = required_string(object, "topic")?;
        let data = required_object(object, "data")?;
        let symbol = required_linear_symbol(data, "symbol")?.to_owned();
        validate_topic_symbol(topic, "/contractMarket/limitCandle:", &symbol, "_1min")?;
        let values = data
            .get("candles")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("candles must be an array"))?;
        if values.len() != 7 {
            return Err(invalid(format!(
                "candles must contain 7 fields, got {}",
                values.len()
            )));
        }
        let start_time_ms = parse_u64(&values[0], "candle start time")?
            .checked_mul(1_000)
            .ok_or_else(|| invalid("candle start time overflow"))?;
        let end_time_ms = start_time_ms
            .checked_add(60_000)
            .ok_or_else(|| invalid("candle end time overflow"))?;
        let exchange_time_ms = parse_u64(required_value(data, "time")?, "time")?;
        let candle = Candle {
            interval: "1m".to_owned(),
            start_time_ms,
            end_time_ms,
            open: decimal_value(&values[1], "candle open")?,
            close: decimal_value(&values[2], "candle close")?,
            high: decimal_value(&values[3], "candle high")?,
            low: decimal_value(&values[4], "candle low")?,
            // KuCoin explicitly documents the Classic Futures websocket candle volume as
            // incorrect. Preserve the warning without exposing either ambiguous value as data.
            base_volume: None,
            contract_volume: None,
            quote_volume: None,
            finality: CandleFinality::Unknown,
            data_quality: vec![DataQuality::ProviderVolumeUntrusted],
        };
        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Kucoin,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(exchange_time_ms),
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload: MarketPayload::Candle(candle),
        }]))
    }

    fn ticker_event(
        &self,
        symbol: String,
        exchange_time_ms: u64,
        received_at_ms: u64,
        source_sequence: Option<SourceSequence>,
        ticker: Ticker,
    ) -> ProviderEvent {
        ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Kucoin,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(exchange_time_ms),
            gateway_received_time_ms: received_at_ms,
            source_sequence,
            payload: MarketPayload::Ticker(ticker),
        }
    }
}

fn collect_symbols(
    subscriptions: &[SubscriptionKey],
) -> Result<(Vec<String>, Vec<String>), AdapterError> {
    let mut ticker_symbols = Vec::new();
    let mut candle_symbols = Vec::new();
    let mut seen_tickers = HashSet::new();
    let mut seen_candles = HashSet::new();
    for subscription in subscriptions {
        validate_kucoin_symbol(&subscription.symbol).map_err(|message| {
            AdapterError::InvalidSubscription {
                provider: Provider::Kucoin,
                message,
            }
        })?;
        match subscription.channel {
            Channel::Ticker if seen_tickers.insert(subscription.symbol.clone()) => {
                ticker_symbols.push(subscription.symbol.clone());
            }
            Channel::Candle1m if seen_candles.insert(subscription.symbol.clone()) => {
                candle_symbols.push(subscription.symbol.clone());
            }
            Channel::Ticker | Channel::Candle1m => {}
        }
    }
    Ok((ticker_symbols, candle_symbols))
}

fn build_topics(ticker_symbols: &[String], candle_symbols: &[String]) -> Vec<String> {
    let mut topics = Vec::new();
    // These three Classic Futures channel specifications guarantee only one symbol per topic.
    for symbol in ticker_symbols {
        topics.push(format!("/contractMarket/tickerV2:{symbol}"));
        topics.push(format!("/contractMarket/execution:{symbol}"));
        topics.push(format!("/contract/instrument:{symbol}"));
    }
    for symbols in candle_symbols.chunks(TOPICS_PER_COMMAND) {
        let symbols = symbols
            .iter()
            .map(|symbol| format!("{symbol}_1min"))
            .collect::<Vec<_>>()
            .join(",");
        topics.push(format!("/contractMarket/limitCandle:{symbols}"));
    }
    topics
}

fn parse_sequence(
    object: &serde_json::Map<String, Value>,
    data: &serde_json::Map<String, Value>,
) -> Result<Option<SourceSequence>, AdapterError> {
    let envelope_sequence = integer_string(required_value(object, "sn")?, "sn")?;
    let data_sequence = integer_string(required_value(data, "sequence")?, "sequence")?;
    if envelope_sequence != data_sequence {
        return Err(invalid(format!(
            "envelope sequence {envelope_sequence} does not match data sequence {data_sequence}"
        )));
    }
    Ok(Some(SourceSequence {
        first: None,
        last: Some(data_sequence),
        previous: None,
    }))
}

fn command_error(object: &serde_json::Map<String, Value>) -> AdapterError {
    let code = object
        .get("code")
        .map_or_else(|| "unknown".to_owned(), Value::to_string);
    let message = object
        .get("msg")
        .or_else(|| object.get("data"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error");
    AdapterError::CommandRejected {
        provider: Provider::Kucoin,
        message: format!("code {code}: {message}"),
    }
}

fn instrument_topic_symbol(topic: &str) -> Result<String, AdapterError> {
    let symbol = topic
        .strip_prefix("/contract/instrument:")
        .filter(|symbol| !symbol.is_empty() && !symbol.contains(','))
        .ok_or_else(|| invalid("invalid instrument topic"))?;
    validate_kucoin_symbol(symbol).map_err(invalid)?;
    Ok(symbol.to_owned())
}

fn validate_topic_symbol(
    topic: &str,
    prefix: &str,
    symbol: &str,
    suffix: &str,
) -> Result<(), AdapterError> {
    let topic_symbols = topic
        .strip_prefix(prefix)
        .ok_or_else(|| invalid(format!("topic must start with {prefix}")))?;
    let expected = format!("{symbol}{suffix}");
    if topic_symbols.split(',').any(|item| item == expected) {
        Ok(())
    } else {
        Err(invalid(format!(
            "payload symbol {symbol} does not match topic {topic}"
        )))
    }
}

fn required_linear_symbol<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, AdapterError> {
    let symbol = required_string(object, field)?;
    validate_kucoin_symbol(symbol).map_err(invalid)?;
    Ok(symbol)
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

fn required_value<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a Value, AdapterError> {
    object
        .get(field)
        .ok_or_else(|| invalid(format!("missing {field}")))
}

fn observation_from_field(
    object: &serde_json::Map<String, Value>,
    field: &str,
    observed_at_ms: u64,
    original_json: Option<&str>,
) -> Result<ObservedDecimal, AdapterError> {
    let value = required_value(object, field)?;
    let value = decimal_string(value, field, original_json)?;
    ObservedDecimal::new(value, observed_at_ms).map_err(|error| invalid(error.to_string()))
}

fn decimal_value(value: &Value, field: &str) -> Result<DecimalValue, AdapterError> {
    let value = decimal_string(value, field, None)?;
    DecimalValue::new(value).map_err(|error| invalid(error.to_string()))
}

fn decimal_string(
    value: &Value,
    field: &str,
    original_json: Option<&str>,
) -> Result<String, AdapterError> {
    match value {
        Value::String(value) if !value.is_empty() => Ok(value.clone()),
        Value::Number(_) => original_json
            .and_then(|text| raw_number_field(text, field))
            .map(str::to_owned)
            .or_else(|| Some(value.to_string()))
            .ok_or_else(|| invalid(format!("could not read {field}"))),
        _ => Err(invalid(format!(
            "{field} must be a decimal string or number"
        ))),
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

fn parse_u64(value: &Value, field: &str) -> Result<u64, AdapterError> {
    integer_string(value, field)?
        .parse::<u64>()
        .map_err(|_| invalid(format!("{field} must be an unsigned integer")))
}

fn nanoseconds_to_milliseconds(value: &Value, field: &str) -> Result<u64, AdapterError> {
    Ok(parse_u64(value, field)? / 1_000_000)
}

fn integer_string(value: &Value, field: &str) -> Result<String, AdapterError> {
    let value = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return Err(invalid(format!("{field} must be an integer"))),
    };
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid(format!("{field} must be an unsigned integer")));
    }
    Ok(value)
}

fn validate_kucoin_symbol(symbol: &str) -> Result<(), String> {
    if symbol.ends_with("USDTM") || symbol.ends_with("USDCM") {
        Ok(())
    } else {
        Err(format!(
            "{symbol} is not a KuCoin USDT/USDC linear perpetual symbol"
        ))
    }
}

fn invalid(message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidPayload {
        provider: Provider::Kucoin,
        message: message.into(),
    }
}

fn discovery(message: impl Into<String>) -> AdapterError {
    AdapterError::Discovery {
        provider: Provider::Kucoin,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn subscription(symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(
            Provider::Kucoin,
            MarketKind::LinearPerpetual,
            symbol,
            channel,
        )
        .unwrap()
    }

    fn session() -> Box<dyn AdapterSession> {
        KucoinAdapter::default().session(EndpointKind::Primary, Uuid::nil())
    }

    fn ticker(frame: ParsedFrame) -> Ticker {
        let ParsedFrame::Events(events) = frame else {
            panic!("expected events");
        };
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        ticker.clone()
    }

    #[test]
    fn discovery_uses_server_heartbeat_and_authenticated_url() {
        let response: BulletResponse = serde_json::from_str(
            r#"{"code":"200000","data":{"token":"public-token","instanceServers":[{"endpoint":"wss://ws-api-futures.kucoin.com/","encrypt":true,"protocol":"websocket","pingInterval":18000,"pingTimeout":10000}]}}"#,
        )
        .unwrap();
        let target = target_from_discovery(response, Uuid::nil()).unwrap();
        let query = target.url.query_pairs().collect::<HashMap<_, _>>();
        assert_eq!(query.get("token").unwrap(), "public-token");
        assert_eq!(query.get("connectId").unwrap().len(), 32);
        assert_eq!(target.heartbeat_interval, Duration::from_secs(9));
        assert_eq!(target.stale_after, Duration::from_secs(28));
        assert_eq!(target.command_interval, Duration::from_millis(125));
        assert_eq!(target.subscription_ack_timeout, Duration::from_secs(15));
        assert_eq!(target.rotate_after, Some(ROTATE_AFTER));
    }

    #[test]
    fn batches_dynamic_ticker_and_candle_commands() {
        let mut session = session();
        let subscriptions = vec![
            subscription("XBTUSDTM", Channel::Ticker),
            subscription("ETHUSDTM", Channel::Ticker),
            subscription("XBTUSDTM", Channel::Candle1m),
        ];
        let subscribe = session
            .subscription_messages(SubscriptionAction::Subscribe, &subscriptions)
            .unwrap();
        let unsubscribe = session
            .subscription_messages(SubscriptionAction::Unsubscribe, &subscriptions)
            .unwrap();
        assert_eq!(subscribe.len(), 7);
        assert_eq!(subscribe[0].expected_acknowledgements, 1);
        assert!(
            subscribe[0]
                .text
                .contains("/contractMarket/tickerV2:XBTUSDTM")
        );
        assert!(subscribe[2].text.contains("/contract/instrument:XBTUSDTM"));
        assert!(
            subscribe[3]
                .text
                .contains("/contractMarket/tickerV2:ETHUSDTM")
        );
        assert!(subscribe[6].text.contains("XBTUSDTM_1min"));
        assert!(
            unsubscribe
                .iter()
                .all(|message| message.text.contains("unsubscribe"))
        );
    }

    #[test]
    fn merges_bbo_execution_and_instrument_state() {
        let mut session = session();
        let bbo = r#"{"topic":"/contractMarket/tickerV2:XBTUSDTM","type":"message","subject":"tickerV2","sn":1713516609293,"data":{"symbol":"XBTUSDTM","sequence":1713516609293,"bestBidSize":5044,"bestBidPrice":"86454.500","bestAskPrice":"86454.6","bestAskSize":73,"ts":1740641976241000000}}"#;
        let execution = r#"{"topic":"/contractMarket/execution:XBTUSDTM","type":"message","subject":"match","sn":1794100537695,"data":{"symbol":"XBTUSDTM","sequence":1794100537695,"price":"90503.9000","ts":1731898619520000000}}"#;
        let instrument = r#"{"topic":"/contract/instrument:XBTUSDTM","type":"message","subject":"mark.index.price","data":{"markPrice":90445.020000000000000001,"indexPrice":90444.9900,"granularity":1000,"timestamp":1731899129000}}"#;

        let value = ticker(session.parse(bbo, 1).unwrap());
        assert_eq!(value.bid.unwrap().value.as_str(), "86454.500");
        let value = ticker(session.parse(execution, 2).unwrap());
        assert_eq!(value.last.unwrap().value.as_str(), "90503.9000");
        assert!(value.bid.is_some());
        let value = ticker(session.parse(instrument, 3).unwrap());
        assert_eq!(
            value.mark.unwrap().value.as_str(),
            "90445.020000000000000001"
        );
        assert_eq!(value.index.unwrap().value.as_str(), "90444.9900");
        assert!(value.last.is_some());
    }

    #[test]
    fn parses_funding_and_untrusted_unknown_finality_candle() {
        let mut session = session();
        let funding = r#"{"topic":"/contract/instrument:XBTUSDTM","type":"message","subject":"funding.rate","data":{"granularity":60000,"fundingRate":-0.002966000000000001,"timestamp":1551770400000}}"#;
        let candle = r#"{"topic":"/contractMarket/limitCandle:XBTUSDTM_1min","type":"message","subject":"candle.stick","data":{"symbol":"XBTUSDTM","candles":["1731898200","90638.60","90638.6","90640.1","90630.2","21.0","1903410.6"],"time":1731898208357}}"#;

        let value = ticker(session.parse(funding, 1).unwrap());
        assert_eq!(
            value.funding_rate.unwrap().value.as_str(),
            "-0.002966000000000001"
        );

        let ParsedFrame::Events(events) = session.parse(candle, 2).unwrap() else {
            panic!("expected candle event");
        };
        let MarketPayload::Candle(value) = &events[0].payload else {
            panic!("expected candle");
        };
        assert_eq!(value.start_time_ms, 1_731_898_200_000);
        assert_eq!(value.open.as_str(), "90638.60");
        assert_eq!(value.finality, CandleFinality::Unknown);
        assert_eq!(
            value.data_quality,
            vec![DataQuality::ProviderVolumeUntrusted]
        );
        assert!(value.base_volume.is_none());
        assert!(value.contract_volume.is_none());
        assert!(value.quote_volume.is_none());
    }

    #[test]
    fn handles_control_frames_and_malformed_payloads() {
        let mut session = session();
        assert_eq!(
            session.parse(r#"{"id":"1","type":"welcome"}"#, 1).unwrap(),
            ParsedFrame::Ignored
        );
        assert_eq!(
            session.parse(r#"{"id":"1","type":"ack"}"#, 1).unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "1".to_owned()
            }
        );
        assert_eq!(
            session.parse(r#"{"id":"1","type":"pong"}"#, 1).unwrap(),
            ParsedFrame::Pong
        );
        assert!(matches!(
            session.parse(
                r#"{"id":"1","type":"error","code":400,"data":"bad topic"}"#,
                1
            ),
            Err(AdapterError::CommandRejected { .. })
        ));
        assert!(session.parse("{", 1).is_err());
        assert!(
            session
                .parse(
                    r#"{"topic":"/contractMarket/tickerV2:XBTUSDTM","type":"message","subject":"tickerV2","sn":1,"data":{"symbol":"XBTUSDTM","sequence":2,"bestBidPrice":"1","bestAskPrice":"2","ts":1000000}}"#,
                    1
                )
                .is_err()
        );
    }

    #[test]
    fn rejects_inverse_futures_symbols() {
        let mut session = session();
        assert!(
            session
                .subscription_messages(
                    SubscriptionAction::Subscribe,
                    &[subscription("XBTUSDM", Channel::Ticker)]
                )
                .is_err()
        );
    }
}
