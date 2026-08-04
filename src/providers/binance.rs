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

// Binance retired the legacy unrouted futures websocket in April 2026. Ticker, mark-price, and
// kline streams all belong on the regular-market route.
const BINANCE_MARKET_URL: &str = "wss://fstream.binance.com/market/ws";
// Subscription commands use at most eight of Binance's ten inbound messages per second,
// leaving capacity for protocol ping/pong traffic.
const BINANCE_COMMAND_INTERVAL: Duration = Duration::from_millis(125);
const BINANCE_SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(15);
const BINANCE_HEARTBEAT_INTERVAL: Duration = Duration::from_mins(3);
const BINANCE_STALE_AFTER: Duration = Duration::from_mins(10);
const BINANCE_ROTATE_AFTER: Duration = Duration::from_mins(23 * 60 + 55);
const BINANCE_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const BINANCE_MAX_STREAMS_PER_COMMAND: usize = 200;
const ENDPOINTS: &[EndpointKind] = &[EndpointKind::Primary];

#[derive(Debug, Clone)]
pub struct BinanceAdapter {
    url: Url,
}

impl Default for BinanceAdapter {
    fn default() -> Self {
        Self {
            url: Url::parse(BINANCE_MARKET_URL).expect("the official Binance URL must be valid"),
        }
    }
}

impl BinanceAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_url(url: Url) -> Self {
        Self { url }
    }
}

#[async_trait]
impl ProviderAdapter for BinanceAdapter {
    fn provider(&self) -> Provider {
        Provider::Binance
    }

    fn endpoints(&self) -> &'static [EndpointKind] {
        ENDPOINTS
    }

    fn endpoint_for(&self, _channel: Channel) -> EndpointKind {
        EndpointKind::Primary
    }

    fn max_subscriptions(&self) -> usize {
        // Conservative: the documented stream batch bound is the tightest limit
        // this repository has verified for Binance, so it is reused as the
        // provider ceiling rather than assuming a larger per-connection budget.
        BINANCE_MAX_STREAMS_PER_COMMAND
    }

    async fn connection_target(
        &self,
        endpoint: EndpointKind,
        _http: &reqwest::Client,
    ) -> Result<ConnectionTarget, AdapterError> {
        if endpoint != EndpointKind::Primary {
            return Err(AdapterError::UnsupportedEndpoint {
                provider: Provider::Binance,
                endpoint,
            });
        }
        Ok(ConnectionTarget {
            url: self.url.clone(),
            command_interval: BINANCE_COMMAND_INTERVAL,
            subscription_ack_timeout: BINANCE_SUBSCRIPTION_ACK_TIMEOUT,
            heartbeat_interval: BINANCE_HEARTBEAT_INTERVAL,
            stale_after: BINANCE_STALE_AFTER,
            max_message_bytes: BINANCE_MAX_MESSAGE_BYTES,
            rotate_after: Some(BINANCE_ROTATE_AFTER),
        })
    }

    fn session(&self, _endpoint: EndpointKind, connection_epoch: Uuid) -> Box<dyn AdapterSession> {
        Box::new(BinanceSession::new(connection_epoch))
    }
}

struct BinanceSession {
    connection_epoch: Uuid,
    next_request_id: u64,
    tickers: HashMap<String, Ticker>,
}

impl BinanceSession {
    fn new(connection_epoch: Uuid) -> Self {
        Self {
            connection_epoch,
            next_request_id: 1,
            tickers: HashMap::new(),
        }
    }

    fn request_id(&mut self) -> u64 {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).unwrap_or(1);
        request_id
    }

    fn command_messages(
        &mut self,
        action: SubscriptionAction,
        streams: &[String],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        let method = match action {
            SubscriptionAction::Subscribe => "SUBSCRIBE",
            SubscriptionAction::Unsubscribe => "UNSUBSCRIBE",
        };
        streams
            .chunks(BINANCE_MAX_STREAMS_PER_COMMAND)
            .map(|batch| {
                let id = self.request_id();
                let text = serde_json::to_string(&json!({
                    "method": method,
                    "params": batch,
                    "id": id,
                }))
                .map_err(|error| invalid_payload(format!("could not encode command: {error}")))?;
                Ok(OutboundCommand {
                    request_id: id.to_string(),
                    text,
                    expected_acknowledgements: 1,
                })
            })
            .collect()
    }

    fn parse_control(object: &Map<String, Value>) -> Option<Result<ParsedFrame, AdapterError>> {
        if object.contains_key("code") {
            return Some(Err(command_rejected(binance_error_message(object))));
        }
        if let Some(error) = object.get("error") {
            let message = error
                .get("msg")
                .or_else(|| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("unknown command error");
            return Some(Err(command_rejected(message)));
        }
        if let Some(result) = object.get("result") {
            let request_id = match object.get("id") {
                Some(Value::Number(id)) if id.as_u64().is_some() => id.to_string(),
                Some(Value::String(id)) if !id.is_empty() && id.len() <= 36 => id.clone(),
                Some(_) => {
                    return Some(Err(invalid_payload(
                        "command response id must be an unsigned integer or short string",
                    )));
                }
                None => return Some(Err(invalid_payload("command response is missing id"))),
            };
            return Some(if result.is_null() {
                Ok(ParsedFrame::Acknowledgement { request_id })
            } else {
                Ok(ParsedFrame::Ignored)
            });
        }
        None
    }

    fn parse_ticker(
        &mut self,
        value: Value,
        received_at_ms: u64,
        stream_name: Option<&str>,
    ) -> Result<ParsedFrame, AdapterError> {
        let update: BinanceTicker = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid ticker message: {error}")))?;
        ensure_usdm(update.symbol_type)?;
        let symbol = normalized_symbol(&update.symbol)?;
        validate_stream_symbol(stream_name, &symbol, "@ticker")?;

        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.last = Some(observation(&update.last, update.event_time, "last price")?);
        ticker.bid = optional_observation(update.bid, update.event_time, "bid price")?;
        ticker.ask = optional_observation(update.ask, update.event_time, "ask price")?;
        self.tickers.insert(symbol.clone(), ticker.clone());

        let source_sequence = match (update.first_trade_id, update.last_trade_id) {
            (None, None) => None,
            (first, last) => Some(SourceSequence {
                first: first.map(|value| value.to_string()),
                last: last.map(|value| value.to_string()),
                previous: None,
            }),
        };
        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Binance,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(update.event_time),
            gateway_received_time_ms: received_at_ms,
            source_sequence,
            payload: MarketPayload::Ticker(ticker),
        }]))
    }

    fn parse_mark_price(
        &mut self,
        value: Value,
        received_at_ms: u64,
        stream_name: Option<&str>,
    ) -> Result<ParsedFrame, AdapterError> {
        let update: BinanceMarkPrice = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid mark-price message: {error}")))?;
        ensure_usdm(update.symbol_type)?;
        let symbol = normalized_symbol(&update.symbol)?;
        validate_stream_symbol(stream_name, &symbol, "@markPrice")?;

        let mut ticker = self.tickers.get(&symbol).cloned().unwrap_or_default();
        ticker.mark = Some(observation(
            &update.mark_price,
            update.event_time,
            "mark price",
        )?);
        ticker.index = Some(observation(
            &update.index_price,
            update.event_time,
            "index price",
        )?);
        ticker.funding_rate = Some(observation(
            &update.funding_rate,
            update.event_time,
            "funding rate",
        )?);
        ticker.next_funding_time_ms = Some(update.next_funding_time);
        self.tickers.insert(symbol.clone(), ticker.clone());

        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Binance,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(update.event_time),
            gateway_received_time_ms: received_at_ms,
            source_sequence: None,
            payload: MarketPayload::Ticker(ticker),
        }]))
    }

    fn parse_kline(
        &self,
        value: Value,
        received_at_ms: u64,
        stream_name: Option<&str>,
    ) -> Result<ParsedFrame, AdapterError> {
        let update: BinanceKlineEnvelope = serde_json::from_value(value)
            .map_err(|error| invalid_payload(format!("invalid kline message: {error}")))?;
        ensure_usdm(update.symbol_type)?;
        let outer_symbol = normalized_symbol(&update.symbol)?;
        let symbol = normalized_symbol(&update.kline.symbol)?;
        if outer_symbol != symbol {
            return Err(invalid_payload(format!(
                "outer kline symbol {outer_symbol} does not match inner symbol {symbol}"
            )));
        }
        validate_stream_symbol(stream_name, &symbol, "@kline_1m")?;
        if update.kline.interval != "1m" {
            return Err(invalid_payload(format!(
                "expected 1m kline interval, got {}",
                update.kline.interval
            )));
        }

        let end_time_ms =
            update.kline.close_time.checked_add(1).ok_or_else(|| {
                invalid_payload("kline close time cannot be normalized to exclusive")
            })?;
        let candle = Candle {
            interval: "1m".to_owned(),
            start_time_ms: update.kline.start_time,
            end_time_ms,
            open: decimal(&update.kline.open, "open")?,
            high: decimal(&update.kline.high, "high")?,
            low: decimal(&update.kline.low, "low")?,
            close: decimal(&update.kline.close, "close")?,
            base_volume: Some(decimal(&update.kline.base_volume, "base volume")?),
            quote_volume: Some(decimal(&update.kline.quote_volume, "quote volume")?),
            contract_volume: None,
            finality: if update.kline.closed {
                CandleFinality::Closed
            } else {
                CandleFinality::Open
            },
            data_quality: Vec::new(),
        };
        Ok(ParsedFrame::Events(vec![ProviderEvent {
            connection_epoch: self.connection_epoch,
            provider: Provider::Binance,
            market: MarketKind::LinearPerpetual,
            symbol,
            exchange_time_ms: Some(update.event_time),
            gateway_received_time_ms: received_at_ms,
            source_sequence: Some(SourceSequence {
                first: Some(update.kline.first_trade_id.to_string()),
                last: Some(update.kline.last_trade_id.to_string()),
                previous: None,
            }),
            payload: MarketPayload::Candle(candle),
        }]))
    }
}

impl AdapterSession for BinanceSession {
    fn subscription_messages(
        &mut self,
        action: SubscriptionAction,
        subscriptions: &[SubscriptionKey],
    ) -> Result<Vec<OutboundCommand>, AdapterError> {
        validate_subscriptions(Provider::Binance, subscriptions)?;

        let mut seen = HashSet::new();
        let mut streams = Vec::with_capacity(subscriptions.len() * 2);
        for subscription in subscriptions {
            let symbol = subscription.symbol.to_ascii_lowercase();
            let channel_streams = match subscription.channel {
                Channel::Ticker => {
                    vec![format!("{symbol}@ticker"), format!("{symbol}@markPrice@1s")]
                }
                Channel::Candle1m => vec![format!("{symbol}@kline_1m")],
            };
            for stream in channel_streams {
                if seen.insert(stream.clone()) {
                    streams.push(stream);
                }
            }
            if action == SubscriptionAction::Unsubscribe && subscription.channel == Channel::Ticker
            {
                self.tickers.remove(&subscription.symbol);
            }
        }
        self.command_messages(action, &streams)
    }

    fn heartbeat(&mut self) -> Heartbeat {
        // Binance's websocket heartbeat uses protocol-level ping/pong frames rather than JSON.
        Heartbeat::WebSocketPing(Vec::new())
    }

    fn parse(&mut self, text: &str, received_at_ms: u64) -> Result<ParsedFrame, AdapterError> {
        let mut value: Value = serde_json::from_str(text)
            .map_err(|error| invalid_payload(format!("invalid JSON: {error}")))?;
        let root = value
            .as_object()
            .ok_or_else(|| invalid_payload("top-level payload must be an object"))?;
        if let Some(result) = Self::parse_control(root) {
            return result;
        }

        let stream_name = root.get("stream").map(|stream| {
            stream
                .as_str()
                .ok_or_else(|| invalid_payload("combined stream name must be a string"))
        });
        let stream_name = match stream_name {
            Some(result) => Some(result?.to_owned()),
            None => None,
        };
        if stream_name.is_some() {
            value = root
                .get("data")
                .cloned()
                .ok_or_else(|| invalid_payload("combined stream payload is missing data"))?;
        }

        let event_type = match value.get("e") {
            Some(Value::String(event_type)) => event_type.as_str(),
            Some(_) => return Err(invalid_payload("event field e must be a string")),
            None => return Ok(ParsedFrame::Ignored),
        };
        match event_type {
            "24hrTicker" => self.parse_ticker(value, received_at_ms, stream_name.as_deref()),
            "markPriceUpdate" => {
                self.parse_mark_price(value, received_at_ms, stream_name.as_deref())
            }
            "kline" => self.parse_kline(value, received_at_ms, stream_name.as_deref()),
            _ => Ok(ParsedFrame::Ignored),
        }
    }
}

#[derive(Debug, Deserialize)]
struct BinanceTicker {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    last: String,
    #[serde(rename = "b")]
    bid: Option<String>,
    #[serde(rename = "a")]
    ask: Option<String>,
    #[serde(rename = "F")]
    first_trade_id: Option<i64>,
    #[serde(rename = "L")]
    last_trade_id: Option<i64>,
    #[serde(rename = "st")]
    symbol_type: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct BinanceMarkPrice {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    mark_price: String,
    #[serde(rename = "i")]
    index_price: String,
    #[serde(rename = "r")]
    funding_rate: String,
    #[serde(rename = "T")]
    next_funding_time: u64,
    #[serde(rename = "st")]
    symbol_type: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct BinanceKlineEnvelope {
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "k")]
    kline: BinanceKline,
    #[serde(rename = "st")]
    symbol_type: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct BinanceKline {
    #[serde(rename = "t")]
    start_time: u64,
    #[serde(rename = "T")]
    close_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "f")]
    first_trade_id: i64,
    #[serde(rename = "L")]
    last_trade_id: i64,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    base_volume: String,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "x")]
    closed: bool,
}

fn observation(
    value: &str,
    observed_at_ms: u64,
    field: &str,
) -> Result<ObservedDecimal, AdapterError> {
    ObservedDecimal::new(value.to_owned(), observed_at_ms)
        .map_err(|error| invalid_payload(format!("invalid {field} {value:?}: {error}")))
}

fn optional_observation(
    value: Option<String>,
    observed_at_ms: u64,
    field: &str,
) -> Result<Option<ObservedDecimal>, AdapterError> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| observation(&value, observed_at_ms, field))
        .transpose()
}

fn decimal(value: &str, field: &str) -> Result<DecimalValue, AdapterError> {
    DecimalValue::new(value.to_owned())
        .map_err(|error| invalid_payload(format!("invalid {field} {value:?}: {error}")))
}

fn normalized_symbol(symbol: &str) -> Result<String, AdapterError> {
    normalize_symbol(symbol)
        .map_err(|error| invalid_payload(format!("invalid symbol {symbol:?}: {error}")))
}

fn ensure_usdm(symbol_type: Option<u8>) -> Result<(), AdapterError> {
    match symbol_type {
        Some(2) => Err(invalid_payload(
            "received a COIN-M payload on the USD-M adapter",
        )),
        Some(value) if value != 1 => Err(invalid_payload(format!(
            "unsupported Binance symbol type {value}"
        ))),
        _ => Ok(()),
    }
}

fn validate_stream_symbol(
    stream_name: Option<&str>,
    symbol: &str,
    expected_suffix: &str,
) -> Result<(), AdapterError> {
    let Some(stream_name) = stream_name else {
        return Ok(());
    };
    let stream_name_lower = stream_name.to_ascii_lowercase();
    let symbol_lower = symbol.to_ascii_lowercase();
    let expected = format!("{symbol_lower}{}", expected_suffix.to_ascii_lowercase());
    let matches = if expected_suffix == "@markPrice" {
        stream_name_lower == expected || stream_name_lower == format!("{expected}@1s")
    } else {
        stream_name_lower == expected
    };
    if !matches {
        return Err(invalid_payload(format!(
            "combined stream {stream_name:?} does not match {symbol} {expected_suffix}"
        )));
    }
    Ok(())
}

fn binance_error_message(object: &Map<String, Value>) -> String {
    let message = object
        .get("msg")
        .or_else(|| object.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown command error");
    let code = object.get("code").and_then(Value::as_i64);
    code.map_or_else(
        || message.to_owned(),
        |code| format!("code {code}: {message}"),
    )
}

fn invalid_payload(message: impl Into<String>) -> AdapterError {
    AdapterError::InvalidPayload {
        provider: Provider::Binance,
        message: message.into(),
    }
}

fn command_rejected(message: impl Into<String>) -> AdapterError {
    AdapterError::CommandRejected {
        provider: Provider::Binance,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn session() -> BinanceSession {
        BinanceSession::new(Uuid::nil())
    }

    fn subscription(symbol: &str, channel: Channel) -> SubscriptionKey {
        SubscriptionKey::new(
            Provider::Binance,
            MarketKind::LinearPerpetual,
            symbol,
            channel,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn target_uses_new_market_route_and_rotates_before_forced_close() {
        let adapter = BinanceAdapter::new();
        let target = adapter
            .connection_target(EndpointKind::Primary, &reqwest::Client::new())
            .await
            .unwrap();
        assert_eq!(target.url.as_str(), BINANCE_MARKET_URL);
        assert_eq!(target.command_interval, Duration::from_millis(125));
        assert_eq!(target.subscription_ack_timeout, Duration::from_secs(15));
        assert_eq!(target.heartbeat_interval, Duration::from_mins(3));
        assert_eq!(target.rotate_after, Some(Duration::from_mins(23 * 60 + 55)));
        assert_eq!(session().heartbeat(), Heartbeat::WebSocketPing(Vec::new()));
    }

    #[test]
    fn ticker_subscription_expands_to_ticker_and_mark_price() {
        let mut session = session();
        let messages = session
            .subscription_messages(
                SubscriptionAction::Subscribe,
                &[
                    subscription("BTCUSDT", Channel::Ticker),
                    subscription("BTCUSDT", Channel::Candle1m),
                    subscription("BTCUSDT", Channel::Ticker),
                ],
            )
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].request_id, "1");
        assert_eq!(messages[0].expected_acknowledgements, 1);
        let command: Value = serde_json::from_str(&messages[0].text).unwrap();
        assert_eq!(command["method"], "SUBSCRIBE");
        assert_eq!(
            command["params"],
            json!(["btcusdt@ticker", "btcusdt@markPrice@1s", "btcusdt@kline_1m"])
        );
        assert_eq!(command["id"], 1);
    }

    #[test]
    fn large_subscription_sets_are_batched() {
        let subscriptions = (0..101)
            .map(|index| subscription(&format!("COIN{index}USDT"), Channel::Ticker))
            .collect::<Vec<_>>();
        let messages = session()
            .subscription_messages(SubscriptionAction::Subscribe, &subscriptions)
            .unwrap();
        assert_eq!(messages.len(), 2);
        let first: Value = serde_json::from_str(&messages[0].text).unwrap();
        let second: Value = serde_json::from_str(&messages[1].text).unwrap();
        assert_eq!(first["params"].as_array().unwrap().len(), 200);
        assert_eq!(second["params"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn ticker_and_mark_price_updates_materialize_one_ticker() {
        let mut session = session();
        let ticker = r#"{
            "e":"24hrTicker","E":1720000000000,"s":"BTCUSDT","c":"60123.40",
            "F":100,"L":200,"p":"1","P":"2","w":"3","Q":"4","o":"5",
            "h":"6","l":"7","v":"8","q":"9","O":1,"C":2,"n":101
        }"#;
        let mark = r#"{
            "e":"markPriceUpdate","E":1720000000100,"s":"BTCUSDT",
            "p":"60120.10","i":"60118.20","P":"60119.00","r":"0.00010000",
            "T":1720022400000
        }"#;

        session.parse(ticker, 1_720_000_000_005).unwrap();
        let ParsedFrame::Events(events) = session.parse(mark, 1_720_000_000_107).unwrap() else {
            panic!("expected events");
        };
        let MarketPayload::Ticker(ticker) = &events[0].payload else {
            panic!("expected ticker");
        };
        assert_eq!(ticker.last.as_ref().unwrap().value.as_str(), "60123.40");
        assert_eq!(
            ticker.last.as_ref().unwrap().observed_at_ms,
            1_720_000_000_000
        );
        assert_eq!(ticker.mark.as_ref().unwrap().value.as_str(), "60120.10");
        assert_eq!(
            ticker.mark.as_ref().unwrap().observed_at_ms,
            1_720_000_000_100
        );
        assert_eq!(ticker.index.as_ref().unwrap().value.as_str(), "60118.20");
        assert_eq!(
            ticker.funding_rate.as_ref().unwrap().value.as_str(),
            "0.00010000"
        );
        assert_eq!(ticker.next_funding_time_ms, Some(1_720_022_400_000));
    }

    #[test]
    fn kline_table_preserves_volume_units_finality_and_sequence() {
        for (closed, finality) in [
            (false, CandleFinality::Open),
            (true, CandleFinality::Closed),
        ] {
            let frame = format!(
                r#"{{
                  "e":"kline","E":1672515782136,"s":"ETHUSDT","k":{{
                    "t":1672515780000,"T":1672515839999,"s":"ETHUSDT","i":"1m",
                    "f":100,"L":200,"o":"0.0010","c":"0.0020","h":"0.0025",
                    "l":"0.0015","v":"1000","n":100,"x":{closed},"q":"1.0000",
                    "V":"500","Q":"0.500","B":"123456"
                  }}
                }}"#
            );
            let ParsedFrame::Events(events) = session().parse(&frame, 1_672_515_782_140).unwrap()
            else {
                panic!("expected events");
            };
            let MarketPayload::Candle(candle) = &events[0].payload else {
                panic!("expected candle");
            };
            assert_eq!(candle.base_volume.as_ref().unwrap().as_str(), "1000");
            assert_eq!(candle.quote_volume.as_ref().unwrap().as_str(), "1.0000");
            assert_eq!(candle.contract_volume, None);
            assert_eq!(candle.end_time_ms, 1_672_515_840_000);
            assert_eq!(candle.finality, finality);
            let sequence = events[0].source_sequence.as_ref().unwrap();
            assert_eq!(sequence.first.as_deref(), Some("100"));
            assert_eq!(sequence.last.as_deref(), Some("200"));
        }
    }

    #[test]
    fn combined_frames_ack_errors_and_malformed_decimals_are_handled() {
        let mut session = session();
        assert_eq!(
            session.parse(r#"{"result":null,"id":1}"#, 1).unwrap(),
            ParsedFrame::Acknowledgement {
                request_id: "1".to_owned()
            }
        );
        assert!(matches!(
            session.parse(r#"{"code":2,"msg":"Invalid request","id":1}"#, 1),
            Err(AdapterError::CommandRejected { .. })
        ));

        let combined = r#"{
            "stream":"btcusdt@markPrice@1s",
            "data":{"e":"markPriceUpdate","E":10,"s":"BTCUSDT","p":"2.1",
                    "i":"2.0","r":"0.01","T":20}
        }"#;
        assert!(matches!(
            session.parse(combined, 11).unwrap(),
            ParsedFrame::Events(_)
        ));
        assert!(matches!(
            session.parse(
                r#"{"e":"24hrTicker","E":10,"s":"BTCUSDT","c":2.1,"F":1,"L":2}"#,
                11
            ),
            Err(AdapterError::InvalidPayload { .. })
        ));
        assert!(matches!(
            session.parse("[]", 1),
            Err(AdapterError::InvalidPayload { .. })
        ));
    }
}
