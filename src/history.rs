//! Bounded, provider-neutral retrieval of historical one-minute candles.
//!
//! The public exchanges use different pagination directions, response envelopes, volume units,
//! and finality signals. This module keeps the venue symbol exact and normalizes only the candle
//! representation. Request bounds use a half-open interval: `[start_time_ms, end_time_ms)`.

use std::str;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;
use url::Url;

use crate::domain::{
    Candle, CandleFinality, DataQuality, DecimalValue, MarketKind, Provider, normalize_symbol,
};

pub const CANDLE_INTERVAL_MS: u64 = 60_000;
pub const MAX_HISTORY_CANDLES: usize = 10_000;
pub const MAX_HISTORY_RANGE_MS: u64 = 600_000_000;

pub const BYBIT_KLINE_PATH: &str = "v5/market/kline";
pub const BINANCE_KLINE_PATH: &str = "fapi/v1/klines";
pub const BINANCE_TIME_PATH: &str = "fapi/v1/time";
pub const OKX_HISTORY_CANDLES_PATH: &str = "api/v5/market/history-candles";
pub const KUCOIN_KLINE_PATH: &str = "api/v1/kline/query";
pub const MEXC_KLINE_PATH_PREFIX: &str = "api/v1/contract/kline/";
pub const MEXC_TIME_PATH: &str = "api/v1/contract/ping";
pub const BINGX_KLINE_PATH: &str = "openApi/swap/v3/quote/klines";
pub const BINGX_TIME_PATH: &str = "openApi/swap/v2/server/time";

const BYBIT_PAGE_LIMIT: usize = 1_000;
const BINANCE_PAGE_LIMIT: usize = 1_500;
const OKX_PAGE_LIMIT: usize = 300;
const KUCOIN_PAGE_LIMIT: u64 = 500;
const MEXC_PAGE_LIMIT: u64 = 2_000;
const BINGX_PAGE_LIMIT: u64 = 1_440;
const MAX_UPSTREAM_PAGES: usize = 40;
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Provider REST roots used by [`HistoryClient`].
///
/// Keeping these independently configurable makes regional endpoints and deterministic local
/// tests possible without changing request semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySources {
    pub bybit: Url,
    pub binance: Url,
    pub okx: Url,
    pub kucoin: Url,
    pub mexc: Url,
    pub bingx: Url,
}

impl Default for HistorySources {
    fn default() -> Self {
        Self {
            bybit: static_url("https://api.bybit.com/"),
            binance: static_url("https://fapi.binance.com/"),
            okx: static_url("https://www.okx.com/"),
            kucoin: static_url("https://api-futures.kucoin.com/"),
            mexc: static_url("https://api.mexc.com/"),
            bingx: static_url("https://open-api.bingx.com/"),
        }
    }
}

fn static_url(value: &str) -> Url {
    Url::parse(value).expect("hard-coded provider URL must be valid")
}

/// A validated request for complete one-minute buckets in a half-open time range.
///
/// The symbol is an exact, uppercase venue symbol. In particular, this layer never translates
/// `BTC` to `XBT`, inserts OKX separators, or otherwise guesses an instrument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistoryRequest {
    provider: Provider,
    symbol: String,
    start_time_ms: u64,
    end_time_ms: u64,
    limit: usize,
}

impl HistoryRequest {
    /// Validate a bounded one-minute history request.
    ///
    /// `end_time_ms` is exclusive. Both timestamps must be UTC minute boundaries. If the range
    /// contains more available candles than `limit`, the earliest candles are returned.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError::InvalidRequest`] for an invalid venue symbol, unaligned or empty
    /// range, excessive range, or a zero/excessive result limit.
    pub fn new(
        provider: Provider,
        symbol: impl AsRef<str>,
        start_time_ms: u64,
        end_time_ms: u64,
        limit: usize,
    ) -> Result<Self, HistoryError> {
        let provided_symbol = symbol.as_ref();
        let normalized = normalize_symbol(provided_symbol)
            .map_err(|error| HistoryError::InvalidRequest(error.to_string()))?;
        if provided_symbol != normalized {
            return Err(HistoryError::InvalidRequest(
                "symbol must be the exact uppercase venue symbol without surrounding whitespace"
                    .to_owned(),
            ));
        }
        validate_provider_symbol(provider, &normalized)?;
        if start_time_ms == 0
            || !start_time_ms.is_multiple_of(CANDLE_INTERVAL_MS)
            || !end_time_ms.is_multiple_of(CANDLE_INTERVAL_MS)
        {
            return Err(HistoryError::InvalidRequest(
                "history bounds must be positive UTC minute boundaries".to_owned(),
            ));
        }
        let range = end_time_ms.checked_sub(start_time_ms).ok_or_else(|| {
            HistoryError::InvalidRequest(
                "end_time_ms must be greater than start_time_ms".to_owned(),
            )
        })?;
        if range == 0 {
            return Err(HistoryError::InvalidRequest(
                "end_time_ms must be greater than start_time_ms".to_owned(),
            ));
        }
        if range > MAX_HISTORY_RANGE_MS {
            return Err(HistoryError::InvalidRequest(format!(
                "history range exceeds {MAX_HISTORY_RANGE_MS} milliseconds"
            )));
        }
        if !(1..=MAX_HISTORY_CANDLES).contains(&limit) {
            return Err(HistoryError::InvalidRequest(format!(
                "history limit must be between 1 and {MAX_HISTORY_CANDLES}"
            )));
        }
        Ok(Self {
            provider,
            symbol: normalized,
            start_time_ms,
            end_time_ms,
            limit,
        })
    }

    pub const fn provider(&self) -> Provider {
        self.provider
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub const fn start_time_ms(&self) -> u64 {
        self.start_time_ms
    }

    pub const fn end_time_ms(&self) -> u64 {
        self.end_time_ms
    }

    pub const fn limit(&self) -> usize {
        self.limit
    }
}

/// One normalized candle with its exact venue identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCandle {
    pub provider: Provider,
    pub market: MarketKind,
    pub symbol: String,
    #[serde(flatten)]
    pub candle: Candle,
}

/// The stable, provider-neutral result of a history request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryResult {
    pub provider: Provider,
    pub market: MarketKind,
    pub symbol: String,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub candles: Vec<HistoryCandle>,
}

/// Public REST client for normalized historical candles.
#[derive(Debug, Clone)]
pub struct HistoryClient {
    http: Client,
    sources: HistorySources,
}

impl HistoryClient {
    /// Build a client with conservative public-market-data timeouts.
    ///
    /// # Errors
    ///
    /// Returns [`HistoryError::ClientBuild`] if the HTTP client cannot be constructed.
    pub fn new(sources: HistorySources) -> Result<Self, HistoryError> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .user_agent("market-stream-gateway/0.1")
            .build()
            .map_err(HistoryError::ClientBuild)?;
        Ok(Self { http, sources })
    }

    /// Use an already configured HTTP client, primarily for service-wide policy and tests.
    pub const fn with_client(http: Client, sources: HistorySources) -> Self {
        Self { http, sources }
    }

    /// Fetch, normalize, order, and deduplicate one-minute candles.
    ///
    /// Bybit and Binance finality is derived against an exchange server timestamp. OKX exposes
    /// an authoritative per-candle `confirm` field. `KuCoin` Classic Futures exposes no equivalent
    /// finality field and warns that candlestick data may be incomplete, so its finality remains
    /// [`CandleFinality::Unknown`]. `KuCoin` volume is always omitted and marked untrusted.
    ///
    /// # Errors
    ///
    /// Returns an error on transport/HTTP/provider failures, malformed economic data, conflicting
    /// duplicates, or pagination that cannot make bounded progress.
    pub async fn fetch(&self, request: &HistoryRequest) -> Result<HistoryResult, HistoryError> {
        let candles = match request.provider {
            Provider::Bybit => self.fetch_bybit(request).await?,
            Provider::Binance => self.fetch_binance(request).await?,
            Provider::Okx => self.fetch_okx(request).await?,
            Provider::Kucoin => self.fetch_kucoin(request).await?,
            Provider::Mexc => self.fetch_mexc(request).await?,
            Provider::Bingx => self.fetch_bingx(request).await?,
        };
        finish_result(request, candles)
    }

    async fn fetch_bybit(&self, request: &HistoryRequest) -> Result<Vec<Candle>, HistoryError> {
        let mut cursor_end = request.end_time_ms;
        let mut pages = 0;
        let mut candles = Vec::new();
        loop {
            count_page(Provider::Bybit, &mut pages)?;
            let mut url = endpoint(&self.sources.bybit, BYBIT_KLINE_PATH, Provider::Bybit)?;
            url.query_pairs_mut()
                .append_pair("category", "linear")
                .append_pair("symbol", &request.symbol)
                .append_pair("interval", "1")
                .append_pair("start", &request.start_time_ms.to_string())
                .append_pair("end", &exclusive_query_end(cursor_end)?.to_string())
                .append_pair("limit", &BYBIT_PAGE_LIMIT.to_string());
            let body = self.get_text(Provider::Bybit, url).await?;
            let page = parse_bybit(&body, &request.symbol)?;
            let page_len = page.len();
            if page.is_empty() {
                break;
            }
            let oldest = page
                .iter()
                .map(|candle| candle.start_time_ms)
                .min()
                .ok_or_else(|| invalid_payload(Provider::Bybit, "non-empty page has no candles"))?;
            if oldest >= cursor_end {
                return Err(HistoryError::PaginationStalled {
                    provider: Provider::Bybit,
                    cursor_ms: cursor_end,
                });
            }
            candles.extend(page);
            if oldest <= request.start_time_ms || page_len < BYBIT_PAGE_LIMIT {
                break;
            }
            cursor_end = oldest;
        }
        Ok(candles)
    }

    async fn fetch_binance(&self, request: &HistoryRequest) -> Result<Vec<Candle>, HistoryError> {
        let server_time_ms = self.binance_server_time().await?;
        let mut cursor_start = request.start_time_ms;
        let mut pages = 0;
        let mut candles = Vec::new();
        while cursor_start < request.end_time_ms {
            count_page(Provider::Binance, &mut pages)?;
            let mut url = endpoint(&self.sources.binance, BINANCE_KLINE_PATH, Provider::Binance)?;
            url.query_pairs_mut()
                .append_pair("symbol", &request.symbol)
                .append_pair("interval", "1m")
                .append_pair("startTime", &cursor_start.to_string())
                .append_pair(
                    "endTime",
                    &exclusive_query_end(request.end_time_ms)?.to_string(),
                )
                .append_pair("limit", &BINANCE_PAGE_LIMIT.to_string());
            let body = self.get_text(Provider::Binance, url).await?;
            let page = parse_binance(&body, server_time_ms)?;
            let page_len = page.len();
            if page.is_empty() {
                break;
            }
            let newest = page
                .iter()
                .map(|candle| candle.start_time_ms)
                .max()
                .ok_or_else(|| {
                    invalid_payload(Provider::Binance, "non-empty page has no candles")
                })?;
            let next = newest.checked_add(CANDLE_INTERVAL_MS).ok_or_else(|| {
                invalid_payload(Provider::Binance, "pagination timestamp overflow")
            })?;
            if next <= cursor_start {
                return Err(HistoryError::PaginationStalled {
                    provider: Provider::Binance,
                    cursor_ms: cursor_start,
                });
            }
            candles.extend(page);
            if next >= request.end_time_ms || page_len < BINANCE_PAGE_LIMIT {
                break;
            }
            cursor_start = next;
        }
        Ok(candles)
    }

    async fn binance_server_time(&self) -> Result<u64, HistoryError> {
        let url = endpoint(&self.sources.binance, BINANCE_TIME_PATH, Provider::Binance)?;
        let body = self.get_text(Provider::Binance, url).await?;
        parse_binance_server_time(&body)
    }

    async fn fetch_okx(&self, request: &HistoryRequest) -> Result<Vec<Candle>, HistoryError> {
        let mut cursor_end = request.end_time_ms;
        let mut pages = 0;
        let mut candles = Vec::new();
        loop {
            count_page(Provider::Okx, &mut pages)?;
            let mut url = endpoint(&self.sources.okx, OKX_HISTORY_CANDLES_PATH, Provider::Okx)?;
            url.query_pairs_mut()
                .append_pair("instId", &request.symbol)
                .append_pair("bar", "1m")
                .append_pair("after", &cursor_end.to_string())
                // OKX cursors are exclusive. One millisecond below the aligned lower bound
                // includes the candle opening exactly at start_time_ms.
                .append_pair("before", &(request.start_time_ms - 1).to_string())
                .append_pair("limit", &OKX_PAGE_LIMIT.to_string());
            let body = self.get_text(Provider::Okx, url).await?;
            let page = parse_okx(&body)?;
            let page_len = page.len();
            if page.is_empty() {
                break;
            }
            let oldest = page
                .iter()
                .map(|candle| candle.start_time_ms)
                .min()
                .ok_or_else(|| invalid_payload(Provider::Okx, "non-empty page has no candles"))?;
            if oldest >= cursor_end {
                return Err(HistoryError::PaginationStalled {
                    provider: Provider::Okx,
                    cursor_ms: cursor_end,
                });
            }
            candles.extend(page);
            if oldest <= request.start_time_ms || page_len < OKX_PAGE_LIMIT {
                break;
            }
            cursor_end = oldest;
        }
        Ok(candles)
    }

    async fn fetch_kucoin(&self, request: &HistoryRequest) -> Result<Vec<Candle>, HistoryError> {
        let page_span = KUCOIN_PAGE_LIMIT
            .checked_mul(CANDLE_INTERVAL_MS)
            .ok_or_else(|| invalid_payload(Provider::Kucoin, "page span overflow"))?;
        let mut cursor_start = request.start_time_ms;
        let mut pages = 0;
        let mut candles = Vec::new();
        while cursor_start < request.end_time_ms {
            count_page(Provider::Kucoin, &mut pages)?;
            let page_end = cursor_start
                .saturating_add(page_span)
                .min(request.end_time_ms);
            let mut url = endpoint(&self.sources.kucoin, KUCOIN_KLINE_PATH, Provider::Kucoin)?;
            url.query_pairs_mut()
                .append_pair("symbol", &request.symbol)
                .append_pair("granularity", "60")
                .append_pair("from", &cursor_start.to_string())
                .append_pair("to", &exclusive_query_end(page_end)?.to_string());
            let body = self.get_text(Provider::Kucoin, url).await?;
            candles.extend(parse_kucoin(&body)?);
            if page_end <= cursor_start {
                return Err(HistoryError::PaginationStalled {
                    provider: Provider::Kucoin,
                    cursor_ms: cursor_start,
                });
            }
            cursor_start = page_end;
        }
        Ok(candles)
    }

    async fn fetch_mexc(&self, request: &HistoryRequest) -> Result<Vec<Candle>, HistoryError> {
        let server_time_ms = self.mexc_server_time().await?;
        let page_span = MEXC_PAGE_LIMIT
            .checked_mul(CANDLE_INTERVAL_MS)
            .ok_or_else(|| invalid_payload(Provider::Mexc, "page span overflow"))?;
        let mut cursor_start = request.start_time_ms;
        let mut pages = 0;
        let mut candles = Vec::new();
        while cursor_start < request.end_time_ms {
            count_page(Provider::Mexc, &mut pages)?;
            let page_end = cursor_start
                .saturating_add(page_span)
                .min(request.end_time_ms);
            let path = format!("{MEXC_KLINE_PATH_PREFIX}{}", request.symbol);
            let mut url = endpoint(&self.sources.mexc, &path, Provider::Mexc)?;
            let inclusive_last_start = page_end
                .checked_sub(CANDLE_INTERVAL_MS)
                .ok_or_else(|| invalid_payload(Provider::Mexc, "page end underflow"))?;
            url.query_pairs_mut()
                .append_pair("interval", "Min1")
                .append_pair("start", &(cursor_start / 1_000).to_string())
                .append_pair("end", &(inclusive_last_start / 1_000).to_string());
            let body = self.get_text(Provider::Mexc, url).await?;
            candles.extend(parse_mexc(&body, server_time_ms)?);
            if page_end <= cursor_start {
                return Err(HistoryError::PaginationStalled {
                    provider: Provider::Mexc,
                    cursor_ms: cursor_start,
                });
            }
            cursor_start = page_end;
        }
        Ok(candles)
    }

    async fn mexc_server_time(&self) -> Result<u64, HistoryError> {
        let url = endpoint(&self.sources.mexc, MEXC_TIME_PATH, Provider::Mexc)?;
        let body = self.get_text(Provider::Mexc, url).await?;
        parse_mexc_server_time(&body)
    }

    async fn fetch_bingx(&self, request: &HistoryRequest) -> Result<Vec<Candle>, HistoryError> {
        let server_time_ms = self.bingx_server_time().await?;
        let page_span = BINGX_PAGE_LIMIT
            .checked_mul(CANDLE_INTERVAL_MS)
            .ok_or_else(|| invalid_payload(Provider::Bingx, "page span overflow"))?;
        let mut cursor_start = request.start_time_ms;
        let mut pages = 0;
        let mut candles = Vec::new();
        while cursor_start < request.end_time_ms {
            count_page(Provider::Bingx, &mut pages)?;
            let page_end = cursor_start
                .saturating_add(page_span)
                .min(request.end_time_ms);
            let mut url = endpoint(&self.sources.bingx, BINGX_KLINE_PATH, Provider::Bingx)?;
            let requested = (page_end - cursor_start) / CANDLE_INTERVAL_MS;
            url.query_pairs_mut()
                .append_pair("symbol", &request.symbol)
                .append_pair("interval", "1m")
                .append_pair("startTime", &cursor_start.to_string())
                .append_pair("endTime", &exclusive_query_end(page_end)?.to_string())
                .append_pair("limit", &requested.to_string());
            let body = self.get_text(Provider::Bingx, url).await?;
            candles.extend(parse_bingx(&body, server_time_ms)?);
            if page_end <= cursor_start {
                return Err(HistoryError::PaginationStalled {
                    provider: Provider::Bingx,
                    cursor_ms: cursor_start,
                });
            }
            cursor_start = page_end;
        }
        Ok(candles)
    }

    async fn bingx_server_time(&self) -> Result<u64, HistoryError> {
        let url = endpoint(&self.sources.bingx, BINGX_TIME_PATH, Provider::Bingx)?;
        let body = self.get_text(Provider::Bingx, url).await?;
        parse_bingx_server_time(&body)
    }

    async fn get_text(&self, provider: Provider, url: Url) -> Result<String, HistoryError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|source| HistoryError::Transport { provider, source })?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|source| HistoryError::Transport { provider, source })?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(invalid_payload(
                provider,
                format!("response exceeds {MAX_RESPONSE_BYTES} bytes"),
            ));
        }
        let body = str::from_utf8(&bytes)
            .map_err(|_| invalid_payload(provider, "response is not UTF-8"))?
            .to_owned();
        if !status.is_success() {
            return Err(http_error(provider, status, &body));
        }
        Ok(body)
    }
}

/// Failures are intentionally provider-labelled so API callers can distinguish bad user input,
/// venue rejection, upstream availability, and corrupt market data.
#[derive(Debug, Error)]
pub enum HistoryError {
    #[error("invalid history request: {0}")]
    InvalidRequest(String),
    #[error("could not build history HTTP client: {0}")]
    ClientBuild(#[source] reqwest::Error),
    #[error("{provider} history transport error: {source}")]
    Transport {
        provider: Provider,
        #[source]
        source: reqwest::Error,
    },
    #[error("{provider} history endpoint returned HTTP {status}: {message}")]
    HttpStatus {
        provider: Provider,
        status: u16,
        message: String,
    },
    #[error("{provider} rejected history request with code {code}: {message}")]
    ProviderRejected {
        provider: Provider,
        code: String,
        message: String,
    },
    #[error("invalid {provider} history payload: {message}")]
    InvalidPayload { provider: Provider, message: String },
    #[error("{provider} history pagination did not advance from {cursor_ms}")]
    PaginationStalled { provider: Provider, cursor_ms: u64 },
}

fn validate_provider_symbol(provider: Provider, symbol: &str) -> Result<(), HistoryError> {
    let valid = match provider {
        Provider::Bybit => {
            ascii_alphanumeric(symbol) && (symbol.ends_with("USDT") || symbol.ends_with("USDC"))
        }
        Provider::Binance => {
            unicode_alphanumeric(symbol) && (symbol.ends_with("USDT") || symbol.ends_with("USDC"))
        }
        Provider::Okx => validate_okx_symbol(symbol),
        Provider::Kucoin => {
            ascii_alphanumeric(symbol) && (symbol.ends_with("USDTM") || symbol.ends_with("USDCM"))
        }
        Provider::Mexc => validate_mexc_symbol(symbol),
        Provider::Bingx => validate_bingx_symbol(symbol),
    };
    if valid {
        Ok(())
    } else {
        Err(HistoryError::InvalidRequest(format!(
            "{symbol} is not an exact {provider} linear perpetual symbol"
        )))
    }
}

fn ascii_alphanumeric(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn unicode_alphanumeric(value: &str) -> bool {
    !value.is_empty() && value.chars().all(char::is_alphanumeric)
}

fn validate_okx_symbol(symbol: &str) -> bool {
    let Some(pair) = symbol.strip_suffix("-SWAP") else {
        return false;
    };
    let Some((base, quote)) = pair.split_once('-') else {
        return false;
    };
    !base.contains('-') && ascii_alphanumeric(base) && matches!(quote, "USDT" | "USDC")
}

fn validate_mexc_symbol(symbol: &str) -> bool {
    let Some((base, quote)) = symbol.split_once('_') else {
        return false;
    };
    !base.contains('_') && ascii_alphanumeric(base) && matches!(quote, "USDT" | "USDC")
}

fn validate_bingx_symbol(symbol: &str) -> bool {
    let Some((base, quote)) = symbol.split_once('-') else {
        return false;
    };
    !base.contains('-') && ascii_alphanumeric(base) && matches!(quote, "USDT" | "USDC")
}

fn endpoint(base: &Url, path: &str, provider: Provider) -> Result<Url, HistoryError> {
    base.join(path)
        .map_err(|error| invalid_payload(provider, format!("invalid REST source: {error}")))
}

fn exclusive_query_end(end_time_ms: u64) -> Result<u64, HistoryError> {
    end_time_ms.checked_sub(1).ok_or_else(|| {
        HistoryError::InvalidRequest("exclusive end timestamp must be positive".to_owned())
    })
}

fn count_page(provider: Provider, pages: &mut usize) -> Result<(), HistoryError> {
    if *pages >= MAX_UPSTREAM_PAGES {
        return Err(HistoryError::PaginationStalled {
            provider,
            cursor_ms: 0,
        });
    }
    *pages += 1;
    Ok(())
}

fn finish_result(
    request: &HistoryRequest,
    mut candles: Vec<Candle>,
) -> Result<HistoryResult, HistoryError> {
    candles.retain(|candle| {
        candle.start_time_ms >= request.start_time_ms && candle.end_time_ms <= request.end_time_ms
    });
    candles.sort_unstable_by_key(|candle| candle.start_time_ms);
    let mut unique: Vec<Candle> = Vec::with_capacity(candles.len().min(request.limit));
    for candle in candles {
        if let Some(previous) = unique.last()
            && previous.start_time_ms == candle.start_time_ms
        {
            if previous != &candle {
                return Err(invalid_payload(
                    request.provider,
                    format!("conflicting duplicate candle at {}", candle.start_time_ms),
                ));
            }
            continue;
        }
        unique.push(candle);
    }
    unique.truncate(request.limit);
    let candles = unique
        .into_iter()
        .map(|candle| HistoryCandle {
            provider: request.provider,
            market: MarketKind::LinearPerpetual,
            symbol: request.symbol.clone(),
            candle,
        })
        .collect();
    Ok(HistoryResult {
        provider: request.provider,
        market: MarketKind::LinearPerpetual,
        symbol: request.symbol.clone(),
        start_time_ms: request.start_time_ms,
        end_time_ms: request.end_time_ms,
        candles,
    })
}

#[derive(Debug)]
struct CandleFields {
    start_time_ms: u64,
    open: String,
    high: String,
    low: String,
    close: String,
    base_volume: Option<String>,
    quote_volume: Option<String>,
    contract_volume: Option<String>,
    finality: CandleFinality,
    data_quality: Vec<DataQuality>,
}

fn build_candle(provider: Provider, fields: CandleFields) -> Result<Candle, HistoryError> {
    if !fields.start_time_ms.is_multiple_of(CANDLE_INTERVAL_MS) {
        return Err(invalid_payload(
            provider,
            format!(
                "candle start {} is not a UTC minute boundary",
                fields.start_time_ms
            ),
        ));
    }
    let end_time_ms = fields
        .start_time_ms
        .checked_add(CANDLE_INTERVAL_MS)
        .ok_or_else(|| invalid_payload(provider, "candle end timestamp overflow"))?;
    let candle = Candle {
        interval: "1m".to_owned(),
        start_time_ms: fields.start_time_ms,
        end_time_ms,
        open: decimal(provider, &fields.open, "open")?,
        high: decimal(provider, &fields.high, "high")?,
        low: decimal(provider, &fields.low, "low")?,
        close: decimal(provider, &fields.close, "close")?,
        base_volume: optional_decimal(provider, fields.base_volume, "base volume")?,
        quote_volume: optional_decimal(provider, fields.quote_volume, "quote volume")?,
        contract_volume: optional_decimal(provider, fields.contract_volume, "contract volume")?,
        finality: fields.finality,
        data_quality: fields.data_quality,
    };
    candle
        .validate()
        .map_err(|error| invalid_payload(provider, error.to_string()))?;
    Ok(candle)
}

fn decimal(provider: Provider, value: &str, field: &str) -> Result<DecimalValue, HistoryError> {
    let normalized = if value.contains(['e', 'E']) {
        Decimal::from_scientific(value)
            .map(|parsed| parsed.to_string())
            .map_err(|_| invalid_payload(provider, format!("invalid {field}: {value}")))?
    } else {
        value.to_owned()
    };
    DecimalValue::new(normalized)
        .map_err(|error| invalid_payload(provider, format!("invalid {field}: {error}")))
}

fn optional_decimal(
    provider: Provider,
    value: Option<String>,
    field: &str,
) -> Result<Option<DecimalValue>, HistoryError> {
    value
        .map(|value| decimal(provider, &value, field))
        .transpose()
}

fn elapsed_finality(end_time_ms: u64, server_time_ms: u64) -> CandleFinality {
    if end_time_ms <= server_time_ms {
        CandleFinality::Closed
    } else {
        CandleFinality::Open
    }
}

#[derive(Debug, Deserialize)]
struct BybitHeader {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
}

#[derive(Debug, Deserialize)]
struct BybitResponse {
    #[serde(rename = "retCode")]
    ret_code: i64,
    result: BybitResult,
    time: u64,
}

#[derive(Debug, Deserialize)]
struct BybitResult {
    category: String,
    symbol: String,
    list: Vec<Vec<String>>,
}

fn parse_bybit(body: &str, expected_symbol: &str) -> Result<Vec<Candle>, HistoryError> {
    let header: BybitHeader = parse_json(body, Provider::Bybit)?;
    if header.ret_code != 0 {
        return Err(provider_rejected(
            Provider::Bybit,
            header.ret_code.to_string(),
            header.ret_msg,
        ));
    }
    let response: BybitResponse = parse_json(body, Provider::Bybit)?;
    if response.ret_code != 0 {
        return Err(invalid_payload(
            Provider::Bybit,
            "successful envelope changed retCode while parsing",
        ));
    }
    if response.result.category != "linear" || response.result.symbol != expected_symbol {
        return Err(invalid_payload(
            Provider::Bybit,
            "response category or symbol does not match request",
        ));
    }
    response
        .result
        .list
        .into_iter()
        .map(|row| parse_bybit_row(&row, response.time))
        .collect()
}

fn parse_bybit_row(row: &[String], server_time_ms: u64) -> Result<Candle, HistoryError> {
    require_len(Provider::Bybit, row.len(), 7)?;
    let start_time_ms = parse_u64_text(Provider::Bybit, &row[0], "start time")?;
    let end_time_ms = start_time_ms
        .checked_add(CANDLE_INTERVAL_MS)
        .ok_or_else(|| invalid_payload(Provider::Bybit, "candle end timestamp overflow"))?;
    build_candle(
        Provider::Bybit,
        CandleFields {
            start_time_ms,
            open: row[1].clone(),
            high: row[2].clone(),
            low: row[3].clone(),
            close: row[4].clone(),
            base_volume: Some(row[5].clone()),
            quote_volume: Some(row[6].clone()),
            contract_volume: None,
            finality: elapsed_finality(end_time_ms, server_time_ms),
            data_quality: Vec::new(),
        },
    )
}

fn parse_binance(body: &str, server_time_ms: u64) -> Result<Vec<Candle>, HistoryError> {
    let rows: Vec<Vec<Box<RawValue>>> = serde_json::from_str(body).map_err(|error| {
        invalid_payload(
            Provider::Binance,
            format!("response is not a kline array: {error}"),
        )
    })?;
    rows.into_iter()
        .map(|row| parse_binance_row(&row, server_time_ms))
        .collect()
}

fn parse_binance_row(row: &[Box<RawValue>], server_time_ms: u64) -> Result<Candle, HistoryError> {
    require_len(Provider::Binance, row.len(), 12)?;
    let start_time_ms = raw_u64(Provider::Binance, &row[0], "open time")?;
    let close_time_ms = raw_u64(Provider::Binance, &row[6], "close time")?;
    let end_time_ms = start_time_ms
        .checked_add(CANDLE_INTERVAL_MS)
        .ok_or_else(|| invalid_payload(Provider::Binance, "candle end timestamp overflow"))?;
    if close_time_ms.checked_add(1) != Some(end_time_ms) {
        return Err(invalid_payload(
            Provider::Binance,
            "one-minute close time is not open time plus 59,999 milliseconds",
        ));
    }
    build_candle(
        Provider::Binance,
        CandleFields {
            start_time_ms,
            open: raw_decimal(Provider::Binance, &row[1], "open")?,
            high: raw_decimal(Provider::Binance, &row[2], "high")?,
            low: raw_decimal(Provider::Binance, &row[3], "low")?,
            close: raw_decimal(Provider::Binance, &row[4], "close")?,
            base_volume: Some(raw_decimal(Provider::Binance, &row[5], "base volume")?),
            quote_volume: Some(raw_decimal(Provider::Binance, &row[7], "quote volume")?),
            contract_volume: None,
            finality: elapsed_finality(end_time_ms, server_time_ms),
            data_quality: Vec::new(),
        },
    )
}

#[derive(Debug, Deserialize)]
struct BinanceTime {
    #[serde(rename = "serverTime")]
    server_time: u64,
}

fn parse_binance_server_time(body: &str) -> Result<u64, HistoryError> {
    let response: BinanceTime = parse_json(body, Provider::Binance)?;
    if response.server_time == 0 {
        return Err(invalid_payload(
            Provider::Binance,
            "serverTime must be positive",
        ));
    }
    Ok(response.server_time)
}

#[derive(Debug, Deserialize)]
struct OkxHeader {
    code: String,
    msg: String,
}

#[derive(Debug, Deserialize)]
struct OkxResponse {
    code: String,
    data: Vec<Vec<String>>,
}

fn parse_okx(body: &str) -> Result<Vec<Candle>, HistoryError> {
    let header: OkxHeader = parse_json(body, Provider::Okx)?;
    if header.code != "0" {
        return Err(provider_rejected(Provider::Okx, header.code, header.msg));
    }
    let response: OkxResponse = parse_json(body, Provider::Okx)?;
    if response.code != "0" {
        return Err(invalid_payload(
            Provider::Okx,
            "successful envelope changed code while parsing",
        ));
    }
    response
        .data
        .into_iter()
        .map(|row| parse_okx_row(&row))
        .collect()
}

fn parse_okx_row(row: &[String]) -> Result<Candle, HistoryError> {
    require_len(Provider::Okx, row.len(), 9)?;
    let finality = match row[8].as_str() {
        "0" => CandleFinality::Open,
        "1" => CandleFinality::Closed,
        value => {
            return Err(invalid_payload(
                Provider::Okx,
                format!("unknown confirm value {value}"),
            ));
        }
    };
    build_candle(
        Provider::Okx,
        CandleFields {
            start_time_ms: parse_u64_text(Provider::Okx, &row[0], "start time")?,
            open: row[1].clone(),
            high: row[2].clone(),
            low: row[3].clone(),
            close: row[4].clone(),
            // For SWAP, OKX documents vol=contracts, volCcy=base currency, and
            // volCcyQuote=quote currency.
            contract_volume: Some(row[5].clone()),
            base_volume: Some(row[6].clone()),
            quote_volume: Some(row[7].clone()),
            finality,
            data_quality: Vec::new(),
        },
    )
}

#[derive(Debug, Deserialize)]
struct KucoinHeader {
    code: String,
    #[serde(default)]
    msg: String,
}

#[derive(Debug, Deserialize)]
struct KucoinResponse {
    code: String,
    data: Vec<Vec<Box<RawValue>>>,
}

fn parse_kucoin(body: &str) -> Result<Vec<Candle>, HistoryError> {
    let header: KucoinHeader = parse_json(body, Provider::Kucoin)?;
    if header.code != "200000" {
        return Err(provider_rejected(Provider::Kucoin, header.code, header.msg));
    }
    // RawValue is deliberate: Classic Futures currently emits OHLC as JSON numbers. Parsing via
    // f64 would silently lose decimal digits before they reach the normalized string contract.
    let response: KucoinResponse = parse_json(body, Provider::Kucoin)?;
    if response.code != "200000" {
        return Err(invalid_payload(
            Provider::Kucoin,
            "successful envelope changed code while parsing",
        ));
    }
    response
        .data
        .into_iter()
        .map(|row| parse_kucoin_row(&row))
        .collect()
}

fn parse_kucoin_row(row: &[Box<RawValue>]) -> Result<Candle, HistoryError> {
    require_len(Provider::Kucoin, row.len(), 7)?;
    build_candle(
        Provider::Kucoin,
        CandleFields {
            start_time_ms: raw_u64(Provider::Kucoin, &row[0], "start time")?,
            open: raw_decimal(Provider::Kucoin, &row[1], "open")?,
            high: raw_decimal(Provider::Kucoin, &row[2], "high")?,
            low: raw_decimal(Provider::Kucoin, &row[3], "low")?,
            close: raw_decimal(Provider::Kucoin, &row[4], "close")?,
            // KuCoin warns that Classic Futures candlestick data may be incomplete and its
            // candle-volume feed is not trustworthy. Neither numeric volume field is exposed.
            base_volume: None,
            quote_volume: None,
            contract_volume: None,
            finality: CandleFinality::Unknown,
            data_quality: vec![DataQuality::ProviderVolumeUntrusted],
        },
    )
}

#[derive(Debug, Deserialize)]
struct MexcHistoryEnvelope {
    success: bool,
    code: i64,
    data: MexcHistoryData,
}

#[derive(Debug, Deserialize)]
struct MexcHistoryData {
    time: Vec<Box<RawValue>>,
    open: Vec<Box<RawValue>>,
    close: Vec<Box<RawValue>>,
    high: Vec<Box<RawValue>>,
    low: Vec<Box<RawValue>>,
    vol: Vec<Box<RawValue>>,
    amount: Vec<Box<RawValue>>,
}

fn parse_mexc(body: &str, server_time_ms: u64) -> Result<Vec<Candle>, HistoryError> {
    let provider = Provider::Mexc;
    let response: MexcHistoryEnvelope = parse_json(body, provider)?;
    if !response.success || response.code != 0 {
        return Err(provider_rejected(
            provider,
            response.code.to_string(),
            format!("success={}", response.success),
        ));
    }
    let data = response.data;
    let expected = data.time.len();
    for (field, actual) in [
        ("open", data.open.len()),
        ("close", data.close.len()),
        ("high", data.high.len()),
        ("low", data.low.len()),
        ("vol", data.vol.len()),
        ("amount", data.amount.len()),
    ] {
        if actual != expected {
            return Err(invalid_payload(
                provider,
                format!("parallel {field} array has {actual} rows; expected {expected}"),
            ));
        }
    }
    (0..expected)
        .map(|index| {
            let start_time_ms = raw_u64(provider, &data.time[index], "time")?
                .checked_mul(1_000)
                .ok_or_else(|| invalid_payload(provider, "candle start timestamp overflow"))?;
            let end_time_ms = start_time_ms
                .checked_add(CANDLE_INTERVAL_MS)
                .ok_or_else(|| invalid_payload(provider, "candle end timestamp overflow"))?;
            build_candle(
                provider,
                CandleFields {
                    start_time_ms,
                    open: raw_decimal(provider, &data.open[index], "open")?,
                    high: raw_decimal(provider, &data.high[index], "high")?,
                    low: raw_decimal(provider, &data.low[index], "low")?,
                    close: raw_decimal(provider, &data.close[index], "close")?,
                    base_volume: None,
                    quote_volume: Some(raw_decimal(provider, &data.amount[index], "quote volume")?),
                    contract_volume: Some(raw_decimal(
                        provider,
                        &data.vol[index],
                        "contract volume",
                    )?),
                    finality: elapsed_finality(end_time_ms, server_time_ms),
                    data_quality: Vec::new(),
                },
            )
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct MexcTimeEnvelope {
    success: bool,
    code: i64,
    data: u64,
}

fn parse_mexc_server_time(body: &str) -> Result<u64, HistoryError> {
    let provider = Provider::Mexc;
    let response: MexcTimeEnvelope = parse_json(body, provider)?;
    if !response.success || response.code != 0 {
        return Err(provider_rejected(
            provider,
            response.code.to_string(),
            format!("success={}", response.success),
        ));
    }
    if response.data == 0 {
        return Err(invalid_payload(provider, "server time must be positive"));
    }
    Ok(response.data)
}

#[derive(Debug, Deserialize)]
struct BingxHistoryEnvelope {
    code: i64,
    #[serde(default)]
    msg: String,
    data: Vec<Box<RawValue>>,
}

#[derive(Debug, Deserialize)]
struct BingxHistoryObject {
    open: Box<RawValue>,
    close: Box<RawValue>,
    high: Box<RawValue>,
    low: Box<RawValue>,
    volume: Box<RawValue>,
    time: Box<RawValue>,
}

fn parse_bingx(body: &str, server_time_ms: u64) -> Result<Vec<Candle>, HistoryError> {
    let provider = Provider::Bingx;
    let response: BingxHistoryEnvelope = parse_json(body, provider)?;
    if response.code != 0 {
        return Err(provider_rejected(
            provider,
            response.code.to_string(),
            response.msg,
        ));
    }
    response
        .data
        .iter()
        .map(|row| parse_bingx_row(row, server_time_ms))
        .collect()
}

fn parse_bingx_row(row: &RawValue, server_time_ms: u64) -> Result<Candle, HistoryError> {
    let provider = Provider::Bingx;
    let raw = row.get().trim_start();
    let fields = if raw.starts_with('{') {
        let row: BingxHistoryObject = serde_json::from_str(raw).map_err(|error| {
            invalid_payload(provider, format!("invalid object candle row: {error}"))
        })?;
        let start_time_ms = raw_u64(provider, &row.time, "time")?;
        let end_time_ms = start_time_ms
            .checked_add(CANDLE_INTERVAL_MS)
            .ok_or_else(|| invalid_payload(provider, "candle end timestamp overflow"))?;
        CandleFields {
            start_time_ms,
            open: raw_decimal(provider, &row.open, "open")?,
            high: raw_decimal(provider, &row.high, "high")?,
            low: raw_decimal(provider, &row.low, "low")?,
            close: raw_decimal(provider, &row.close, "close")?,
            base_volume: Some(raw_decimal(provider, &row.volume, "base volume")?),
            quote_volume: None,
            contract_volume: None,
            finality: elapsed_finality(end_time_ms, server_time_ms),
            data_quality: Vec::new(),
        }
    } else if raw.starts_with('[') {
        let row: Vec<Box<RawValue>> = serde_json::from_str(raw).map_err(|error| {
            invalid_payload(provider, format!("invalid array candle row: {error}"))
        })?;
        require_len(provider, row.len(), 11)?;
        let start_time_ms = raw_u64(provider, &row[0], "open time")?;
        let end_time_ms = start_time_ms
            .checked_add(CANDLE_INTERVAL_MS)
            .ok_or_else(|| invalid_payload(provider, "candle end timestamp overflow"))?;
        let close_time_ms = raw_u64(provider, &row[6], "close time")?;
        if close_time_ms.checked_add(1) != Some(end_time_ms) {
            return Err(invalid_payload(
                provider,
                "one-minute candle close time is inconsistent with open time",
            ));
        }
        CandleFields {
            start_time_ms,
            open: raw_decimal(provider, &row[1], "open")?,
            high: raw_decimal(provider, &row[2], "high")?,
            low: raw_decimal(provider, &row[3], "low")?,
            close: raw_decimal(provider, &row[4], "close")?,
            base_volume: Some(raw_decimal(provider, &row[5], "base volume")?),
            quote_volume: Some(raw_decimal(provider, &row[7], "quote volume")?),
            contract_volume: None,
            finality: elapsed_finality(end_time_ms, server_time_ms),
            data_quality: Vec::new(),
        }
    } else {
        return Err(invalid_payload(
            provider,
            "candle row must be an object or array",
        ));
    };
    build_candle(provider, fields)
}

#[derive(Debug, Deserialize)]
struct BingxTimeEnvelope {
    code: i64,
    #[serde(default)]
    msg: String,
    data: BingxTimeData,
}

#[derive(Debug, Deserialize)]
struct BingxTimeData {
    #[serde(rename = "serverTime")]
    server_time: u64,
}

fn parse_bingx_server_time(body: &str) -> Result<u64, HistoryError> {
    let provider = Provider::Bingx;
    let response: BingxTimeEnvelope = parse_json(body, provider)?;
    if response.code != 0 {
        return Err(provider_rejected(
            provider,
            response.code.to_string(),
            response.msg,
        ));
    }
    if response.data.server_time == 0 {
        return Err(invalid_payload(provider, "serverTime must be positive"));
    }
    Ok(response.data.server_time)
}

fn raw_decimal(provider: Provider, value: &RawValue, field: &str) -> Result<String, HistoryError> {
    let raw = value.get();
    if raw.starts_with('"') {
        serde_json::from_str::<String>(raw)
            .map_err(|error| invalid_payload(provider, format!("invalid {field}: {error}")))
    } else {
        // DecimalValue performs the finite, non-exponent decimal validation later. Returning the
        // original token here preserves all provider digits and trailing zeroes.
        Ok(raw.to_owned())
    }
}

fn raw_u64(provider: Provider, value: &RawValue, field: &str) -> Result<u64, HistoryError> {
    let text = raw_decimal(provider, value, field)?;
    parse_u64_text(provider, &text, field)
}

fn parse_u64_text(provider: Provider, value: &str, field: &str) -> Result<u64, HistoryError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_payload(
            provider,
            format!("{field} must be an unsigned integer"),
        ));
    }
    value
        .parse::<u64>()
        .map_err(|_| invalid_payload(provider, format!("{field} is out of range")))
}

fn require_len(provider: Provider, actual: usize, expected: usize) -> Result<(), HistoryError> {
    if actual == expected {
        Ok(())
    } else {
        Err(invalid_payload(
            provider,
            format!("candle row has {actual} fields; expected {expected}"),
        ))
    }
}

fn parse_json<'a, T>(body: &'a str, provider: Provider) -> Result<T, HistoryError>
where
    T: Deserialize<'a>,
{
    serde_json::from_str(body)
        .map_err(|error| invalid_payload(provider, format!("invalid JSON: {error}")))
}

fn http_error(provider: Provider, status: StatusCode, body: &str) -> HistoryError {
    if let Some((code, message)) = provider_error_details(provider, body) {
        provider_rejected(provider, code, message)
    } else {
        HistoryError::HttpStatus {
            provider,
            status: status.as_u16(),
            message: summarize(body),
        }
    }
}

fn provider_error_details(provider: Provider, body: &str) -> Option<(String, String)> {
    let object = serde_json::from_str::<serde_json::Value>(body).ok()?;
    let (code_field, message_field) = match provider {
        Provider::Bybit => ("retCode", "retMsg"),
        Provider::Binance | Provider::Okx | Provider::Kucoin | Provider::Mexc | Provider::Bingx => {
            ("code", "msg")
        }
    };
    let code = object.get(code_field)?;
    let code = code
        .as_str()
        .map_or_else(|| code.to_string(), ToOwned::to_owned);
    let message = object
        .get(message_field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("provider rejected request")
        .to_owned();
    Some((code, message))
}

fn summarize(value: &str) -> String {
    let summary: String = value.chars().take(256).collect();
    if summary.is_empty() {
        "empty response".to_owned()
    } else {
        summary
    }
}

fn provider_rejected(provider: Provider, code: String, message: String) -> HistoryError {
    HistoryError::ProviderRejected {
        provider,
        code,
        message,
    }
}

fn invalid_payload(provider: Provider, message: impl Into<String>) -> HistoryError {
    HistoryError::InvalidPayload {
        provider,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Body;
    use axum::extract::{Request, State};
    use axum::http::{Response, StatusCode};
    use axum::routing::get;
    use pretty_assertions::assert_eq;
    use tokio::net::TcpListener;

    use super::*;

    const START: u64 = 1_800_000_000_000;
    type ObservedQueries = Arc<Mutex<Vec<HashMap<String, String>>>>;

    #[test]
    fn validates_exact_symbols_and_bounded_half_open_ranges() {
        assert!(HistoryRequest::new(Provider::Bybit, "BTCUSDT", START, START + 60_000, 1).is_ok());
        assert!(
            HistoryRequest::new(Provider::Okx, "BTC-USDT-SWAP", START, START + 60_000, 1).is_ok()
        );
        assert!(
            HistoryRequest::new(Provider::Kucoin, "XBTUSDTM", START, START + 60_000, 1).is_ok()
        );
        assert!(
            HistoryRequest::new(Provider::Binance, "btcusdt", START, START + 60_000, 1).is_err()
        );
        assert!(
            HistoryRequest::new(
                Provider::Binance,
                "\u{5e01}\u{5b89}\u{4eba}\u{751f}USDT",
                START,
                START + 60_000,
                1,
            )
            .is_ok()
        );
        assert!(
            HistoryRequest::new(
                Provider::Binance,
                "\u{5e01}\u{5b89}-\u{4eba}\u{751f}USDT",
                START,
                START + 60_000,
                1,
            )
            .is_err()
        );
        assert!(HistoryRequest::new(Provider::Okx, "BTCUSDT", START, START + 60_000, 1).is_err());
        assert!(
            HistoryRequest::new(Provider::Bybit, "BTCUSDT", START + 1, START + 60_000, 1).is_err()
        );
        assert!(
            HistoryRequest::new(
                Provider::Bybit,
                "BTCUSDT",
                START,
                START + MAX_HISTORY_RANGE_MS + CANDLE_INTERVAL_MS,
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn parses_bybit_reverse_rows_with_documented_linear_volume_units() {
        let body = format!(
            r#"{{"retCode":0,"retMsg":"OK","result":{{"symbol":"BTCUSDT","category":"linear","list":[["{}","2.0000","3.0","1.5","2.5","4.000","10.0000"],["{}","1.0000","2.0","0.5","2.0","3.000","5.0000"]]}},"time":{}}}"#,
            START + 60_000,
            START,
            START + 120_000,
        );
        let candles = parse_bybit(&body, "BTCUSDT").unwrap();
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].base_volume.as_ref().unwrap().as_str(), "4.000");
        assert_eq!(
            candles[0].quote_volume.as_ref().unwrap().as_str(),
            "10.0000"
        );
        assert_eq!(candles[0].finality, CandleFinality::Closed);
        assert_eq!(candles[1].open.as_str(), "1.0000");
    }

    #[test]
    fn parses_binance_close_time_and_usdm_volume_units_losslessly() {
        let body = format!(
            r#"[[{},"1.00000000","2.00000000","0.50000000","1.50000000","12.34000000",{},"18.510000000000000001",7,"1","2","0"]]"#,
            START,
            START + 59_999,
        );
        let candle = parse_binance(&body, START + 60_000).unwrap().remove(0);
        assert_eq!(candle.open.as_str(), "1.00000000");
        assert_eq!(
            candle.quote_volume.as_ref().unwrap().as_str(),
            "18.510000000000000001"
        );
        assert!(candle.contract_volume.is_none());
        assert_eq!(candle.finality, CandleFinality::Closed);
    }

    #[test]
    fn rejects_binance_non_one_minute_close_time() {
        let body = format!(
            r#"[[{},"1","2","0.5","1.5","12",{},"18",7,"1","2","0"]]"#,
            START,
            START + 60_000,
        );
        assert!(parse_binance(&body, START + 120_000).is_err());
    }

    #[test]
    fn parses_okx_confirm_and_all_three_documented_swap_volume_units() {
        let body = format!(
            r#"{{"code":"0","msg":"","data":[["{START}","1.0","2.0","0.5","1.5","123","0.0123","18.4500","1"]]}}"#,
        );
        let candle = parse_okx(&body).unwrap().remove(0);
        assert_eq!(candle.contract_volume.as_ref().unwrap().as_str(), "123");
        assert_eq!(candle.base_volume.as_ref().unwrap().as_str(), "0.0123");
        assert_eq!(candle.quote_volume.as_ref().unwrap().as_str(), "18.4500");
        assert_eq!(candle.finality, CandleFinality::Closed);
    }

    #[test]
    fn kucoin_preserves_numeric_lexemes_but_omits_untrusted_volume_and_finality() {
        let body = format!(
            r#"{{"code":"200000","data":[[{START},1.0000000000000000001,2.0000000000000000002,0.5000000000000000003,1.5000000000000000004,602,69424.9528]]}}"#,
        );
        let candle = parse_kucoin(&body).unwrap().remove(0);
        assert_eq!(candle.open.as_str(), "1.0000000000000000001");
        assert_eq!(candle.close.as_str(), "1.5000000000000000004");
        assert!(candle.base_volume.is_none());
        assert!(candle.quote_volume.is_none());
        assert!(candle.contract_volume.is_none());
        assert_eq!(candle.finality, CandleFinality::Unknown);
        assert_eq!(
            candle.data_quality,
            vec![DataQuality::ProviderVolumeUntrusted]
        );
    }

    #[test]
    fn parses_mexc_parallel_arrays_and_server_time_finality() {
        let start_seconds = START / 1_000;
        let body = format!(
            r#"{{"success":true,"code":0,"data":{{"time":[{start_seconds}],"open":[0.1400],"close":[0.1420],"high":[0.1430],"low":[0.1390],"vol":[12],"amount":[1.140119803232E7],"realOpen":[0.1400],"realClose":[0.1420],"realHigh":[0.1430],"realLow":[0.1390]}}}}"#,
        );

        let candle = parse_mexc(&body, START + CANDLE_INTERVAL_MS)
            .unwrap()
            .remove(0);

        assert_eq!(candle.open.as_str(), "0.1400");
        assert_eq!(candle.contract_volume.as_ref().unwrap().as_str(), "12");
        assert_eq!(
            candle.quote_volume.as_ref().unwrap().as_str(),
            "11401198.03232"
        );
        assert_eq!(candle.finality, CandleFinality::Closed);
        assert_eq!(
            parse_mexc_server_time(r#"{"success":true,"code":0,"data":1785558988408}"#).unwrap(),
            1_785_558_988_408
        );
    }

    #[test]
    fn parses_bingx_object_rows_and_distinguishes_open_interval() {
        let body = format!(
            r#"{{"code":0,"msg":"","data":[{{"open":"0.1419","close":"0.1420","high":"0.1421","low":"0.1418","volume":"6835","time":{}}},{{"open":"0.1420","close":"0.1422","high":"0.1423","low":"0.1419","volume":"123","time":{}}}]}}"#,
            START,
            START + CANDLE_INTERVAL_MS,
        );

        let candles = parse_bingx(&body, START + 90_000).unwrap();

        assert_eq!(candles[0].finality, CandleFinality::Closed);
        assert_eq!(candles[1].finality, CandleFinality::Open);
        assert_eq!(candles[0].base_volume.as_ref().unwrap().as_str(), "6835");
        assert!(candles[0].quote_volume.is_none());
        let documented = format!(
            r#"{{"code":0,"msg":"","data":[[{START},"0.1419","0.1421","0.1418","0.1420","6835",{},"970.57",12,"3000","426.0"]]}}"#,
            START + CANDLE_INTERVAL_MS - 1,
        );
        let documented = parse_bingx(&documented, START + CANDLE_INTERVAL_MS)
            .unwrap()
            .remove(0);
        assert_eq!(documented.base_volume.as_ref().unwrap().as_str(), "6835");
        assert_eq!(documented.quote_volume.as_ref().unwrap().as_str(), "970.57");
        assert_eq!(
            parse_bingx_server_time(r#"{"code":0,"msg":"","data":{"serverTime":1785558987873}}"#)
                .unwrap(),
            1_785_558_987_873
        );
    }

    #[test]
    fn surfaces_provider_response_codes() {
        assert!(matches!(
            parse_bybit(
                r#"{"retCode":10001,"retMsg":"bad symbol","result":{},"time":1}"#,
                "BTCUSDT"
            ),
            Err(HistoryError::ProviderRejected {
                provider: Provider::Bybit,
                code,
                ..
            }) if code == "10001"
        ));
        assert!(matches!(
            parse_okx(r#"{"code":"51000","msg":"bad instId","data":[]}"#),
            Err(HistoryError::ProviderRejected {
                provider: Provider::Okx,
                code,
                ..
            }) if code == "51000"
        ));
        assert!(matches!(
            parse_kucoin(r#"{"code":"400100","msg":"bad symbol","data":[]}"#),
            Err(HistoryError::ProviderRejected {
                provider: Provider::Kucoin,
                code,
                ..
            }) if code == "400100"
        ));
    }

    #[test]
    fn ordering_deduplication_and_exclusive_end_are_deterministic() {
        let request =
            HistoryRequest::new(Provider::Bybit, "BTCUSDT", START, START + 120_000, 10).unwrap();
        let first = test_candle(START, "1");
        let second = test_candle(START + 60_000, "2");
        let outside = test_candle(START + 120_000, "3");
        let result = finish_result(
            &request,
            vec![second.clone(), first.clone(), second, outside],
        )
        .unwrap();
        assert_eq!(result.candles.len(), 2);
        assert_eq!(result.candles[0].candle, first);
        assert_eq!(result.candles[1].candle.start_time_ms, START + 60_000);
    }

    #[tokio::test]
    async fn kucoin_paginates_ranges_larger_than_five_hundred_minutes() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(KUCOIN_ROUTE, get(kucoin_mock))
            .with_state(Arc::clone(&observed));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let base = Url::parse(&format!("http://{address}/")).unwrap();
        let sources = HistorySources {
            bybit: base.clone(),
            binance: base.clone(),
            okx: base.clone(),
            kucoin: base.clone(),
            mexc: base.clone(),
            bingx: base,
        };
        let client = HistoryClient::with_client(Client::new(), sources);
        let end = START + 501 * CANDLE_INTERVAL_MS;
        let request = HistoryRequest::new(Provider::Kucoin, "XBTUSDTM", START, end, 10).unwrap();

        let result = client.fetch(&request).await.unwrap();

        server.abort();
        assert_eq!(result.candles.len(), 2);
        assert_eq!(result.candles[0].candle.start_time_ms, START);
        assert_eq!(
            result.candles[1].candle.start_time_ms,
            START + 500 * CANDLE_INTERVAL_MS
        );
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0]["from"], START.to_string());
        assert_eq!(
            observed[0]["to"],
            (START + 500 * CANDLE_INTERVAL_MS - 1).to_string()
        );
        assert_eq!(
            observed[1]["from"],
            (START + 500 * CANDLE_INTERVAL_MS).to_string()
        );
        assert_eq!(observed[1]["to"], (end - 1).to_string());
    }

    const KUCOIN_ROUTE: &str = "/api/v1/kline/query";

    async fn kucoin_mock(
        State(observed): State<ObservedQueries>,
        request: Request,
    ) -> Response<Body> {
        let query = request.uri().query().unwrap_or_default();
        let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let start = params["from"].parse::<u64>().unwrap();
        observed.lock().unwrap().push(params);
        let body = format!(r#"{{"code":"200000","data":[[{start},1.0,2.0,0.5,1.5,10,15.0]]}}"#);
        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    fn test_candle(start_time_ms: u64, close: &str) -> Candle {
        Candle {
            interval: "1m".to_owned(),
            start_time_ms,
            end_time_ms: start_time_ms + CANDLE_INTERVAL_MS,
            open: DecimalValue::new("1").unwrap(),
            high: DecimalValue::new("3").unwrap(),
            low: DecimalValue::new("0.5").unwrap(),
            close: DecimalValue::new(close).unwrap(),
            base_volume: None,
            quote_volume: None,
            contract_volume: None,
            finality: CandleFinality::Closed,
            data_quality: Vec::new(),
        }
    }
}
