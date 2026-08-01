//! Public, provider-neutral linear-perpetual instrument catalog.
//!
//! Every upstream is refreshed independently. A successful refresh replaces one provider's
//! complete snapshot while a failed refresh leaves that provider's last-good snapshot intact.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::str::FromStr;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::future::join_all;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use tokio::sync::Mutex;
use url::Url;

use crate::domain::{
    Channel, DecimalValue, MarketKind, Provider, SubscriptionKey, normalize_symbol,
};

const BYBIT_CATALOG_URL: &str = "https://api.bybit.com/v5/market/instruments-info";
const BINANCE_CATALOG_URL: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
const OKX_CATALOG_URL: &str = "https://www.okx.com/api/v5/public/instruments";
const KUCOIN_CATALOG_URL: &str = "https://api-futures.kucoin.com/api/v1/contracts/active";
const BYBIT_PAGE_LIMIT: &str = "1000";
const BYBIT_MAX_PAGES: usize = 10_000;
const MAX_CATALOG_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const ALL_PROVIDERS: [Provider; 4] = [
    Provider::Bybit,
    Provider::Binance,
    Provider::Okx,
    Provider::Kucoin,
];

/// Normalized lifecycle state. Catalog snapshots contain live instruments only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentStatus {
    Live,
}

/// Whether a streamed one-minute candle has an authoritative closed/open state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleFinalitySupport {
    Authoritative,
    Unknown,
}

/// Whether the provider exposes usable one-minute candle volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleVolumeSupport {
    Available,
    Unavailable,
}

/// Stream features and their provider-specific data-quality constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentCapabilities {
    pub ticker: bool,
    pub candle_1m: bool,
    pub candle_1m_finality: CandleFinalitySupport,
    pub candle_1m_volume: CandleVolumeSupport,
}

impl InstrumentCapabilities {
    pub fn supports(self, channel: Channel) -> bool {
        match channel {
            Channel::Ticker => self.ticker,
            Channel::Candle1m => self.candle_1m,
        }
    }
}

/// Provider-neutral metadata for one exact venue instrument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    pub instrument_id: String,
    /// Exact symbol used by the provider's public APIs.
    pub symbol: String,
    pub provider: Provider,
    pub market: MarketKind,
    /// Exact base-asset identifier reported by the venue.
    pub base_asset: String,
    /// Exact quote-asset identifier reported by the venue.
    pub quote_asset: String,
    /// Exact settlement-asset identifier reported by the venue.
    pub settle_asset: String,
    pub status: InstrumentStatus,
    /// Unmodified provider lifecycle label, such as `Trading`, `TRADING`, `live`, or `Open`.
    pub venue_status: String,
    pub tick_size: DecimalValue,
    pub quantity_step: DecimalValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_size: Option<DecimalValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_size_asset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_leverage: Option<DecimalValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub listing_time_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry_time_ms: Option<u64>,
    pub capabilities: InstrumentCapabilities,
}

/// Public REST endpoints and provider enablement for catalog refreshes.
#[derive(Debug, Clone)]
pub struct CatalogSources {
    pub bybit: Url,
    pub binance: Url,
    pub okx: Url,
    pub kucoin: Url,
    pub enabled_providers: BTreeSet<Provider>,
}

impl CatalogSources {
    pub fn endpoint(&self, provider: Provider) -> &Url {
        match provider {
            Provider::Bybit => &self.bybit,
            Provider::Binance => &self.binance,
            Provider::Okx => &self.okx,
            Provider::Kucoin => &self.kucoin,
        }
    }

    pub fn is_enabled(&self, provider: Provider) -> bool {
        self.enabled_providers.contains(&provider)
    }
}

impl Default for CatalogSources {
    fn default() -> Self {
        Self {
            bybit: static_url(BYBIT_CATALOG_URL),
            binance: static_url(BINANCE_CATALOG_URL),
            okx: static_url(OKX_CATALOG_URL),
            kucoin: static_url(KUCOIN_CATALOG_URL),
            enabled_providers: ALL_PROVIDERS.into_iter().collect(),
        }
    }
}

/// Current refresh state for one provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCatalogStatus {
    pub provider: Provider,
    pub enabled: bool,
    pub instrument_count: usize,
    pub last_attempt_at_ms: Option<u64>,
    pub last_success_at_ms: Option<u64>,
    pub last_error: Option<String>,
}

/// Successful refresh summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshOutcome {
    pub provider: Provider,
    pub instrument_count: usize,
    pub refreshed_at_ms: u64,
}

/// Optional exact-match filters for catalog listings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CatalogFilter {
    pub provider: Option<Provider>,
    pub market: Option<MarketKind>,
    pub symbol: Option<String>,
    pub base_asset: Option<String>,
    pub quote_asset: Option<String>,
    pub settle_asset: Option<String>,
}

impl CatalogFilter {
    fn matches(&self, instrument: &Instrument) -> bool {
        self.provider
            .is_none_or(|provider| instrument.provider == provider)
            && self.market.is_none_or(|market| instrument.market == market)
            && matches_optional(self.symbol.as_ref(), &instrument.symbol)
            && matches_optional(self.base_asset.as_ref(), &instrument.base_asset)
            && matches_optional(self.quote_asset.as_ref(), &instrument.quote_asset)
            && matches_optional(self.settle_asset.as_ref(), &instrument.settle_asset)
    }
}

/// Catalog fetch, parse, and subscription-validation failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("{0} instrument catalog is disabled")]
    ProviderDisabled(Provider),
    #[error("{provider} catalog request failed: {message}")]
    Request { provider: Provider, message: String },
    #[error("{provider} catalog endpoint returned HTTP {status}")]
    HttpStatus { provider: Provider, status: u16 },
    #[error("{provider} catalog payload is invalid: {message}")]
    InvalidPayload { provider: Provider, message: String },
    #[error("{provider} catalog returned duplicate instrument {symbol}")]
    DuplicateInstrument { provider: Provider, symbol: String },
    #[error("{provider} catalog pagination repeated cursor {cursor}")]
    RepeatedCursor { provider: Provider, cursor: String },
    #[error("{provider} catalog exceeded the {limit}-page pagination safety limit")]
    PaginationLimit { provider: Provider, limit: usize },
    #[error("{0} catalog returned no live linear perpetual instruments")]
    EmptySnapshot(Provider),
    #[error("instrument is not available: {provider}:{market}:{symbol}")]
    InstrumentUnavailable {
        provider: Provider,
        market: MarketKind,
        symbol: String,
    },
    #[error("{channel} is not available for {provider}:{market}:{symbol}")]
    UnsupportedChannel {
        provider: Provider,
        market: MarketKind,
        symbol: String,
        channel: Channel,
    },
}

/// A cloneable, thread-safe collection of independent provider snapshots.
#[derive(Clone)]
pub struct Catalog {
    http: Client,
    sources: CatalogSources,
    state: Arc<RwLock<CatalogState>>,
    refresh_locks: Arc<ProviderRefreshLocks>,
}

impl Catalog {
    pub fn new(http: Client, sources: CatalogSources) -> Self {
        let providers = ALL_PROVIDERS
            .into_iter()
            .map(|provider| {
                (
                    provider,
                    ProviderSnapshot {
                        instruments: BTreeMap::new(),
                        status: ProviderCatalogStatus {
                            provider,
                            enabled: sources.is_enabled(provider),
                            instrument_count: 0,
                            last_attempt_at_ms: None,
                            last_success_at_ms: None,
                            last_error: None,
                        },
                    },
                )
            })
            .collect();
        Self {
            http,
            sources,
            state: Arc::new(RwLock::new(CatalogState { providers })),
            refresh_locks: Arc::new(ProviderRefreshLocks::default()),
        }
    }

    pub fn enabled_providers(&self) -> BTreeSet<Provider> {
        self.sources.enabled_providers.clone()
    }

    pub fn list(&self) -> Vec<Instrument> {
        self.filter(&CatalogFilter::default())
    }

    pub fn filter(&self, filter: &CatalogFilter) -> Vec<Instrument> {
        read_lock(&self.state)
            .providers
            .values()
            .flat_map(|snapshot| snapshot.instruments.values())
            .filter(|instrument| filter.matches(instrument))
            .cloned()
            .collect()
    }

    pub fn get(&self, provider: Provider, market: MarketKind, symbol: &str) -> Option<Instrument> {
        if market != MarketKind::LinearPerpetual {
            return None;
        }
        let symbol = symbol.trim();
        let state = read_lock(&self.state);
        let instruments = &state.providers.get(&provider)?.instruments;
        instruments.get(symbol).cloned().or_else(|| {
            normalize_symbol(symbol)
                .ok()
                .and_then(|normalized| instruments.get(&normalized).cloned())
        })
    }

    pub fn get_by_id(&self, instrument_id: &str) -> Option<Instrument> {
        read_lock(&self.state)
            .providers
            .values()
            .flat_map(|snapshot| snapshot.instruments.values())
            .find(|instrument| instrument.instrument_id == instrument_id)
            .cloned()
    }

    pub fn status(&self, provider: Provider) -> ProviderCatalogStatus {
        read_lock(&self.state).providers[&provider].status.clone()
    }

    pub fn statuses(&self) -> Vec<ProviderCatalogStatus> {
        read_lock(&self.state)
            .providers
            .values()
            .map(|snapshot| snapshot.status.clone())
            .collect()
    }

    /// Validate that a subscription resolves to a live linear perpetual and supported channel.
    ///
    /// # Errors
    ///
    /// Returns an error for disabled providers, unknown or rejected instruments, and unsupported
    /// channels.
    pub fn validate_subscription(
        &self,
        subscription: &SubscriptionKey,
    ) -> Result<Instrument, CatalogError> {
        if !self.sources.is_enabled(subscription.provider) {
            return Err(CatalogError::ProviderDisabled(subscription.provider));
        }
        let instrument = self
            .get(
                subscription.provider,
                subscription.market,
                &subscription.symbol,
            )
            .ok_or_else(|| CatalogError::InstrumentUnavailable {
                provider: subscription.provider,
                market: subscription.market,
                symbol: subscription.symbol.clone(),
            })?;
        if !instrument.capabilities.supports(subscription.channel) {
            return Err(CatalogError::UnsupportedChannel {
                provider: subscription.provider,
                market: subscription.market,
                symbol: subscription.symbol.clone(),
                channel: subscription.channel,
            });
        }
        Ok(instrument)
    }

    /// Refresh one provider without changing any other provider snapshot.
    ///
    /// # Errors
    ///
    /// Returns a provider-specific request or parse error. The previous snapshot is retained when
    /// any error occurs.
    pub async fn refresh_provider(
        &self,
        provider: Provider,
    ) -> Result<RefreshOutcome, CatalogError> {
        if !self.sources.is_enabled(provider) {
            return Err(CatalogError::ProviderDisabled(provider));
        }

        let refresh_lock = self.refresh_locks.for_provider(provider);
        let _guard = refresh_lock.lock().await;
        let attempted_at_ms = unix_time_ms();
        let result = self.fetch_provider(provider).await.and_then(|instruments| {
            if instruments.is_empty() {
                Err(CatalogError::EmptySnapshot(provider))
            } else {
                Ok(instruments)
            }
        });

        match result {
            Ok(instruments) => {
                let refreshed_at_ms = unix_time_ms();
                let instrument_count = instruments.len();
                let mut state = write_lock(&self.state);
                let Some(snapshot) = state.providers.get_mut(&provider) else {
                    return Err(invalid_payload(
                        provider,
                        "internal catalog state is missing provider",
                    ));
                };
                snapshot.instruments = instruments;
                snapshot.status.instrument_count = instrument_count;
                snapshot.status.last_attempt_at_ms = Some(attempted_at_ms);
                snapshot.status.last_success_at_ms = Some(refreshed_at_ms);
                snapshot.status.last_error = None;
                Ok(RefreshOutcome {
                    provider,
                    instrument_count,
                    refreshed_at_ms,
                })
            }
            Err(error) => {
                let mut state = write_lock(&self.state);
                if let Some(snapshot) = state.providers.get_mut(&provider) {
                    snapshot.status.last_attempt_at_ms = Some(attempted_at_ms);
                    snapshot.status.last_error = Some(error.to_string());
                }
                Err(error)
            }
        }
    }

    /// Refresh all enabled providers concurrently and report each result independently.
    pub async fn refresh_all(&self) -> BTreeMap<Provider, Result<RefreshOutcome, CatalogError>> {
        let providers = self.enabled_providers();
        let results = join_all(
            providers
                .into_iter()
                .map(|provider| async move { (provider, self.refresh_provider(provider).await) }),
        )
        .await;
        results.into_iter().collect()
    }

    async fn fetch_provider(
        &self,
        provider: Provider,
    ) -> Result<BTreeMap<String, Instrument>, CatalogError> {
        match provider {
            Provider::Bybit => self.fetch_bybit().await,
            Provider::Binance => {
                let body = self
                    .get_body(provider, self.sources.endpoint(provider))
                    .await?;
                parse_binance_catalog(&body)
            }
            Provider::Okx => {
                let mut url = self.sources.endpoint(provider).clone();
                url.query_pairs_mut()
                    .clear()
                    .append_pair("instType", "SWAP");
                let body = self.get_body(provider, &url).await?;
                parse_okx_catalog(&body)
            }
            Provider::Kucoin => {
                let body = self
                    .get_body(provider, self.sources.endpoint(provider))
                    .await?;
                parse_kucoin_catalog(&body)
            }
        }
    }

    async fn fetch_bybit(&self) -> Result<BTreeMap<String, Instrument>, CatalogError> {
        let provider = Provider::Bybit;
        let mut instruments = BTreeMap::new();
        let mut cursor: Option<String> = None;
        let mut requested_cursors = HashSet::new();

        for _ in 0..BYBIT_MAX_PAGES {
            if let Some(current) = &cursor
                && !requested_cursors.insert(current.clone())
            {
                return Err(CatalogError::RepeatedCursor {
                    provider,
                    cursor: current.clone(),
                });
            }

            let mut url = self.sources.endpoint(provider).clone();
            {
                let mut query = url.query_pairs_mut();
                query
                    .clear()
                    .append_pair("category", "linear")
                    .append_pair("limit", BYBIT_PAGE_LIMIT);
                if let Some(current) = &cursor {
                    query.append_pair("cursor", current);
                }
            }

            let body = self.get_body(provider, &url).await?;
            let page = parse_bybit_page(&body)?;
            for instrument in page.instruments {
                insert_unique(&mut instruments, instrument)?;
            }

            let Some(next) = nonempty(page.next_page_cursor) else {
                return Ok(instruments);
            };
            if cursor.as_ref() == Some(&next) || requested_cursors.contains(&next) {
                return Err(CatalogError::RepeatedCursor {
                    provider,
                    cursor: next,
                });
            }
            cursor = Some(next);
        }

        Err(CatalogError::PaginationLimit {
            provider,
            limit: BYBIT_MAX_PAGES,
        })
    }

    async fn get_body(&self, provider: Provider, url: &Url) -> Result<String, CatalogError> {
        let mut response = self
            .http
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|error| CatalogError::Request {
                provider,
                message: error.to_string(),
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(CatalogError::HttpStatus {
                provider,
                status: status.as_u16(),
            });
        }
        let content_length = response.content_length();
        if content_length.is_some_and(|length| length > MAX_CATALOG_RESPONSE_BYTES) {
            return Err(invalid_payload(
                provider,
                format!("response exceeds the {MAX_CATALOG_RESPONSE_BYTES} byte limit"),
            ));
        }
        let capacity = usize::try_from(content_length.unwrap_or(0)).unwrap_or(0);
        let mut body = Vec::with_capacity(capacity);
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| CatalogError::Request {
                provider,
                message: format!("could not read response body: {error}"),
            })?
        {
            let total_length = body.len().saturating_add(chunk.len());
            if u64::try_from(total_length).unwrap_or(u64::MAX) > MAX_CATALOG_RESPONSE_BYTES {
                return Err(invalid_payload(
                    provider,
                    format!("response exceeds the {MAX_CATALOG_RESPONSE_BYTES} byte limit"),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        std::str::from_utf8(&body)
            .map(str::to_owned)
            .map_err(|_| invalid_payload(provider, "response body is not UTF-8"))
    }
}

#[derive(Debug, Default)]
struct CatalogState {
    providers: BTreeMap<Provider, ProviderSnapshot>,
}

#[derive(Debug)]
struct ProviderSnapshot {
    instruments: BTreeMap<String, Instrument>,
    status: ProviderCatalogStatus,
}

#[derive(Debug, Default)]
struct ProviderRefreshLocks {
    bybit: Mutex<()>,
    binance: Mutex<()>,
    okx: Mutex<()>,
    kucoin: Mutex<()>,
}

impl ProviderRefreshLocks {
    fn for_provider(&self, provider: Provider) -> &Mutex<()> {
        match provider {
            Provider::Bybit => &self.bybit,
            Provider::Binance => &self.binance,
            Provider::Okx => &self.okx,
            Provider::Kucoin => &self.kucoin,
        }
    }
}

#[derive(Debug)]
struct BybitPage {
    instruments: Vec<Instrument>,
    next_page_cursor: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitEnvelope {
    ret_code: i64,
    #[serde(default)]
    ret_msg: String,
    result: Option<BybitResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitResult {
    category: String,
    #[serde(default)]
    next_page_cursor: String,
    list: Vec<BybitInstrument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitInstrument {
    symbol: String,
    contract_type: String,
    status: String,
    base_coin: String,
    quote_coin: String,
    settle_coin: String,
    launch_time: Option<WireScalar>,
    delivery_time: Option<WireScalar>,
    price_filter: BybitPriceFilter,
    lot_size_filter: BybitLotSizeFilter,
    leverage_filter: Option<BybitLeverageFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitPriceFilter {
    tick_size: WireScalar,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLotSizeFilter {
    qty_step: WireScalar,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BybitLeverageFilter {
    max_leverage: WireScalar,
}

fn parse_bybit_page(body: &str) -> Result<BybitPage, CatalogError> {
    let provider = Provider::Bybit;
    let envelope: BybitEnvelope = serde_json::from_str(body)
        .map_err(|error| invalid_payload(provider, format!("invalid JSON: {error}")))?;
    if envelope.ret_code != 0 {
        return Err(invalid_payload(
            provider,
            format!(
                "endpoint returned code {}: {}",
                envelope.ret_code, envelope.ret_msg
            ),
        ));
    }
    let result = envelope
        .result
        .ok_or_else(|| invalid_payload(provider, "missing result"))?;
    if result.category != "linear" {
        return Err(invalid_payload(
            provider,
            format!("expected linear category, got {}", result.category),
        ));
    }
    let instruments = result
        .list
        .into_iter()
        .filter_map(|item| match bybit_instrument(item) {
            Ok(Some(instrument)) => Some(Ok(instrument)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BybitPage {
        instruments,
        next_page_cursor: result.next_page_cursor,
    })
}

fn bybit_instrument(item: BybitInstrument) -> Result<Option<Instrument>, CatalogError> {
    let provider = Provider::Bybit;
    if item.contract_type != "LinearPerpetual" || item.status != "Trading" {
        return Ok(None);
    }
    let symbol = exact_symbol(provider, item.symbol)?;
    let max_leverage = item
        .leverage_filter
        .map(|filter| positive_decimal(provider, "leverageFilter.maxLeverage", filter.max_leverage))
        .transpose()?;
    Ok(Some(Instrument {
        instrument_id: instrument_id(provider, &symbol),
        symbol,
        provider,
        market: MarketKind::LinearPerpetual,
        base_asset: exact_asset(provider, "baseCoin", item.base_coin)?,
        quote_asset: exact_asset(provider, "quoteCoin", item.quote_coin)?,
        settle_asset: exact_asset(provider, "settleCoin", item.settle_coin)?,
        status: InstrumentStatus::Live,
        venue_status: item.status,
        tick_size: positive_decimal(
            provider,
            "priceFilter.tickSize",
            item.price_filter.tick_size,
        )?,
        quantity_step: positive_decimal(
            provider,
            "lotSizeFilter.qtyStep",
            item.lot_size_filter.qty_step,
        )?,
        contract_size: None,
        contract_size_asset: None,
        max_leverage,
        listing_time_ms: optional_timestamp_ms(provider, "launchTime", item.launch_time)?,
        expiry_time_ms: optional_timestamp_ms(provider, "deliveryTime", item.delivery_time)?,
        capabilities: capabilities(provider),
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceEnvelope {
    symbols: Vec<BinanceInstrument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceInstrument {
    symbol: String,
    contract_type: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    margin_asset: String,
    onboard_date: Option<u64>,
    filters: Vec<BinanceFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceFilter {
    filter_type: String,
    tick_size: Option<WireScalar>,
    step_size: Option<WireScalar>,
}

fn parse_binance_catalog(body: &str) -> Result<BTreeMap<String, Instrument>, CatalogError> {
    let provider = Provider::Binance;
    let envelope: BinanceEnvelope = serde_json::from_str(body)
        .map_err(|error| invalid_payload(provider, format!("invalid JSON: {error}")))?;
    let mut instruments = BTreeMap::new();
    for item in envelope.symbols {
        if let Some(instrument) = binance_instrument(item)? {
            insert_unique(&mut instruments, instrument)?;
        }
    }
    Ok(instruments)
}

fn binance_instrument(item: BinanceInstrument) -> Result<Option<Instrument>, CatalogError> {
    let provider = Provider::Binance;
    if item.contract_type != "PERPETUAL" || item.status != "TRADING" {
        return Ok(None);
    }
    let tick_size = unique_filter_decimal(
        provider,
        &item.filters,
        "PRICE_FILTER",
        "tickSize",
        |filter| filter.tick_size.clone(),
    )?;
    let quantity_step =
        unique_filter_decimal(provider, &item.filters, "LOT_SIZE", "stepSize", |filter| {
            filter.step_size.clone()
        })?;
    let symbol = exact_symbol(provider, item.symbol)?;
    Ok(Some(Instrument {
        instrument_id: instrument_id(provider, &symbol),
        symbol,
        provider,
        market: MarketKind::LinearPerpetual,
        base_asset: exact_asset(provider, "baseAsset", item.base_asset)?,
        quote_asset: exact_asset(provider, "quoteAsset", item.quote_asset)?,
        settle_asset: exact_asset(provider, "marginAsset", item.margin_asset)?,
        status: InstrumentStatus::Live,
        venue_status: item.status,
        tick_size,
        quantity_step,
        contract_size: None,
        contract_size_asset: None,
        max_leverage: None,
        listing_time_ms: item.onboard_date.filter(|value| *value != 0),
        // USD-M perpetuals expose a distant sentinel deliveryDate; it is not an expiry.
        expiry_time_ms: None,
        capabilities: capabilities(provider),
    }))
}

fn unique_filter_decimal<F>(
    provider: Provider,
    filters: &[BinanceFilter],
    filter_type: &str,
    field: &str,
    value: F,
) -> Result<DecimalValue, CatalogError>
where
    F: Fn(&BinanceFilter) -> Option<WireScalar>,
{
    let matching = filters
        .iter()
        .filter(|filter| filter.filter_type == filter_type)
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        return Err(invalid_payload(
            provider,
            format!("expected exactly one {filter_type}, got {}", matching.len()),
        ));
    }
    let scalar = value(matching[0])
        .ok_or_else(|| invalid_payload(provider, format!("{filter_type} is missing {field}")))?;
    positive_decimal(provider, &format!("{filter_type}.{field}"), scalar)
}

#[derive(Debug, Deserialize)]
struct OkxEnvelope {
    code: String,
    #[serde(default)]
    msg: String,
    data: Vec<OkxInstrument>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OkxInstrument {
    inst_id: String,
    inst_type: String,
    state: String,
    #[serde(default)]
    base_ccy: String,
    #[serde(default)]
    quote_ccy: String,
    settle_ccy: String,
    ct_type: String,
    ct_val: WireScalar,
    #[serde(default)]
    ct_val_ccy: String,
    tick_sz: WireScalar,
    lot_sz: WireScalar,
    #[serde(default)]
    lever: Option<WireScalar>,
    #[serde(default)]
    list_time: Option<WireScalar>,
    #[serde(default)]
    exp_time: Option<WireScalar>,
    #[serde(default)]
    uly: String,
    #[serde(default)]
    inst_family: String,
}

fn parse_okx_catalog(body: &str) -> Result<BTreeMap<String, Instrument>, CatalogError> {
    let provider = Provider::Okx;
    let envelope: OkxEnvelope = serde_json::from_str(body)
        .map_err(|error| invalid_payload(provider, format!("invalid JSON: {error}")))?;
    if envelope.code != "0" {
        return Err(invalid_payload(
            provider,
            format!("endpoint returned code {}: {}", envelope.code, envelope.msg),
        ));
    }
    let mut instruments = BTreeMap::new();
    for item in envelope.data {
        if let Some(instrument) = okx_instrument(item)? {
            insert_unique(&mut instruments, instrument)?;
        }
    }
    Ok(instruments)
}

fn okx_instrument(item: OkxInstrument) -> Result<Option<Instrument>, CatalogError> {
    let provider = Provider::Okx;
    if item.inst_type != "SWAP" || item.ct_type != "linear" || item.state != "live" {
        return Ok(None);
    }
    let (derived_base, derived_quote) = okx_underlying_assets(&item)?;
    let base_asset = if item.base_ccy.is_empty() {
        derived_base
    } else {
        exact_asset(provider, "baseCcy", item.base_ccy)?
    };
    let quote_asset = if item.quote_ccy.is_empty() {
        derived_quote
    } else {
        exact_asset(provider, "quoteCcy", item.quote_ccy)?
    };
    let contract_size_asset = if item.ct_val_ccy.is_empty() {
        base_asset.clone()
    } else {
        exact_asset(provider, "ctValCcy", item.ct_val_ccy)?
    };
    let symbol = exact_symbol(provider, item.inst_id)?;
    Ok(Some(Instrument {
        instrument_id: instrument_id(provider, &symbol),
        symbol,
        provider,
        market: MarketKind::LinearPerpetual,
        base_asset,
        quote_asset,
        settle_asset: exact_asset(provider, "settleCcy", item.settle_ccy)?,
        status: InstrumentStatus::Live,
        venue_status: item.state,
        tick_size: positive_decimal(provider, "tickSz", item.tick_sz)?,
        quantity_step: positive_decimal(provider, "lotSz", item.lot_sz)?,
        contract_size: Some(positive_decimal(provider, "ctVal", item.ct_val)?),
        contract_size_asset: Some(contract_size_asset),
        max_leverage: optional_positive_decimal(provider, "lever", item.lever)?,
        listing_time_ms: optional_timestamp_ms(provider, "listTime", item.list_time)?,
        expiry_time_ms: optional_timestamp_ms(provider, "expTime", item.exp_time)?,
        capabilities: capabilities(provider),
    }))
}

fn okx_underlying_assets(item: &OkxInstrument) -> Result<(String, String), CatalogError> {
    let provider = Provider::Okx;
    let underlying = if item.uly.is_empty() {
        &item.inst_family
    } else {
        &item.uly
    };
    let (base, quote) = underlying.split_once('-').ok_or_else(|| {
        invalid_payload(
            provider,
            format!("could not derive assets from underlying {underlying}"),
        )
    })?;
    if quote.contains('-') {
        return Err(invalid_payload(
            provider,
            format!("underlying must contain exactly two assets: {underlying}"),
        ));
    }
    Ok((
        exact_asset(provider, "underlying base", base.to_owned())?,
        exact_asset(provider, "underlying quote", quote.to_owned())?,
    ))
}

#[derive(Debug, Deserialize)]
struct KucoinEnvelope {
    code: String,
    data: Option<OneOrMany<KucoinInstrument>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(item) => vec![item],
            Self::Many(items) => items,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KucoinInstrument {
    symbol: String,
    #[serde(rename = "type")]
    contract_type: String,
    status: String,
    base_currency: String,
    quote_currency: String,
    settle_currency: String,
    first_open_date: Option<WireScalar>,
    expire_date: Option<WireScalar>,
    tick_size: WireScalar,
    lot_size: WireScalar,
    multiplier: WireScalar,
    max_leverage: Option<WireScalar>,
    is_inverse: bool,
}

fn parse_kucoin_catalog(body: &str) -> Result<BTreeMap<String, Instrument>, CatalogError> {
    let provider = Provider::Kucoin;
    let envelope: KucoinEnvelope = serde_json::from_str(body)
        .map_err(|error| invalid_payload(provider, format!("invalid JSON: {error}")))?;
    if envelope.code != "200000" {
        return Err(invalid_payload(
            provider,
            format!("endpoint returned code {}", envelope.code),
        ));
    }
    let items = envelope
        .data
        .ok_or_else(|| invalid_payload(provider, "missing data"))?
        .into_vec();
    let mut instruments = BTreeMap::new();
    for item in items {
        if let Some(instrument) = kucoin_instrument(item)? {
            insert_unique(&mut instruments, instrument)?;
        }
    }
    Ok(instruments)
}

fn kucoin_instrument(item: KucoinInstrument) -> Result<Option<Instrument>, CatalogError> {
    let provider = Provider::Kucoin;
    if item.contract_type != "FFWCSX" || item.status != "Open" || item.is_inverse {
        return Ok(None);
    }
    if optional_timestamp_ms(provider, "expireDate", item.expire_date)?.is_some() {
        return Ok(None);
    }
    if !(item.symbol.ends_with("USDTM") || item.symbol.ends_with("USDCM")) {
        return Ok(None);
    }
    let symbol = exact_symbol(provider, item.symbol)?;
    let base_asset = exact_asset(provider, "baseCurrency", item.base_currency)?;
    Ok(Some(Instrument {
        instrument_id: instrument_id(provider, &symbol),
        symbol,
        provider,
        market: MarketKind::LinearPerpetual,
        base_asset: base_asset.clone(),
        quote_asset: exact_asset(provider, "quoteCurrency", item.quote_currency)?,
        settle_asset: exact_asset(provider, "settleCurrency", item.settle_currency)?,
        status: InstrumentStatus::Live,
        venue_status: item.status,
        tick_size: positive_decimal(provider, "tickSize", item.tick_size)?,
        quantity_step: positive_decimal(provider, "lotSize", item.lot_size)?,
        contract_size: Some(positive_decimal(provider, "multiplier", item.multiplier)?),
        contract_size_asset: Some(base_asset),
        max_leverage: optional_positive_decimal(provider, "maxLeverage", item.max_leverage)?,
        listing_time_ms: optional_timestamp_ms(provider, "firstOpenDate", item.first_open_date)?,
        expiry_time_ms: None,
        capabilities: capabilities(provider),
    }))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum WireScalar {
    String(String),
    Number(Number),
}

impl WireScalar {
    fn text(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Number(value) => value.to_string(),
        }
    }
}

fn positive_decimal(
    provider: Provider,
    field: &str,
    scalar: WireScalar,
) -> Result<DecimalValue, CatalogError> {
    let raw = scalar.text();
    let parsed = Decimal::from_str(&raw)
        .or_else(|_| Decimal::from_scientific(&raw))
        .map_err(|_| invalid_payload(provider, format!("{field} is not a decimal: {raw}")))?;
    if parsed <= Decimal::ZERO {
        return Err(invalid_payload(
            provider,
            format!("{field} must be greater than zero"),
        ));
    }
    DecimalValue::new(raw.clone())
        .or_else(|_| DecimalValue::new(parsed.to_string()))
        .map_err(|_| invalid_payload(provider, format!("{field} is not a decimal: {raw}")))
}

fn optional_positive_decimal(
    provider: Provider,
    field: &str,
    scalar: Option<WireScalar>,
) -> Result<Option<DecimalValue>, CatalogError> {
    match scalar {
        Some(WireScalar::String(value)) if value.is_empty() => Ok(None),
        Some(value) => positive_decimal(provider, field, value).map(Some),
        None => Ok(None),
    }
}

fn optional_timestamp_ms(
    provider: Provider,
    field: &str,
    scalar: Option<WireScalar>,
) -> Result<Option<u64>, CatalogError> {
    let Some(scalar) = scalar else {
        return Ok(None);
    };
    let value = scalar.text();
    if value.is_empty() || value == "0" {
        return Ok(None);
    }
    value.parse::<u64>().map(Some).map_err(|_| {
        invalid_payload(
            provider,
            format!("{field} must be an unsigned millisecond timestamp"),
        )
    })
}

fn capabilities(provider: Provider) -> InstrumentCapabilities {
    let (candle_1m_finality, candle_1m_volume) = if provider == Provider::Kucoin {
        (
            CandleFinalitySupport::Unknown,
            CandleVolumeSupport::Unavailable,
        )
    } else {
        (
            CandleFinalitySupport::Authoritative,
            CandleVolumeSupport::Available,
        )
    };
    InstrumentCapabilities {
        ticker: true,
        candle_1m: true,
        candle_1m_finality,
        candle_1m_volume,
    }
}

fn insert_unique(
    instruments: &mut BTreeMap<String, Instrument>,
    instrument: Instrument,
) -> Result<(), CatalogError> {
    let provider = instrument.provider;
    let symbol = instrument.symbol.clone();
    if instruments.insert(symbol.clone(), instrument).is_some() {
        return Err(CatalogError::DuplicateInstrument { provider, symbol });
    }
    Ok(())
}

fn exact_symbol(provider: Provider, value: String) -> Result<String, CatalogError> {
    let normalized = normalize_symbol(&value)
        .map_err(|error| invalid_payload(provider, format!("invalid symbol: {error}")))?;
    if normalized != value {
        return Err(invalid_payload(
            provider,
            format!("symbol is not in its exact canonical form: {value}"),
        ));
    }
    Ok(value)
}

fn exact_asset(provider: Provider, field: &str, value: String) -> Result<String, CatalogError> {
    if value.is_empty()
        || value.len() > 64
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        return Err(invalid_payload(
            provider,
            format!("{field} must be a non-empty venue asset identifier"),
        ));
    }
    Ok(value)
}

fn instrument_id(provider: Provider, symbol: &str) -> String {
    format!("{provider}:{}:{symbol}", MarketKind::LinearPerpetual)
}

fn invalid_payload(provider: Provider, message: impl Into<String>) -> CatalogError {
    CatalogError::InvalidPayload {
        provider,
        message: message.into(),
    }
}

fn matches_optional(expected: Option<&String>, actual: &str) -> bool {
    expected.is_none_or(|expected| expected.eq_ignore_ascii_case(actual))
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn static_url(value: &str) -> Url {
    Url::parse(value).expect("hard-coded public catalog URL must be valid")
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::body::Body;
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use axum::{Json, Router};
    use pretty_assertions::assert_eq;
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::*;

    #[test]
    fn parses_bybit_linear_perpetuals_only() {
        let body = bybit_payload(
            &[
                bybit_item("BTCUSDT", "LinearPerpetual", "Trading"),
                bybit_item("ETHUSDT-30DEC30", "LinearFutures", "Trading"),
                bybit_item("SOLUSDT", "LinearPerpetual", "Settling"),
            ],
            "next-token",
        )
        .to_string();

        let page = parse_bybit_page(&body).unwrap();

        assert_eq!(page.next_page_cursor, "next-token");
        assert_eq!(page.instruments.len(), 1);
        let instrument = &page.instruments[0];
        assert_eq!(instrument.instrument_id, "bybit:linear_perpetual:BTCUSDT");
        assert_eq!(instrument.base_asset, "BTC");
        assert_eq!(instrument.tick_size.as_str(), "0.10");
        assert_eq!(instrument.quantity_step.as_str(), "0.001");
        assert_eq!(instrument.max_leverage.as_ref().unwrap().as_str(), "100.00");
        assert_eq!(instrument.listing_time_ms, Some(1_584_230_400_000));
        assert_eq!(instrument.expiry_time_ms, None);
    }

    #[test]
    fn parses_binance_filters_instead_of_precision_hints() {
        let body = json!({
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "onboardDate": 1_567_965_300_000_u64,
                    "deliveryDate": 4_133_404_800_000_u64,
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001"}
                    ]
                },
                {
                    "symbol": "BTCUSDT_300925",
                    "contractType": "CURRENT_QUARTER",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "filters": []
                },
                {
                    "symbol": "OLDUSDT",
                    "contractType": "PERPETUAL",
                    "status": "CLOSE",
                    "baseAsset": "OLD",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "filters": []
                }
            ]
        })
        .to_string();

        let instruments = parse_binance_catalog(&body).unwrap();

        assert_eq!(instruments.len(), 1);
        let instrument = &instruments["BTCUSDT"];
        assert_eq!(instrument.tick_size.as_str(), "0.10");
        assert_eq!(instrument.quantity_step.as_str(), "0.001");
        assert_eq!(instrument.settle_asset, "USDT");
        assert_eq!(instrument.expiry_time_ms, None);
    }

    #[test]
    fn preserves_current_non_ascii_binance_symbols_exactly() {
        let body = json!({
            "symbols": [{
                "symbol": "币安人生USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "baseAsset": "币安人生",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "onboardDate": 1,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.00001"},
                    {"filterType": "LOT_SIZE", "stepSize": "1"}
                ]
            }]
        })
        .to_string();

        let instruments = parse_binance_catalog(&body).unwrap();

        assert_eq!(instruments.len(), 1);
        let instrument = &instruments["币安人生USDT"];
        assert_eq!(instrument.base_asset, "币安人生");
        assert_eq!(
            instrument.instrument_id,
            "binance:linear_perpetual:币安人生USDT"
        );
    }

    #[test]
    fn parses_okx_swap_assets_and_contract_units() {
        let body = json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "instType": "SWAP",
                    "state": "live",
                    "baseCcy": "",
                    "quoteCcy": "",
                    "settleCcy": "USDT",
                    "ctType": "linear",
                    "ctVal": "0.01",
                    "ctValCcy": "BTC",
                    "tickSz": "0.1",
                    "lotSz": "0.01",
                    "lever": "100",
                    "listTime": "1573557408000",
                    "expTime": "",
                    "uly": "BTC-USDT",
                    "instFamily": "BTC-USDT"
                },
                {
                    "instId": "BTC-USD-SWAP",
                    "instType": "SWAP",
                    "state": "live",
                    "settleCcy": "BTC",
                    "ctType": "inverse",
                    "ctVal": "100",
                    "tickSz": "0.1",
                    "lotSz": "1",
                    "uly": "BTC-USD"
                },
                {
                    "instId": "BTC-USDT-300925",
                    "instType": "FUTURES",
                    "state": "live",
                    "settleCcy": "USDT",
                    "ctType": "linear",
                    "ctVal": "0.01",
                    "tickSz": "0.1",
                    "lotSz": "1",
                    "uly": "BTC-USDT"
                }
            ]
        })
        .to_string();

        let instruments = parse_okx_catalog(&body).unwrap();

        assert_eq!(instruments.len(), 1);
        let instrument = &instruments["BTC-USDT-SWAP"];
        assert_eq!(instrument.base_asset, "BTC");
        assert_eq!(instrument.quote_asset, "USDT");
        assert_eq!(instrument.settle_asset, "USDT");
        assert_eq!(instrument.contract_size.as_ref().unwrap().as_str(), "0.01");
        assert_eq!(instrument.contract_size_asset.as_deref(), Some("BTC"));
    }

    #[test]
    fn parses_only_live_kucoin_linear_perpetuals_with_quality_limits() {
        let body = json!({
            "code": "200000",
            "data": [
                kucoin_item("XBTUSDTM", "Open", false, &Value::Null),
                kucoin_item("XBTUSDM", "Open", true, &Value::Null),
                kucoin_item("WLUSDTM", "Open", false, &json!(1_785_567_600_000_u64)),
                kucoin_item("OLDUSDTM", "Closed", false, &Value::Null)
            ]
        })
        .to_string();

        let instruments = parse_kucoin_catalog(&body).unwrap();

        assert_eq!(instruments.len(), 1);
        let instrument = &instruments["XBTUSDTM"];
        assert_eq!(instrument.base_asset, "XBT");
        assert_eq!(instrument.contract_size.as_ref().unwrap().as_str(), "0.001");
        assert_eq!(
            instrument.capabilities.candle_1m_finality,
            CandleFinalitySupport::Unknown
        );
        assert_eq!(
            instrument.capabilities.candle_1m_volume,
            CandleVolumeSupport::Unavailable
        );
    }

    #[test]
    fn catalog_queries_and_validates_exact_venue_symbols() {
        let parsed = parse_binance_catalog(
            &json!({
                "symbols": [{
                    "symbol": "ETHUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "onboardDate": 1,
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"}
                    ]
                }]
            })
            .to_string(),
        )
        .unwrap();
        let sources = CatalogSources {
            enabled_providers: BTreeSet::from([Provider::Binance]),
            ..CatalogSources::default()
        };
        let catalog = Catalog::new(Client::new(), sources);
        let mut state = write_lock(&catalog.state);
        let snapshot = state.providers.get_mut(&Provider::Binance).unwrap();
        snapshot.instruments = parsed;
        snapshot.status.instrument_count = 1;
        drop(state);

        assert_eq!(catalog.list().len(), 1);
        assert!(
            catalog
                .get(Provider::Binance, MarketKind::LinearPerpetual, "ethusdt")
                .is_some()
        );
        assert!(
            catalog
                .get_by_id("binance:linear_perpetual:ETHUSDT")
                .is_some()
        );
        assert_eq!(
            catalog
                .filter(&CatalogFilter {
                    quote_asset: Some("usdt".to_owned()),
                    ..CatalogFilter::default()
                })
                .len(),
            1
        );
        let subscription = SubscriptionKey::new(
            Provider::Binance,
            MarketKind::LinearPerpetual,
            "ethusdt",
            Channel::Ticker,
        )
        .unwrap();
        assert!(catalog.validate_subscription(&subscription).is_ok());

        let unknown = SubscriptionKey::new(
            Provider::Binance,
            MarketKind::LinearPerpetual,
            "missingusdt",
            Channel::Ticker,
        )
        .unwrap();
        assert!(matches!(
            catalog.validate_subscription(&unknown),
            Err(CatalogError::InstrumentUnavailable { .. })
        ));
    }

    #[tokio::test]
    async fn bybit_refresh_paginates_and_replaces_one_complete_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route("/", get(paginated_bybit))
            .with_state(Arc::clone(&calls));
        let (url, server) = spawn_server(router).await;
        let catalog = bybit_catalog(url);

        let outcome = catalog.refresh_provider(Provider::Bybit).await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(outcome.instrument_count, 2);
        assert_eq!(catalog.list().len(), 2);
        assert!(
            catalog
                .get(Provider::Bybit, MarketKind::LinearPerpetual, "BTCUSDT")
                .is_some()
        );
        assert!(
            catalog
                .get(Provider::Bybit, MarketKind::LinearPerpetual, "ETHUSDT")
                .is_some()
        );
        assert_eq!(catalog.status(Provider::Bybit).instrument_count, 2);
        assert!(catalog.status(Provider::Bybit).last_error.is_none());
        server.abort();
    }

    #[tokio::test]
    async fn bybit_refresh_rejects_repeated_cursors_and_cross_page_duplicates() {
        let (cycle_url, cycle_server) =
            spawn_server(Router::new().route("/", get(cyclic_bybit))).await;
        let cycle_error = bybit_catalog(cycle_url)
            .refresh_provider(Provider::Bybit)
            .await
            .unwrap_err();
        assert_eq!(
            cycle_error,
            CatalogError::RepeatedCursor {
                provider: Provider::Bybit,
                cursor: "repeat".to_owned()
            }
        );
        cycle_server.abort();

        let (duplicate_url, duplicate_server) =
            spawn_server(Router::new().route("/", get(duplicate_bybit))).await;
        let duplicate_error = bybit_catalog(duplicate_url)
            .refresh_provider(Provider::Bybit)
            .await
            .unwrap_err();
        assert_eq!(
            duplicate_error,
            CatalogError::DuplicateInstrument {
                provider: Provider::Bybit,
                symbol: "BTCUSDT".to_owned()
            }
        );
        duplicate_server.abort();
    }

    #[tokio::test]
    async fn failed_refresh_retains_last_good_provider_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route("/", get(flaky_bybit))
            .with_state(Arc::clone(&calls));
        let (url, server) = spawn_server(router).await;
        let catalog = bybit_catalog(url);

        catalog.refresh_provider(Provider::Bybit).await.unwrap();
        let last_success = catalog.status(Provider::Bybit).last_success_at_ms;
        let error = catalog.refresh_provider(Provider::Bybit).await.unwrap_err();

        assert_eq!(
            error,
            CatalogError::HttpStatus {
                provider: Provider::Bybit,
                status: 503
            }
        );
        assert_eq!(catalog.list().len(), 1);
        let status = catalog.status(Provider::Bybit);
        assert_eq!(status.instrument_count, 1);
        assert_eq!(status.last_success_at_ms, last_success);
        assert!(status.last_error.is_some());
        server.abort();
    }

    #[tokio::test]
    async fn refresh_rejects_oversized_and_non_utf8_responses() {
        let (oversized_url, oversized_server) =
            spawn_server(Router::new().route("/", get(oversized_body))).await;
        let oversized_error = bybit_catalog(oversized_url)
            .refresh_provider(Provider::Bybit)
            .await
            .unwrap_err();
        assert!(matches!(
            oversized_error,
            CatalogError::InvalidPayload {
                provider: Provider::Bybit,
                message
            } if message.contains("byte limit")
        ));
        oversized_server.abort();

        let (invalid_url, invalid_server) =
            spawn_server(Router::new().route("/", get(non_utf8_body))).await;
        let invalid_error = bybit_catalog(invalid_url)
            .refresh_provider(Provider::Bybit)
            .await
            .unwrap_err();
        assert_eq!(
            invalid_error,
            CatalogError::InvalidPayload {
                provider: Provider::Bybit,
                message: "response body is not UTF-8".to_owned()
            }
        );
        invalid_server.abort();
    }

    #[tokio::test]
    #[ignore = "explicit live smoke test against public exchange metadata"]
    async fn live_public_catalogs_parse() {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();
        let catalog = Catalog::new(http, CatalogSources::default());
        let results = catalog.refresh_all().await;
        for (provider, result) in &results {
            eprintln!("{provider}: {result:?}");
        }
        assert!(results.values().all(Result::is_ok));
        assert!(catalog.list().len() > 100);
    }

    fn bybit_item(symbol: &str, contract_type: &str, status: &str) -> Value {
        json!({
            "symbol": symbol,
            "contractType": contract_type,
            "status": status,
            "baseCoin": symbol.strip_suffix("USDT").unwrap_or("BTC"),
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "launchTime": "1584230400000",
            "deliveryTime": "0",
            "priceFilter": {"tickSize": "0.10"},
            "lotSizeFilter": {"qtyStep": "0.001"},
            "leverageFilter": {"maxLeverage": "100.00"}
        })
    }

    fn bybit_payload(items: &[Value], next_cursor: &str) -> Value {
        json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "category": "linear",
                "nextPageCursor": next_cursor,
                "list": items
            }
        })
    }

    fn kucoin_item(symbol: &str, status: &str, is_inverse: bool, expiry: &Value) -> Value {
        json!({
            "symbol": symbol,
            "type": "FFWCSX",
            "status": status,
            "baseCurrency": if symbol.starts_with("XBT") { "XBT" } else { "OLD" },
            "quoteCurrency": if is_inverse { "USD" } else { "USDT" },
            "settleCurrency": if is_inverse { "XBT" } else { "USDT" },
            "firstOpenDate": 1_585_555_200_000_u64,
            "expireDate": expiry,
            "tickSize": 0.1,
            "lotSize": 1,
            "multiplier": if is_inverse { -1.0 } else { 0.001 },
            "maxLeverage": 125,
            "isInverse": is_inverse
        })
    }

    fn bybit_catalog(url: Url) -> Catalog {
        Catalog::new(
            Client::new(),
            CatalogSources {
                bybit: url,
                enabled_providers: BTreeSet::from([Provider::Bybit]),
                ..CatalogSources::default()
            },
        )
    }

    async fn spawn_server(router: Router) -> (Url, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (Url::parse(&format!("http://{address}/")).unwrap(), server)
    }

    async fn paginated_bybit(
        State(calls): State<Arc<AtomicUsize>>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Json<Value> {
        calls.fetch_add(1, Ordering::SeqCst);
        if query
            .get("cursor")
            .is_some_and(|cursor| cursor == "page-two")
        {
            Json(bybit_payload(
                &[bybit_item("ETHUSDT", "LinearPerpetual", "Trading")],
                "",
            ))
        } else {
            Json(bybit_payload(
                &[bybit_item("BTCUSDT", "LinearPerpetual", "Trading")],
                "page-two",
            ))
        }
    }

    async fn cyclic_bybit(Query(query): Query<BTreeMap<String, String>>) -> Json<Value> {
        let symbol = if query.contains_key("cursor") {
            "ETHUSDT"
        } else {
            "BTCUSDT"
        };
        Json(bybit_payload(
            &[bybit_item(symbol, "LinearPerpetual", "Trading")],
            "repeat",
        ))
    }

    async fn duplicate_bybit(Query(query): Query<BTreeMap<String, String>>) -> Json<Value> {
        let next = if query.contains_key("cursor") {
            ""
        } else {
            "page-two"
        };
        Json(bybit_payload(
            &[bybit_item("BTCUSDT", "LinearPerpetual", "Trading")],
            next,
        ))
    }

    async fn flaky_bybit(State(calls): State<Arc<AtomicUsize>>) -> Response {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Json(bybit_payload(
                &[bybit_item("BTCUSDT", "LinearPerpetual", "Trading")],
                "",
            ))
            .into_response()
        } else {
            (StatusCode::SERVICE_UNAVAILABLE, "temporary failure").into_response()
        }
    }

    async fn oversized_body() -> Response {
        let length = usize::try_from(MAX_CATALOG_RESPONSE_BYTES + 1).unwrap();
        Response::new(Body::from(vec![b' '; length]))
    }

    async fn non_utf8_body() -> Response {
        Response::new(Body::from(vec![0xff]))
    }
}
