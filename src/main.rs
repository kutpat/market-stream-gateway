use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use futures_util::stream::{FuturesUnordered, StreamExt};
use market_stream_gateway::SERVICE_NAME;
use market_stream_gateway::api::{AppState, router};
use market_stream_gateway::catalog::{Catalog, CatalogError, CatalogSources, RefreshOutcome};
use market_stream_gateway::config::{LogFormat, Settings};
use market_stream_gateway::domain::Provider;
use market_stream_gateway::gateway::{GatewayHub, SubscriptionRegistry};
use market_stream_gateway::health::HealthRegistry;
use market_stream_gateway::history::{HistoryClient, HistorySources};
use market_stream_gateway::metrics::{Metrics, ProviderLabels};
use market_stream_gateway::providers::ProviderAdapter;
use market_stream_gateway::providers::binance::BinanceAdapter;
use market_stream_gateway::providers::bingx::BingxAdapter;
use market_stream_gateway::providers::bybit::BybitAdapter;
use market_stream_gateway::providers::kucoin::KucoinAdapter;
use market_stream_gateway::providers::mexc::MexcAdapter;
use market_stream_gateway::providers::okx::OkxAdapter;
use market_stream_gateway::runtime::{RuntimeContext, spawn_provider_supervisors};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::parse();
    settings.validate().map_err(anyhow::Error::msg)?;
    init_logging(settings.log_format);
    let settings = Arc::new(settings);

    let enabled_providers = settings.enabled_providers();
    let adapters = configured_adapters(&settings, &enabled_providers);
    let hub = Arc::new(GatewayHub::with_client_limit(
        settings.downstream_buffer,
        settings.max_downstream_clients,
    ));
    let subscriptions = Arc::new(SubscriptionRegistry::with_provider_limit(
        settings.max_client_subscriptions,
        settings.max_provider_subscriptions,
    ));
    let health = Arc::new(HealthRegistry::default());
    let metrics = Arc::new(Metrics::new());
    let shutdown = CancellationToken::new();
    let http = reqwest::Client::builder()
        .user_agent(format!("{SERVICE_NAME}/{}", env!("CARGO_PKG_VERSION")))
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .context("build provider discovery client")?;
    let catalog = Arc::new(Catalog::new(
        http.clone(),
        catalog_sources(&settings, enabled_providers.clone())?,
    ));
    report_catalog_refresh(&catalog.refresh_all().await, "initial", &metrics);
    let history = Arc::new(HistoryClient::with_client(
        http.clone(),
        HistorySources {
            bybit: settings.bybit_rest_url.clone(),
            binance: settings.binance_futures_rest_url.clone(),
            okx: settings.okx_rest_url.clone(),
            kucoin: settings.kucoin_futures_rest_url.clone(),
            mexc: settings.mexc_futures_rest_url.clone(),
            bingx: settings.bingx_swap_rest_url.clone(),
        },
    ));

    let runtime = RuntimeContext {
        registry: Arc::clone(&subscriptions),
        hub: Arc::clone(&hub),
        health: Arc::clone(&health),
        metrics: Arc::clone(&metrics),
        http,
        shutdown: shutdown.clone(),
        settings: Arc::clone(&settings),
    };
    let mut tasks = spawn_provider_supervisors(adapters, runtime);
    tasks.push(spawn_catalog_refresher(
        Arc::clone(&catalog),
        Arc::clone(&metrics),
        settings.catalog_refresh_interval(),
        shutdown.clone(),
    ));
    let task_monitor = monitor_background_tasks(tasks, shutdown.clone(), settings.shutdown_grace());

    let state = AppState {
        hub,
        subscriptions,
        health,
        metrics,
        catalog,
        history,
        history_slots: Arc::new(Semaphore::new(settings.max_history_requests)),
        enabled_providers: Arc::new(enabled_providers),
        allowed_origins: Arc::new(settings.allowed_origins.iter().cloned().collect()),
        max_command_bytes: settings.max_command_bytes,
        shutdown: shutdown.clone(),
    };
    let listener = TcpListener::bind(settings.bind)
        .await
        .with_context(|| format!("bind HTTP listener to {}", settings.bind))?;
    info!(
        bind = %settings.bind,
        providers = state.enabled_providers.len(),
        "gateway_ready"
    );

    let signal_shutdown = shutdown.clone();
    let runtime_shutdown = shutdown.clone();
    let server_result = axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            tokio::select! {
                () = wait_for_shutdown_signal() => signal_shutdown.cancel(),
                () = runtime_shutdown.cancelled() => {}
            }
        })
        .await;
    shutdown.cancel();
    if let Err(error) = task_monitor.await {
        error!(%error, "background_task_monitor_failed");
    }
    server_result.context("serve gateway HTTP API")
}

fn configured_adapters(
    settings: &Settings,
    enabled: &BTreeSet<Provider>,
) -> Vec<Arc<dyn ProviderAdapter>> {
    let mut adapters: Vec<Arc<dyn ProviderAdapter>> = Vec::new();
    if enabled.contains(&Provider::Bybit) {
        adapters.push(Arc::new(BybitAdapter::with_url(
            settings.bybit_ws_url.clone(),
        )));
    }
    if enabled.contains(&Provider::Binance) {
        adapters.push(Arc::new(BinanceAdapter::with_url(
            settings.binance_ws_url.clone(),
        )));
    }
    if enabled.contains(&Provider::Okx) {
        adapters.push(Arc::new(OkxAdapter::new(
            settings.okx_public_ws_url.clone(),
            settings.okx_business_ws_url.clone(),
        )));
    }
    if enabled.contains(&Provider::Kucoin) {
        adapters.push(Arc::new(KucoinAdapter::new(
            settings.kucoin_futures_rest_url.clone(),
        )));
    }
    if enabled.contains(&Provider::Mexc) {
        adapters.push(Arc::new(MexcAdapter::with_url(
            settings.mexc_ws_url.clone(),
        )));
    }
    if enabled.contains(&Provider::Bingx) {
        adapters.push(Arc::new(BingxAdapter::with_url(
            settings.bingx_ws_url.clone(),
        )));
    }
    adapters
}

fn catalog_sources(
    settings: &Settings,
    enabled_providers: BTreeSet<Provider>,
) -> anyhow::Result<CatalogSources> {
    Ok(CatalogSources {
        bybit: provider_endpoint(
            &settings.bybit_rest_url,
            "v5/market/instruments-info",
            Provider::Bybit,
        )?,
        binance: provider_endpoint(
            &settings.binance_futures_rest_url,
            "fapi/v1/exchangeInfo",
            Provider::Binance,
        )?,
        okx: provider_endpoint(
            &settings.okx_rest_url,
            "api/v5/public/instruments",
            Provider::Okx,
        )?,
        kucoin: provider_endpoint(
            &settings.kucoin_futures_rest_url,
            "api/v1/contracts/active",
            Provider::Kucoin,
        )?,
        mexc: provider_endpoint(
            &settings.mexc_futures_rest_url,
            "api/v1/contract/detail/country",
            Provider::Mexc,
        )?,
        bingx: provider_endpoint(
            &settings.bingx_swap_rest_url,
            "openApi/swap/v2/quote/contracts",
            Provider::Bingx,
        )?,
        enabled_providers,
    })
}

fn provider_endpoint(root: &url::Url, path: &str, provider: Provider) -> anyhow::Result<url::Url> {
    root.join(path)
        .with_context(|| format!("build {provider} REST endpoint from {root}"))
}

fn spawn_catalog_refresher(
    catalog: Arc<Catalog>,
    metrics: Arc<Metrics>,
    interval: std::time::Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {
                    report_catalog_refresh(&catalog.refresh_all().await, "scheduled", &metrics);
                }
                () = shutdown.cancelled() => break,
            }
        }
    })
}

fn report_catalog_refresh(
    results: &BTreeMap<Provider, Result<RefreshOutcome, CatalogError>>,
    trigger: &'static str,
    metrics: &Metrics,
) {
    for (provider, result) in results {
        let labels = ProviderLabels::new(*provider);
        match result {
            Ok(outcome) => {
                metrics
                    .catalog_refresh_successes
                    .get_or_create(&labels)
                    .inc();
                metrics
                    .catalog_instruments
                    .get_or_create(&labels)
                    .set(i64::try_from(outcome.instrument_count).unwrap_or(i64::MAX));
                info!(
                    %provider,
                    trigger,
                    instruments = outcome.instrument_count,
                    "catalog_refreshed"
                );
            }
            Err(error) => {
                metrics
                    .catalog_refresh_failures
                    .get_or_create(&labels)
                    .inc();
                warn!(%provider, trigger, %error, "catalog_refresh_failed");
            }
        }
    }
}

fn monitor_background_tasks(
    tasks: Vec<tokio::task::JoinHandle<()>>,
    shutdown: CancellationToken,
    grace: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tasks = tasks.into_iter().collect::<FuturesUnordered<_>>();
        if tasks.is_empty() {
            shutdown.cancelled().await;
            return;
        }

        tokio::select! {
            result = tasks.next() => {
                match result {
                    Some(Ok(())) => error!("background_task_exited_unexpectedly"),
                    Some(Err(error)) => error!(%error, "background_task_failed"),
                    None => error!("all_background_tasks_exited_unexpectedly"),
                }
                shutdown.cancel();
            }
            () = shutdown.cancelled() => {}
        }

        let deadline = Instant::now() + grace;
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(error))) => error!(%error, "background_task_failed"),
                Ok(None) => break,
                Err(_) => {
                    warn!("background_shutdown_timed_out");
                    break;
                }
            }
        }
        for task in &tasks {
            task.abort();
        }
    })
}

fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("market_stream_gateway=info,tower_http=info"));
    match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init(),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .pretty()
            .init(),
    }
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                error!(%error, "ctrl_c_handler_failed");
            }
        }
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "ctrl_c_handler_failed");
    }
}
