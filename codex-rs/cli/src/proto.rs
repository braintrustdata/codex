use std::io::IsTerminal;
use std::sync::Arc;

use clap::Parser;
use codex_common::CliConfigOverrides;
use codex_core::Codex;
use codex_core::config::Config;
use codex_core::config::ConfigOverrides;
use codex_core::protocol::Submission;
use codex_core::util::notify_on_sigint;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tracing::error;
use tracing::info;
use tracing_subscriber::prelude::*;
use tracing_opentelemetry::OpenTelemetryLayer;
use opentelemetry::sdk::trace as sdktrace;
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;

fn otel_layer() -> Option<OpenTelemetryLayer<tracing_subscriber::Registry, sdktrace::Tracer>> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    let tracer = opentelemetry_otlp::new_exporter()
        .http()
        .with_endpoint(endpoint)
        .into_pipeline()
        .tracing()
        .with_trace_config(
            sdktrace::config().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                "codex",
            )])),
        )
        .install_simple()
        .ok()?;
    Some(tracing_opentelemetry::layer().with_tracer(tracer))
}

#[derive(Debug, Parser)]
pub struct ProtoCli {
    #[clap(skip)]
    pub config_overrides: CliConfigOverrides,
}

pub async fn run_main(opts: ProtoCli) -> anyhow::Result<()> {
    if std::io::stdin().is_terminal() {
        anyhow::bail!("Protocol mode expects stdin to be a pipe, not a terminal");
    }

    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    if let Some(otel) = otel_layer() {
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(otel)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(fmt_layer)
            .init();
    }

    let ProtoCli { config_overrides } = opts;
    let overrides_vec = config_overrides
        .parse_overrides()
        .map_err(anyhow::Error::msg)?;

    let config = Config::load_with_cli_overrides(overrides_vec, ConfigOverrides::default())?;
    let ctrl_c = notify_on_sigint();
    let (codex, _init_id) = Codex::spawn(config, ctrl_c.clone()).await?;
    let codex = Arc::new(codex);

    // Task that reads JSON lines from stdin and forwards to Submission Queue
    let sq_fut = {
        let codex = codex.clone();
        let ctrl_c = ctrl_c.clone();
        async move {
            let stdin = BufReader::new(tokio::io::stdin());
            let mut lines = stdin.lines();
            loop {
                let result = tokio::select! {
                    _ = ctrl_c.notified() => {
                        info!("Interrupted, exiting");
                        break
                    },
                    res = lines.next_line() => res,
                };

                match result {
                    Ok(Some(line)) => {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<Submission>(line) {
                            Ok(sub) => {
                                if let Err(e) = codex.submit_with_id(sub).await {
                                    error!("{e:#}");
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("invalid submission: {e}");
                            }
                        }
                    }
                    _ => {
                        info!("Submission queue closed");
                        break;
                    }
                }
            }
        }
    };

    // Task that reads events from the agent and prints them as JSON lines to stdout
    let eq_fut = async move {
        loop {
            let event = tokio::select! {
                _ = ctrl_c.notified() => break,
                event = codex.next_event() => event,
            };
            match event {
                Ok(event) => {
                    let event_str = match serde_json::to_string(&event) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to serialize event: {e}");
                            continue;
                        }
                    };
                    println!("{event_str}");
                }
                Err(e) => {
                    error!("{e:#}");
                    break;
                }
            }
        }
        info!("Event queue closed");
    };

    tokio::join!(sq_fut, eq_fut);
    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}
