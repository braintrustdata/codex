//! Prototype MCP server.
#![deny(clippy::print_stdout, clippy::print_stderr)]

use std::io::Result as IoResult;
use std::path::PathBuf;

use mcp_types::JSONRPCMessage;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::{self};
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing_subscriber::prelude::*;
use tracing_opentelemetry::OpenTelemetryLayer;
use opentelemetry::sdk::trace as sdktrace;
use opentelemetry::sdk::Resource;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;

mod codex_tool_config;
mod codex_tool_runner;
mod json_to_toml;
mod message_processor;

use crate::message_processor::MessageProcessor;

/// Size of the bounded channels used to communicate between tasks. The value
/// is a balance between throughput and memory usage – 128 messages should be
/// plenty for an interactive CLI.
const CHANNEL_CAPACITY: usize = 128;

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

pub async fn run_main(codex_linux_sandbox_exe: Option<PathBuf>) -> IoResult<()> {
    // Install a simple subscriber so `tracing` output is visible.  Users can
    // control the log level with `RUST_LOG`.
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

    // Set up channels.
    let (incoming_tx, mut incoming_rx) = mpsc::channel::<JSONRPCMessage>(CHANNEL_CAPACITY);
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<JSONRPCMessage>(CHANNEL_CAPACITY);

    // Task: read from stdin, push to `incoming_tx`.
    let stdin_reader_handle = tokio::spawn({
        let incoming_tx = incoming_tx.clone();
        async move {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await.unwrap_or_default() {
                match serde_json::from_str::<JSONRPCMessage>(&line) {
                    Ok(msg) => {
                        if incoming_tx.send(msg).await.is_err() {
                            // Receiver gone – nothing left to do.
                            break;
                        }
                    }
                    Err(e) => error!("Failed to deserialize JSONRPCMessage: {e}"),
                }
            }

            debug!("stdin reader finished (EOF)");
        }
    });

    // Task: process incoming messages.
    let processor_handle = tokio::spawn({
        let mut processor = MessageProcessor::new(outgoing_tx.clone(), codex_linux_sandbox_exe);
        async move {
            while let Some(msg) = incoming_rx.recv().await {
                match msg {
                    JSONRPCMessage::Request(r) => processor.process_request(r),
                    JSONRPCMessage::Response(r) => processor.process_response(r),
                    JSONRPCMessage::Notification(n) => processor.process_notification(n),
                    JSONRPCMessage::BatchRequest(b) => processor.process_batch_request(b),
                    JSONRPCMessage::Error(e) => processor.process_error(e),
                    JSONRPCMessage::BatchResponse(b) => processor.process_batch_response(b),
                }
            }

            info!("processor task exited (channel closed)");
        }
    });

    // Task: write outgoing messages to stdout.
    let stdout_writer_handle = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(msg) = outgoing_rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(json) => {
                    if let Err(e) = stdout.write_all(json.as_bytes()).await {
                        error!("Failed to write to stdout: {e}");
                        break;
                    }
                    if let Err(e) = stdout.write_all(b"\n").await {
                        error!("Failed to write newline to stdout: {e}");
                        break;
                    }
                    if let Err(e) = stdout.flush().await {
                        error!("Failed to flush stdout: {e}");
                        break;
                    }
                }
                Err(e) => error!("Failed to serialize JSONRPCMessage: {e}"),
            }
        }

        info!("stdout writer exited (channel closed)");
    });

    // Wait for all tasks to finish.  The typical exit path is the stdin reader
    // hitting EOF which, once it drops `incoming_tx`, propagates shutdown to
    // the processor and then to the stdout task.
    let _ = tokio::join!(stdin_reader_handle, processor_handle, stdout_writer_handle);

    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}
