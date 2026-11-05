use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use pretty_assertions::assert_eq;
use serde_json::Value as JsonValue;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::cell_actor::CompletionCommit;

struct RecordingDelegate;

struct PanickingClosedDelegate;

impl SessionRuntimeDelegate for RecordingDelegate {
    async fn invoke_tool(
        &self,
        _invocation: NestedToolCall,
        _cancellation_token: CancellationToken,
    ) -> Result<JsonValue, String> {
        Ok(JsonValue::Null)
    }

    async fn notify(
        &self,
        _call_id: String,
        _cell_id: CellId,
        _text: String,
        _cancellation_token: CancellationToken,
    ) -> Result<(), String> {
        Ok(())
    }

    fn cell_closed(&self, _cell_id: &CellId) {}
}

impl SessionRuntimeDelegate for PanickingClosedDelegate {
    async fn invoke_tool(
        &self,
        _invocation: NestedToolCall,
        _cancellation_token: CancellationToken,
    ) -> Result<JsonValue, String> {
        Ok(JsonValue::Null)
    }

    async fn notify(
        &self,
        _call_id: String,
        _cell_id: CellId,
        _text: String,
        _cancellation_token: CancellationToken,
    ) -> Result<(), String> {
        Ok(())
    }

    fn cell_closed(&self, _cell_id: &CellId) {
        panic!("cell close panic probe");
    }
}

#[tokio::test]
async fn reports_cell_actor_panics_to_the_owner() {
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime = SessionRuntime::new_with_task_failure_handler(Some(Arc::new(move |reason| {
        let _ = failure_tx.send(reason);
    })));
    let started = runtime
        .execute(
            execute_request(r#"text("done");"#),
            ObserveMode::YieldAfter(Duration::from_secs(1)),
            Arc::new(PanickingClosedDelegate),
        )
        .await
        .expect("start cell");
    assert_eq!(
        started.initial_event().await,
        Ok(CellEvent::Completed {
            content_items: vec![OutputItem::Text {
                text: "done".to_string(),
            }],
            error_text: None,
        })
    );
    runtime.shutdown().await.expect("shutdown runtime");
    let failure = failure_rx
        .try_recv()
        .expect("shutdown should wait for the cell failure watcher");
    assert!(failure.contains("code-mode cell 1 task failed"));
}

#[tokio::test]
async fn termination_rejects_a_waiting_store_commit_before_the_next_cell_can_load_it() {
    let runtime = SessionRuntime::new();
    let cell_state = Arc::new(CellState::new(CancellationToken::new()));
    let host = RuntimeCellHost {
        delegate: Arc::new(RecordingDelegate),
        cell_id: CellId::new("terminating-writer"),
        inner: Arc::clone(&runtime.inner),
        execution_context: opentelemetry::Context::new(),
    };
    let completion = CellEvent::Completed {
        content_items: vec![OutputItem::Text {
            text: "uncommitted output".to_string(),
        }],
        error_text: None,
    };

    let stored_values = runtime.inner.stored_values.lock().await;
    let commit = host.commit_completion(
        HashMap::from([(
            "candidate".to_string(),
            Arc::new(JsonValue::String("lost".to_string())),
        )]),
        completion.clone(),
        /*pending_initial_yield_items*/ None,
        Arc::clone(&cell_state),
    );
    tokio::pin!(commit);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    assert!(matches!(commit.as_mut().poll(&mut context), Poll::Pending));

    let termination = cell_state.request_termination();
    drop(stored_values);
    assert_eq!(commit.await, CompletionCommit::Rejected(completion));
    let terminated = CellEvent::Terminated {
        content_items: Vec::new(),
    };
    assert_eq!(
        cell_state.finish_termination(terminated.clone()),
        Some(terminated.clone())
    );
    assert_eq!(termination.await, Ok(terminated));
    assert!(
        !runtime
            .inner
            .stored_values
            .lock()
            .await
            .contains_key("candidate")
    );

    let reader = runtime
        .execute(
            CreateCellRequest {
                tool_call_id: "reader".to_string(),
                enabled_tools: Vec::new(),
                source: r#"text(String(load("candidate")));"#.to_string(),
            },
            ObserveMode::YieldAfter(Duration::from_secs(1)),
            Arc::new(RecordingDelegate),
        )
        .await
        .unwrap();
    assert_eq!(
        reader.initial_event().await,
        Ok(CellEvent::Completed {
            content_items: vec![OutputItem::Text {
                text: "undefined".to_string(),
            }],
            error_text: None,
        })
    );
    runtime.shutdown().await.unwrap();
}

fn execute_request(source: &str) -> CreateCellRequest {
    CreateCellRequest {
        tool_call_id: "call-1".to_string(),
        enabled_tools: Vec::new(),
        source: source.to_string(),
    }
}

#[tokio::test]
async fn cell_id_allocation_fails_before_wrapping() {
    let runtime = SessionRuntime::new();
    runtime
        .inner
        .next_cell_id
        .store(u64::MAX, Ordering::Relaxed);

    assert_eq!(
        runtime
            .execute(
                execute_request(r#"text("unreachable");"#),
                ObserveMode::YieldAfter(Duration::from_secs(1)),
                Arc::new(RecordingDelegate)
            )
            .await
            .err(),
        Some(Error::CellIdSpaceExhausted)
    );
}

#[tokio::test]
#[expect(
    clippy::await_holding_invalid_type,
    reason = "test holds the registry lock to force admission ahead of shutdown"
)]
async fn shutdown_rejects_cell_admission_queued_before_the_registry_lock() {
    let runtime = Arc::new(SessionRuntime::new());
    let cells = runtime.inner.cells.lock().await;

    let execution = runtime.execute(
        execute_request("while (true) {}"),
        ObserveMode::YieldAfter(Duration::from_millis(/*millis*/ 1)),
        Arc::new(RecordingDelegate),
    );
    tokio::pin!(execution);
    std::future::poll_fn(|context| match execution.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(Ok(_)) => panic!("execution completed before the registry lock was released"),
        Poll::Ready(Err(error)) => {
            panic!("execution failed before the registry lock was released: {error}")
        }
    })
    .await;

    let shutdown = runtime.shutdown();
    tokio::pin!(shutdown);
    std::future::poll_fn(|context| match shutdown.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(Ok(())) => panic!("shutdown completed before acquiring the registry lock"),
        Poll::Ready(Err(error)) => {
            panic!("shutdown failed before acquiring the registry lock: {error}")
        }
    })
    .await;

    drop(cells);
    assert!(matches!(execution.await, Err(Error::ShuttingDown)));
    assert_eq!(shutdown.await, Ok(()));
}

#[tokio::test]
async fn drop_terminates_cells_when_the_registry_is_locked() {
    let runtime = SessionRuntime::new();
    let started = runtime
        .execute(
            execute_request("while (true) {}"),
            ObserveMode::YieldAfter(Duration::from_millis(/*millis*/ 1)),
            Arc::new(RecordingDelegate),
        )
        .await
        .unwrap();
    assert_eq!(started.cell_id, CellId::new("1"));
    assert_eq!(
        started.initial_event().await,
        Ok(CellEvent::Yielded {
            content_items: Vec::new(),
        })
    );

    let inner = Arc::clone(&runtime.inner);
    let cells = inner.cells.lock().await;
    drop(runtime);
    drop(cells);

    tokio::time::timeout(Duration::from_secs(/*secs*/ 1), inner.cell_tasks.wait())
        .await
        .unwrap();
    assert!(inner.cell_tasks.is_empty());
}

#[tokio::test]
async fn stored_values_transfer_to_independent_fresh_runtimes() {
    let source = SessionRuntime::new();
    let started = source
        .execute(
            execute_request(r#"store("value", {n: 7, values: [null, true, "雪"]});"#),
            ObserveMode::YieldAfter(Duration::from_secs(1)),
            Arc::new(RecordingDelegate),
        )
        .await
        .unwrap();
    assert!(matches!(
        started.initial_event().await.unwrap(),
        CellEvent::Completed {
            error_text: None,
            ..
        }
    ));
    source.inner.cell_tasks.wait().await;
    let snapshot = source.export_state().await.unwrap();
    assert_eq!(snapshot.next_cell_id, 2);
    source.shutdown().await.unwrap();
    drop(source);
    let left = SessionRuntime::new();
    let right = SessionRuntime::new();
    left.import_state(snapshot.clone()).await.unwrap();
    right.import_state(snapshot).await.unwrap();
    for (runtime, source, expected) in [
        (
            &left,
            r#"store("value", "left"); text(load("value"));"#,
            "left",
        ),
        (&right, r#"text(load("value").n);"#, "7"),
    ] {
        let cell = runtime
            .execute(
                execute_request(source),
                ObserveMode::YieldAfter(Duration::from_secs(1)),
                Arc::new(RecordingDelegate),
            )
            .await
            .unwrap();
        assert_eq!(cell.cell_id.as_str(), "2");
        assert_eq!(
            cell.initial_event().await.unwrap(),
            CellEvent::Completed {
                content_items: vec![OutputItem::Text {
                    text: expected.to_string()
                }],
                error_text: None,
            }
        );
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn busy_export_does_not_cancel_or_consume_the_source_cell() {
    let runtime = SessionRuntime::new();
    let started = runtime
        .execute(
            execute_request("yield_control(); await new Promise(() => {});"),
            ObserveMode::YieldAfter(Duration::from_secs(1)),
            Arc::new(RecordingDelegate),
        )
        .await
        .unwrap();
    let id = started.cell_id.clone();
    assert!(matches!(
        started.initial_event().await.unwrap(),
        CellEvent::Yielded { .. }
    ));
    assert!(
        runtime
            .export_state()
            .await
            .unwrap_err()
            .contains("active cells")
    );
    assert!(matches!(
        runtime
            .observe(&id, ObserveMode::YieldAfter(Duration::ZERO))
            .await
            .unwrap(),
        CellEvent::Yielded { .. }
    ));
    assert!(matches!(
        runtime.terminate(&id).await.unwrap(),
        CellEvent::Terminated { .. }
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn buffered_terminal_observation_survives_state_transfer_once() {
    let source = SessionRuntime::new();
    let started = source
        .execute(
            execute_request(r#"yield_control(); store("done", true); text("buffered");"#),
            ObserveMode::YieldAfter(Duration::from_secs(1)),
            Arc::new(RecordingDelegate),
        )
        .await
        .unwrap();
    let id = started.cell_id.clone();
    assert!(matches!(
        started.initial_event().await.unwrap(),
        CellEvent::Yielded { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !source.inner.stored_values.lock().await.contains_key("done") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let snapshot = source.export_state().await.unwrap();
    assert!(snapshot.completed_cells.contains_key(id.as_str()));
    let target = SessionRuntime::new();
    target.import_state(snapshot).await.unwrap();
    source.shutdown().await.unwrap();
    assert_eq!(
        target
            .observe(&id, ObserveMode::YieldAfter(Duration::ZERO))
            .await
            .unwrap(),
        CellEvent::Completed {
            content_items: vec![OutputItem::Text {
                text: "buffered".to_string()
            }],
            error_text: None,
        }
    );
    assert!(matches!(
        target
            .observe(&id, ObserveMode::YieldAfter(Duration::ZERO))
            .await,
        Err(Error::MissingCell(_))
    ));
    target.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_import_leaves_receiver_fresh_and_used_receiver_intact() {
    let runtime = SessionRuntime::new();
    let mut snapshot = codex_code_mode_protocol::CodeModeSnapshot {
        format_version: 2,
        next_cell_id: 3,
        stored_values: HashMap::from([("value".to_string(), serde_json::json!(7))]),
        completed_cells: HashMap::new(),
    };
    assert!(runtime.import_state(snapshot.clone()).await.is_err());
    assert!(
        runtime
            .export_state()
            .await
            .unwrap()
            .stored_values
            .is_empty()
    );
    snapshot.format_version = 1;
    runtime.import_state(snapshot.clone()).await.unwrap();
    snapshot
        .stored_values
        .insert("value".to_string(), serde_json::json!(8));
    assert!(runtime.import_state(snapshot).await.is_err());
    assert_eq!(
        runtime.export_state().await.unwrap().stored_values["value"],
        serde_json::json!(7)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshot_preserves_restored_public_terminal_identity() {
    let runtime = super::SessionRuntime::new();
    let snapshot: codex_code_mode_protocol::CodeModeSnapshot =
        serde_json::from_value(serde_json::json!({
            "format_version": 1, "next_cell_id": 2, "stored_values": {},
            "completed_cells": { "1": {
                "public_cell_id": "g2:1", "pending_initial_yield_items": null,
                "event": { "Completed": { "content_items": [], "error_text": null } }
            } }
        }))
        .unwrap();
    runtime.import_state(snapshot.clone()).await.unwrap();
    assert_eq!(runtime.export_state().await.unwrap(), snapshot);
}
