use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use codex_code_mode_protocol::CellId;
use codex_code_mode_protocol::CodeModeNestedToolCall;
use codex_code_mode_protocol::CodeModeSession;
use codex_code_mode_protocol::CodeModeSessionCellExecutionLimits;
use codex_code_mode_protocol::CodeModeSessionDelegate;
use codex_code_mode_protocol::ExecuteRequest;
use codex_code_mode_protocol::NotificationFuture;
use codex_code_mode_protocol::ToolInvocationFuture;
use codex_code_mode_protocol::WaitRequest;
use codex_code_mode_protocol::host::CapabilitySet;
use codex_code_mode_protocol::host::ClientToHost;
use codex_code_mode_protocol::host::DelegateRequest;
use codex_code_mode_protocol::host::DelegateRequestId;
use codex_code_mode_protocol::host::EncodedFrame;
use codex_code_mode_protocol::host::HostRequest;
use codex_code_mode_protocol::host::HostResponse;
use codex_code_mode_protocol::host::HostToClient;
use codex_code_mode_protocol::host::MAX_PENDING_DELEGATE_CALLS;
use codex_code_mode_protocol::host::RequestId;
use codex_code_mode_protocol::host::SessionId;
use codex_code_mode_protocol::host::WireNestedToolCall;
use codex_code_mode_protocol::host::WireResult;
use codex_code_mode_protocol::host::WireRuntimeResponse;
use codex_code_mode_protocol::host::WireSessionCellExecutionLimits;
use codex_code_mode_protocol::host::WireWaitOutcome;
use codex_protocol::ToolName;
use pretty_assertions::assert_eq;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::super::Connection;
use super::super::DEFAULT_HOST_WAIT_TRANSPORT_TIMEOUT;
use super::ConnectionDriver;
use super::DriverCommand;
use super::DriverEvent;
use super::DriverLifecycle;
use super::RemoteSession;
use super::SessionCleanup;

struct DriverHarness {
    command_tx: mpsc::Sender<DriverCommand>,
    event_tx: mpsc::Sender<DriverEvent>,
    execute_claim_tx: mpsc::UnboundedSender<RequestId>,
    outgoing_rx: mpsc::Receiver<codex_code_mode_protocol::host::EncodedFrame>,
    cancellation: CancellationToken,
    alive: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<String>>>,
    driver_task: tokio::task::JoinHandle<()>,
}

impl DriverHarness {
    fn start() -> Self {
        let (command_tx, command_rx) = mpsc::channel(/*max_capacity*/ 16);
        let (event_tx, event_rx) = mpsc::channel(/*max_capacity*/ 16);
        let (outgoing_tx, outgoing_rx) = mpsc::channel(/*max_capacity*/ 16);
        let cancellation = CancellationToken::new();
        let alive = Arc::new(AtomicBool::new(true));
        let failure = Arc::new(StdMutex::new(None));
        let (driver, execute_claim_tx) = ConnectionDriver::new(
            command_rx,
            event_rx,
            event_tx.clone(),
            outgoing_tx,
            DriverLifecycle {
                alive: Arc::clone(&alive),
                failure: Arc::clone(&failure),
                cancellation: cancellation.clone(),
            },
        );
        let driver_task = tokio::spawn(driver.run());
        Self {
            command_tx,
            event_tx,
            execute_claim_tx,
            outgoing_rx,
            cancellation,
            alive,
            failure,
            driver_task,
        }
    }

    async fn open(&mut self, session: RemoteSession) -> SessionCleanup {
        let cleanup = SessionCleanup::new();
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DriverCommand::OpenSession {
                session: session.clone(),

                limits: Default::default(),
                cleanup: cleanup.clone(),
                caller_cancellation: CancellationToken::new(),
                response_tx,
            })
            .await
            .expect("open command");
        let frame = self.outgoing_rx.recv().await.expect("open frame");
        let ClientToHost::Request {
            id,
            request: HostRequest::OpenSession { .. },
        } = EncodedFrame::decode_framed::<ClientToHost>(&frame.into_framed_bytes()).unwrap()
        else {
            panic!("Expected open request")
        };
        self.event_tx
            .send(DriverEvent::HostMessage(HostToClient::Response {
                id,
                result: WireResult::Ok {
                    value: HostResponse::SessionReady {
                        session_id: session.id,
                    },
                },
            }))
            .await
            .expect("open response");
        response_rx
            .await
            .expect("open reply")
            .expect("open session");
        cleanup
    }

    async fn start_cell(
        &mut self,
        session: RemoteSession,
        request_id: i64,
        cell_id: &str,
        delegate: Arc<dyn CodeModeSessionDelegate>,
    ) -> codex_code_mode_protocol::StartedCell {
        let (response_tx, response_rx) = oneshot::channel();
        self.command_tx
            .send(DriverCommand::Execute {
                session,
                delegate,
                request: ExecuteRequest {
                    tool_call_id: format!("call-{request_id}"),
                    enabled_tools: Vec::new(),
                    source: "await new Promise(() => {})".to_string(),
                    yield_time_ms: Some(1),
                    max_output_tokens: None,
                },
                caller_cancellation: CancellationToken::new(),
                yield_signal: None,
                response_tx,
            })
            .await
            .expect("execute command");
        self.outgoing_rx.recv().await.expect("execute frame");
        self.event_tx
            .send(DriverEvent::HostMessage(HostToClient::Response {
                id: RequestId::new(request_id),
                result: WireResult::Ok {
                    value: HostResponse::ExecutionStarted {
                        cell_id: CellId::new(cell_id.to_string()).into(),
                    },
                },
            }))
            .await
            .expect("execute response");
        let delivered = response_rx
            .await
            .expect("execute reply")
            .expect("execute session");
        self.execute_claim_tx
            .send(delivered.request_id)
            .expect("claim execute");
        delivered.started
    }

    async fn start_tool_delegate(&self, session: &RemoteSession, id: DelegateRequestId) {
        self.event_tx
            .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
                id,
                session_id: session.id.clone(),
                request: DelegateRequest::InvokeTool {
                    invocation: WireNestedToolCall {
                        cell_id: CellId::new("1".to_string()).into(),
                        runtime_tool_call_id: "tool-1".to_string(),
                        tool_name: ToolName::plain("slow").into(),
                        tool_kind: codex_code_mode_protocol::CodeModeToolKind::Function.into(),
                        input: None,
                    },
                },
            }))
            .await
            .expect("delegate request");
    }
}

impl Drop for DriverHarness {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[tokio::test(start_paused = true)]
async fn old_host_does_not_receive_yield_frames() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let _cleanup = harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            Arc::new(RecordingDelegate::default()),
        )
        .await;
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::InitialResponse {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: WireRuntimeResponse::Yielded {
                    cell_id: CellId::new("1".to_string()).into(),
                    content_items: Vec::new(),
                    code_mode_host_duration_ns: 1,
                },
            },
        }))
        .await
        .unwrap();
    let connection = Connection {
        command_tx: harness.command_tx.clone(),
        execute_claim_tx: harness.execute_claim_tx.clone(),
        alive: Arc::clone(&harness.alive),
        failure: Arc::clone(&harness.failure),
        cancellation: harness.cancellation.clone(),
        capabilities: CapabilitySet::empty(),
    };
    let signal = CancellationToken::new();
    signal.cancel();
    let wait = tokio::spawn(async move {
        connection
            .wait(
                session,
                WaitRequest {
                    cell_id: CellId::new("1".to_string()),
                    yield_time_ms: 60_000,
                },
                Some(signal),
            )
            .await
    });
    let frame = harness.outgoing_rx.recv().await.unwrap();
    assert!(
        matches!(EncodedFrame::decode_framed::<ClientToHost>(&frame.into_framed_bytes()).unwrap(), ClientToHost::Request { id, request: HostRequest::Wait { .. } } if id == RequestId::new(/*value*/ 3))
    );
    // Let any yield watcher run while the wait is still active. A response would
    // drop the watcher and could otherwise hide an incorrectly sent yield frame.
    assert!(
        tokio::time::timeout(Duration::from_millis(1), harness.outgoing_rx.recv())
            .await
            .is_err()
    );
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Yielded {
                        cell_id: CellId::new("1".to_string()).into(),
                        content_items: Vec::new(),
                        code_mode_host_duration_ns: 1,
                    }),
                },
            },
        }))
        .await
        .unwrap();
    wait.await.unwrap().unwrap();
}

#[tokio::test]
async fn open_session_includes_nondefault_cell_execution_limits() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let limits = CodeModeSessionCellExecutionLimits {
        max_yield_time_ms: Some(250),
        max_heap_size_bytes: Some(16 * 1024 * 1024),
    };
    let (response_tx, _response_rx) = oneshot::channel();

    harness
        .command_tx
        .send(DriverCommand::OpenSession {
            session: session.clone(),

            limits,
            cleanup: SessionCleanup::new(),
            caller_cancellation: CancellationToken::new(),
            response_tx,
        })
        .await
        .expect("limited session open command");
    let frame = harness
        .outgoing_rx
        .recv()
        .await
        .expect("limited session open frame");

    assert_eq!(
        EncodedFrame::decode_framed::<ClientToHost>(&frame.into_framed_bytes())
            .expect("decode limited session open request"),
        ClientToHost::Request {
            id: RequestId::new(/*value*/ 1),
            request: HostRequest::OpenSession {
                session_id: session.id,
                cell_execution_limits: Some(WireSessionCellExecutionLimits {
                    max_yield_time_ms: Some(250),
                    max_heap_size_bytes: Some(16 * 1024 * 1024),
                }),
            },
        }
    );
}

#[derive(Default)]
struct RecordingDelegate {
    closed_cells: StdMutex<Vec<CellId>>,
    invocations: AtomicUsize,
    notifications: AtomicUsize,
}

struct PanickingDelegate;

#[derive(Debug, Eq, PartialEq)]
enum HeldDelegateEvent {
    Started,
    Cancelled,
    Finished,
    CellClosed(CellId),
}

struct HeldDelegate {
    events_tx: mpsc::UnboundedSender<HeldDelegateEvent>,
    release: CancellationToken,
}

impl HeldDelegate {
    fn new() -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<HeldDelegateEvent>,
        CancellationToken,
    ) {
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let release = CancellationToken::new();
        (
            Arc::new(Self {
                events_tx,
                release: release.clone(),
            }),
            events_rx,
            release,
        )
    }
}

impl CodeModeSessionDelegate for HeldDelegate {
    fn invoke_tool<'a>(
        &'a self,
        _invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        let events_tx = self.events_tx.clone();
        let release = self.release.clone();
        Box::pin(async move {
            let _ = events_tx.send(HeldDelegateEvent::Started);
            cancellation_token.cancelled().await;
            let _ = events_tx.send(HeldDelegateEvent::Cancelled);
            release.cancelled().await;
            let _ = events_tx.send(HeldDelegateEvent::Finished);
            Err("cancelled".to_string())
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: String,
        _cell_id: CellId,
        _text: String,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, cell_id: &CellId) {
        let _ = self
            .events_tx
            .send(HeldDelegateEvent::CellClosed(cell_id.clone()));
    }
}

impl CodeModeSessionDelegate for PanickingDelegate {
    fn invoke_tool<'a>(
        &'a self,
        _invocation: CodeModeNestedToolCall,
        _cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        Box::pin(async { panic!("delegate panic probe") })
    }

    fn notify<'a>(
        &'a self,
        _call_id: String,
        _cell_id: CellId,
        _text: String,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, _cell_id: &CellId) {}
}

impl CodeModeSessionDelegate for RecordingDelegate {
    fn invoke_tool<'a>(
        &'a self,
        _invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> ToolInvocationFuture<'a> {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            cancellation_token.cancelled().await;
            Err("cancelled".to_string())
        })
    }

    fn notify<'a>(
        &'a self,
        _call_id: String,
        _cell_id: CellId,
        _text: String,
        _cancellation_token: CancellationToken,
    ) -> NotificationFuture<'a> {
        self.notifications.fetch_add(1, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }

    fn cell_closed(&self, cell_id: &CellId) {
        self.closed_cells
            .lock()
            .expect("closed cells lock")
            .push(cell_id.clone());
    }
}

fn remote_session() -> RemoteSession {
    RemoteSession {
        id: SessionId::new("session-1").expect("session ID"),
        generation: 1,
    }
}

async fn next_held_delegate_event(
    events_rx: &mut mpsc::UnboundedReceiver<HeldDelegateEvent>,
) -> HeldDelegateEvent {
    tokio::time::timeout(Duration::from_secs(1), events_rx.recv())
        .await
        .expect("delegate event timeout")
        .expect("delegate event stream")
}

#[tokio::test]
async fn deferred_delegates_follow_cell_readiness_and_cancellation() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let first_delegate = Arc::new(RecordingDelegate::default());
    let second_delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;

    let (first_response_tx, first_response_rx) = oneshot::channel();
    let (second_response_tx, second_response_rx) = oneshot::channel();
    for (tool_call_id, response_tx, delegate) in [
        ("first-cell", first_response_tx, first_delegate.clone()),
        ("second-cell", second_response_tx, second_delegate.clone()),
    ] {
        harness
            .command_tx
            .send(DriverCommand::Execute {
                delegate: delegate.clone(),
                session: session.clone(),
                request: ExecuteRequest {
                    tool_call_id: tool_call_id.to_string(),
                    enabled_tools: Vec::new(),
                    source: "text('done')".to_string(),
                    yield_time_ms: Some(/*yield_time_ms*/ 1),
                    max_output_tokens: None,
                },
                caller_cancellation: CancellationToken::new(),
                yield_signal: None,
                response_tx,
            })
            .await
            .expect("execute command");
        harness.outgoing_rx.recv().await.expect("execute frame");
    }

    let first_cell_id = CellId::new("first-cell".to_string());
    let second_cell_id = CellId::new("second-cell".to_string());
    let first_delegate_id = DelegateRequestId::new(/*value*/ 7);
    let second_delegate_id = DelegateRequestId::new(/*value*/ 8);
    let cancelled_delegate_id = DelegateRequestId::new(/*value*/ 9);
    for (delegate_id, cell_id) in [
        (first_delegate_id, first_cell_id.clone()),
        (second_delegate_id, second_cell_id.clone()),
        (cancelled_delegate_id, first_cell_id.clone()),
    ] {
        harness
            .event_tx
            .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
                id: delegate_id,
                session_id: session.id.clone(),
                request: DelegateRequest::Notify {
                    call_id: format!("notify-{}", cell_id.as_str()),
                    cell_id: (&cell_id).into(),
                    text: "hello".to_string(),
                },
            }))
            .await
            .expect("early delegate request");
    }

    harness
        .event_tx
        .send(DriverEvent::HostMessage(
            HostToClient::CancelDelegateRequest {
                id: cancelled_delegate_id,
            },
        ))
        .await
        .expect("deferred delegate cancellation");

    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::ExecutionStarted {
                    cell_id: (&second_cell_id).into(),
                },
            },
        }))
        .await
        .expect("second execution-started response");
    let _second_started = second_response_rx
        .await
        .expect("second execute response")
        .expect("second started cell");
    let second_response = tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv())
        .await
        .expect("second delegate response timeout")
        .expect("second delegate response frame");
    assert_eq!(
        EncodedFrame::decode_framed::<ClientToHost>(&second_response.into_framed_bytes())
            .expect("decode second delegate response"),
        ClientToHost::DelegateResponse {
            id: second_delegate_id,
            result: WireResult::Ok {
                value: codex_code_mode_protocol::host::DelegateResponse::NotificationDelivered,
            },
        }
    );
    assert_eq!(
        (
            first_delegate.notifications.load(Ordering::Relaxed),
            second_delegate.notifications.load(Ordering::Relaxed)
        ),
        (0, 1)
    );

    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: HostResponse::ExecutionStarted {
                    cell_id: (&first_cell_id).into(),
                },
            },
        }))
        .await
        .expect("first execution-started response");
    let _first_started = first_response_rx
        .await
        .expect("first execute response")
        .expect("first started cell");
    let first_response = tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv())
        .await
        .expect("first delegate response timeout")
        .expect("first delegate response frame");
    assert_eq!(
        EncodedFrame::decode_framed::<ClientToHost>(&first_response.into_framed_bytes())
            .expect("decode first delegate response"),
        ClientToHost::DelegateResponse {
            id: first_delegate_id,
            result: WireResult::Ok {
                value: codex_code_mode_protocol::host::DelegateResponse::NotificationDelivered,
            },
        }
    );
    assert_eq!(
        (
            first_delegate.notifications.load(Ordering::Relaxed),
            second_delegate.notifications.load(Ordering::Relaxed)
        ),
        (1, 1)
    );
    assert!(matches!(
        harness.outgoing_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(harness.alive.load(Ordering::Relaxed));
}

#[tokio::test]
async fn dropped_open_waiter_shuts_down_committed_session() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let (open_tx, open_rx) = oneshot::channel();
    let cleanup = SessionCleanup::new();
    harness
        .command_tx
        .send(DriverCommand::OpenSession {
            session: session.clone(),

            limits: Default::default(),
            cleanup,
            caller_cancellation: CancellationToken::new(),
            response_tx: open_tx,
        })
        .await
        .expect("open command");
    drop(open_rx);
    harness.outgoing_rx.recv().await.expect("open frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 1),
            result: WireResult::Ok {
                value: HostResponse::SessionReady {
                    session_id: session.id.clone(),
                },
            },
        }))
        .await
        .expect("open response");
    harness
        .outgoing_rx
        .recv()
        .await
        .expect("abandoned session shutdown frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: HostResponse::SessionClosed {
                    session_id: session.id.clone(),
                },
            },
        }))
        .await
        .expect("shutdown response");

    let (execute_tx, execute_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Execute {
            delegate: Arc::new(RecordingDelegate::default()),
            session: session.clone(),
            request: ExecuteRequest {
                tool_call_id: "call-1".to_string(),
                enabled_tools: Vec::new(),
                source: "text('ok')".to_string(),
                yield_time_ms: None,
                max_output_tokens: None,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx: execute_tx,
        })
        .await
        .expect("execute command");
    assert_eq!(
        execute_rx
            .await
            .expect("execute reply")
            .err()
            .expect("closed session should reject execute"),
        "unknown code-mode session session-1"
    );
}

#[tokio::test]
async fn delegate_cancel_is_best_effort_and_sends_no_late_response() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let request_id = DelegateRequestId::new(/*value*/ 7);
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id: request_id,
            session_id: session.id.clone(),
            request: DelegateRequest::InvokeTool {
                invocation: WireNestedToolCall {
                    cell_id: CellId::new("1".to_string()).into(),
                    runtime_tool_call_id: "tool-1".to_string(),
                    tool_name: ToolName::plain("slow").into(),
                    tool_kind: codex_code_mode_protocol::CodeModeToolKind::Function.into(),
                    input: None,
                },
            },
        }))
        .await
        .expect("delegate request");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(
            HostToClient::CancelDelegateRequest { id: request_id },
        ))
        .await
        .expect("delegate cancel");
    tokio::task::yield_now().await;
    assert!(matches!(
        harness.outgoing_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id: request_id,
            session_id: session.id,
            request: DelegateRequest::Notify {
                call_id: "notify-reused".to_string(),
                cell_id: CellId::new("1".to_string()).into(),
                text: "duplicate".to_string(),
            },
        }))
        .await
        .expect("reused delegate request");
    tokio::task::yield_now().await;

    assert!(!harness.alive.load(Ordering::Acquire));
    assert_eq!(delegate.invocations.load(Ordering::Relaxed), 1);
    assert_eq!(delegate.notifications.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn delegate_limit_returns_an_error_without_disconnecting() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;

    for value in 1..=MAX_PENDING_DELEGATE_CALLS {
        harness
            .start_tool_delegate(&session, DelegateRequestId::new(value as i64))
            .await;
    }

    let overflow_id = DelegateRequestId::new(MAX_PENDING_DELEGATE_CALLS as i64 + 1);
    harness.start_tool_delegate(&session, overflow_id).await;
    let response = tokio::time::timeout(Duration::from_secs(5), harness.outgoing_rx.recv())
        .await
        .expect("delegate overflow response timeout")
        .expect("delegate overflow response frame");

    assert_eq!(
        EncodedFrame::decode_framed::<ClientToHost>(&response.into_framed_bytes())
            .expect("decode delegate overflow response"),
        ClientToHost::DelegateResponse {
            id: overflow_id,
            result: WireResult::Err {
                message: format!(
                    "code-mode host exceeded the limit of {MAX_PENDING_DELEGATE_CALLS} pending delegate calls"
                ),
            },
        }
    );
    assert!(harness.alive.load(Ordering::Acquire));

    harness
        .event_tx
        .send(DriverEvent::HostMessage(
            HostToClient::CancelDelegateRequest {
                id: DelegateRequestId::new(/*value*/ 1),
            },
        ))
        .await
        .expect("cancel pending delegate");
    harness
        .start_tool_delegate(
            &session,
            DelegateRequestId::new(MAX_PENDING_DELEGATE_CALLS as i64 + 2),
        )
        .await;

    tokio::time::timeout(Duration::from_secs(5), async {
        while delegate.invocations.load(Ordering::Relaxed) <= MAX_PENDING_DELEGATE_CALLS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delegate capacity should be available after cancellation");
    assert_eq!(
        delegate.invocations.load(Ordering::Relaxed),
        MAX_PENDING_DELEGATE_CALLS + 1
    );
    assert!(harness.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn terminate_closes_cell_without_waiting_for_delegate_cleanup() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let (delegate, mut events_rx, release) = HeldDelegate::new();
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let delegate_id = DelegateRequestId::new(/*value*/ 7);
    harness.start_tool_delegate(&session, delegate_id).await;
    assert_eq!(
        next_held_delegate_event(&mut events_rx).await,
        HeldDelegateEvent::Started
    );

    let (response_tx, response_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Terminate {
            session: session.clone(),
            cell_id: CellId::new("1".to_string()),
            response_tx,
        })
        .await
        .expect("terminate command");
    harness.outgoing_rx.recv().await.expect("terminate frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(
            HostToClient::CancelDelegateRequest { id: delegate_id },
        ))
        .await
        .expect("delegate cancel");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::CellClosed {
            session_id: session.id,
            cell_id: CellId::new("1".to_string()).into(),
        }))
        .await
        .expect("cell close");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Terminated {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("1".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("terminate response");

    let closure_events = [
        next_held_delegate_event(&mut events_rx).await,
        next_held_delegate_event(&mut events_rx).await,
    ];
    assert!(closure_events.contains(&HeldDelegateEvent::Cancelled));
    assert!(closure_events.contains(&HeldDelegateEvent::CellClosed(CellId::new("1".to_string()))));
    assert_eq!(
        response_rx.await.expect("terminate reply"),
        Ok(codex_code_mode_protocol::WaitOutcome::LiveCell(
            codex_code_mode_protocol::RuntimeResponse::Terminated {
                code_mode_host_duration: Some(Duration::ZERO),
                cell_id: CellId::new("1".to_string()),
                content_items: Vec::new(),
            }
        ))
    );
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));

    release.cancel();
    assert_eq!(
        next_held_delegate_event(&mut events_rx).await,
        HeldDelegateEvent::Finished
    );
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        harness.outgoing_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(harness.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn shutdown_closes_cell_without_waiting_for_delegate_cleanup() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let (delegate, mut events_rx, release) = HeldDelegate::new();
    let cleanup = harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let delegate_id = DelegateRequestId::new(/*value*/ 7);
    harness.start_tool_delegate(&session, delegate_id).await;
    assert_eq!(
        next_held_delegate_event(&mut events_rx).await,
        HeldDelegateEvent::Started
    );

    let (response_tx, response_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::ShutdownSession {
            session: session.clone(),
            cleanup,
            response_tx,
        })
        .await
        .expect("shutdown command");
    harness.outgoing_rx.recv().await.expect("shutdown frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(
            HostToClient::CancelDelegateRequest { id: delegate_id },
        ))
        .await
        .expect("delegate cancel");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::CellClosed {
            session_id: session.id.clone(),
            cell_id: CellId::new("1".to_string()).into(),
        }))
        .await
        .expect("cell close");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::SessionClosed {
                    session_id: session.id,
                },
            },
        }))
        .await
        .expect("shutdown response");

    let closure_events = [
        next_held_delegate_event(&mut events_rx).await,
        next_held_delegate_event(&mut events_rx).await,
    ];
    assert!(closure_events.contains(&HeldDelegateEvent::Cancelled));
    assert!(closure_events.contains(&HeldDelegateEvent::CellClosed(CellId::new("1".to_string()))));
    assert_eq!(response_rx.await.expect("shutdown reply"), Ok(()));
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    release.cancel();
    assert_eq!(
        next_held_delegate_event(&mut events_rx).await,
        HeldDelegateEvent::Finished
    );
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(matches!(
        harness.outgoing_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert!(harness.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn completed_delegate_request_id_cannot_be_reused() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let request_id = DelegateRequestId::new(/*value*/ 7);
    let request = || DelegateRequest::Notify {
        call_id: "notify-1".to_string(),
        cell_id: CellId::new("1".to_string()).into(),
        text: "once".to_string(),
    };
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id: request_id,
            session_id: session.id.clone(),
            request: request(),
        }))
        .await
        .expect("delegate request");
    harness
        .outgoing_rx
        .recv()
        .await
        .expect("delegate response frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id: request_id,
            session_id: session.id,
            request: request(),
        }))
        .await
        .expect("reused delegate request");
    tokio::task::yield_now().await;

    assert!(!harness.alive.load(Ordering::Acquire));
    assert_eq!(delegate.notifications.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn delegate_task_panic_becomes_tool_error_without_killing_connection() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            Arc::new(PanickingDelegate),
        )
        .await;
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id: DelegateRequestId::new(/*value*/ 7),
            session_id: session.id.clone(),
            request: DelegateRequest::InvokeTool {
                invocation: WireNestedToolCall {
                    cell_id: CellId::new("1".to_string()).into(),
                    runtime_tool_call_id: "tool-1".to_string(),
                    tool_name: ToolName::plain("panic").into(),
                    tool_kind: codex_code_mode_protocol::CodeModeToolKind::Function.into(),
                    input: None,
                },
            },
        }))
        .await
        .expect("delegate request");
    tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv())
        .await
        .expect("delegate response timeout")
        .expect("delegate response frame");

    assert!(harness.alive.load(Ordering::Acquire));
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::CellClosed {
            session_id: session.id,
            cell_id: CellId::new("1".to_string()).into(),
        }))
        .await
        .expect("cell close");
}

#[tokio::test]
async fn delegate_for_unknown_cell_returns_error_without_invocation() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "known",
            delegate.clone(),
        )
        .await;

    let id = DelegateRequestId::new(/*value*/ 7);
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id,
            session_id: session.id,
            request: DelegateRequest::InvokeTool {
                invocation: WireNestedToolCall {
                    cell_id: CellId::new("missing".to_string()).into(),
                    runtime_tool_call_id: "tool-1".to_string(),
                    tool_name: ToolName::plain("slow").into(),
                    tool_kind: codex_code_mode_protocol::CodeModeToolKind::Function.into(),
                    input: None,
                },
            },
        }))
        .await
        .expect("delegate request");
    let response = tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv())
        .await
        .expect("delegate response timeout")
        .expect("delegate response frame");

    assert_eq!(
        EncodedFrame::decode_framed::<ClientToHost>(&response.into_framed_bytes())
            .expect("decode delegate response"),
        ClientToHost::DelegateResponse {
            id,
            result: WireResult::Err {
                message: "code-mode host delegated for unknown cell missing in session session-1"
                    .to_string(),
            },
        }
    );
    assert!(harness.alive.load(Ordering::Acquire));
    assert_eq!(delegate.invocations.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn delegate_after_cell_close_returns_error_without_invocation() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::CellClosed {
            session_id: session.id.clone(),
            cell_id: CellId::new("1".to_string()).into(),
        }))
        .await
        .expect("cell close");
    let id = DelegateRequestId::new(/*value*/ 7);
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::DelegateRequest {
            id,
            session_id: session.id,
            request: DelegateRequest::Notify {
                call_id: "notify-1".to_string(),
                cell_id: CellId::new("1".to_string()).into(),
                text: "late".to_string(),
            },
        }))
        .await
        .expect("delegate request");
    let response = tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv())
        .await
        .expect("delegate response timeout")
        .expect("delegate response frame");

    assert_eq!(
        EncodedFrame::decode_framed::<ClientToHost>(&response.into_framed_bytes())
            .expect("decode delegate response"),
        ClientToHost::DelegateResponse {
            id,
            result: WireResult::Err {
                message: "code-mode host delegated for unknown cell 1 in session session-1"
                    .to_string(),
            },
        }
    );
    assert!(harness.alive.load(Ordering::Acquire));
    assert_eq!(delegate.notifications.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn mismatched_initial_response_fails_connection_and_closes_cell_once() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let started = harness
        .start_cell(session, /*request_id*/ 2, "1", delegate.clone())
        .await;
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::InitialResponse {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: WireRuntimeResponse::Yielded {
                    code_mode_host_duration_ns: 0,
                    cell_id: CellId::new("2".to_string()).into(),
                    content_items: Vec::new(),
                },
            },
        }))
        .await
        .expect("initial response");

    assert!(started.initial_response().await.is_err());
    assert!(!harness.alive.load(Ordering::Acquire));
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn mismatched_wait_response_fails_connection() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let (response_tx, response_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Wait {
            session,
            request: WaitRequest {
                cell_id: CellId::new("1".to_string()),
                yield_time_ms: 1,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx,
        })
        .await
        .expect("wait command");
    harness.outgoing_rx.recv().await.expect("wait frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Yielded {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("2".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("wait response");

    assert!(response_rx.await.expect("wait reply").is_err());
    assert!(!harness.alive.load(Ordering::Acquire));
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn mismatched_terminate_response_fails_connection() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let (response_tx, response_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Terminate {
            session,
            cell_id: CellId::new("1".to_string()),
            response_tx,
        })
        .await
        .expect("terminate command");
    harness.outgoing_rx.recv().await.expect("terminate frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::MissingCell(WireRuntimeResponse::Terminated {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("2".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("terminate response");

    assert!(response_rx.await.expect("terminate reply").is_err());
    assert!(!harness.alive.load(Ordering::Acquire));
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn remote_wait_accepts_durations_longer_than_five_minutes() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            Arc::new(RecordingDelegate::default()),
        )
        .await;
    let (response_tx, response_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Wait {
            session,
            request: WaitRequest {
                cell_id: CellId::new("1".to_string()),
                yield_time_ms: 300_001,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx,
        })
        .await
        .expect("wait command");
    tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv())
        .await
        .expect("wait frame timeout")
        .expect("wait frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Yielded {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("1".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("wait response");

    assert_eq!(
        response_rx.await.expect("wait reply"),
        Ok(codex_code_mode_protocol::WaitOutcome::LiveCell(
            codex_code_mode_protocol::RuntimeResponse::Yielded {
                code_mode_host_duration: Some(Duration::ZERO),
                cell_id: CellId::new("1".to_string()),
                content_items: Vec::new(),
            }
        ))
    );
}

/// A stalled remote wait must expire even when its request never reaches the host.
#[tokio::test(start_paused = true)]
async fn queued_remote_wait_times_out_and_invalidates_the_connection() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            Arc::new(RecordingDelegate::default()),
        )
        .await;
    let connection = Connection {
        command_tx: harness.command_tx.clone(),
        execute_claim_tx: harness.execute_claim_tx.clone(),
        alive: Arc::clone(&harness.alive),
        failure: Arc::clone(&harness.failure),
        cancellation: harness.cancellation.clone(),
        capabilities: CapabilitySet::empty(),
    };
    let response = tokio::spawn(async move {
        let result = connection
            .wait(
                session,
                WaitRequest {
                    cell_id: CellId::new("1".to_string()),
                    yield_time_ms: 1,
                },
                /*yield_signal*/ None,
            )
            .await;
        (connection, result)
    });
    while harness.outgoing_rx.is_empty() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(DEFAULT_HOST_WAIT_TRANSPORT_TIMEOUT + Duration::from_secs(2)).await;

    let (connection, result) = response.await.expect("wait task");
    assert_eq!(
        result,
        Err("code-mode host timed out waiting for wait response".to_string())
    );
    assert!(!harness.alive.load(Ordering::Acquire));
    assert!(harness.cancellation.is_cancelled());
    drop(connection);
}

/// A stalled termination must expire and invalidate the same connection as a stalled wait.
#[tokio::test(start_paused = true)]
async fn queued_remote_termination_times_out_and_invalidates_the_connection() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            Arc::new(RecordingDelegate::default()),
        )
        .await;
    let connection = Connection {
        command_tx: harness.command_tx.clone(),
        execute_claim_tx: harness.execute_claim_tx.clone(),
        alive: Arc::clone(&harness.alive),
        failure: Arc::clone(&harness.failure),
        cancellation: harness.cancellation.clone(),
        capabilities: CapabilitySet::empty(),
    };
    let response = tokio::spawn(async move {
        let result = connection
            .terminate(session, CellId::new("1".to_string()))
            .await;
        (connection, result)
    });
    while harness.outgoing_rx.is_empty() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(DEFAULT_HOST_WAIT_TRANSPORT_TIMEOUT + Duration::from_secs(1)).await;

    let (connection, result) = response.await.expect("termination task");
    assert_eq!(
        result,
        Err("code-mode host timed out waiting for terminate response".to_string())
    );
    assert!(!harness.alive.load(Ordering::Acquire));
    assert!(harness.cancellation.is_cancelled());
    drop(connection);
}

#[tokio::test]
async fn cancelled_wait_is_retired_before_next_wait_is_sent() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            Arc::new(RecordingDelegate::default()),
        )
        .await;
    let first_cancellation = CancellationToken::new();
    let (first_tx, first_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Wait {
            session: session.clone(),
            request: WaitRequest {
                cell_id: CellId::new("1".to_string()),
                yield_time_ms: 60_000,
            },
            caller_cancellation: first_cancellation.clone(),
            yield_signal: None,
            response_tx: first_tx,
        })
        .await
        .expect("first wait command");
    harness.outgoing_rx.recv().await.expect("first wait frame");
    first_cancellation.cancel();
    drop(first_rx);

    let (second_tx, second_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Wait {
            session,
            request: WaitRequest {
                cell_id: CellId::new("1".to_string()),
                yield_time_ms: 1,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx: second_tx,
        })
        .await
        .expect("second wait command");
    harness
        .outgoing_rx
        .recv()
        .await
        .expect("cancel request frame");
    assert!(matches!(
        harness.outgoing_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Err {
                message: "code-mode request cancelled".to_string(),
            },
        }))
        .await
        .expect("cancelled wait response");
    harness.outgoing_rx.recv().await.expect("second wait frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 4),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Yielded {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("1".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("second wait response");

    assert_eq!(
        second_rx.await.expect("second wait reply"),
        Ok(codex_code_mode_protocol::WaitOutcome::LiveCell(
            codex_code_mode_protocol::RuntimeResponse::Yielded {
                code_mode_host_duration: Some(Duration::ZERO),
                cell_id: CellId::new("1".to_string()),
                content_items: Vec::new(),
            }
        ))
    );
}

#[tokio::test]
async fn abandoned_execute_is_tracked_and_terminated_after_admission() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let cancellation = CancellationToken::new();
    let (execute_tx, execute_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Execute {
            delegate: delegate.clone(),
            session: session.clone(),
            request: ExecuteRequest {
                tool_call_id: "call-1".to_string(),
                enabled_tools: Vec::new(),
                source: "await new Promise(() => {})".to_string(),
                yield_time_ms: Some(1),
                max_output_tokens: None,
            },
            caller_cancellation: cancellation.clone(),
            yield_signal: None,
            response_tx: execute_tx,
        })
        .await
        .expect("execute command");
    harness.outgoing_rx.recv().await.expect("execute frame");
    cancellation.cancel();
    drop(execute_rx);
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: HostResponse::ExecutionStarted {
                    cell_id: CellId::new("1".to_string()).into(),
                },
            },
        }))
        .await
        .expect("execute response");

    harness
        .outgoing_rx
        .recv()
        .await
        .expect("execute cancellation frame");
    harness
        .outgoing_rx
        .recv()
        .await
        .expect("abandoned cell termination frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::InitialResponse {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: WireRuntimeResponse::Terminated {
                    code_mode_host_duration_ns: 0,
                    cell_id: CellId::new("1".to_string()).into(),
                    content_items: Vec::new(),
                },
            },
        }))
        .await
        .expect("initial response");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Terminated {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("1".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("terminate response");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::CellClosed {
            session_id: session.id,
            cell_id: CellId::new("1".to_string()).into(),
        }))
        .await
        .expect("cell close");
    tokio::task::yield_now().await;

    assert!(harness.alive.load(Ordering::Acquire));
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn delivered_but_unclaimed_execute_is_terminated_when_the_caller_is_cancelled() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let cancellation = CancellationToken::new();
    let (execute_tx, execute_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Execute {
            delegate: delegate.clone(),
            session: session.clone(),
            request: ExecuteRequest {
                tool_call_id: "call-1".to_string(),
                enabled_tools: Vec::new(),
                source: "await new Promise(() => {})".to_string(),
                yield_time_ms: Some(1),
                max_output_tokens: None,
            },
            caller_cancellation: cancellation.clone(),
            yield_signal: None,
            response_tx: execute_tx,
        })
        .await
        .expect("execute command");
    harness.outgoing_rx.recv().await.expect("execute frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: HostResponse::ExecutionStarted {
                    cell_id: CellId::new("1".to_string()).into(),
                },
            },
        }))
        .await
        .expect("execute response");
    let delivered = execute_rx
        .await
        .expect("execute reply")
        .expect("delivered execute");
    assert_eq!(delivered.request_id, RequestId::new(/*value*/ 2));
    cancellation.cancel();

    harness
        .outgoing_rx
        .recv()
        .await
        .expect("execute cancellation frame");
    harness
        .outgoing_rx
        .recv()
        .await
        .expect("unclaimed cell termination frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::InitialResponse {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: WireRuntimeResponse::Terminated {
                    code_mode_host_duration_ns: 0,
                    cell_id: CellId::new("1".to_string()).into(),
                    content_items: Vec::new(),
                },
            },
        }))
        .await
        .expect("initial response");
    assert!(delivered.started.initial_response().await.is_ok());
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 3),
            result: WireResult::Ok {
                value: HostResponse::WaitCompleted {
                    outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Terminated {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new("1".to_string()).into(),
                        content_items: Vec::new(),
                    }),
                },
            },
        }))
        .await
        .expect("terminate response");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::CellClosed {
            session_id: session.id,
            cell_id: CellId::new("1".to_string()).into(),
        }))
        .await
        .expect("cell close");
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&delegate) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("abandoned execution should release its delegate");

    assert!(harness.alive.load(Ordering::Acquire));
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn session_accepts_more_than_4096_cells_without_growing_a_tombstone_set() {
    const CELL_COUNT: usize = 4097;

    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;

    for sequence in 1..=CELL_COUNT {
        let request_id = i64::try_from(sequence).expect("cell sequence fits in i64") + 1;
        let cell_id = sequence.to_string();
        let started = harness
            .start_cell(session.clone(), request_id, &cell_id, delegate.clone())
            .await;
        harness
            .event_tx
            .send(DriverEvent::HostMessage(HostToClient::InitialResponse {
                id: RequestId::new(request_id),
                result: WireResult::Ok {
                    value: WireRuntimeResponse::Yielded {
                        code_mode_host_duration_ns: 0,
                        cell_id: CellId::new(cell_id.clone()).into(),
                        content_items: Vec::new(),
                    },
                },
            }))
            .await
            .expect("initial response");
        assert!(started.initial_response().await.is_ok());
        harness
            .event_tx
            .send(DriverEvent::HostMessage(HostToClient::CellClosed {
                session_id: session.id.clone(),
                cell_id: CellId::new(cell_id).into(),
            }))
            .await
            .expect("cell close");
    }

    tokio::time::timeout(Duration::from_secs(1), async {
        while delegate
            .closed_cells
            .lock()
            .expect("closed cells lock")
            .len()
            != CELL_COUNT
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cell close callbacks timeout");
    assert!(harness.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn connection_failure_closes_every_live_cell_once() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    let cleanup = harness.open(session.clone()).await;
    let (execute_tx, execute_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Execute {
            delegate: delegate.clone(),
            session,
            request: ExecuteRequest {
                tool_call_id: "call-1".to_string(),
                enabled_tools: Vec::new(),
                source: "await new Promise(() => {})".to_string(),
                yield_time_ms: Some(1),
                max_output_tokens: None,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx: execute_tx,
        })
        .await
        .expect("execute command");
    harness.outgoing_rx.recv().await.expect("execute frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: HostResponse::ExecutionStarted {
                    cell_id: CellId::new("1".to_string()).into(),
                },
            },
        }))
        .await
        .expect("execute response");
    let _started = execute_rx
        .await
        .expect("execute reply")
        .expect("execute session");
    harness
        .event_tx
        .send(DriverEvent::Failed("host crashed".to_string()))
        .await
        .expect("failure event");
    tokio::time::timeout(Duration::from_secs(1), cleanup.wait())
        .await
        .expect("session cleanup timeout");
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn session_cleanup_does_not_wait_for_delegate_completion() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let (delegate, mut events_rx, release) = HeldDelegate::new();
    let cleanup = harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    harness
        .start_tool_delegate(&session, DelegateRequestId::new(/*value*/ 7))
        .await;
    assert_eq!(
        next_held_delegate_event(&mut events_rx).await,
        HeldDelegateEvent::Started
    );

    harness
        .event_tx
        .send(DriverEvent::Failed("host crashed".to_string()))
        .await
        .expect("failure event");
    let closure_events = [
        next_held_delegate_event(&mut events_rx).await,
        next_held_delegate_event(&mut events_rx).await,
    ];
    assert!(closure_events.contains(&HeldDelegateEvent::Cancelled));
    assert!(closure_events.contains(&HeldDelegateEvent::CellClosed(CellId::new("1".to_string()))));
    tokio::time::timeout(Duration::from_secs(1), cleanup.wait())
        .await
        .expect("session cleanup timeout");
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));

    release.cancel();
    assert_eq!(
        next_held_delegate_event(&mut events_rx).await,
        HeldDelegateEvent::Finished
    );
    assert!(matches!(
        events_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn aborting_driver_marks_connection_dead_and_closes_cells() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let delegate = Arc::new(RecordingDelegate::default());
    harness.open(session.clone()).await;
    let _started = harness
        .start_cell(
            session.clone(),
            /*request_id*/ 2,
            "1",
            delegate.clone(),
        )
        .await;
    let (wait_tx, wait_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Wait {
            session,
            request: WaitRequest {
                cell_id: CellId::new("1".to_string()),
                yield_time_ms: 60_000,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx: wait_tx,
        })
        .await
        .expect("wait command");
    harness.outgoing_rx.recv().await.expect("wait frame");

    harness.driver_task.abort();
    for _ in 0..10 {
        if !harness.alive.load(Ordering::Acquire) {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert!(!harness.alive.load(Ordering::Acquire));
    assert!(harness.cancellation.is_cancelled());
    assert!(wait_rx.await.expect("wait failure").is_err());
    assert_eq!(
        *delegate.closed_cells.lock().expect("closed cells lock"),
        vec![CellId::new("1".to_string())]
    );
}

#[tokio::test]
async fn dropped_shutdown_waiter_does_not_abort_remote_cleanup() {
    let mut harness = DriverHarness::start();
    let session = remote_session();
    let cleanup = harness.open(session.clone()).await;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::ShutdownSession {
            session: session.clone(),
            cleanup,
            response_tx: shutdown_tx,
        })
        .await
        .expect("shutdown command");
    drop(shutdown_rx);
    harness.outgoing_rx.recv().await.expect("shutdown frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(/*value*/ 2),
            result: WireResult::Ok {
                value: HostResponse::SessionClosed {
                    session_id: session.id.clone(),
                },
            },
        }))
        .await
        .expect("shutdown response");

    let (execute_tx, execute_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::Execute {
            delegate: Arc::new(RecordingDelegate::default()),
            session,
            request: ExecuteRequest {
                tool_call_id: "call-2".to_string(),
                enabled_tools: Vec::new(),
                source: "text('unreachable')".to_string(),
                yield_time_ms: None,
                max_output_tokens: None,
            },
            caller_cancellation: CancellationToken::new(),
            yield_signal: None,
            response_tx: execute_tx,
        })
        .await
        .expect("execute command");
    assert_eq!(
        execute_rx
            .await
            .expect("execute reply")
            .err()
            .expect("closed session should reject execute"),
        "unknown code-mode session session-1"
    );
    assert!(matches!(
        harness.outgoing_rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

async fn import_cleanup_order(order: &str) {
    let mut harness = DriverHarness::start();
    let receiver = remote_session();
    let cleanup = harness.open(receiver.clone()).await;
    let sibling = RemoteSession {
        id: SessionId::new("sibling").unwrap(),
        generation: 2,
    };
    let _sibling_cleanup = harness.open(sibling.clone()).await;
    let connection = Connection {
        command_tx: harness.command_tx.clone(),
        execute_claim_tx: harness.execute_claim_tx.clone(),
        alive: Arc::clone(&harness.alive),
        failure: Arc::clone(&harness.failure),
        cancellation: harness.cancellation.clone(),
        capabilities: CapabilitySet::empty(),
    };
    let (import_tx, import_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::ImportState {
            session: receiver.clone(),
            state: codex_code_mode_protocol::CodeModeSnapshot {
                format_version: 1,
                next_cell_id: 1,
                stored_values: Default::default(),
                completed_cells: Default::default(),
            },
            response_tx: import_tx,
        })
        .await
        .unwrap();
    harness.outgoing_rx.recv().await.expect("import request");
    drop(import_rx);

    let import_reply = DriverEvent::HostMessage(HostToClient::Response {
        id: RequestId::new(3),
        result: WireResult::Ok {
            value: HostResponse::SessionImported,
        },
    });
    let owner = connection.shutdown_session(receiver.clone(), cleanup.clone());
    tokio::pin!(owner);
    if order == "owner-first" {
        // Poll through command admission, then hold the actual host acknowledgement.
        tokio::select! {
            biased;
            result = &mut owner => panic!("Owner finished before ack: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        harness
            .outgoing_rx
            .recv()
            .await
            .expect("owner shutdown request");
        harness.event_tx.send(import_reply).await.unwrap();
    } else {
        harness.event_tx.send(import_reply).await.unwrap();
        // The only initiator here is complete_request's abandoned ImportState path.
        harness
            .outgoing_rx
            .recv()
            .await
            .expect("abandoned import shutdown request");
    }
    if order != "ack-first" {
        tokio::select! {
            biased;
            result = &mut owner => panic!("Owner finished before ack: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        // FIFO command receipt proves the owner's duplicate reached the driver.
        let (barrier_tx, barrier_rx) = oneshot::channel();
        harness
            .command_tx
            .send(DriverCommand::ExportState {
                session: receiver.clone(),
                response_tx: barrier_tx,
            })
            .await
            .unwrap();
        assert!(barrier_rx.await.unwrap().is_err());
        tokio::select! {
            biased;
            result = &mut owner => panic!("Duplicate failed or completed before ack: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        assert!(!cleanup.is_complete());
        assert!(
            connection
                .shutdown_session(receiver.clone(), SessionCleanup::new())
                .await
                .is_err()
        );
    }
    if order == "transport-failure" || order == "host-error" {
        assert!(harness.alive.load(Ordering::Acquire));
        let failure = if order == "host-error" {
            DriverEvent::HostMessage(HostToClient::Response {
                id: RequestId::new(4),
                result: WireResult::Err {
                    message: "synthetic-import-shutdown-io-failure".to_string(),
                },
            })
        } else {
            DriverEvent::Failed("synthetic-import-shutdown-io-failure".to_string())
        };
        harness.event_tx.send(failure).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), &mut owner)
            .await
            .unwrap();
        assert!(
            result
                .unwrap_err()
                .contains("synthetic-import-shutdown-io-failure")
        );
        cleanup.wait().await;
        assert!(!cleanup.acknowledged_for(&receiver));
        assert!(!harness.alive.load(Ordering::Acquire));
        return;
    }
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(4),
            result: WireResult::Ok {
                value: HostResponse::SessionClosed {
                    session_id: receiver.id.clone(),
                },
            },
        }))
        .await
        .unwrap();
    if order == "ack-first" {
        cleanup.wait().await;
    }
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), &mut owner)
            .await
            .unwrap(),
        Ok(())
    );
    assert!(cleanup.acknowledged_for(&receiver));
    assert!(harness.alive.load(Ordering::Acquire));
    assert!(
        harness.outgoing_rx.try_recv().is_err(),
        "Duplicate emitted another shutdown"
    );
    let (sibling_tx, sibling_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::ExportState {
            session: sibling,
            response_tx: sibling_tx,
        })
        .await
        .unwrap();
    let frame = harness.outgoing_rx.recv().await.unwrap();
    let ClientToHost::Request {
        id,
        request: HostRequest::ExportSession { .. },
    } = EncodedFrame::decode_framed::<ClientToHost>(&frame.into_framed_bytes()).unwrap()
    else {
        panic!("Sibling authority was lost")
    };
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id,
            result: WireResult::Ok {
                value: HostResponse::SessionExported {
                    state: codex_code_mode_protocol::CodeModeSnapshot {
                        format_version: 1,
                        next_cell_id: 1,
                        stored_values: Default::default(),
                        completed_cells: Default::default(),
                    },
                },
            },
        }))
        .await
        .unwrap();
    assert!(sibling_rx.await.unwrap().is_ok());
    let stale = RemoteSession {
        generation: receiver.generation + 1,
        ..receiver.clone()
    };
    assert!(
        connection
            .shutdown_session(stale.clone(), cleanup.clone())
            .await
            .is_err()
    );
    let next_cleanup = harness.open(stale.clone()).await;
    assert!(!next_cleanup.is_complete());
    assert!(
        connection
            .shutdown_session(receiver, cleanup)
            .await
            .is_err()
    );
    assert!(harness.alive.load(Ordering::Acquire));
}

#[tokio::test]
async fn import_cleanup_response_first_joins_owner_until_ack() {
    import_cleanup_order("response-first").await;
}

#[tokio::test]
async fn import_cleanup_owner_first_joins_late_import_until_ack() {
    import_cleanup_order("owner-first").await;
}

#[tokio::test]
async fn import_cleanup_ack_first_preserves_exact_identity() {
    import_cleanup_order("ack-first").await;
}

#[tokio::test]
async fn import_cleanup_transport_failure_wakes_duplicate_with_error() {
    import_cleanup_order("transport-failure").await;
}

#[tokio::test]
async fn import_cleanup_host_error_wakes_duplicate_with_error() {
    import_cleanup_order("host-error").await;
}

#[tokio::test]
async fn import_cleanup_error_receipt_joins_while_connection_appears_alive() {
    let (command_tx, mut command_rx) = mpsc::channel(1);
    let (execute_claim_tx, _claim_rx) = mpsc::unbounded_channel();
    let failure = Arc::new(StdMutex::new(None));
    let alive = Arc::new(AtomicBool::new(true));
    let connection = Connection {
        command_tx,
        execute_claim_tx,
        alive: alive.clone(),
        failure: failure.clone(),
        cancellation: CancellationToken::new(),
        capabilities: CapabilitySet::empty(),
    };
    let session = remote_session();
    let cleanup = SessionCleanup::new();
    let owner = connection.shutdown_session(session.clone(), cleanup.clone());
    tokio::pin!(owner);
    tokio::select! {
        biased;
        result = &mut owner => panic!("Finished before command admission: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    let DriverCommand::ShutdownSession {
        response_tx,
        cleanup: admitted,
        ..
    } = command_rx.recv().await.unwrap()
    else {
        panic!("Expected shutdown admission")
    };
    response_tx.send(Ok(())).unwrap();
    // Model the old error-reply/driver.fail scheduling window explicitly: the
    // error is known, but the connection's alive flag has not changed yet.
    *failure.lock().unwrap() = Some("synthetic-shutdown-error-before-dead-flag".to_string());
    assert!(alive.load(Ordering::Acquire));
    tokio::select! {
        biased;
        result = &mut owner => panic!("Skipped receipt join while alive: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    admitted.fail(Vec::new());
    assert_eq!(
        owner.await.unwrap_err(),
        "synthetic-shutdown-error-before-dead-flag"
    );
    assert!(cleanup.is_complete());
    assert!(!cleanup.acknowledged_for(&session));
    assert!(alive.load(Ordering::Acquire));
}

fn raw_shutdown_binding(
    harness: &DriverHarness,
    remote: RemoteSession,
    cleanup: SessionCleanup,
) -> super::super::super::ProcessOwnedCodeModeSession {
    let connection = Arc::new(Connection {
        command_tx: harness.command_tx.clone(),
        execute_claim_tx: harness.execute_claim_tx.clone(),
        alive: Arc::clone(&harness.alive),
        failure: Arc::clone(&harness.failure),
        cancellation: harness.cancellation.clone(),
        capabilities: serde_json::from_value(serde_json::json!([
            codex_code_mode_protocol::host::STATE_TRANSFER_CAPABILITY
        ]))
        .unwrap(),
    });
    let raw = super::super::super::ProcessOwnedCodeModeSession::new();
    *raw.inner.state.lock().unwrap() =
        super::super::super::SessionState::Open(super::super::super::SessionBinding {
            connection,
            remote,
            cleanup,
            retained_cell_ids: Arc::new(StdMutex::new(Default::default())),
        });
    raw
}

#[tokio::test]
async fn import_cleanup_raw_failure_before_shutdown_entry_stays_failed() {
    let mut harness = DriverHarness::start();
    let remote = remote_session();
    let cleanup = harness.open(remote.clone()).await;
    let raw = raw_shutdown_binding(&harness, remote.clone(), cleanup.clone());
    let failure = "synthetic-connection-death-before-raw-shutdown";
    *harness.failure.lock().unwrap() = Some(failure.to_string());
    harness.alive.store(false, Ordering::Release);
    {
        let first = raw.shutdown();
        tokio::pin!(first);
        tokio::select! {
            biased;
            result = &mut first => panic!("Returned before failure cleanup: {result:?}"),
            _ = std::future::ready(()) => {}
        }
        // Drop this waiter. The existing spawned shutdown owner must survive.
    }
    tokio::task::yield_now().await;
    let joined = raw.shutdown();
    tokio::pin!(joined);
    tokio::select! {
        biased;
        result = &mut joined => panic!("Cached owner skipped cleanup: {result:?}"),
        _ = std::future::ready(()) => {}
    }
    assert!(!cleanup.is_complete());
    assert!(harness.outgoing_rx.try_recv().is_err());
    harness
        .event_tx
        .send(DriverEvent::Failed(failure.to_string()))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), joined)
            .await
            .unwrap(),
        Err(failure.to_string())
    );
    assert!(cleanup.is_complete());
    assert!(!cleanup.acknowledged_for(&remote));
    assert_eq!(raw.shutdown().await, Err(failure.to_string()));
}

#[tokio::test]
async fn import_cleanup_raw_acknowledged_before_connection_death_stays_successful() {
    let mut harness = DriverHarness::start();
    let remote = remote_session();
    let cleanup = harness.open(remote.clone()).await;
    let raw = raw_shutdown_binding(&harness, remote.clone(), cleanup.clone());
    let (response_tx, response_rx) = oneshot::channel();
    harness
        .command_tx
        .send(DriverCommand::ShutdownSession {
            session: remote.clone(),
            cleanup: cleanup.clone(),
            response_tx,
        })
        .await
        .unwrap();
    assert_eq!(response_rx.await.unwrap(), Ok(()));
    harness.outgoing_rx.recv().await.expect("shutdown frame");
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id: RequestId::new(2),
            result: WireResult::Ok {
                value: HostResponse::SessionClosed {
                    session_id: remote.id.clone(),
                },
            },
        }))
        .await
        .unwrap();
    cleanup.wait().await;
    assert!(cleanup.acknowledged_for(&remote));
    harness
        .event_tx
        .send(DriverEvent::Failed("synthetic-death-after-ack".to_string()))
        .await
        .unwrap();
    harness.cancellation.cancelled().await;
    assert!(!harness.alive.load(Ordering::Acquire));
    assert_eq!(raw.shutdown().await, Ok(()));
    assert_eq!(raw.shutdown().await, Ok(()));
    assert!(harness.outgoing_rx.try_recv().is_err());
}

async fn pending_vm_request<F: std::future::Future>(
    harness: &mut DriverHarness,
    future: std::pin::Pin<&mut F>,
) -> (RequestId, HostRequest) {
    let frame = tokio::select! {
        biased;
        _ = future => panic!("VM operation completed before its native request"),
        frame = tokio::time::timeout(Duration::from_secs(1), harness.outgoing_rx.recv()) => frame.unwrap().unwrap(),
    };
    let ClientToHost::Request { id, request } =
        EncodedFrame::decode_framed::<ClientToHost>(&frame.into_framed_bytes()).unwrap()
    else {
        panic!("Expected VM request")
    };
    (id, request)
}

async fn vm_reply(harness: &DriverHarness, id: RequestId, value: HostResponse) {
    harness
        .event_tx
        .send(DriverEvent::HostMessage(HostToClient::Response {
            id,
            result: WireResult::Ok { value },
        }))
        .await
        .unwrap();
}

fn terminal_vm_image() -> codex_code_mode_protocol::CodeModeSnapshot {
    serde_json::from_value(serde_json::json!({
        "format_version": 1, "next_cell_id": 3, "stored_values": { "kept": "original" },
        "completed_cells": {
            "1": { "pending_initial_yield_items": null, "event": { "Completed": { "content_items": [], "error_text": null } } },
            "2": { "pending_initial_yield_items": null, "event": { "Terminated": { "content_items": [] } } }
        }
    })).unwrap()
}

#[tokio::test]
async fn import_cleanup_passive_export_preserves_original_source_failure() {
    let mut harness = DriverHarness::start();
    let remote = remote_session();
    let cleanup = harness.open(remote.clone()).await;
    let raw = raw_shutdown_binding(&harness, remote.clone(), cleanup.clone());
    harness
        .event_tx
        .send(DriverEvent::Failed(
            "synthetic-original-vm-source-loss".to_string(),
        ))
        .await
        .unwrap();
    cleanup.wait().await;
    assert_eq!(
        raw.export_state().await.unwrap_err(),
        "synthetic-original-vm-source-loss"
    );
    assert!(harness.outgoing_rx.try_recv().is_err());
    let state = raw.inner.state.lock().unwrap();
    assert!(
        matches!(&*state, super::super::super::SessionState::Open(binding) if binding.remote == remote)
    );
}

#[tokio::test]
async fn import_cleanup_unused_export_does_not_start_a_helper() {
    let raw = super::super::super::ProcessOwnedCodeModeSession::with_host(
        Arc::new(super::super::super::OwnedCodeModeHost::new(
            "synthetic-missing-unused-vm-helper".into(),
        )),
        Default::default(),
    );
    let state = raw.export_state().await.unwrap();
    assert_eq!(state.next_cell_id, 1);
    assert!(state.stored_values.is_empty() && state.completed_cells.is_empty());
    assert!(matches!(
        *raw.inner.state.lock().unwrap(),
        super::super::super::SessionState::New
    ));
    assert_eq!(raw.inner.next_generation.load(Ordering::Relaxed), 1);
    raw.shutdown().await.unwrap();
}

#[tokio::test]
async fn import_cleanup_attempted_source_export_does_not_retry_failed_open() {
    let raw = super::super::super::ProcessOwnedCodeModeSession::with_host(
        Arc::new(super::super::super::OwnedCodeModeHost::new(
            "synthetic-missing-attempted-vm-helper".into(),
        )),
        Default::default(),
    );
    assert!(raw.connection().await.is_err());
    let generation = raw.inner.next_generation.load(Ordering::Relaxed);
    assert!(generation > 1);
    assert!(
        raw.export_state()
            .await
            .unwrap_err()
            .contains("no retained source")
    );
    assert_eq!(
        raw.inner.next_generation.load(Ordering::Relaxed),
        generation
    );
}

#[tokio::test]
async fn import_cleanup_terminal_identity_survives_generation_change_and_consumption() {
    let mut source_host = DriverHarness::start();
    let source = RemoteSession {
        generation: 2,
        ..remote_session()
    };
    let cleanup = source_host.open(source.clone()).await;
    let source_raw = raw_shutdown_binding(&source_host, source, cleanup);
    let exporting = source_raw.export_state();
    tokio::pin!(exporting);
    let (id, request) = pending_vm_request(&mut source_host, exporting.as_mut()).await;
    assert!(matches!(request, HostRequest::ExportSession { .. }));
    vm_reply(
        &source_host,
        id,
        HostResponse::SessionExported {
            state: terminal_vm_image(),
        },
    )
    .await;
    let image = exporting.await.unwrap();
    assert_eq!(
        image.completed_cells["1"]
            .public_cell_id
            .as_ref()
            .unwrap()
            .as_str(),
        "g2:1"
    );
    assert_eq!(
        image.completed_cells["2"]
            .public_cell_id
            .as_ref()
            .unwrap()
            .as_str(),
        "g2:2"
    );

    let mut receiver_host = DriverHarness::start();
    let remote = remote_session();
    let cleanup = receiver_host.open(remote.clone()).await;
    let receiver = raw_shutdown_binding(&receiver_host, remote.clone(), cleanup);
    let importing = receiver.import_state(image.clone());
    tokio::pin!(importing);
    let (id, request) = pending_vm_request(&mut receiver_host, importing.as_mut()).await;
    let HostRequest::ImportSession {
        state: native_image,
        ..
    } = request
    else {
        panic!("Expected import")
    };
    assert_eq!(native_image, image);
    vm_reply(&receiver_host, id, HostResponse::SessionImported).await;
    importing.await.unwrap();
    let exporting = receiver.export_state();
    tokio::pin!(exporting);
    let (id, _) = pending_vm_request(&mut receiver_host, exporting.as_mut()).await;
    vm_reply(
        &receiver_host,
        id,
        HostResponse::SessionExported {
            state: native_image,
        },
    )
    .await;
    assert_eq!(exporting.await.unwrap(), image);

    for rejected in ["1", "2", "g9:1", "g2:3"] {
        assert!(
            receiver
                .wait(
                    WaitRequest {
                        cell_id: CellId::new(rejected.to_string()),
                        yield_time_ms: 1
                    },
                    None
                )
                .await
                .unwrap_err()
                .contains("stale")
        );
    }
    assert!(receiver_host.outgoing_rx.try_recv().is_err());
    for missing in [false, true] {
        let waiting = receiver.wait(
            WaitRequest {
                cell_id: CellId::new("g2:1".to_string()),
                yield_time_ms: 1,
            },
            None,
        );
        tokio::pin!(waiting);
        let (id, request) = pending_vm_request(&mut receiver_host, waiting.as_mut()).await;
        assert!(
            matches!(request, HostRequest::Wait { request, .. } if request.cell_id.as_str() == "1")
        );
        let result = WireRuntimeResponse::Result {
            cell_id: CellId::new("1".to_string()).into(),
            content_items: Vec::new(),
            error_text: None,
            code_mode_host_duration_ns: 0,
        };
        let outcome = if missing {
            WireWaitOutcome::MissingCell(result)
        } else {
            WireWaitOutcome::LiveCell(result)
        };
        vm_reply(&receiver_host, id, HostResponse::WaitCompleted { outcome }).await;
        match waiting.await.unwrap() {
            codex_code_mode_protocol::WaitOutcome::LiveCell(
                codex_code_mode_protocol::RuntimeResponse::Result { cell_id, .. },
            ) if !missing => assert_eq!(cell_id.as_str(), "g2:1"),
            codex_code_mode_protocol::WaitOutcome::MissingCell(
                codex_code_mode_protocol::RuntimeResponse::Result { cell_id, .. },
            ) if missing => assert_eq!(cell_id.as_str(), "g2:1"),
            other => panic!("Wrong retained result: {other:?}"),
        }
    }
    let terminating = receiver.terminate(CellId::new("g2:2".to_string()));
    tokio::pin!(terminating);
    let (id, request) = pending_vm_request(&mut receiver_host, terminating.as_mut()).await;
    assert!(matches!(request, HostRequest::Terminate { cell_id, .. } if cell_id.as_str() == "2"));
    vm_reply(
        &receiver_host,
        id,
        HostResponse::WaitCompleted {
            outcome: WireWaitOutcome::LiveCell(WireRuntimeResponse::Terminated {
                cell_id: CellId::new("2".to_string()).into(),
                content_items: Vec::new(),
                code_mode_host_duration_ns: 0,
            }),
        },
    )
    .await;
    assert!(
        matches!(terminating.await.unwrap(), codex_code_mode_protocol::WaitOutcome::LiveCell(codex_code_mode_protocol::RuntimeResponse::Terminated { cell_id, .. }) if cell_id.as_str() == "g2:2")
    );

    let executing = receiver.execute(
        ExecuteRequest {
            tool_call_id: "new-destination-cell".to_string(),
            enabled_tools: Vec::new(),
            source: "text('fresh');".to_string(),
            yield_time_ms: Some(1),
            max_output_tokens: None,
        },
        Arc::new(RecordingDelegate::default()),
        None,
    );
    tokio::pin!(executing);
    let (id, _) = pending_vm_request(&mut receiver_host, executing.as_mut()).await;
    vm_reply(
        &receiver_host,
        id,
        HostResponse::ExecutionStarted {
            cell_id: CellId::new("3".to_string()).into(),
        },
    )
    .await;
    assert_eq!(executing.await.unwrap().cell_id.as_str(), "3");

    assert_eq!(receiver.inner.next_generation.load(Ordering::Relaxed), 3);
    let reserved = receiver.inner.allocate_generation().unwrap();
    assert_eq!(reserved, 3);
    let mut fresh_host = DriverHarness::start();
    let fresh_remote = RemoteSession {
        generation: reserved,
        ..remote_session()
    };
    let cleanup = fresh_host.open(fresh_remote.clone()).await;
    let fresh = raw_shutdown_binding(&fresh_host, fresh_remote, cleanup);
    assert!(
        fresh
            .wait(
                WaitRequest {
                    cell_id: CellId::new("g2:1".to_string()),
                    yield_time_ms: 1
                },
                None
            )
            .await
            .unwrap_err()
            .contains("stale")
    );
    assert!(fresh_host.outgoing_rx.try_recv().is_err());
    let mut maximum = terminal_vm_image();
    maximum.completed_cells.get_mut("1").unwrap().public_cell_id =
        Some(CellId::new(format!("g{}:1", u64::MAX)));
    let importing = fresh.import_state(maximum);
    tokio::pin!(importing);
    let (id, _) = pending_vm_request(&mut fresh_host, importing.as_mut()).await;
    vm_reply(&fresh_host, id, HostResponse::SessionImported).await;
    importing.await.unwrap();
    assert_eq!(
        fresh.inner.next_generation.load(Ordering::Relaxed),
        u64::MAX
    );
    assert!(
        fresh
            .inner
            .allocate_generation()
            .unwrap_err()
            .contains("exhausted")
    );
    assert_eq!(
        fresh.inner.next_generation.load(Ordering::Relaxed),
        u64::MAX
    );
}

#[tokio::test]
async fn import_cleanup_legacy_terminal_identity_stays_numeric_in_new_generation() {
    let mut harness = DriverHarness::start();
    let remote = RemoteSession {
        generation: 3,
        ..remote_session()
    };
    let cleanup = harness.open(remote.clone()).await;
    let raw = raw_shutdown_binding(&harness, remote, cleanup);
    let importing = raw.import_state(terminal_vm_image());
    tokio::pin!(importing);
    let (id, request) = pending_vm_request(&mut harness, importing.as_mut()).await;
    let HostRequest::ImportSession { state, .. } = request else {
        panic!("Expected import")
    };
    assert_eq!(
        state.completed_cells["1"]
            .public_cell_id
            .as_ref()
            .unwrap()
            .as_str(),
        "1"
    );
    vm_reply(&harness, id, HostResponse::SessionImported).await;
    importing.await.unwrap();
    let waiting = raw.wait(
        WaitRequest {
            cell_id: CellId::new("1".to_string()),
            yield_time_ms: 1,
        },
        None,
    );
    tokio::pin!(waiting);
    let (id, request) = pending_vm_request(&mut harness, waiting.as_mut()).await;
    assert!(
        matches!(request, HostRequest::Wait { request, .. } if request.cell_id.as_str() == "1")
    );
    vm_reply(
        &harness,
        id,
        HostResponse::WaitCompleted {
            outcome: WireWaitOutcome::MissingCell(WireRuntimeResponse::Result {
                cell_id: CellId::new("1".to_string()).into(),
                content_items: Vec::new(),
                error_text: None,
                code_mode_host_duration_ns: 0,
            }),
        },
    )
    .await;
    assert!(
        matches!(waiting.await.unwrap(), codex_code_mode_protocol::WaitOutcome::MissingCell(codex_code_mode_protocol::RuntimeResponse::Result { cell_id, .. }) if cell_id.as_str() == "1")
    );
    assert!(
        raw.wait(
            WaitRequest {
                cell_id: CellId::new("g3:1".to_string()),
                yield_time_ms: 1
            },
            None
        )
        .await
        .unwrap_err()
        .contains("stale")
    );
    assert!(harness.outgoing_rx.try_recv().is_err());
}
