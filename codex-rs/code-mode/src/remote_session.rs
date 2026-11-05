use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use codex_code_mode_protocol::CellId;
use codex_code_mode_protocol::CodeModeSession;
use codex_code_mode_protocol::CodeModeSessionCellExecutionLimits;
use codex_code_mode_protocol::CodeModeSessionDelegate;
use codex_code_mode_protocol::CodeModeSessionProvider;
use codex_code_mode_protocol::CodeModeSessionProviderFuture;
use codex_code_mode_protocol::CodeModeSessionResultFuture;
use codex_code_mode_protocol::ExecuteRequest;
use codex_code_mode_protocol::StartedCell;
use codex_code_mode_protocol::WaitOutcome;
use codex_code_mode_protocol::WaitRequest;
use codex_code_mode_protocol::host::SessionId;
use codex_install_context::InstallContext;
use tokio::sync::Semaphore;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use self::connection::Connection;
use self::connection::ConnectionError;
use self::connection::RemoteSession;
use self::connection::SessionCleanup;

mod connection;

pub(crate) type ShutdownResultReceiver = watch::Receiver<Option<Result<(), String>>>;

/// Creates code-mode sessions backed by one lazily spawned process host.
pub struct ProcessOwnedCodeModeSessionProvider {
    host: Arc<OwnedCodeModeHost>,
}

/// Rejects code-mode sessions when the standalone host is disabled.
#[derive(Default)]
pub struct DisabledCodeModeSessionProvider;

impl ProcessOwnedCodeModeSessionProvider {
    pub fn with_host_program(host_program: PathBuf) -> Self {
        Self {
            host: Arc::new(OwnedCodeModeHost::new(host_program)),
        }
    }

    fn process_host(&self) -> Arc<OwnedCodeModeHost> {
        Arc::clone(&self.host)
    }
}

impl Default for ProcessOwnedCodeModeSessionProvider {
    fn default() -> Self {
        Self::with_host_program(InstallContext::current().code_mode_host_program())
    }
}

impl CodeModeSessionProvider for ProcessOwnedCodeModeSessionProvider {
    fn availability(&self) -> Result<(), String> {
        let host_program = &self.host.host_program;
        if host_program.is_file() {
            Ok(())
        } else {
            Err(ConnectionError::Spawn {
                host_program: host_program.clone(),
                error: io::Error::new(io::ErrorKind::NotFound, "host executable was not found"),
            }
            .to_string())
        }
    }

    fn create_session(&self) -> CodeModeSessionProviderFuture<'_> {
        self.create_session_with_limits(CodeModeSessionCellExecutionLimits::default())
    }

    fn create_session_with_limits<'a>(
        &'a self,
        limits: CodeModeSessionCellExecutionLimits,
    ) -> CodeModeSessionProviderFuture<'a> {
        Box::pin(create_host_session(self.process_host(), limits))
    }
}

impl CodeModeSessionProvider for DisabledCodeModeSessionProvider {
    fn availability(&self) -> Result<(), String> {
        Err("code-mode host is disabled".to_string())
    }

    fn create_session(&self) -> CodeModeSessionProviderFuture<'_> {
        Box::pin(async { Err("code-mode host is disabled".to_string()) })
    }

    fn create_session_with_limits<'a>(
        &'a self,
        _limits: CodeModeSessionCellExecutionLimits,
    ) -> CodeModeSessionProviderFuture<'a> {
        self.create_session()
    }
}

async fn create_host_session(
    host: Arc<OwnedCodeModeHost>,
    limits: CodeModeSessionCellExecutionLimits,
) -> Result<Arc<dyn CodeModeSession>, String> {
    let session = ProcessOwnedCodeModeSession::with_host(host, limits);
    session.connection().await?;
    Ok(Arc::new(session))
}

struct OwnedCodeModeHost {
    host_program: PathBuf,
    connection: StdMutex<Option<Arc<Connection>>>,
    connect_permit: Semaphore,
    connection_generation: AtomicU64,
    last_connection_error: StdMutex<Option<(u64, String)>>,
    next_session_id: AtomicU64,
}

impl OwnedCodeModeHost {
    fn new(host_program: PathBuf) -> Self {
        Self {
            host_program,
            connection: StdMutex::new(None),
            connect_permit: Semaphore::new(/*permits*/ 1),
            connection_generation: AtomicU64::new(0),
            last_connection_error: StdMutex::new(None),
            next_session_id: AtomicU64::new(1),
        }
    }

    async fn connection(&self) -> Result<Arc<Connection>, ConnectionError> {
        if let Some(connection) = self.live_connection() {
            return Ok(connection);
        }

        let observed_generation = self.connection_generation.load(Ordering::Acquire);
        let _connect_permit = self.connect_permit.acquire().await.map_err(|_| {
            ConnectionError::Other("code-mode host connection coordinator closed".into())
        })?;
        if let Some(connection) = self.live_connection() {
            return Ok(connection);
        }
        let completed_generation = self.connection_generation.load(Ordering::Acquire);
        if completed_generation != observed_generation
            && let Some((generation, error)) = self
                .last_connection_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
            && *generation == completed_generation
        {
            return Err(ConnectionError::Other(error.clone()));
        }
        let connection = Connection::spawn(&self.host_program).await;
        let new_connection = match connection {
            Ok(connection) => connection,
            Err(error) => {
                let generation = self.connection_generation.fetch_add(1, Ordering::AcqRel) + 1;
                *self
                    .last_connection_error
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((generation, error.to_string()));
                return Err(error);
            }
        };
        let new_connection = Arc::new(new_connection);
        *self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&new_connection));
        Ok(new_connection)
    }

    fn live_connection(&self) -> Option<Arc<Connection>> {
        self.connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|connection| connection.is_alive())
            .cloned()
    }

    fn allocate_session_id(&self) -> SessionId {
        let value = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        match SessionId::new(format!("session-{value}")) {
            Ok(session_id) => session_id,
            Err(_) => unreachable!("a generated code-mode session ID is nonempty"),
        }
    }
}

enum SessionState {
    New,
    Opening {
        remote: RemoteSession,
        result_rx: watch::Receiver<Option<Result<SessionBinding, String>>>,
    },
    Open(SessionBinding),
    Closing,
    Closed,
}

#[derive(Clone)]
struct SessionBinding {
    connection: Arc<Connection>,
    remote: RemoteSession,
    cleanup: SessionCleanup,
    retained_cell_ids: Arc<StdMutex<HashMap<String, CellId>>>,
}

impl SessionBinding {
    fn request_cell_id(&self, public: &CellId) -> Result<CellId, String> {
        let ids = self
            .retained_cell_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let wire = public
            .as_str()
            .rsplit(':')
            .next()
            .unwrap_or(public.as_str());
        if let Some(expected) = ids.get(wire) {
            if expected != public {
                return Err(format!(
                    "cell {public} belongs to a stale code-mode host generation"
                ));
            }
            return Ok(self.remote.public_cell_id(&CellId::new(wire.to_owned())));
        }
        Ok(public.clone())
    }
}

fn retained_wait_identity(mut outcome: WaitOutcome, public: CellId) -> WaitOutcome {
    let response = match &mut outcome {
        WaitOutcome::LiveCell(response) | WaitOutcome::MissingCell(response) => response,
    };
    match response {
        codex_code_mode_protocol::RuntimeResponse::Yielded { cell_id, .. }
        | codex_code_mode_protocol::RuntimeResponse::Result { cell_id, .. }
        | codex_code_mode_protocol::RuntimeResponse::Terminated { cell_id, .. } => {
            *cell_id = public
        }
    }
    outcome
}

struct SessionInner {
    host: Arc<OwnedCodeModeHost>,
    limits: CodeModeSessionCellExecutionLimits,
    state: StdMutex<SessionState>,
    next_generation: AtomicU64,
    shutdown_requested: AtomicBool,
    shutdown_result: StdMutex<Option<ShutdownResultReceiver>>,
    retired_cleanups: StdMutex<Vec<SessionCleanup>>,
}

/// A logical code-mode session assigned to a process host.
pub struct ProcessOwnedCodeModeSession {
    inner: Arc<SessionInner>,
}

impl ProcessOwnedCodeModeSession {
    pub fn new() -> Self {
        Self::with_host(
            Arc::new(OwnedCodeModeHost::new(
                InstallContext::current().code_mode_host_program(),
            )),
            CodeModeSessionCellExecutionLimits::default(),
        )
    }

    fn with_host(host: Arc<OwnedCodeModeHost>, limits: CodeModeSessionCellExecutionLimits) -> Self {
        Self {
            inner: Arc::new(SessionInner {
                host,
                limits,
                state: StdMutex::new(SessionState::New),
                next_generation: AtomicU64::new(1),
                shutdown_requested: AtomicBool::new(false),
                shutdown_result: StdMutex::new(None),
                retired_cleanups: StdMutex::new(Vec::new()),
            }),
        }
    }

    async fn connection(&self) -> Result<SessionBinding, String> {
        self.inner.connection().await
    }

    pub async fn execute(
        &self,
        request: ExecuteRequest,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        preempt: Option<CancellationToken>,
    ) -> Result<StartedCell, String> {
        let binding = self.connection().await?;
        binding
            .connection
            .execute(binding.remote, request, delegate, preempt)
            .await
    }

    pub async fn wait(
        &self,
        mut request: WaitRequest,
        preempt: Option<CancellationToken>,
    ) -> Result<WaitOutcome, String> {
        let binding = self.connection().await?;
        let public = request.cell_id.clone();
        request.cell_id = binding.request_cell_id(&public)?;
        let outcome = binding
            .connection
            .wait(binding.remote, request, preempt)
            .await?;
        Ok(retained_wait_identity(outcome, public))
    }

    pub async fn terminate(&self, cell_id: CellId) -> Result<WaitOutcome, String> {
        let binding = self.connection().await?;
        let target = binding.request_cell_id(&cell_id)?;
        let outcome = binding.connection.terminate(binding.remote, target).await?;
        Ok(retained_wait_identity(outcome, cell_id))
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        wait_for_watch(self.inner.request_shutdown()).await
    }
}

impl SessionInner {
    fn allocate_generation(&self) -> Result<u64, String> {
        self.next_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
                generation.checked_add(1)
            })
            .map_err(|_| "code-mode host generation space exhausted".to_string())
    }

    async fn export_binding(&self) -> Result<Option<SessionBinding>, String> {
        if self.shutdown_requested.load(Ordering::Acquire) {
            return Err("code mode session is shutting down".to_string());
        }
        let opening = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*state {
                SessionState::New if self.next_generation.load(Ordering::Relaxed) == 1 => {
                    return Ok(None);
                }
                SessionState::New => {
                    return Err("code-mode export has no retained source session".to_string());
                }
                SessionState::Open(binding) => {
                    if !binding.connection.is_alive() {
                        return Err(binding.connection.failure_message());
                    }
                    return Ok(Some(binding.clone()));
                }
                SessionState::Opening { result_rx, .. } => result_rx.clone(),
                SessionState::Closing | SessionState::Closed => {
                    return Err("code mode session is shutting down".to_string());
                }
            }
        };
        wait_for_watch(opening).await.map(Some)
    }

    async fn connection(self: &Arc<Self>) -> Result<SessionBinding, String> {
        loop {
            if self.shutdown_requested.load(Ordering::Acquire) {
                return Err("code mode session is shutting down".to_string());
            }
            let (result_rx, start) = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &*state {
                    SessionState::New => {
                        let generation = self.allocate_generation()?;
                        let remote = RemoteSession {
                            id: self.host.allocate_session_id(),
                            generation,
                        };
                        let (result_tx, result_rx) = watch::channel(None);
                        *state = SessionState::Opening {
                            remote: remote.clone(),
                            result_rx: result_rx.clone(),
                        };
                        (result_rx, Some((remote, result_tx)))
                    }
                    SessionState::Opening { result_rx, .. } => (result_rx.clone(), None),
                    SessionState::Open(binding) if binding.connection.is_alive() => {
                        return Ok(binding.clone());
                    }
                    SessionState::Open(binding) => {
                        self.retain_cleanup(binding.cleanup.clone());
                        *state = SessionState::New;
                        continue;
                    }
                    SessionState::Closing | SessionState::Closed => {
                        return Err("code mode session is shutting down".to_string());
                    }
                }
            };
            if let Some((remote, result_tx)) = start {
                let inner = Arc::clone(self);
                tokio::spawn(async move {
                    inner.open(remote, result_tx).await;
                });
            }
            return wait_for_watch(result_rx).await;
        }
    }

    async fn open(
        self: Arc<Self>,
        remote: RemoteSession,
        result_tx: watch::Sender<Option<Result<SessionBinding, String>>>,
    ) {
        let result = match self.host.connection().await {
            Ok(connection) => {
                let cleanup = connection
                    .open_session(remote.clone(), self.limits.clone())
                    .await;
                cleanup.map(|cleanup| SessionBinding {
                    connection,
                    remote: remote.clone(),
                    cleanup,
                    retained_cell_ids: Arc::new(StdMutex::new(HashMap::new())),
                })
            }
            Err(err) => Err(err.to_string()),
        };
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if matches!(
                &*state,
                SessionState::Opening {
                    remote: opening_remote,
                    ..
                } if opening_remote == &remote
            ) {
                *state = match &result {
                    Ok(binding) => SessionState::Open(binding.clone()),
                    Err(_) => SessionState::New,
                };
            }
        }
        result_tx.send_replace(Some(result));
    }

    fn request_shutdown(self: &Arc<Self>) -> ShutdownResultReceiver {
        self.shutdown_requested.store(true, Ordering::Release);
        let mut shutdown_result = self
            .shutdown_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(result_rx) = shutdown_result.as_ref() {
            return result_rx.clone();
        }
        let (result_tx, result_rx) = watch::channel(None);
        *shutdown_result = Some(result_rx.clone());
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            let result = inner.drive_shutdown().await;
            result_tx.send_replace(Some(result));
        });
        result_rx
    }

    async fn drive_shutdown(self: &Arc<Self>) -> Result<(), String> {
        loop {
            let action = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &*state {
                    SessionState::New => {
                        *state = SessionState::Closed;
                        ShutdownAction::Finish
                    }
                    SessionState::Opening { result_rx, .. } => {
                        ShutdownAction::WaitForOpen(result_rx.clone())
                    }
                    SessionState::Open(binding) => {
                        let binding = binding.clone();
                        *state = SessionState::Closing;
                        ShutdownAction::Close(binding)
                    }
                    SessionState::Closing => {
                        return Err("code-mode session shutdown driver entered twice".to_string());
                    }
                    SessionState::Closed => return Ok(()),
                }
            };
            match action {
                ShutdownAction::WaitForOpen(result_rx) => {
                    let _ = wait_for_watch(result_rx).await;
                }
                ShutdownAction::Finish => {
                    self.wait_for_retired_cleanups().await;
                    return Ok(());
                }
                ShutdownAction::Close(binding) => {
                    let result = binding
                        .connection
                        .shutdown_session(binding.remote, binding.cleanup.clone())
                        .await;
                    self.wait_for_retired_cleanups().await;
                    *self
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = SessionState::Closed;
                    return result;
                }
            }
        }
    }

    fn retain_cleanup(&self, cleanup: SessionCleanup) {
        let mut retired = self
            .retired_cleanups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retired.retain(|cleanup| !cleanup.is_complete());
        if !cleanup.is_complete() {
            retired.push(cleanup);
        }
    }

    async fn wait_for_retired_cleanups(&self) {
        let retired = std::mem::take(
            &mut *self
                .retired_cleanups
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for cleanup in retired {
            cleanup.wait().await;
        }
    }
}

enum ShutdownAction {
    WaitForOpen(watch::Receiver<Option<Result<SessionBinding, String>>>),
    Finish,
    Close(SessionBinding),
}

pub(crate) async fn wait_for_watch<T>(
    mut result_rx: watch::Receiver<Option<Result<T, String>>>,
) -> Result<T, String>
where
    T: Clone,
{
    loop {
        if let Some(result) = result_rx.borrow().clone() {
            return result;
        }
        result_rx
            .changed()
            .await
            .map_err(|_| "code-mode session transition stopped".to_string())?;
    }
}

impl Drop for ProcessOwnedCodeModeSession {
    fn drop(&mut self) {
        if tokio::runtime::Handle::try_current().is_ok() {
            self.inner.request_shutdown();
        }
    }
}

impl Default for ProcessOwnedCodeModeSession {
    fn default() -> Self {
        Self::new()
    }
}

impl CodeModeSession for ProcessOwnedCodeModeSession {
    fn export_state(
        &self,
    ) -> CodeModeSessionResultFuture<'_, codex_code_mode_protocol::CodeModeSnapshot> {
        Box::pin(async move {
            let Some(binding) = self.inner.export_binding().await? else {
                return Ok(codex_code_mode_protocol::CodeModeSnapshot {
                    format_version: 1,
                    next_cell_id: 1,
                    stored_values: HashMap::new(),
                    completed_cells: HashMap::new(),
                });
            };
            let mut state = binding
                .connection
                .export_state(binding.remote.clone())
                .await?;
            for (wire, cell) in &mut state.completed_cells {
                if cell.public_cell_id.is_none() {
                    cell.public_cell_id =
                        Some(binding.remote.public_cell_id(&CellId::new(wire.clone())));
                }
            }
            state.validate()?;
            Ok(state)
        })
    }
    fn import_state(
        &self,
        mut state: codex_code_mode_protocol::CodeModeSnapshot,
    ) -> CodeModeSessionResultFuture<'_, ()> {
        Box::pin(async move {
            state.validate()?;
            let mut ids = HashMap::new();
            let mut retained_generation = 1;
            for (wire, cell) in &mut state.completed_cells {
                let public = cell
                    .public_cell_id
                    .get_or_insert_with(|| CellId::new(wire.clone()));
                let generation = public
                    .as_str()
                    .strip_prefix('g')
                    .and_then(|value| value.split_once(':'))
                    .and_then(|(generation, _)| generation.parse::<u64>().ok())
                    .unwrap_or(1);
                retained_generation = retained_generation.max(generation);
                ids.insert(wire.clone(), public.clone());
            }
            let binding = self.connection().await?;
            // A later binding must never reuse an imported public generation for
            // unrelated cells. Reserve the existing monotonic allocator before
            // import can publish identities; MAX makes future allocation fail closed.
            self.inner.next_generation.fetch_max(
                retained_generation
                    .max(binding.remote.generation)
                    .saturating_add(1),
                Ordering::Relaxed,
            );
            binding
                .connection
                .import_state(binding.remote, state)
                .await?;
            *binding
                .retained_cell_ids
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = ids;
            Ok(())
        })
    }

    fn execute<'a>(
        &'a self,
        request: ExecuteRequest,
        delegate: Arc<dyn CodeModeSessionDelegate>,
        preempt: Option<CancellationToken>,
    ) -> CodeModeSessionResultFuture<'a, StartedCell> {
        Box::pin(ProcessOwnedCodeModeSession::execute(
            self, request, delegate, preempt,
        ))
    }

    fn wait<'a>(
        &'a self,
        request: WaitRequest,
        preempt: Option<CancellationToken>,
    ) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(ProcessOwnedCodeModeSession::wait(self, request, preempt))
    }

    fn terminate<'a>(&'a self, cell_id: CellId) -> CodeModeSessionResultFuture<'a, WaitOutcome> {
        Box::pin(ProcessOwnedCodeModeSession::terminate(self, cell_id))
    }

    fn shutdown<'a>(&'a self) -> CodeModeSessionResultFuture<'a, ()> {
        Box::pin(ProcessOwnedCodeModeSession::shutdown(self))
    }
}

#[cfg(test)]
#[path = "remote_session_tests.rs"]
mod tests;
