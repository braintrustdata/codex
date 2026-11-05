use std::sync::{Arc, Mutex};

use super::types::RemoteSession;

use tokio_util::sync::CancellationToken;

use super::notify_cell_closed;
use super::session_registry::CellOwner;

struct CleanupInner {
    complete: CancellationToken,
    acknowledged: Mutex<Option<RemoteSession>>,
}

#[derive(Clone)]
pub(in crate::remote_session) struct SessionCleanup {
    inner: Arc<CleanupInner>,
}

impl SessionCleanup {
    pub(in crate::remote_session) fn new() -> Self {
        Self {
            inner: Arc::new(CleanupInner {
                complete: CancellationToken::new(),
                acknowledged: Mutex::new(None),
            }),
        }
    }

    pub(super) fn same_receipt(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(in crate::remote_session) fn acknowledged_for(&self, session: &RemoteSession) -> bool {
        self.inner
            .acknowledged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            == Some(session)
    }

    pub(super) fn acknowledge(&self, session: RemoteSession) {
        *self
            .inner
            .acknowledged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session);
        self.inner.complete.cancel();
    }

    pub(super) fn fail(&self, cells: Vec<CellOwner>) {
        for owner in cells {
            notify_cell_closed(&owner.delegate, &owner.cell_id);
        }
        self.inner.complete.cancel();
    }

    pub(in crate::remote_session) async fn wait(&self) {
        self.inner.complete.cancelled().await;
    }

    pub(in crate::remote_session) fn is_complete(&self) -> bool {
        self.inner.complete.is_cancelled()
    }
}
