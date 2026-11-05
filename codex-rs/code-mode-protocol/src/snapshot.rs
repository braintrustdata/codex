use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::CellId;

/// Portable committed values and settled, unconsumed cell observations.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodeModeSnapshot {
    pub format_version: u32,
    pub next_cell_id: u64,
    pub stored_values: HashMap<String, JsonValue>,
    pub completed_cells: HashMap<String, CompletedCellSnapshot>,
}

impl CodeModeSnapshot {
    /// Validates the complete snapshot before any runtime or reply authority opens.
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != 1 || self.next_cell_id == 0 {
            return Err("unsupported or invalid code-mode snapshot".to_string());
        }
        for (id, cell) in &self.completed_cells {
            let number = id.parse::<u64>().map_err(|_| "invalid saved cell ID")?;
            if number == 0 || number >= self.next_cell_id || number.to_string() != *id {
                return Err("invalid saved cell ID".to_string());
            }
            if let Some(public) = &cell.public_cell_id {
                let valid = public.as_str() == id
                    || public
                        .as_str()
                        .strip_prefix('g')
                        .and_then(|value| value.split_once(':'))
                        .is_some_and(|(generation, wire)| {
                            generation
                                .parse::<u64>()
                                .is_ok_and(|n| n > 1 && n.to_string() == generation)
                                && wire == id
                        });
                if !valid {
                    return Err("invalid retained public cell identity".to_string());
                }
            }
            if !matches!(
                cell.event,
                SnapshotCellEvent::Completed { .. } | SnapshotCellEvent::Terminated { .. }
            ) {
                return Err("saved code-mode cell is not terminal".to_string());
            }
        }
        Ok(())
    }
}

/// A committed terminal outcome, with the cell's original observation order.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedCellSnapshot {
    /// Exact process-facing identity, distinct from this record's numeric wire key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_cell_id: Option<CellId>,
    pub pending_initial_yield_items: Option<Vec<SnapshotOutputItem>>,
    pub event: SnapshotCellEvent,
}

/// An observable cell lifecycle event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SnapshotCellEvent {
    Yielded {
        content_items: Vec<SnapshotOutputItem>,
    },
    Pending {
        content_items: Vec<SnapshotOutputItem>,
        pending_tool_call_ids: Vec<String>,
    },
    Completed {
        content_items: Vec<SnapshotOutputItem>,
        error_text: Option<String>,
    },
    Terminated {
        content_items: Vec<SnapshotOutputItem>,
    },
}

/// Output emitted by a cell since its preceding observation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SnapshotOutputItem {
    Text {
        text: String,
    },
    Image {
        image_url: String,
        detail: Option<SnapshotImageDetail>,
    },
    Audio {
        audio_url: String,
    },
}

/// Requested image fidelity for an output image.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SnapshotImageDetail {
    Auto,
    Low,
    High,
    Original,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> serde_json::Value {
        serde_json::json!({
            "format_version": 1,
            "next_cell_id": 2,
            "stored_values": {"kept": {"n": 7}},
            "completed_cells": {
                "1": {
                    "pending_initial_yield_items": [{"Text": {"text": "yield"}}],
                    "event": {"Completed": {
                        "content_items": [{"Image": {"image_url": "data:image/png;base64,AA==", "detail": "Low"}}],
                        "error_text": null
                    }}
                }
            }
        })
    }

    #[test]
    fn typed_snapshot_validates_and_round_trips_all_terminal_payloads() {
        let value = snapshot();
        let parsed: CodeModeSnapshot = serde_json::from_value(value.clone()).unwrap();
        parsed.validate().unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);
    }

    #[test]
    fn malformed_nested_terminal_payloads_reject_before_runtime_creation() {
        for malformed in [
            serde_json::json!({"Completed": {"content_items": [{"Text": {"text": 7}}], "error_text": null}}),
            serde_json::json!({"Completed": {"content_items": [{"Image": {"image_url": "synthetic", "detail": "Invalid"}}], "error_text": null}}),
            serde_json::json!({"Completed": {"content_items": [], "error_text": {"bad": true}}}),
            serde_json::json!({"Completed": {"content_items": [], "unexpected": "payload"}}),
        ] {
            let mut value = snapshot();
            value["completed_cells"]["1"]["event"] = malformed;
            assert!(serde_json::from_value::<CodeModeSnapshot>(value).is_err());
        }
    }

    #[test]
    fn nonterminal_cells_reject_through_the_shared_validator() {
        let mut value = snapshot();
        value["completed_cells"]["1"]["event"] =
            serde_json::json!({"Pending": {"content_items": [], "pending_tool_call_ids": []}});
        let parsed: CodeModeSnapshot = serde_json::from_value(value).unwrap();
        assert!(parsed.validate().unwrap_err().contains("not terminal"));
    }
    #[test]
    fn terminal_public_identity_accepts_exact_ids_and_rejects_conflicts() {
        let mut value = snapshot();
        for public in ["1", "g2:1", "g18446744073709551615:1"] {
            value["completed_cells"]["1"]["public_cell_id"] = public.into();
            assert!(
                serde_json::from_value::<CodeModeSnapshot>(value.clone())
                    .unwrap()
                    .validate()
                    .is_ok()
            );
        }
        for public in [
            "g1:1",
            "g0:1",
            "g02:1",
            "g2:2",
            "g2:01",
            "g2:1:1",
            "g18446744073709551616:1",
        ] {
            value["completed_cells"]["1"]["public_cell_id"] = public.into();
            assert!(
                serde_json::from_value::<CodeModeSnapshot>(value.clone())
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        value["completed_cells"]["1"]["public_cell_id"] = "g2:1".into();
        value["next_cell_id"] = 3.into();
        value["completed_cells"]["2"] = value["completed_cells"]["1"].clone();
        assert!(
            serde_json::from_value::<CodeModeSnapshot>(value)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}
