//! Reviews opted-in function calls synchronously before dispatch, preserving their native wire data.

use crate::function_tool::FunctionCallError;
use crate::guardian::GuardianApprovalRequest;
use crate::guardian::GuardianReviewContext;
use crate::guardian::GuardianReviewOptions;
use crate::guardian::new_guardian_review_id;
use crate::guardian::spawn_approval_decision;
use crate::tools::context::ToolCallSource;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolPayload;
use crate::tools::sandboxing::ApprovalRequestReasons;
use codex_analytics::GuardianApprovalRequestSource;
use codex_protocol::ToolName;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::ReviewDecision;
use serde_json::Value;
use std::collections::HashSet;

pub(crate) struct DynamicToolReviewPolicy(HashSet<ToolName>);

impl DynamicToolReviewPolicy {
    pub(crate) fn new(tools: impl IntoIterator<Item = ToolName>) -> Self {
        Self(
            tools
                .into_iter()
                .map(ToolName::with_default_namespace)
                .collect(),
        )
    }

    pub(crate) fn reviews(&self, tool: &ToolName) -> bool {
        if tool.namespace.as_deref().is_none_or(str::is_empty) {
            self.0.contains(&tool.clone().with_default_namespace())
        } else {
            self.0.contains(tool)
        }
    }
}

pub(crate) async fn review(invocation: &ToolInvocation) -> Result<(), FunctionCallError> {
    let ToolPayload::Function { arguments } = &invocation.payload else {
        return Err(FunctionCallError::RespondToModel(
            "Native review requires a function call".to_owned(),
        ));
    };
    let mut item = invocation
        .native_call
        .as_deref()
        .cloned()
        .unwrap_or_else(|| ResponseItem::FunctionCall {
            id: None,
            name: invocation.tool_name.name.clone(),
            namespace: invocation.tool_name.namespace.clone(),
            arguments: arguments.clone(),
            call_id: invocation.call_id.clone(),
            encrypted_content: None,
            status: None,
            encrypted_function_args: matches!(
                invocation.source,
                ToolCallSource::DirectPlaintextMessage | ToolCallSource::CodeMode { .. }
            )
            .then(Vec::new),
            internal_chat_message_metadata_passthrough: None,
        });
    if let ResponseItem::FunctionCall {
        arguments: reviewed_arguments,
        encrypted_content,
        ..
    } = &mut item
        && reviewed_arguments != arguments
    {
        // A hook rewrite invalidates replay of the original call, but not encrypted argument fields.
        reviewed_arguments.clone_from(arguments);
        *encrypted_content = None;
    }
    let context = GuardianReviewContext::from(&invocation.step_context);
    let require_guardian = codex_guardian_reviewer::routes_approval_policy_to_guardian(
        context.approval_policy,
        context.approvals_reviewer,
    );
    review_action(
        invocation,
        context,
        GuardianApprovalRequest::NativeToolCall {
            id: invocation.call_id.clone(),
            call: Box::new(item),
        },
        require_guardian,
    )
    .await
}

pub(crate) async fn review_dynamic_tool_call(
    invocation: &ToolInvocation,
    arguments: &Value,
    tool_description: Option<String>,
) -> Result<(), FunctionCallError> {
    let mut context = GuardianReviewContext::from(&invocation.step_context);
    let reviewer = codex_protocol::config_types::ApprovalsReviewer::AutoReview;
    invocation
        .turn
        .config
        .config_layer_stack
        .requirements()
        .approvals_reviewer
        .can_set(&reviewer)
        .map_err(|error| FunctionCallError::RespondToModel(error.to_string()))?;
    context.approvals_reviewer = reviewer;
    review_action(
        invocation,
        context,
        GuardianApprovalRequest::DynamicToolCall {
            id: invocation.call_id.clone(),
            tool: invocation.tool_name.clone(),
            arguments: arguments.clone(),
            tool_description,
        },
        true,
    )
    .await
}

async fn review_action(
    invocation: &ToolInvocation,
    context: GuardianReviewContext,
    action: GuardianApprovalRequest,
    require_guardian: bool,
) -> Result<(), FunctionCallError> {
    let dynamic_tool_call = matches!(action, GuardianApprovalRequest::DynamicToolCall { .. });
    let decision = spawn_approval_decision(
        invocation.session.clone(),
        context,
        new_guardian_review_id(),
        action,
        ApprovalRequestReasons::default(),
        GuardianReviewOptions {
            require_guardian,
            require_synchronous_review: true,
            plugin_attribution_override: None,
            approval_request_source: GuardianApprovalRequestSource::MainTurn,
            external_cancel: Some(invocation.cancellation_token.clone()),
        },
    )
    .await
    .map_err(|_| {
        FunctionCallError::RespondToModel("automatic approval review did not complete".to_owned())
    })?;
    match decision {
        None if !dynamic_tool_call => Ok(()),
        None => Err(FunctionCallError::RespondToModel(
            "automatic approval review did not authorize the dynamic tool call".to_owned(),
        )),
        Some(ReviewDecision::Approved | ReviewDecision::ApprovedForSession) => Ok(()),
        Some(ReviewDecision::Denied { rejection }) => {
            Err(FunctionCallError::RespondToModel(rejection))
        }
        Some(ReviewDecision::TimedOut) => Err(FunctionCallError::RespondToModel(
            codex_prompts::ResolvedModelMessages::from_model(
                &invocation.step_context.settings.model_info,
            )
            .auto_review()
            .timeout_instructions
            .to_owned(),
        )),
        Some(ReviewDecision::Abort) => Err(FunctionCallError::RespondToModel(
            "automatic approval review was cancelled".to_owned(),
        )),
        Some(
            ReviewDecision::ApprovedExecpolicyAmendment { .. }
            | ReviewDecision::ApprovedMcpPolicyAmendment
            | ReviewDecision::NetworkPolicyAmendment { .. },
        ) => Err(FunctionCallError::RespondToModel(
            "automatic approval review returned an unsupported decision".to_owned(),
        )),
    }
}
