package mcp

import (
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewServer builds the Trail Replay MCP server with its tool surface.
// Keep the surface deliberately small: every extra tool is extra room for an
// agent to improvise. Read-only tools carry ReadOnlyHint; the destructive one
// carries DestructiveHint so MCP clients gate it behind user approval.
func NewServer(client *Client) *mcpsdk.Server {
	h := &toolHandlers{client: client}

	server := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "trail-replay",
		Version: "v0.1.0",
	}, nil)

	destructive := true
	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name: "preview_revert",
		Description: "Preview what reverting a time window would do, WITHOUT applying anything. " +
			"Runs the revert as a rolled-back dry run on the source database and returns each " +
			"affected row per table with its current state, its state after the revert " +
			"(update/restore/delete), and any constraint conflicts that would make the revert " +
			"fail. ALWAYS show this to the user and get their confirmation before calling " +
			"create_revert_job for the same window.",
		Annotations: &mcpsdk.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, h.previewRevert)

	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name: "create_revert_job",
		Description: "Undo all database changes captured by Trail Replay within a time window " +
			"by executing compensating (reverse) SQL on the source database, newest change first. " +
			"Use this when asked to undo, roll back or revert database writes (for example after " +
			"bad data was inserted, updated or deleted). The job runs asynchronously: this call " +
			"returns immediately with a job id — poll get_revert_job until the status is " +
			"completed, failed or cancelled. WARNING: this modifies the source database.",
		Annotations: &mcpsdk.ToolAnnotations{
			DestructiveHint: &destructive,
			ReadOnlyHint:    false,
		},
	}, h.createRevertJob)

	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name: "get_revert_job",
		Description: "Get the status and progress of a Trail Replay revert job created by " +
			"create_revert_job. Progress is reported as completed_count out of total_changes, " +
			"with failed_count and last_error surfacing problems. Poll every few seconds until " +
			"the status is completed, failed or cancelled.",
		Annotations: &mcpsdk.ToolAnnotations{
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, h.getRevertJob)

	return server
}
