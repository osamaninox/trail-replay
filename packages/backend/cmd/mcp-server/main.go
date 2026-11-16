// Command mcp-server exposes Trail Replay revert operations as MCP tools over
// stdio. It is a thin wrapper around the REST API: credentials come from env
// (single service-account user), the JWT is cached and refreshed on 401.
//
// Configuration:
//
//	TRAIL_REPLAY_URL       API base URL (default http://localhost:8080)
//	TRAIL_REPLAY_EMAIL     login email (required)
//	TRAIL_REPLAY_PASSWORD  login password (required)
//
// IMPORTANT: stdout is the MCP JSON-RPC channel. All logging goes to stderr —
// never write anything else to stdout.
package main

import (
	"context"
	"log/slog"
	"os"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	mcpadapter "trail-replay/internal/adapters/inbound/mcp"
)

func main() {
	// slog's default handler writes to stderr; keep it that way.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	baseURL := os.Getenv("TRAIL_REPLAY_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	email := os.Getenv("TRAIL_REPLAY_EMAIL")
	password := os.Getenv("TRAIL_REPLAY_PASSWORD")
	if email == "" || password == "" {
		slog.Error("TRAIL_REPLAY_EMAIL and TRAIL_REPLAY_PASSWORD must be set")
		os.Exit(1)
	}

	ctx := context.Background()

	client := mcpadapter.NewClient(baseURL, email, password)
	if err := client.Login(ctx); err != nil {
		slog.Error("failed to log in to trail-replay API", "url", baseURL, "error", err)
		os.Exit(1)
	}
	slog.Info("logged in to trail-replay API", "url", baseURL, "email", email)

	server := mcpadapter.NewServer(client)
	if err := server.Run(ctx, &mcpsdk.StdioTransport{}); err != nil {
		slog.Error("mcp server failed", "error", err)
		os.Exit(1)
	}
}
