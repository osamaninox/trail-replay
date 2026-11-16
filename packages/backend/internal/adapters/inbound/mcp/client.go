// Package mcp is an inbound adapter that exposes Trail Replay's revert
// capabilities as MCP tools over stdio. It is a thin client of the existing
// REST API — no business logic lives here. Auth is service-account style:
// one user's credentials are provided via env, the JWT is held by the client
// and never enters the MCP protocol (or the agent's context).
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"trail-replay/internal/core/trail/domain"
)

// errUnauthorized marks a 401 from the API so do() can re-login and retry.
var errUnauthorized = errors.New("unauthorized")

// Client is an authenticated HTTP client for the Trail Replay REST API.
type Client struct {
	baseURL    string
	email      string
	password   string
	httpClient *http.Client

	mu    sync.Mutex
	token string
}

func NewClient(baseURL, email, password string) *Client {
	return &Client{
		baseURL:    strings.TrimSuffix(baseURL, "/"),
		email:      email,
		password:   password,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// Login authenticates against POST /auth/login and caches the JWT.
// Called eagerly at startup (fail fast) and again on any 401.
func (c *Client) Login(ctx context.Context) error {
	body := map[string]string{"email": c.email, "password": c.password}
	var result domain.AuthResult
	if err := c.doRaw(ctx, http.MethodPost, "/auth/login", body, "", &result); err != nil {
		return fmt.Errorf("login as %s: %w", c.email, err)
	}
	c.mu.Lock()
	c.token = result.Token
	c.mu.Unlock()
	return nil
}

// CreateRevertJob calls POST /reverts/jobs.
func (c *Client) CreateRevertJob(ctx context.Context, from, to time.Time) (*domain.RevertJobResponse, error) {
	req := domain.CreateRevertJobRequest{From: from, To: to}
	var job domain.RevertJobResponse
	if err := c.do(ctx, http.MethodPost, "/reverts/jobs", req, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// PreviewRevert calls POST /reverts/preview: a dry run (nothing applied)
// showing each affected row's current and post-revert state.
func (c *Client) PreviewRevert(ctx context.Context, from, to time.Time) (*domain.RevertPreview, error) {
	req := domain.PreviewRevertRequest{From: from, To: to}
	var preview domain.RevertPreview
	if err := c.do(ctx, http.MethodPost, "/reverts/preview", req, &preview); err != nil {
		return nil, err
	}
	return &preview, nil
}

// GetRevertJob calls GET /reverts/jobs/{id}.
func (c *Client) GetRevertJob(ctx context.Context, id string) (*domain.RevertJobResponse, error) {
	var job domain.RevertJobResponse
	if err := c.do(ctx, http.MethodGet, "/reverts/jobs/"+id, nil, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// do performs an authenticated request. On 401 it re-logs in and retries once.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	c.mu.Lock()
	token := c.token
	c.mu.Unlock()

	err := c.doRaw(ctx, method, path, body, token, out)
	if errors.Is(err, errUnauthorized) {
		if loginErr := c.Login(ctx); loginErr != nil {
			return fmt.Errorf("re-login after 401: %w", loginErr)
		}
		c.mu.Lock()
		token = c.token
		c.mu.Unlock()
		err = c.doRaw(ctx, method, path, body, token, out)
	}
	return err
}

// doRaw performs one HTTP request. Non-2xx responses become errors carrying
// the API's {"error": msg} body so agents can read and act on them.
func (c *Client) doRaw(ctx context.Context, method, path string, body any, token string, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized && token != "" {
		return errUnauthorized
	}
	if resp.StatusCode >= 400 {
		var apiErr struct {
			Error string `json:"error"`
		}
		data, _ := io.ReadAll(resp.Body)
		if json.Unmarshal(data, &apiErr) == nil && apiErr.Error != "" {
			return fmt.Errorf("trail-replay API (%d): %s", resp.StatusCode, apiErr.Error)
		}
		return fmt.Errorf("trail-replay API (%d): %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode %s %s response: %w", method, path, err)
		}
	}
	return nil
}
