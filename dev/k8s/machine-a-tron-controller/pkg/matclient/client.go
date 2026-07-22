// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package matclient provides an HTTP client for the machine-a-tron API.
package matclient

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"
)

// Client is an HTTP client for the machine-a-tron API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     zerolog.Logger
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(c *http.Client) Option {
	return func(client *Client) {
		client.httpClient = c
	}
}

// WithLogger sets the logger.
func WithLogger(logger zerolog.Logger) Option {
	return func(client *Client) {
		client.logger = logger
	}
}

// WithInsecureSkipVerify disables TLS certificate verification.
// Use only for development with self-signed certificates.
func WithInsecureSkipVerify() Option {
	return func(client *Client) {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // Intentional for dev/test with self-signed certs
			},
		}
		client.httpClient.Transport = transport
	}
}

// NewClient creates a new machine-a-tron API client.
func NewClient(baseURL string, opts ...Option) (*Client, error) {
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: zerolog.Nop(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// GetMachinesStatus fetches the current machine status from machine-a-tron.
func (c *Client) GetMachinesStatus(ctx context.Context) (*MachinesStatusResponse, error) {
	reqURL := c.baseURL + "/machines/status"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	c.logger.Debug().Str("url", reqURL).Msg("fetching machine status")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var result MachinesStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	c.logger.Debug().Int("machine_count", len(result.Machines)).Msg("fetched machine status")

	return &result, nil
}
