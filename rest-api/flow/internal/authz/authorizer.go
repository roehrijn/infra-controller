// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package authz

import (
	"context"
	"fmt"
	"strings"
)

// Config configures the service identities allowed to invoke Flow.
type Config struct {
	AllowedServiceIdentities []string
	Mode                     Mode
}

// Mode controls whether authorization failures are enforced or only recorded.
type Mode string

const (
	ModeUndefined Mode = ""
	ModeEnforce   Mode = "ENFORCE"
	ModeAudit     Mode = "AUDIT"
	DefaultMode        = ModeAudit

	AuditUnidentifiedIdentity = "audit-unidentified"
)

// NewMode constructs and validates a Mode from its string representation.
func NewMode(value string) (Mode, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return DefaultMode, nil
	}

	mode := Mode(value)
	if err := mode.Validate(); err != nil {
		return ModeUndefined, err
	}

	return mode, nil
}

// Validate checks that the mode is supported.
func (m Mode) Validate() error {
	switch m {
	case ModeEnforce, ModeAudit:
		return nil
	default:
		return fmt.Errorf(
			"authorization mode must be %q or %q, got %q",
			ModeEnforce,
			ModeAudit,
			m,
		)
	}
}

func (m Mode) enforce() bool {
	return m == ModeEnforce
}

// Validate checks that the allowlist contains unique canonical SPIFFE URIs.
func (c Config) Validate() error {
	_, err := c.buildAllowedServiceIdentities()
	return err
}

func (c Config) buildAllowedServiceIdentities() (map[string]struct{}, error) {
	if err := c.Mode.Validate(); err != nil {
		return nil, err
	}

	if c.Mode == ModeEnforce && len(c.AllowedServiceIdentities) == 0 {
		return nil, fmt.Errorf("at least one allowed service identity is required")
	}

	allowed := make(map[string]struct{}, len(c.AllowedServiceIdentities))
	for _, identity := range c.AllowedServiceIdentities {
		if err := validateSPIFFEIdentity(identity); err != nil {
			return nil, fmt.Errorf("invalid allowed service identity: %w", err)
		}

		if _, ok := allowed[identity]; ok {
			return nil, fmt.Errorf("duplicate allowed service identity %q", identity)
		}

		allowed[identity] = struct{}{}
	}

	return allowed, nil
}

// Authorizer checks the mTLS-authenticated service identity against an exact
// allowlist.
type Authorizer struct {
	allowed map[string]struct{}
	mode    Mode
}

// New constructs a production authorizer.
func New(config Config) (*Authorizer, error) {
	allowed, err := config.buildAllowedServiceIdentities()
	if err != nil {
		return nil, err
	}

	return &Authorizer{allowed: allowed, mode: config.Mode}, nil
}

type decision struct {
	identity  string
	message   string
	rejection rejection
}

type rejection int

const (
	rejectionNone rejection = iota
	rejectionUnauthenticated
	rejectionPermissionDenied
)

func (a *Authorizer) authorize(ctx context.Context) decision {
	identity, err := identityFromContext(ctx)
	if err != nil {
		return decision{
			message:   err.Error(),
			rejection: rejectionUnauthenticated,
		}
	}

	if _, ok := a.allowed[identity]; !ok {
		return decision{
			identity:  identity,
			message:   "service identity is not allowed",
			rejection: rejectionPermissionDenied,
		}
	}

	return decision{
		identity: identity,
	}
}
