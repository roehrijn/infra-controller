// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package authz authorizes direct callers of the Flow gRPC service using the
// workload identity authenticated by mTLS.
package authz

import (
	"context"
	"errors"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func identityFromContext(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("gRPC peer information is missing")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", errors.New("gRPC peer is not authenticated with TLS")
	}

	if len(tlsInfo.State.VerifiedChains) == 0 {
		return "", errors.New("TLS peer has no verified certificate chain")
	}

	id, err := spiffetls.PeerIDFromConnectionState(tlsInfo.State)
	if err != nil {
		return "", fmt.Errorf("extract SPIFFE peer identity: %w", err)
	}

	return id.String(), nil
}

func validateSPIFFEIdentity(identity string) error {
	id, err := spiffeid.FromString(identity)
	if err != nil {
		return fmt.Errorf("identity %q is not a valid SPIFFE ID: %w", identity, err)
	}

	if id.Path() == "" {
		return fmt.Errorf("identity %q must include a workload path", identity)
	}

	if id.String() != identity {
		return fmt.Errorf("identity %q is not canonical", identity)
	}

	return nil
}

type serviceIdentityContextKey struct{}

func withServiceIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, serviceIdentityContextKey{}, identity)
}

// ServiceIdentityFromContext returns the service identity attached by the Flow
// authorization interceptor.
//
// Keep the context value focused on the identity while that is the complete
// caller contract. Introduce a richer principal type here if authorization
// later needs to propagate additional verified attributes such as roles, trust
// domain metadata, or authentication method.
func ServiceIdentityFromContext(ctx context.Context) (string, bool) {
	identity, ok := ctx.Value(serviceIdentityContextKey{}).(string)
	return identity, ok
}
