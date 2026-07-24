// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package authz

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryServerInterceptor(t *testing.T) {
	authorizer, err := New(Config{
		AllowedServiceIdentities: []string{allowedIdentity},
		Mode:                     ModeEnforce,
	})
	require.NoError(t, err)

	tests := map[string]struct {
		ctx         context.Context
		wantCode    codes.Code
		handlerRuns bool
	}{
		"allowed": {
			ctx:         tlsPeerContext(certificateWithURIs(t, allowedIdentity)),
			wantCode:    codes.OK,
			handlerRuns: true,
		},
		"denied": {
			ctx:      tlsPeerContext(certificateWithURIs(t, deniedIdentity)),
			wantCode: codes.PermissionDenied,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handlerRan := false
			_, err := UnaryServerInterceptor(authorizer)(
				test.ctx,
				struct{}{},
				&grpc.UnaryServerInfo{FullMethod: "/flow.v1.Flow/Test"},
				func(ctx context.Context, _ any) (any, error) {
					handlerRan = true
					identity, ok := ServiceIdentityFromContext(ctx)
					require.True(t, ok)
					assert.Equal(t, allowedIdentity, identity)
					return struct{}{}, nil
				},
			)

			assert.Equal(t, test.wantCode, status.Code(err))
			assert.Equal(t, test.handlerRuns, handlerRan)
		})
	}
}

func TestStreamServerInterceptorPropagatesServiceIdentity(t *testing.T) {
	authorizer, err := New(Config{
		AllowedServiceIdentities: []string{allowedIdentity},
		Mode:                     DefaultMode,
	})
	require.NoError(t, err)

	stream := &testServerStream{
		ctx: tlsPeerContext(certificateWithURIs(t, allowedIdentity)),
	}
	handlerRan := false
	err = StreamServerInterceptor(authorizer)(
		struct{}{},
		stream,
		&grpc.StreamServerInfo{FullMethod: "/flow.v1.Flow/TestStream"},
		func(_ any, stream grpc.ServerStream) error {
			handlerRan = true
			identity, ok := ServiceIdentityFromContext(stream.Context())
			require.True(t, ok)
			assert.Equal(t, allowedIdentity, identity)
			return nil
		},
	)

	require.NoError(t, err)
	assert.True(t, handlerRan)
}

func TestUnaryServerInterceptorAuditMode(t *testing.T) {
	authorizer, err := New(Config{Mode: ModeAudit})
	require.NoError(t, err)

	tests := map[string]struct {
		ctx      context.Context
		identity string
	}{
		"denied identity is propagated": {
			ctx:      tlsPeerContext(certificateWithURIs(t, deniedIdentity)),
			identity: deniedIdentity,
		},
		"unidentified caller gets audit identity": {
			ctx:      context.Background(),
			identity: AuditUnidentifiedIdentity,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := UnaryServerInterceptor(authorizer)(
				test.ctx,
				struct{}{},
				&grpc.UnaryServerInfo{FullMethod: "/flow.v1.Flow/Test"},
				func(ctx context.Context, _ any) (any, error) {
					identity, ok := ServiceIdentityFromContext(ctx)
					require.True(t, ok)
					assert.Equal(t, test.identity, identity)
					return struct{}{}, nil
				},
			)
			require.NoError(t, err)
		})
	}
}

func TestGRPCError(t *testing.T) {
	tests := map[string]struct {
		rejection rejection
		wantCode  codes.Code
	}{
		"none": {
			rejection: rejectionNone,
			wantCode:  codes.OK,
		},
		"unauthenticated": {
			rejection: rejectionUnauthenticated,
			wantCode:  codes.Unauthenticated,
		},
		"permission denied": {
			rejection: rejectionPermissionDenied,
			wantCode:  codes.PermissionDenied,
		},
		"unknown": {
			rejection: rejection(100),
			wantCode:  codes.Internal,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.wantCode, status.Code(grpcError(test.rejection)))
		})
	}
}

type testServerStream struct {
	ctx context.Context
}

func (*testServerStream) SetHeader(metadata.MD) error  { return nil }
func (*testServerStream) SendHeader(metadata.MD) error { return nil }
func (*testServerStream) SetTrailer(metadata.MD)       {}
func (s *testServerStream) Context() context.Context   { return s.ctx }
func (*testServerStream) SendMsg(any) error            { return nil }
func (*testServerStream) RecvMsg(any) error            { return nil }
