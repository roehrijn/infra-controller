// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package grpcrecovery

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryServerInterceptor(t *testing.T) {
	tests := map[string]struct {
		panicValue any
	}{
		"string panic": {
			panicValue: "boom",
		},
		"error panic": {
			panicValue: errors.New("boom"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			interceptor := UnaryServerInterceptor()
			info := &grpc.UnaryServerInfo{FullMethod: "/flow.v1.Flow/Test"}

			response, err := interceptor(
				context.Background(),
				struct{}{},
				info,
				func(context.Context, any) (any, error) {
					panic(test.panicValue)
				},
			)
			require.Error(t, err)
			assert.Nil(t, response)
			assert.Equal(t, codes.Internal, status.Code(err))
			assert.Equal(t, internalErrorMessage, status.Convert(err).Message())

			response, err = interceptor(
				context.Background(),
				struct{}{},
				info,
				func(context.Context, any) (any, error) {
					return "ok", nil
				},
			)
			require.NoError(t, err)
			assert.Equal(t, "ok", response)
		})
	}
}

func TestStreamServerInterceptor(t *testing.T) {
	interceptor := StreamServerInterceptor()
	info := &grpc.StreamServerInfo{FullMethod: "/flow.v1.Flow/TestStream"}
	stream := &testServerStream{ctx: context.Background()}

	err := interceptor(
		struct{}{},
		stream,
		info,
		func(any, grpc.ServerStream) error {
			panic("boom")
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Equal(t, internalErrorMessage, status.Convert(err).Message())

	err = interceptor(
		struct{}{},
		stream,
		info,
		func(any, grpc.ServerStream) error {
			return nil
		},
	)
	require.NoError(t, err)
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
