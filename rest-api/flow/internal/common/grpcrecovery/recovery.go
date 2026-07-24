// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package grpcrecovery contains gRPC server interceptors that isolate panics
// to the request in which they occur.
package grpcrecovery

import (
	"context"
	"runtime/debug"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NVIDIA/infra-controller/rest-api/flow/internal/authz"
)

const internalErrorMessage = "internal server error"

// UnaryServerInterceptor converts a unary handler panic into an Internal gRPC
// error so one malformed request cannot terminate the Flow process.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer recoverPanic(ctx, info.FullMethod, &err)

		return handler(ctx, req)
	}
}

// StreamServerInterceptor converts a streaming handler panic into an Internal
// gRPC error so one malformed request cannot terminate the Flow process.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer recoverPanic(stream.Context(), info.FullMethod, &err)

		return handler(srv, stream)
	}
}

func recoverPanic(ctx context.Context, method string, err *error) {
	if recovered := recover(); recovered != nil {
		event := log.Error().
			Str("grpc_method", method).
			Interface("panic", recovered).
			Str("stack", string(debug.Stack()))

		if identity, ok := authz.ServiceIdentityFromContext(ctx); ok {
			event = event.Str("service_identity", identity)
		}

		event.Msg("Recovered panic while handling Flow gRPC request")
		*err = status.Error(codes.Internal, internalErrorMessage)
	}
}
