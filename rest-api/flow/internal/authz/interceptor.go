// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package authz

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor authorizes unary RPCs using the mTLS service identity.
func UnaryServerInterceptor(authorizer *Authorizer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		authorization := authorizer.authorize(ctx)
		if err := checkAuthorizationResult(
			info.FullMethod,
			&authorization,
			authorizer.mode,
		); err != nil {
			return nil, err
		}

		return handler(withServiceIdentity(ctx, authorization.identity), req)
	}
}

// StreamServerInterceptor authorizes streaming RPCs using the mTLS service
// identity.
func StreamServerInterceptor(authorizer *Authorizer) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		authorization := authorizer.authorize(stream.Context())
		if err := checkAuthorizationResult(
			info.FullMethod,
			&authorization,
			authorizer.mode,
		); err != nil {
			return err
		}

		return handler(
			srv,
			&serviceIdentityServerStream{
				ServerStream: stream,
				ctx:          withServiceIdentity(stream.Context(), authorization.identity),
			},
		)
	}
}

func checkAuthorizationResult(method string, authorization *decision, mode Mode) error {
	if err := grpcError(authorization.rejection); err != nil {
		enforce := mode.enforce()

		level := zerolog.WarnLevel
		if enforce {
			level = zerolog.ErrorLevel
		}

		log.WithLevel(level).
			Str("grpc_method", method).
			Str("grpc_code", status.Code(err).String()).
			Str("authorization_mode", string(mode)).
			Str("service_identity", authorization.identity).
			Msg(authorization.message)

		if enforce {
			return err
		}

		if authorization.identity == "" {
			authorization.identity = AuditUnidentifiedIdentity
		}
	}

	return nil
}

func grpcError(rejection rejection) error {
	switch rejection {
	case rejectionNone:
		return nil
	case rejectionUnauthenticated:
		return status.Error(codes.Unauthenticated, "authenticated service identity is required")
	case rejectionPermissionDenied:
		return status.Error(codes.PermissionDenied, "caller is not authorized to invoke Flow")
	default:
		return status.Error(codes.Internal, "authorization decision is invalid")
	}
}

type serviceIdentityServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serviceIdentityServerStream) Context() context.Context {
	return s.ctx
}
