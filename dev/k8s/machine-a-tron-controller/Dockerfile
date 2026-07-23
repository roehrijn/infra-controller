# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build stage
FROM golang:1.26.4-alpine@sha256:4cfbe7d6f07723e1efe21b4251ccf17d92a8c2a9e6f9e88d3f3f5026e5db8f7d AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o mat-k8s-controller ./cmd/mat-k8s-controller

# Runtime stage
FROM gcr.io/distroless/static:nonroot@sha256:9ecc53c269509f63c69a266168b81a2f4c9d10c2f03e0d53dce6b8a2f9e1c1f1

COPY --from=builder /app/mat-k8s-controller /mat-k8s-controller

USER nonroot:nonroot

ENTRYPOINT ["/mat-k8s-controller"]
