# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.26.4-alpine AS builder

WORKDIR /build

# Copy go mod files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o mat-k8s-controller ./cmd/mat-k8s-controller

# Runtime image
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /build/mat-k8s-controller /mat-k8s-controller

USER nonroot:nonroot

ENTRYPOINT ["/mat-k8s-controller"]
