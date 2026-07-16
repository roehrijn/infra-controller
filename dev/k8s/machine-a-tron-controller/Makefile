# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

.PHONY: build test lint docker-build clean

BINARY := mat-k8s-controller
DOCKER_IMAGE := mat-k8s-controller
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

# Build the binary
build:
	CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=$(VERSION)" -o $(BINARY) ./cmd/mat-k8s-controller

# Run all tests
test:
	go test -v -race ./...

# Run tests with coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	golangci-lint run ./...

# Format code
fmt:
	go fmt ./...

# Build Docker image
docker-build:
	docker build -t $(DOCKER_IMAGE):$(VERSION) .

# Build Docker image with latest tag
docker-build-latest: docker-build
	docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):latest

# Clean build artifacts
clean:
	rm -f $(BINARY) coverage.out coverage.html

# Run locally (requires kubeconfig and machine-a-tron running)
run:
	go run ./cmd/mat-k8s-controller --log-level debug

# Verify dependencies
verify:
	go mod verify
	go mod tidy
	@if [ -n "$$(git status --porcelain go.mod go.sum)" ]; then \
		echo "go.mod or go.sum changed after tidy"; \
		exit 1; \
	fi
