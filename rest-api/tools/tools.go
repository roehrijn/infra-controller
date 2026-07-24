// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package tools pins CLI tool versions in go.mod so every developer and CI
// run uses the same binary versions without a separate install step.
//
// The //go:build tools tag excludes this file from normal compilation.
// See tools/README.md for how to update tool versions.

//go:build tools

package tools

import (
	_ "github.com/bufbuild/buf/cmd/buf"                        // v1.70.0
	_ "github.com/golangci/golangci-lint/v2/cmd/golangci-lint" // v2.12.2; config version: "2" (.golangci.yml)
	_ "github.com/mgechev/revive"                              // v1.15.0
	_ "google.golang.org/grpc/cmd/protoc-gen-go-grpc"          // v1.6.1
	_ "google.golang.org/protobuf/cmd/protoc-gen-go"           // v1.36.11
)
