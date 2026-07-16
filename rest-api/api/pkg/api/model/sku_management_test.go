// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"testing"

	corev1 "github.com/NVIDIA/infra-controller/rest-api/proto/core/gen/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPISkuCreateRequest_ToProto(t *testing.T) {
	deviceType := "gpu-server"
	req := APISkuCreateRequest{
		SiteID:        uuid.NewString(),
		ID:            "dgx-h100",
		Description:   "DGX H100",
		SchemaVersion: 4,
		DeviceType:    &deviceType,
		Components:    testAPISkuComponents(),
	}

	require.NoError(t, req.Validate())
	proto := req.ToProto()
	require.Len(t, proto.Skus, 1)
	sku := proto.Skus[0]
	assert.Equal(t, "dgx-h100", sku.Id)
	assert.Equal(t, "DGX H100", sku.GetDescription())
	assert.Equal(t, uint32(4), sku.SchemaVersion)
	assert.Equal(t, "gpu-server", sku.GetDeviceType())
	require.NotNil(t, sku.Components)
	require.NotNil(t, sku.Components.Chassis)
	assert.Equal(t, "x86_64", sku.Components.Chassis.Architecture)
	require.Len(t, sku.Components.InfinibandDevices, 1)
	assert.Equal(t, []uint32{1}, sku.Components.InfinibandDevices[0].InactiveDevices)
}

func TestAPISkuCreateRequest_Validate(t *testing.T) {
	req := APISkuCreateRequest{SiteID: "not-a-uuid", ID: "", SchemaVersion: 0}
	assert.Error(t, req.Validate())
}

func TestAPISkuUpdateRequest_ApplyToProto(t *testing.T) {
	description := "updated description"
	req := APISkuUpdateRequest{
		SiteID:      uuid.NewString(),
		Description: &description,
	}
	existing := &corev1.Sku{
		Id:                   "dgx-h100",
		Description:          skuStringPtr("old description"),
		SchemaVersion:        4,
		DeviceType:           skuStringPtr("gpu-server"),
		Components:           testAPISkuComponents().ToProto(),
		AssociatedMachineIds: []*corev1.MachineId{{Id: "machine-1"}},
	}

	require.NoError(t, req.Validate())
	updated := req.ApplyToProto(existing, "dgx-h100")
	assert.Equal(t, "updated description", updated.GetDescription())
	assert.Equal(t, uint32(4), updated.SchemaVersion)
	assert.Equal(t, "gpu-server", updated.GetDeviceType())
	assert.Equal(t, existing.Components, updated.Components)
	assert.Equal(t, existing.AssociatedMachineIds, updated.AssociatedMachineIds)
	assert.NotSame(t, existing, updated)
}

func TestAPISkuUpdateRequest_ValidateRequiresChange(t *testing.T) {
	req := APISkuUpdateRequest{SiteID: uuid.NewString()}
	assert.Error(t, req.Validate())
}

func TestAPISkuDeleteRequest_Validate(t *testing.T) {
	assert.NoError(t, (APISkuDeleteRequest{SiteID: uuid.NewString()}).Validate())
	assert.Error(t, (APISkuDeleteRequest{SiteID: "bad"}).Validate())
}

func testAPISkuComponents() *APISkuComponents {
	return &APISkuComponents{
		Chassis: &APISkuChassis{Vendor: "NVIDIA", Model: "DGX H100", Architecture: "x86_64"},
		Cpus:    []APISkuCpu{{Vendor: "Intel", Model: "Xeon", ThreadCount: 112, Count: 2}},
		InfinibandDevices: []APISkuInfinibandDevice{{
			Vendor: "NVIDIA", Model: "ConnectX-7", Count: 2, InactiveDevices: []uint32{1},
		}},
	}
}

func skuStringPtr(value string) *string {
	return &value
}
