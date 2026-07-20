// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tmocks "go.temporal.io/sdk/mocks"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/NVIDIA/infra-controller/rest-api/api/pkg/api/handler/util/common"
	"github.com/NVIDIA/infra-controller/rest-api/api/pkg/api/model"
	sc "github.com/NVIDIA/infra-controller/rest-api/api/pkg/client/site"
	authz "github.com/NVIDIA/infra-controller/rest-api/auth/pkg/authorization"
	"github.com/NVIDIA/infra-controller/rest-api/common/pkg/coreproxy"
	cutil "github.com/NVIDIA/infra-controller/rest-api/common/pkg/util"
	cdbm "github.com/NVIDIA/infra-controller/rest-api/db/pkg/db/model"
	corev1 "github.com/NVIDIA/infra-controller/rest-api/proto/core/gen/v1"
)

func TestCreateSkuHandler_ProxiesCreateAndReturnsCreatedSku(t *testing.T) {
	fixture := newSkuManagementFixture(t, []string{authz.ProviderAdminRole})
	req := validSkuCreateRequest(fixture.siteID)

	rec := fixture.request(t, http.MethodPost, "", req, fixture.createHandler.Handle)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Len(t, fixture.requests, 2)
	assert.Equal(t, createSkuMethod, fixture.requests[0].FullMethod)
	assert.Equal(t, findSkusByIDsMethod, fixture.requests[1].FullMethod)

	var coreReq corev1.SkuList
	require.NoError(t, protojson.Unmarshal(fixture.requests[0].RequestJSON, &coreReq))
	require.Len(t, coreReq.Skus, 1)
	assert.Equal(t, req.ID, coreReq.Skus[0].Id)

	var response model.APISkuMutationResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.Equal(t, req.ID, response.ID)
	assert.Equal(t, fixture.siteID, response.SiteID)
	assert.Empty(t, response.AssociatedMachineIDs)
}

func TestCreateSkuHandler_ReturnsCreatedWhenPostCreateFetchFails(t *testing.T) {
	fixture := newSkuManagementFixtureWithFindError(t, []string{authz.ProviderAdminRole}, errors.New("post-create fetch failed"))
	req := validSkuCreateRequest(fixture.siteID)

	rec := fixture.request(t, http.MethodPost, "", req, fixture.createHandler.Handle)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Len(t, fixture.requests, 2)
	assert.Equal(t, createSkuMethod, fixture.requests[0].FullMethod)
	assert.Equal(t, findSkusByIDsMethod, fixture.requests[1].FullMethod)

	var response model.APISkuMutationResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.Equal(t, req.ID, response.ID)
	assert.Equal(t, fixture.siteID, response.SiteID)
	assert.Equal(t, req.Description, response.Description)
	assert.Equal(t, req.SchemaVersion, response.SchemaVersion)
	assert.Equal(t, req.DeviceType, response.DeviceType)
	assert.Equal(t, req.Components, response.Components)
	assert.Empty(t, response.AssociatedMachineIDs)
	assert.Nil(t, response.Created)
}

func TestUpdateSkuHandler_MergesPatchBeforeReplace(t *testing.T) {
	fixture := newSkuManagementFixture(t, []string{authz.ProviderAdminRole})
	description := "updated description"

	rec := fixture.request(t, http.MethodPatch, "sku-1", model.APISkuUpdateRequest{
		SiteID:      fixture.siteID,
		Description: &description,
	}, fixture.updateHandler.Handle)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, fixture.requests, 2)
	assert.Equal(t, findSkusByIDsMethod, fixture.requests[0].FullMethod)
	assert.Equal(t, replaceSkuMethod, fixture.requests[1].FullMethod)

	var coreReq corev1.Sku
	require.NoError(t, protojson.Unmarshal(fixture.requests[1].RequestJSON, &coreReq))
	assert.Equal(t, "sku-1", coreReq.Id)
	assert.Equal(t, "updated description", coreReq.GetDescription())
	assert.Equal(t, uint32(4), coreReq.SchemaVersion)
	require.NotNil(t, coreReq.Components)
	require.NotNil(t, coreReq.Components.Chassis)
	assert.Equal(t, "existing chassis", coreReq.Components.Chassis.Model)
}

func TestDeleteSkuHandler_ProxiesDelete(t *testing.T) {
	fixture := newSkuManagementFixture(t, []string{authz.ProviderAdminRole})

	rec := fixture.request(t, http.MethodDelete, "sku-1", model.APISkuDeleteRequest{SiteID: fixture.siteID}, fixture.deleteHandler.Handle)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	require.Len(t, fixture.requests, 1)
	assert.Equal(t, deleteSkuMethod, fixture.requests[0].FullMethod)

	var coreReq corev1.SkuIdList
	require.NoError(t, protojson.Unmarshal(fixture.requests[0].RequestJSON, &coreReq))
	assert.Equal(t, []string{"sku-1"}, coreReq.Ids)
}

func TestCreateSkuHandler_RejectsTenantAdmin(t *testing.T) {
	fixture := newSkuManagementFixture(t, []string{authz.TenantAdminRole})

	rec := fixture.request(t, http.MethodPost, "", validSkuCreateRequest(fixture.siteID), fixture.createHandler.Handle)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Empty(t, fixture.requests)
}

type skuManagementFixture struct {
	org           string
	siteID        string
	user          *cdbm.User
	createHandler CreateSkuHandler
	updateHandler UpdateSkuHandler
	deleteHandler DeleteSkuHandler
	requests      []coreproxy.Request
}

func newSkuManagementFixture(t *testing.T, roles []string) *skuManagementFixture {
	return newSkuManagementFixtureWithFindError(t, roles, nil)
}

func newSkuManagementFixtureWithFindError(t *testing.T, roles []string, findErr error) *skuManagementFixture {
	t.Helper()
	dbSession := common.TestInitDB(t)
	t.Cleanup(dbSession.Close)
	common.TestSetupSchema(t, dbSession)

	org := "test-org"
	user := common.TestBuildUser(t, dbSession, uuid.NewString(), org, roles)
	ip := common.TestBuildInfrastructureProvider(t, dbSession, "Test Provider", org, user)
	site := common.TestBuildSite(t, dbSession, ip, "Test Site", user)
	sDAO := cdbm.NewSiteDAO(dbSession)
	_, err := sDAO.Update(context.Background(), nil, cdbm.SiteUpdateInput{
		SiteID: site.ID,
		Status: cutil.GetPtr(cdbm.SiteStatusRegistered),
	})
	require.NoError(t, err)

	fixture := &skuManagementFixture{org: org, siteID: site.ID.String(), user: user}
	client := &tmocks.Client{}
	existing := existingSkuProto()
	fixture.addWorkflow(t, client, createSkuMethod, &corev1.SkuIdList{Ids: []string{"sku-1"}})
	if findErr == nil {
		fixture.addWorkflow(t, client, findSkusByIDsMethod, &corev1.SkuList{Skus: []*corev1.Sku{existing}})
	} else {
		fixture.addWorkflowError(client, findSkusByIDsMethod, findErr)
	}
	fixture.addWorkflow(t, client, replaceSkuMethod, existing)
	fixture.addWorkflow(t, client, deleteSkuMethod, nil)

	scp := sc.NewClientPool(nil)
	scp.IDClientMap[site.ID.String()] = client
	fixture.createHandler = NewCreateSkuHandler(dbSession, scp)
	fixture.updateHandler = NewUpdateSkuHandler(dbSession, scp)
	fixture.deleteHandler = NewDeleteSkuHandler(dbSession, scp)
	return fixture
}

func (f *skuManagementFixture) addWorkflowError(client *tmocks.Client, method string, getErr error) {
	run := &tmocks.WorkflowRun{}
	run.On("Get", mock.Anything, mock.Anything).Return(getErr)
	client.On(
		"ExecuteWorkflow",
		mock.Anything,
		mock.Anything,
		coreproxy.WorkflowName,
		mock.MatchedBy(func(req coreproxy.Request) bool { return req.FullMethod == method }),
	).Run(func(args mock.Arguments) {
		f.requests = append(f.requests, args.Get(3).(coreproxy.Request))
	}).Return(run, nil).Maybe()
}

func (f *skuManagementFixture) addWorkflow(t *testing.T, client *tmocks.Client, method string, response proto.Message) {
	t.Helper()
	run := &tmocks.WorkflowRun{}
	var responseJSON []byte
	if response != nil {
		var err error
		responseJSON, err = protojson.Marshal(response)
		require.NoError(t, err)
	}
	run.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		out, ok := args.Get(1).(*coreproxy.Response)
		require.True(t, ok)
		out.ResponseJSON = responseJSON
	}).Return(nil)
	client.On(
		"ExecuteWorkflow",
		mock.Anything,
		mock.Anything,
		coreproxy.WorkflowName,
		mock.MatchedBy(func(req coreproxy.Request) bool { return req.FullMethod == method }),
	).Run(func(args mock.Arguments) {
		f.requests = append(f.requests, args.Get(3).(coreproxy.Request))
	}).Return(run, nil).Maybe()
}

func (f *skuManagementFixture) request(t *testing.T, method, skuID string, body any, handler func(echo.Context) error) *httptest.ResponseRecorder {
	t.Helper()
	requestJSON, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(method, "/", strings.NewReader(string(requestJSON)))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()
	ec := echo.New().NewContext(req, rec)
	ec.SetParamNames("orgName", "id")
	ec.SetParamValues(f.org, skuID)
	ec.Set("user", f.user)
	require.NoError(t, handler(ec))
	return rec
}

func validSkuCreateRequest(siteID string) model.APISkuCreateRequest {
	deviceType := "gpu-server"
	return model.APISkuCreateRequest{
		SiteID:        siteID,
		ID:            "sku-1",
		Description:   "test SKU",
		SchemaVersion: 4,
		DeviceType:    &deviceType,
		Components: &model.APISkuComponents{
			Chassis: &model.APISkuChassis{Vendor: "NVIDIA", Model: "DGX H100", Architecture: "x86_64"},
		},
	}
}

func existingSkuProto() *corev1.Sku {
	description := "old description"
	deviceType := "gpu-server"
	return &corev1.Sku{
		Id:            "sku-1",
		Description:   &description,
		SchemaVersion: 4,
		DeviceType:    &deviceType,
		Components: &corev1.SkuComponents{
			Chassis: &corev1.SkuComponentChassis{Vendor: "NVIDIA", Model: "existing chassis", Architecture: "x86_64"},
		},
	}
}
