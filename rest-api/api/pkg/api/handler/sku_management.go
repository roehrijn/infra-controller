// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/NVIDIA/infra-controller/rest-api/api/pkg/api/handler/util/common"
	"github.com/NVIDIA/infra-controller/rest-api/api/pkg/api/model"
	sc "github.com/NVIDIA/infra-controller/rest-api/api/pkg/client/site"
	cutil "github.com/NVIDIA/infra-controller/rest-api/common/pkg/util"
	cdb "github.com/NVIDIA/infra-controller/rest-api/db/pkg/db"
	corev1 "github.com/NVIDIA/infra-controller/rest-api/proto/core/gen/v1"
	tclient "go.temporal.io/sdk/client"
)

const (
	createSkuMethod     = "/forge.Forge/CreateSku"
	findSkusByIDsMethod = "/forge.Forge/FindSkusByIds"
	replaceSkuMethod    = "/forge.Forge/ReplaceSku"
	deleteSkuMethod     = "/forge.Forge/DeleteSku"
)

// CreateSkuHandler creates one SKU on a Site's Core service.
type CreateSkuHandler struct {
	dbSession  *cdb.Session
	scp        *sc.ClientPool
	tracerSpan *cutil.TracerSpan
}

// NewCreateSkuHandler returns a new CreateSkuHandler.
func NewCreateSkuHandler(dbSession *cdb.Session, scp *sc.ClientPool) CreateSkuHandler {
	return CreateSkuHandler{dbSession: dbSession, scp: scp, tracerSpan: cutil.NewTracerSpan()}
}

// Handle godoc
// @Summary Create SKU
// @Description Create a SKU on the selected Site's Core service.
// @Tags SKU
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param org path string true "Name of NGC organization"
// @Param request body model.APISkuCreateRequest true "SKU create request"
// @Success 201 {object} model.APISkuMutationResponse
// @Router /v2/org/{org}/nico/sku [post]
func (h CreateSkuHandler) Handle(c echo.Context) error {
	org, dbUser, ctx, logger, handlerSpan := common.SetupHandler("SKU", "Create", c, h.tracerSpan)
	if handlerSpan != nil {
		defer handlerSpan.End()
	}

	var apiReq model.APISkuCreateRequest
	if err := c.Bind(&apiReq); err != nil {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "Invalid request body", nil)
	}
	if err := apiReq.Validate(); err != nil {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "Error validating SKU create request", err)
	}

	stc, siteID, apiErr := common.AuthorizeProviderSiteForCore(common.AuthorizeProviderSiteForCoreInput{
		Ctx: ctx, Logger: logger, DBSession: h.dbSession, SCP: h.scp,
		Org: org, User: dbUser, SiteID: apiReq.SiteID,
	})
	if apiErr != nil {
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, apiErr.Data)
	}

	logger.Info().Str("skuID", apiReq.ID).Str("siteID", siteID).Msg("creating SKU via Core proxy")
	var ids corev1.SkuIdList
	if apiErr = common.ExecuteCoreGRPC(ctx, stc, createSkuMethod, apiReq.ToProto(), &ids, siteID); apiErr != nil {
		logAPIError(logger, apiErr, "failed to create SKU via Core proxy")
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, nil)
	}
	if len(ids.Ids) != 1 {
		return cutil.NewAPIErrorResponse(c, http.StatusInternalServerError, "Core returned an unexpected SKU create response", nil)
	}

	sku, apiErr := findSkuByIDViaCore(ctx, stc, siteID, ids.Ids[0])
	if apiErr != nil {
		logAPIError(logger, apiErr, "failed to retrieve created SKU via Core proxy")
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, nil)
	}
	return c.JSON(http.StatusCreated, model.NewAPISkuMutationResponse(sku, siteID))
}

// UpdateSkuHandler partially updates one SKU on a Site's Core service.
type UpdateSkuHandler struct {
	dbSession  *cdb.Session
	scp        *sc.ClientPool
	tracerSpan *cutil.TracerSpan
}

// NewUpdateSkuHandler returns a new UpdateSkuHandler.
func NewUpdateSkuHandler(dbSession *cdb.Session, scp *sc.ClientPool) UpdateSkuHandler {
	return UpdateSkuHandler{dbSession: dbSession, scp: scp, tracerSpan: cutil.NewTracerSpan()}
}

// Handle godoc
// @Summary Update SKU
// @Description Update selected mutable fields on a SKU owned by the selected Site's Core service.
// @Tags SKU
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param org path string true "Name of NGC organization"
// @Param id path string true "SKU ID"
// @Param request body model.APISkuUpdateRequest true "SKU update request"
// @Success 200 {object} model.APISkuMutationResponse
// @Router /v2/org/{org}/nico/sku/{id} [patch]
func (h UpdateSkuHandler) Handle(c echo.Context) error {
	org, dbUser, ctx, logger, handlerSpan := common.SetupHandler("SKU", "Update", c, h.tracerSpan)
	if handlerSpan != nil {
		defer handlerSpan.End()
	}

	skuID := c.Param("id")
	if skuID == "" {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "SKU ID must be specified", nil)
	}
	var apiReq model.APISkuUpdateRequest
	if err := c.Bind(&apiReq); err != nil {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "Invalid request body", nil)
	}
	if err := apiReq.Validate(); err != nil {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "Error validating SKU update request", err)
	}

	stc, siteID, apiErr := common.AuthorizeProviderSiteForCore(common.AuthorizeProviderSiteForCoreInput{
		Ctx: ctx, Logger: logger, DBSession: h.dbSession, SCP: h.scp,
		Org: org, User: dbUser, SiteID: apiReq.SiteID,
	})
	if apiErr != nil {
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, apiErr.Data)
	}

	current, apiErr := findSkuByIDViaCore(ctx, stc, siteID, skuID)
	if apiErr != nil {
		logAPIError(logger, apiErr, "failed to retrieve SKU before update")
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, nil)
	}
	updatedReq := apiReq.ApplyToProto(current, skuID)
	var updated corev1.Sku
	logger.Info().Str("skuID", skuID).Str("siteID", siteID).Msg("updating SKU via Core proxy")
	if apiErr = common.ExecuteCoreGRPC(ctx, stc, replaceSkuMethod, updatedReq, &updated, siteID); apiErr != nil {
		logAPIError(logger, apiErr, "failed to update SKU via Core proxy")
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, nil)
	}
	return c.JSON(http.StatusOK, model.NewAPISkuMutationResponse(&updated, siteID))
}

// DeleteSkuHandler deletes one unused SKU from a Site's Core service.
type DeleteSkuHandler struct {
	dbSession  *cdb.Session
	scp        *sc.ClientPool
	tracerSpan *cutil.TracerSpan
}

// NewDeleteSkuHandler returns a new DeleteSkuHandler.
func NewDeleteSkuHandler(dbSession *cdb.Session, scp *sc.ClientPool) DeleteSkuHandler {
	return DeleteSkuHandler{dbSession: dbSession, scp: scp, tracerSpan: cutil.NewTracerSpan()}
}

// Handle godoc
// @Summary Delete SKU
// @Description Delete an unused SKU from the selected Site's Core service.
// @Tags SKU
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param org path string true "Name of NGC organization"
// @Param id path string true "SKU ID"
// @Param request body model.APISkuDeleteRequest true "SKU delete request"
// @Success 204
// @Router /v2/org/{org}/nico/sku/{id} [delete]
func (h DeleteSkuHandler) Handle(c echo.Context) error {
	org, dbUser, ctx, logger, handlerSpan := common.SetupHandler("SKU", "Delete", c, h.tracerSpan)
	if handlerSpan != nil {
		defer handlerSpan.End()
	}

	skuID := c.Param("id")
	if skuID == "" {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "SKU ID must be specified", nil)
	}
	var apiReq model.APISkuDeleteRequest
	if err := c.Bind(&apiReq); err != nil {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "Invalid request body", nil)
	}
	if err := apiReq.Validate(); err != nil {
		return cutil.NewAPIErrorResponse(c, http.StatusBadRequest, "Error validating SKU delete request", err)
	}

	stc, siteID, apiErr := common.AuthorizeProviderSiteForCore(common.AuthorizeProviderSiteForCoreInput{
		Ctx: ctx, Logger: logger, DBSession: h.dbSession, SCP: h.scp,
		Org: org, User: dbUser, SiteID: apiReq.SiteID,
	})
	if apiErr != nil {
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, apiErr.Data)
	}

	logger.Info().Str("skuID", skuID).Str("siteID", siteID).Msg("deleting SKU via Core proxy")
	if apiErr = common.ExecuteCoreGRPC(ctx, stc, deleteSkuMethod, &corev1.SkuIdList{Ids: []string{skuID}}, nil, siteID); apiErr != nil {
		logAPIError(logger, apiErr, "failed to delete SKU via Core proxy")
		return cutil.NewAPIErrorResponse(c, apiErr.Code, apiErr.Message, nil)
	}
	return c.NoContent(http.StatusNoContent)
}

func findSkuByIDViaCore(ctx context.Context, stc tclient.Client, siteID, skuID string) (*corev1.Sku, *cutil.APIError) {
	var response corev1.SkuList
	apiErr := common.ExecuteCoreGRPC(
		ctx,
		stc,
		findSkusByIDsMethod,
		&corev1.SkusByIdsRequest{Ids: []string{skuID}},
		&response,
		siteID,
	)
	if apiErr != nil {
		return nil, apiErr
	}
	if len(response.Skus) != 1 {
		return nil, cutil.NewAPIError(http.StatusNotFound, "Could not find SKU with the specified ID", nil)
	}
	return response.Skus[0], nil
}
