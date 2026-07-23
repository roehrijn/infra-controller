/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashSet;

use ::rpc::forge as rpc;
use ::rpc::forge_api_client::{EXPECTED_SWITCH_UPDATE_MASK_HEADER, ExpectedSwitchUpdateField};
use db::{DatabaseError, expected_switch as db_expected_switch};
use mac_address::MacAddress;
use model::expected_switch::{ExpectedSwitch, ExpectedSwitchRequest};
use model::machine_interface::InterfaceType;
use tonic::{Request, Response, Status};

use crate::CarbideError;
use crate::api::Api;
use crate::handlers::machine_interface_address::update_preallocated_machine_interface_after_locks;

fn parse_expected_switch_update_mask(
    request: &Request<rpc::ExpectedSwitch>,
) -> Result<Option<HashSet<ExpectedSwitchUpdateField>>, CarbideError> {
    let Some(value) = request.metadata().get(EXPECTED_SWITCH_UPDATE_MASK_HEADER) else {
        return Ok(None);
    };

    let value = value.to_str().map_err(|error| {
        CarbideError::InvalidArgument(format!("invalid expected-switch update mask: {error}"))
    })?;

    let fields = value
        .split(',')
        .map(str::parse)
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|_| {
            CarbideError::InvalidArgument(format!("invalid expected-switch update mask: {value}"))
        })?;

    Ok(Some(fields))
}

fn merge_expected_switch_patch(
    mut patch: rpc::ExpectedSwitch,
    current: rpc::ExpectedSwitch,
    fields: &HashSet<ExpectedSwitchUpdateField>,
) -> rpc::ExpectedSwitch {
    patch.expected_switch_id = current.expected_switch_id;
    patch.bmc_mac_address = current.bmc_mac_address;

    if !fields.contains(&ExpectedSwitchUpdateField::BmcUsername) {
        patch.bmc_username = current.bmc_username;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::BmcPassword) {
        patch.bmc_password = current.bmc_password;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::SwitchSerialNumber) {
        patch.switch_serial_number = current.switch_serial_number;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::NvosMacAddresses) {
        patch.nvos_mac_addresses = current.nvos_mac_addresses;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::NvosUsername) {
        patch.nvos_username = current.nvos_username;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::NvosPassword) {
        patch.nvos_password = current.nvos_password;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::RackId) {
        patch.rack_id = current.rack_id;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::BmcIpAddress) {
        patch.bmc_ip_address = current.bmc_ip_address;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::NvosIpAddress) {
        patch.nvos_ip_address = current.nvos_ip_address;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::BmcRetainCredentials) {
        patch.bmc_retain_credentials = current.bmc_retain_credentials;
    }

    let mut patch_metadata = patch.metadata.unwrap_or_default();
    let current_metadata = current.metadata.unwrap_or_default();

    if !fields.contains(&ExpectedSwitchUpdateField::MetadataName) {
        patch_metadata.name = current_metadata.name;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::MetadataDescription) {
        patch_metadata.description = current_metadata.description;
    }

    if !fields.contains(&ExpectedSwitchUpdateField::MetadataLabels) {
        patch_metadata.labels = current_metadata.labels;
    }

    patch.metadata = Some(patch_metadata);

    patch
}

/// `nvos_ip_address` is paired with the single wired NVOS port. We reject any
/// caller that sets it alongside zero-or-multiple `nvos_mac_addresses`, so the
/// (mac, ip) pairing stays unambiguous for the discover hook and the
/// reconciliation pass.
fn validate_nvos_ip_pairing(switch: &ExpectedSwitch) -> Result<(), CarbideError> {
    if switch.nvos_ip_address.is_some() && switch.nvos_mac_addresses.len() != 1 {
        return Err(CarbideError::InvalidArgument(format!(
            "nvos_ip_address requires exactly one nvos_mac_addresses entry, got {}",
            switch.nvos_mac_addresses.len(),
        )));
    }
    Ok(())
}

/// Requires NVOS username and password to be present together and non-empty.
fn validate_nvos_credentials_pair(switch: &ExpectedSwitch) -> Result<(), CarbideError> {
    match (&switch.nvos_username, &switch.nvos_password) {
        (Some(username), Some(_)) if username.is_empty() => Err(CarbideError::InvalidArgument(
            "nvos_username must not be empty".to_string(),
        )),
        (Some(_), Some(password)) if password.is_empty() => Err(CarbideError::InvalidArgument(
            "nvos_password must not be empty".to_string(),
        )),
        (Some(_), Some(_)) | (None, None) => Ok(()),
        _ => Err(CarbideError::InvalidArgument(
            "nvos_username and nvos_password must be set together".to_string(),
        )),
    }
}

fn validate_expected_switch(switch: &ExpectedSwitch) -> Result<(), CarbideError> {
    validate_nvos_ip_pairing(switch)?;
    validate_nvos_credentials_pair(switch)?;

    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ExpectedSwitchStaticInterface {
    mac_address: MacAddress,
    ip_address: std::net::IpAddr,
    interface_type: InterfaceType,
}

struct ResolvedExpectedSwitchStaticInterface {
    interface: ExpectedSwitchStaticInterface,
    segment: model::network_segment::NetworkSegment,
}

fn expected_switch_static_interfaces(
    switch: &ExpectedSwitch,
) -> Vec<ExpectedSwitchStaticInterface> {
    let mut interfaces = Vec::with_capacity(2);
    if let Some(ip_address) = switch.bmc_ip_address {
        interfaces.push(ExpectedSwitchStaticInterface {
            mac_address: switch.bmc_mac_address,
            ip_address,
            interface_type: InterfaceType::Bmc,
        });
    }
    // Request-derived switches are validated before this helper is called.
    // A malformed stored pairing has no unambiguous target to reconcile.
    if let (Some(ip_address), [mac_address]) =
        (switch.nvos_ip_address, switch.nvos_mac_addresses.as_slice())
    {
        interfaces.push(ExpectedSwitchStaticInterface {
            mac_address: *mac_address,
            ip_address,
            interface_type: InterfaceType::Data,
        });
    }
    interfaces
}

async fn resolve_and_lock_expected_switch_static_interfaces<'a>(
    txn: &mut sqlx::PgConnection,
    switches: impl IntoIterator<Item = &'a ExpectedSwitch>,
) -> Result<Vec<ResolvedExpectedSwitchStaticInterface>, CarbideError> {
    let static_interfaces = switches
        .into_iter()
        .flat_map(expected_switch_static_interfaces)
        .collect::<Vec<_>>();
    let mut resolved_interfaces = Vec::with_capacity(static_interfaces.len());
    for interface in static_interfaces {
        let segment =
            db::network_segment::for_static_address(&mut *txn, interface.ip_address, None).await?;
        resolved_interfaces.push(ResolvedExpectedSwitchStaticInterface { interface, segment });
    }

    let allocations = resolved_interfaces
        .iter()
        .map(|resolved| (resolved.segment.id, resolved.interface.ip_address))
        .collect::<Vec<_>>();
    db::machine_interface::lock_static_address_allocations(txn, &allocations).await?;

    Ok(resolved_interfaces)
}

async fn reconcile_expected_switch_static_interfaces(
    api: &Api,
    txn: &mut sqlx::PgConnection,
    mut resolved_interfaces: Vec<ResolvedExpectedSwitchStaticInterface>,
) -> Result<(), CarbideError> {
    resolved_interfaces.sort_by(|left, right| {
        left.interface
            .mac_address
            .to_string()
            .cmp(&right.interface.mac_address.to_string())
            .then_with(|| left.interface.ip_address.cmp(&right.interface.ip_address))
    });
    for resolved in resolved_interfaces {
        update_preallocated_machine_interface_after_locks(
            txn,
            resolved.interface.mac_address,
            resolved.interface.ip_address,
            resolved.interface.interface_type,
            &resolved.segment,
            api.runtime_config.retained_boot_interface_window,
        )
        .await?;
    }

    Ok(())
}

pub async fn add_expected_switch(
    api: &Api,
    request: Request<rpc::ExpectedSwitch>,
) -> Result<Response<()>, Status> {
    let switch: ExpectedSwitch =
        request
            .into_inner()
            .try_into()
            .map_err(|e: ::rpc::errors::RpcDataConversionError| {
                CarbideError::InvalidArgument(e.to_string())
            })?;

    validate_expected_switch(&switch)?;

    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;

    db_expected_switch::create(&mut txn, switch)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    Ok(Response::new(()))
}

pub async fn delete_expected_switch(
    api: &Api,
    request: Request<rpc::ExpectedSwitchRequest>,
) -> Result<Response<()>, Status> {
    let req: ExpectedSwitchRequest =
        request
            .into_inner()
            .try_into()
            .map_err(|e: ::rpc::errors::RpcDataConversionError| {
                CarbideError::InvalidArgument(e.to_string())
            })?;

    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;

    db_expected_switch::delete(&mut txn, &req)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    Ok(Response::new(()))
}

pub async fn update_expected_switch(
    api: &Api,
    request: Request<rpc::ExpectedSwitch>,
) -> Result<Response<()>, Status> {
    let update_mask = parse_expected_switch_update_mask(&request)?;
    let patch = request.into_inner();
    let lookup: ExpectedSwitchRequest = rpc::ExpectedSwitchRequest {
        bmc_mac_address: patch.bmc_mac_address.clone(),
        expected_switch_id: patch.expected_switch_id.clone(),
    }
    .try_into()
    .map_err(|e: ::rpc::errors::RpcDataConversionError| {
        CarbideError::InvalidArgument(e.to_string())
    })?;

    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;
    db_expected_switch::lock_writes(&mut txn).await?;

    let current = db_expected_switch::find(&mut txn, &lookup)
        .await
        .map_err(CarbideError::from)?
        .ok_or_else(|| DatabaseError::NotFoundError {
            kind: "expected_switch",
            id: lookup
                .expected_switch_id
                .map(|id| id.to_string())
                .or_else(|| lookup.bmc_mac_address.map(|mac| mac.to_string()))
                .unwrap_or_default(),
        })?;

    let switch: ExpectedSwitch = if let Some(update_mask) = update_mask {
        merge_expected_switch_patch(patch, current.clone().into(), &update_mask)
            .try_into()
            .map_err(|e: ::rpc::errors::RpcDataConversionError| {
                CarbideError::InvalidArgument(e.to_string())
            })?
    } else {
        patch
            .try_into()
            .map_err(|e: ::rpc::errors::RpcDataConversionError| {
                CarbideError::InvalidArgument(e.to_string())
            })?
    };

    validate_expected_switch(&switch)?;

    db::machine_interface::lock_expected_machine_interface_macs(
        &mut txn,
        current
            .nvos_mac_addresses
            .iter()
            .copied()
            .chain(switch.nvos_mac_addresses.iter().copied())
            .chain([current.bmc_mac_address, switch.bmc_mac_address]),
    )
    .await?;

    let resolved =
        resolve_and_lock_expected_switch_static_interfaces(&mut txn, std::iter::once(&switch))
            .await?;
    reconcile_expected_switch_static_interfaces(api, &mut txn, resolved).await?;

    db_expected_switch::update(&mut txn, &switch)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    Ok(Response::new(()))
}

pub async fn get_expected_switch(
    api: &Api,
    request: Request<rpc::ExpectedSwitchRequest>,
) -> Result<Response<rpc::ExpectedSwitch>, Status> {
    let req: ExpectedSwitchRequest =
        request
            .into_inner()
            .try_into()
            .map_err(|e: ::rpc::errors::RpcDataConversionError| {
                CarbideError::InvalidArgument(e.to_string())
            })?;

    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;

    let expected_switch = db_expected_switch::find(&mut txn, &req)
        .await
        .map_err(CarbideError::from)?
        .ok_or_else(|| CarbideError::NotFoundError {
            kind: "expected_switch",
            id: req
                .expected_switch_id
                .map(|u| u.to_string())
                .or(req.bmc_mac_address.map(|m| m.to_string()))
                .unwrap_or_default(),
        })?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    let response = rpc::ExpectedSwitch::from(expected_switch);
    Ok(Response::new(response))
}

pub async fn get_all_expected_switches(
    api: &Api,
    _request: Request<()>,
) -> Result<Response<rpc::ExpectedSwitchList>, Status> {
    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;

    let expected_switches = db_expected_switch::find_all(&mut txn)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    let expected_switches: Vec<rpc::ExpectedSwitch> = expected_switches
        .into_iter()
        .map(rpc::ExpectedSwitch::from)
        .collect();

    Ok(Response::new(rpc::ExpectedSwitchList { expected_switches }))
}

pub async fn replace_all_expected_switches(
    api: &Api,
    request: Request<rpc::ExpectedSwitchList>,
) -> Result<Response<()>, Status> {
    let replacements = request
        .into_inner()
        .expected_switches
        .into_iter()
        .map(|expected_switch| {
            let switch: ExpectedSwitch = expected_switch.try_into().map_err(
                |error: ::rpc::errors::RpcDataConversionError| {
                    CarbideError::InvalidArgument(error.to_string())
                },
            )?;
            validate_expected_switch(&switch)?;
            Ok(switch)
        })
        .collect::<Result<Vec<_>, CarbideError>>()?;

    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;
    db_expected_switch::lock_writes(&mut txn).await?;
    let previous = db_expected_switch::find_all(&mut txn).await?;
    db::machine_interface::lock_expected_machine_interface_macs(
        &mut txn,
        previous
            .iter()
            .chain(&replacements)
            .flat_map(|switch| {
                switch
                    .nvos_mac_addresses
                    .iter()
                    .copied()
                    .chain(std::iter::once(switch.bmc_mac_address))
            })
            .collect::<Vec<_>>(),
    )
    .await?;

    db_expected_switch::clear(&mut txn)
        .await
        .map_err(CarbideError::from)?;

    // Keep bulk replacement aligned with add: store declarations and let
    // discovery materialize their interfaces. Eager reservations cannot be
    // distinguished from operator-managed static assignments during cleanup.
    for switch in &replacements {
        db_expected_switch::create(&mut txn, switch.clone())
            .await
            .map_err(CarbideError::from)?;
    }

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    Ok(Response::new(()))
}

pub async fn delete_all_expected_switches(
    api: &Api,
    _request: Request<()>,
) -> Result<Response<()>, Status> {
    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;

    db_expected_switch::clear(&mut txn)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    Ok(Response::new(()))
}

pub async fn get_all_expected_switches_linked(
    api: &Api,
    _request: Request<()>,
) -> Result<Response<rpc::LinkedExpectedSwitchList>, Status> {
    let mut txn = api
        .database_connection
        .begin()
        .await
        .map_err(|e| CarbideError::Internal {
            message: format!("Database error: {}", e),
        })?;

    let linked_expected_switches = db_expected_switch::find_all_linked(&mut txn)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await.map_err(|e| CarbideError::Internal {
        message: format!("Failed to commit transaction: {}", e),
    })?;

    let linked_expected_switches: Vec<rpc::LinkedExpectedSwitch> = linked_expected_switches
        .into_iter()
        .map(rpc::LinkedExpectedSwitch::from)
        .collect();

    Ok(Response::new(rpc::LinkedExpectedSwitchList {
        expected_switches: linked_expected_switches,
    }))
}

// Utility method called by `explore`. Not a grpc handler.
// TODO(chet): Remove dead_code once wired up with the explorer.
pub(crate) async fn query(
    api: &Api,
    mac: MacAddress,
) -> Result<Option<model::expected_switch::ExpectedSwitch>, CarbideError> {
    let mut txn = api.database_connection.begin().await.map_err(|e| {
        CarbideError::from(DatabaseError::new("begin find_many_by_bmc_mac_address", e))
    })?;

    let mut expected = db_expected_switch::find_many_by_bmc_mac_address(&mut txn, &[mac]).await?;

    txn.commit().await.map_err(|e| {
        CarbideError::from(DatabaseError::new("commit find_many_by_bmc_mac_address", e))
    })?;

    Ok(expected.remove(&mac))
}
