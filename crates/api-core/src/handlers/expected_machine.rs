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
use ::rpc::forge as rpc;
use lazy_static::lazy_static;
use mac_address::MacAddress;
use model::expected_machine::{
    ExpectedHostNic, ExpectedMachine, ExpectedMachineData, ExpectedMachineRequest,
};
use model::machine_interface::InterfaceType;
use regex::Regex;
use uuid::Uuid;

use crate::CarbideError;
use crate::api::{Api, log_request_data};
use crate::handlers::machine_interface_address::update_preallocated_machine_interface;

lazy_static! {
    // Verify what serial is alphanumeric string with, allows dashes '-' and underscores '_'
    static ref CHASSIS_SERIAL_REGEX: Regex = Regex::new(r"^[A-Za-z0-9_-]{4,64}$").unwrap();
}

/// Returns one expected machine by database id or BMC MAC (from `ExpectedMachineRequest`).
pub(crate) async fn get(
    api: &Api,
    request: tonic::Request<rpc::ExpectedMachineRequest>,
) -> Result<tonic::Response<rpc::ExpectedMachine>, tonic::Status> {
    log_request_data(&request);

    let req: ExpectedMachineRequest = request
        .into_inner()
        .try_into()
        .map_err(|e| CarbideError::InvalidArgument(format!("{}", e)))?;

    let target_id = req
        .id
        .map(|u| u.to_string())
        .or(req.bmc_mac_address.map(|m| m.to_string()))
        .unwrap_or_default();

    let expected_machine = db::expected_machine::find(&api.database_connection, &req)
        .await
        .map_err(CarbideError::from)?
        .ok_or(CarbideError::NotFoundError {
            kind: "expected_machine",
            id: target_id,
        })?;

    let response = rpc::ExpectedMachine::from(expected_machine);
    Ok(tonic::Response::new(response))
}

/// Adds an expected machine.
pub(crate) async fn add(
    api: &Api,
    request: tonic::Request<rpc::ExpectedMachine>,
) -> Result<tonic::Response<()>, tonic::Status> {
    log_request_data(&request);

    let machine = parse_expected_machine(request.into_inner())?;

    let mut txn = api.txn_begin().await?;
    db::expected_machine::lock_config_mutations_shared(&mut txn).await?;
    db::expected_machine::lock_identity_macs(&mut txn, identity_macs(&machine)).await?;
    db::expected_machine::validate_identity_macs_available(&mut txn, &machine, None).await?;
    let reservation_macs = machine
        .data
        .host_nics
        .iter()
        .map(|interface| interface.mac_address)
        .chain(std::iter::once(machine.bmc_mac_address))
        .collect::<Vec<_>>();
    db::machine_interface::lock_expected_machine_interface_macs(&mut txn, reservation_macs).await?;
    db::expected_machine::create(&mut txn, machine).await?;

    txn.commit().await?;

    Ok(tonic::Response::new(()))
}

fn parse_expected_machine(request: rpc::ExpectedMachine) -> Result<ExpectedMachine, CarbideError> {
    if carbide_utils::has_duplicates(&request.fallback_dpu_serial_numbers) {
        return Err(CarbideError::InvalidArgument(
            "duplicate dpu serial number found".to_string(),
        ));
    }

    if !CHASSIS_SERIAL_REGEX.is_match(&request.chassis_serial_number) {
        return Err(CarbideError::InvalidArgument(format!(
            "chassis serial is not formatted properly {}",
            request.chassis_serial_number
        )));
    }

    let parsed_mac: MacAddress = request
        .bmc_mac_address
        .parse::<MacAddress>()
        .map_err(CarbideError::from)?;

    let id = request
        .id
        .as_ref()
        .map(|u| {
            Uuid::parse_str(&u.value).map_err(|_| {
                CarbideError::InvalidArgument("invalid expected_machine id".to_string())
            })
        })
        .transpose()?;
    let db_data: ExpectedMachineData = request.try_into()?;

    let machine = ExpectedMachine {
        id,
        bmc_mac_address: parsed_mac,
        data: db_data,
    };

    validate_expected_machine(&machine)?;
    Ok(machine)
}

/// Validate an ExpectedMachine before an API or configuration-import write.
pub(crate) fn validate_expected_machine(machine: &ExpectedMachine) -> Result<(), CarbideError> {
    validate_expected_interfaces(machine.bmc_mac_address, &machine.data.host_nics)?;
    machine
        .data
        .bmc_ip_allocation
        .validate(machine.data.bmc_ip_address.is_some())
        .map_err(|msg| CarbideError::InvalidArgument(msg.to_string()))?;

    Ok(())
}

/// Create missing expected_machines that aren't already in the database,
/// calling `validate_expected_machine` for each new entry. This is currently
/// purely used by the expected_machines.json import path only, but lives
/// here so it can re-leverage `validate_expected_machine` and share the
/// same validation codepath as the API handler. The set-wide exclusive lock
/// covers both the initial read and inserts so concurrent API startups remain
/// idempotent.
pub(crate) async fn create_missing_from(
    txn: &mut sqlx::PgConnection,
    expected_machines: &[ExpectedMachine],
) -> Result<(), CarbideError> {
    db::expected_machine::lock_config_mutations_exclusive(&mut *txn).await?;
    let existing_macs: std::collections::HashSet<String> =
        db::expected_machine::find_all(&mut *txn)
            .await?
            .into_iter()
            .map(|m| m.bmc_mac_address.to_string())
            .collect();

    let mut missing = Vec::new();
    for expected_machine in expected_machines {
        if existing_macs.contains(&expected_machine.bmc_mac_address.to_string()) {
            tracing::debug!(
                bmc_mac_address = %expected_machine.bmc_mac_address,
                "Expected machine already exists; not overwriting",
            );
            continue;
        }
        validate_expected_machine(expected_machine)?;
        missing.push(expected_machine);
    }

    db::expected_machine::lock_identity_macs(
        &mut *txn,
        missing.iter().flat_map(|machine| identity_macs(machine)),
    )
    .await?;
    let reservation_macs = missing
        .iter()
        .flat_map(|machine| {
            machine
                .data
                .host_nics
                .iter()
                .map(|interface| interface.mac_address)
                .chain(std::iter::once(machine.bmc_mac_address))
        })
        .collect::<Vec<_>>();
    db::machine_interface::lock_expected_machine_interface_macs(&mut *txn, reservation_macs)
        .await?;
    for expected_machine in missing {
        db::expected_machine::validate_identity_macs_available(&mut *txn, expected_machine, None)
            .await?;
        db::expected_machine::create(&mut *txn, expected_machine.clone()).await?;
    }

    Ok(())
}

/// Deletes an expected machine by id or BMC MAC.
pub(crate) async fn delete(
    api: &Api,
    request: tonic::Request<rpc::ExpectedMachineRequest>,
) -> Result<tonic::Response<()>, tonic::Status> {
    log_request_data(&request);

    let req: ExpectedMachineRequest = request
        .into_inner()
        .try_into()
        .map_err(|e| CarbideError::InvalidArgument(format!("{}", e)))?;

    let mut txn = api.txn_begin().await?;
    db::expected_machine::lock_config_mutations_shared(&mut txn).await?;

    let existing_machines = db::expected_machine::find_all_for_update(&mut txn).await?;
    let existing = existing_machines.iter().find(|machine| {
        req.id
            .map(|id| machine.id == Some(id))
            .or_else(|| {
                req.bmc_mac_address
                    .map(|mac_address| machine.bmc_mac_address == mac_address)
            })
            .unwrap_or(false)
    });
    if let Some(existing) = existing {
        let reservation_macs = existing
            .data
            .host_nics
            .iter()
            .map(|interface| interface.mac_address)
            .chain(std::iter::once(existing.bmc_mac_address))
            .collect::<Vec<_>>();
        db::machine_interface::lock_expected_machine_interface_macs(&mut txn, reservation_macs)
            .await?;
        lock_configured_fixed_allocations(&mut txn, &[existing]).await?;
        let retained_reservations = fixed_reservations_from_models(
            existing_machines
                .iter()
                .filter(|machine| machine.bmc_mac_address != existing.bmc_mac_address),
        );
        let retained_interface_macs = existing_machines
            .iter()
            .filter(|machine| machine.bmc_mac_address != existing.bmc_mac_address)
            .flat_map(|machine| &machine.data.host_nics)
            .map(|interface| interface.mac_address)
            .collect();
        release_removed_fixed_reservations(
            &mut txn,
            std::slice::from_ref(existing),
            &retained_reservations,
            &retained_interface_macs,
            &std::collections::HashSet::new(),
        )
        .await?;
    }

    db::expected_machine::delete(&mut txn, &req)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await?;

    Ok(tonic::Response::new(()))
}

/// Updates an expected machine, preserving stored roles omitted by older
/// clients and materializing fixed reservations for interfaces that have not
/// joined an ingested entity. Managed interface state remains operator-owned.
pub(crate) async fn update(
    api: &Api,
    request: tonic::Request<rpc::ExpectedMachine>,
) -> Result<tonic::Response<()>, tonic::Status> {
    log_request_data(&request);

    let request = request.into_inner();
    let unspecified_interface_fields = unspecified_interface_fields(&request);
    if carbide_utils::has_duplicates(&request.fallback_dpu_serial_numbers) {
        return Err(
            CarbideError::InvalidArgument("duplicate dpu serial number found".to_string()).into(),
        );
    }
    // Save fields needed later before moving `request` into data conversion
    let id = request
        .id
        .as_ref()
        .map(|u| {
            Uuid::parse_str(&u.value).map_err(|_| {
                CarbideError::InvalidArgument("invalid expected_machine id".to_string())
            })
        })
        .transpose()?;
    let parsed_mac: MacAddress = request
        .bmc_mac_address
        .parse::<MacAddress>()
        .map_err(CarbideError::from)?;
    let data: ExpectedMachineData = request.try_into()?;

    let mut machine = ExpectedMachine {
        id,
        bmc_mac_address: parsed_mac,
        data,
    };

    let mut txn = api.txn_begin().await?;
    db::expected_machine::lock_config_mutations_shared(&mut txn).await?;
    let existing = db::expected_machine::find_for_update(
        &mut txn,
        &ExpectedMachineRequest {
            id: machine.id,
            bmc_mac_address: Some(machine.bmc_mac_address),
        },
    )
    .await?;
    reject_bmc_mac_change(existing.as_ref(), machine.bmc_mac_address)?;
    preserve_unspecified_interface_fields(
        &mut machine,
        &unspecified_interface_fields,
        existing.as_ref(),
    );
    validate_expected_machine(&machine)?;
    let locked_identity_macs = existing
        .iter()
        .flat_map(identity_macs)
        .chain(identity_macs(&machine))
        .collect::<Vec<_>>();
    db::expected_machine::lock_identity_macs(&mut txn, locked_identity_macs).await?;
    if existing
        .as_ref()
        .is_none_or(|existing| identity_macs(existing) != identity_macs(&machine))
    {
        db::expected_machine::validate_identity_macs_available(
            &mut txn,
            &machine,
            existing.as_ref().map(|machine| machine.bmc_mac_address),
        )
        .await?;
    }
    reconcile_configured_interfaces(
        &mut txn,
        existing.as_ref(),
        &machine,
        api.runtime_config.retained_boot_interface_window,
    )
    .await?;

    db::expected_machine::update(&mut txn, &machine)
        .await
        .map_err(CarbideError::from)?;

    txn.commit().await?;

    Ok(tonic::Response::new(()))
}

fn find_previous_expected_machine<'a>(
    previous: &'a [ExpectedMachine],
    replacement: &ExpectedMachine,
) -> Option<&'a ExpectedMachine> {
    replacement
        .id
        .and_then(|id| previous.iter().find(|machine| machine.id == Some(id)))
        .or_else(|| {
            previous
                .iter()
                .find(|machine| machine.bmc_mac_address == replacement.bmc_mac_address)
        })
}

/// Atomically replace all ExpectedMachines. New identities keep `add`'s
/// deferred interface materialization, while changed or transferred
/// declarations are reconciled before commit.
pub(crate) async fn replace_all(
    api: &Api,
    request: tonic::Request<rpc::ExpectedMachineList>,
) -> Result<tonic::Response<()>, tonic::Status> {
    log_request_data(&request);
    let request = request.into_inner();
    let mut txn = api.txn_begin().await?;
    db::expected_machine::lock_config_mutations_exclusive(&mut txn).await?;
    let previous = db::expected_machine::find_all_for_update(&mut txn).await?;

    let mut replacements = Vec::with_capacity(request.expected_machines.len());
    let mut changed_identity_machines = std::collections::HashSet::new();
    for requested in request.expected_machines {
        let unspecified_fields = unspecified_interface_fields(&requested);
        let mut replacement = parse_expected_machine(requested)?;
        let existing = find_previous_expected_machine(&previous, &replacement);
        preserve_unspecified_interface_fields(&mut replacement, &unspecified_fields, existing);
        validate_expected_machine(&replacement)?;
        if existing.is_none_or(|existing| identity_macs(existing) != identity_macs(&replacement)) {
            changed_identity_machines.insert(replacement.bmc_mac_address);
        }
        replacements.push(replacement);
    }
    validate_changed_identity_macs_unique(&replacements, &changed_identity_machines)?;
    let reconciled_replacement_macs = replacements
        .iter()
        .filter(|replacement| {
            let existing = find_previous_expected_machine(&previous, replacement);
            let declaration_changed = existing.is_some_and(|existing| {
                !expected_interface_declarations_equal(
                    &existing.data.host_nics,
                    &replacement.data.host_nics,
                )
            });
            let owner_changed = replacement.data.host_nics.iter().any(|interface| {
                let previous_owners = expected_interface_owners(&previous, interface.mac_address);
                !previous_owners.is_empty()
                    && previous_owners
                        != expected_interface_owners(&replacements, interface.mac_address)
            });
            declaration_changed || owner_changed
        })
        .map(|replacement| replacement.bmc_mac_address)
        .collect::<std::collections::HashSet<_>>();

    let retained_reservations = fixed_reservations_from_models(&replacements);
    let mut same_owner_transition_macs = std::collections::HashSet::new();
    for replacement in &replacements {
        let Some(existing) = find_previous_expected_machine(&previous, replacement) else {
            continue;
        };
        same_owner_transition_macs.extend(
            existing
                .data
                .host_nics
                .iter()
                .filter(|interface| interface.fixed_ip.is_some())
                .filter(|interface| {
                    replacement
                        .data
                        .host_nics
                        .iter()
                        .any(|candidate| candidate.mac_address == interface.mac_address)
                })
                .map(|interface| interface.mac_address),
        );
    }
    let transitioned_macs = replacements
        .iter()
        .flat_map(|machine| &machine.data.host_nics)
        .map(|interface| interface.mac_address)
        .collect();
    let locked_identity_macs = previous
        .iter()
        .chain(&replacements)
        .flat_map(identity_macs)
        .collect::<Vec<_>>();
    let replacement_interface_macs = previous
        .iter()
        .chain(&replacements)
        .flat_map(|machine| {
            machine
                .data
                .host_nics
                .iter()
                .map(|interface| interface.mac_address)
                .chain(std::iter::once(machine.bmc_mac_address))
        })
        .collect::<Vec<_>>();

    db::expected_machine::lock_identity_macs(&mut txn, locked_identity_macs).await?;
    db::machine_interface::lock_expected_machine_interface_macs(
        &mut txn,
        replacement_interface_macs,
    )
    .await?;
    let configured_machines = previous.iter().chain(&replacements).collect::<Vec<_>>();
    lock_configured_fixed_allocations(&mut txn, &configured_machines).await?;
    release_removed_fixed_reservations(
        &mut txn,
        &previous,
        &retained_reservations,
        &transitioned_macs,
        &same_owner_transition_macs,
    )
    .await?;
    db::expected_machine::clear(&mut txn).await?;
    for replacement in &replacements {
        db::expected_machine::create(&mut txn, replacement.clone()).await?;
    }

    // Existing ExpectedMachines and declarations transferred between them have
    // update behavior for anonymous interfaces. Apply role, segment guard,
    // fixed reservation, and Retained transitions now, while replacements with
    // only new identities retain Add's deferred materialization behavior.
    let mut reconciled_replacements = replacements
        .iter()
        .filter(|replacement| reconciled_replacement_macs.contains(&replacement.bmc_mac_address))
        .collect::<Vec<_>>();
    reconciled_replacements.sort_by_key(|replacement| replacement.bmc_mac_address.to_string());
    for replacement in reconciled_replacements {
        apply_configured_interface_reservations(
            &mut txn,
            replacement,
            api.runtime_config.retained_boot_interface_window,
        )
        .await?;
    }

    txn.commit().await?;

    Ok(tonic::Response::new(()))
}

/// Lists all expected machines (includes configured `bmc_ip_address` when set).
pub(crate) async fn get_all(
    api: &Api,
    request: tonic::Request<()>,
) -> Result<tonic::Response<rpc::ExpectedMachineList>, tonic::Status> {
    log_request_data(&request);

    let expected_machine_list: Vec<ExpectedMachine> =
        db::expected_machine::find_all(&api.database_connection).await?;

    Ok(tonic::Response::new(rpc::ExpectedMachineList {
        expected_machines: expected_machine_list.into_iter().map(Into::into).collect(),
    }))
}

/// Lists expected machines joined to explored interfaces / machines (linkage view).
pub(crate) async fn get_linked(
    api: &Api,
    request: tonic::Request<()>,
) -> Result<tonic::Response<rpc::LinkedExpectedMachineList>, tonic::Status> {
    log_request_data(&request);

    let out = db::expected_machine::find_all_linked(&api.database_connection).await?;
    let list = rpc::LinkedExpectedMachineList {
        expected_machines: out.into_iter().map(|m| m.into()).collect(),
    };
    Ok(tonic::Response::new(list))
}

/// Lists host BMC endpoints that Site Explorer has explored but whose MAC is
/// not listed in any of `expected_machines`, `expected_power_shelf`, or
/// `expected_switch`. DPUs, power shelves, and switches are filtered out so the
/// response only contains actual host BMCs.
///
/// An entry with a non-null `machine_id` is an orphan: the host was ingested
/// before its `expected_machines` row was removed.
pub(crate) async fn get_all_unexpected_machines(
    api: &Api,
    request: tonic::Request<()>,
) -> Result<tonic::Response<rpc::UnexpectedMachineList>, tonic::Status> {
    log_request_data(&request);

    let out = db::expected_machine::find_all_unexpected(&api.database_connection).await?;
    let list = rpc::UnexpectedMachineList {
        unexpected_machines: out.into_iter().map(Into::into).collect(),
    };
    Ok(tonic::Response::new(list))
}

/// Deletes every expected machine row.
pub(crate) async fn delete_all(
    api: &Api,
    request: tonic::Request<()>,
) -> Result<tonic::Response<()>, tonic::Status> {
    log_request_data(&request);

    let mut txn = api.txn_begin().await?;
    db::expected_machine::lock_config_mutations_exclusive(&mut txn).await?;

    let previous = db::expected_machine::find_all_for_update(&mut txn).await?;
    let reservation_macs = previous
        .iter()
        .flat_map(|machine| {
            machine
                .data
                .host_nics
                .iter()
                .map(|interface| interface.mac_address)
                .chain(std::iter::once(machine.bmc_mac_address))
        })
        .collect::<Vec<_>>();
    db::machine_interface::lock_expected_machine_interface_macs(&mut txn, reservation_macs).await?;
    let configured_machines = previous.iter().collect::<Vec<_>>();
    lock_configured_fixed_allocations(&mut txn, &configured_machines).await?;
    release_removed_fixed_reservations(
        &mut txn,
        &previous,
        &std::collections::HashSet::new(),
        &std::collections::HashSet::new(),
        &std::collections::HashSet::new(),
    )
    .await?;
    db::expected_machine::clear(&mut txn).await?;

    txn.commit().await?;

    Ok(tonic::Response::new(()))
}

/// Reject invalid expected-interface declarations: duplicate MACs or fixed IPs,
/// non-host primary declarations, and more than one host primary.
fn validate_expected_interfaces(
    host_bmc_mac: MacAddress,
    host_nics: &[ExpectedHostNic],
) -> Result<(), CarbideError> {
    let mut seen_macs = std::collections::HashSet::new();
    let mut seen_fixed_ips = std::collections::HashSet::new();
    for interface in host_nics {
        if interface.mac_address == host_bmc_mac {
            return Err(CarbideError::InvalidArgument(format!(
                "host_nics MAC address {} duplicates the expected machine BMC MAC address",
                interface.mac_address,
            )));
        }
        if !seen_macs.insert(interface.mac_address) {
            return Err(CarbideError::InvalidArgument(format!(
                "host_nics contains duplicate MAC address {}",
                interface.mac_address,
            )));
        }
        interface.validate_ip_allocation().map_err(|message| {
            CarbideError::InvalidArgument(format!(
                "host_nics interface {}: {message}",
                interface.mac_address,
            ))
        })?;
        if let Some(fixed_ip) = interface.fixed_ip
            && !seen_fixed_ips.insert(fixed_ip)
        {
            return Err(CarbideError::InvalidArgument(format!(
                "host_nics contains duplicate fixed_ip {fixed_ip}",
            )));
        }
        if interface.primary == Some(true) && !interface.role.is_host() {
            return Err(CarbideError::InvalidArgument(format!(
                "only a role=host interface may be flagged primary=true; {} has role {:?}",
                interface.mac_address, interface.role,
            )));
        }
    }

    let primaries: Vec<_> = host_nics
        .iter()
        .filter(|n| n.primary == Some(true))
        .map(|n| n.mac_address.to_string())
        .collect();
    if primaries.len() > 1 {
        return Err(CarbideError::InvalidArgument(format!(
            "at most one host_nic may be flagged primary=true, got {}: {}",
            primaries.len(),
            primaries.join(", ")
        )));
    }
    Ok(())
}

#[derive(Default)]
struct UnspecifiedInterfaceFields {
    roles: std::collections::HashSet<MacAddress>,
    ip_allocations: std::collections::HashSet<MacAddress>,
}

fn unspecified_interface_fields(machine: &rpc::ExpectedMachine) -> UnspecifiedInterfaceFields {
    let mut fields = UnspecifiedInterfaceFields::default();
    for interface in &machine.host_nics {
        let Ok(mac_address) = interface.mac_address.parse() else {
            continue;
        };
        if interface
            .role
            .is_none_or(|role| role == rpc::ExpectedInterfaceRole::Unspecified as i32)
        {
            fields.roles.insert(mac_address);
        }
        if interface.ip_allocation.is_none_or(|allocation| {
            allocation == rpc::ExpectedInterfaceIpAllocation::Unspecified as i32
        }) {
            fields.ip_allocations.insert(mac_address);
        }
    }
    fields
}

fn preserve_unspecified_interface_fields(
    machine: &mut ExpectedMachine,
    unspecified_fields: &UnspecifiedInterfaceFields,
    existing: Option<&ExpectedMachine>,
) {
    if unspecified_fields.roles.is_empty() && unspecified_fields.ip_allocations.is_empty() {
        return;
    }

    let Some(existing) = existing else {
        return;
    };

    for interface in &mut machine.data.host_nics {
        let Some(existing_interface) = existing
            .data
            .host_nics
            .iter()
            .find(|candidate| candidate.mac_address == interface.mac_address)
        else {
            // A new interface has no stored value to preserve. Missing fields
            // retain their normal inference/default behavior.
            continue;
        };
        if unspecified_fields.roles.contains(&interface.mac_address) {
            interface.role = existing_interface.role;
        }
        if unspecified_fields
            .ip_allocations
            .contains(&interface.mac_address)
            // An old client still controls legacy fixed/dynamic intent through
            // fixed_ip. Preserve a newer explicit policy only while that
            // legacy signal remains unchanged.
            && existing_interface.fixed_ip.is_some() == interface.fixed_ip.is_some()
            && existing_interface.ip_allocation.is_some()
        {
            interface.ip_allocation = existing_interface.ip_allocation;
        }
    }
}

fn fixed_reservations_from_models<'a>(
    machines: impl IntoIterator<Item = &'a ExpectedMachine>,
) -> std::collections::HashSet<(MacAddress, std::net::IpAddr)> {
    machines
        .into_iter()
        .flat_map(|machine| &machine.data.host_nics)
        .filter_map(|interface| {
            interface
                .fixed_ip
                .map(|fixed_ip| (interface.mac_address, fixed_ip))
        })
        .collect()
}

fn expected_interface_declarations_equal(
    previous: &[ExpectedHostNic],
    replacement: &[ExpectedHostNic],
) -> bool {
    if previous.len() != replacement.len() {
        return false;
    }

    let mut matched = vec![false; replacement.len()];
    for previous_interface in previous {
        let Some((index, _)) =
            replacement
                .iter()
                .enumerate()
                .find(|(index, replacement_interface)| {
                    !matched[*index] && previous_interface == *replacement_interface
                })
        else {
            return false;
        };
        matched[index] = true;
    }
    true
}

fn expected_interface_owners(
    machines: &[ExpectedMachine],
    mac_address: MacAddress,
) -> std::collections::HashSet<MacAddress> {
    machines
        .iter()
        .filter(|machine| {
            machine
                .data
                .host_nics
                .iter()
                .any(|interface| interface.mac_address == mac_address)
        })
        .map(|machine| machine.bmc_mac_address)
        .collect()
}

fn identity_macs(machine: &ExpectedMachine) -> std::collections::HashSet<MacAddress> {
    machine
        .data
        .host_nics
        .iter()
        .map(|interface| interface.mac_address)
        .chain(std::iter::once(machine.bmc_mac_address))
        .collect()
}

fn validate_identity_macs_unique(machines: &[ExpectedMachine]) -> Result<(), CarbideError> {
    let mut owners = std::collections::HashMap::new();
    for machine in machines {
        for mac_address in identity_macs(machine) {
            if let Some(conflicting_bmc_mac) = owners.insert(mac_address, machine.bmc_mac_address) {
                return Err(CarbideError::InvalidArgument(format!(
                    "expected machine identity MAC {mac_address} conflicts with the machine whose BMC MAC is {conflicting_bmc_mac}",
                )));
            }
        }
    }
    Ok(())
}

fn validate_changed_identity_macs_unique(
    machines: &[ExpectedMachine],
    changed_machines: &std::collections::HashSet<MacAddress>,
) -> Result<(), CarbideError> {
    let mut changed_owners = std::collections::HashMap::new();
    for machine in machines
        .iter()
        .filter(|machine| changed_machines.contains(&machine.bmc_mac_address))
    {
        for mac_address in identity_macs(machine) {
            if let Some(conflicting_bmc_mac) =
                changed_owners.insert(mac_address, machine.bmc_mac_address)
            {
                return Err(CarbideError::InvalidArgument(format!(
                    "expected machine identity MAC {mac_address} conflicts with the machine whose BMC MAC is {conflicting_bmc_mac}",
                )));
            }
        }
    }

    for machine in machines
        .iter()
        .filter(|machine| !changed_machines.contains(&machine.bmc_mac_address))
    {
        for mac_address in identity_macs(machine) {
            if changed_owners.contains_key(&mac_address) {
                return Err(CarbideError::InvalidArgument(format!(
                    "expected machine identity MAC {mac_address} conflicts with the machine whose BMC MAC is {}",
                    machine.bmc_mac_address,
                )));
            }
        }
    }

    Ok(())
}

fn reject_bmc_mac_change(
    existing: Option<&ExpectedMachine>,
    requested_bmc_mac: MacAddress,
) -> Result<(), CarbideError> {
    if let Some(existing) = existing
        && existing.bmc_mac_address != requested_bmc_mac
    {
        return Err(CarbideError::InvalidArgument(format!(
            "expected machine BMC MAC address cannot be changed from {} to {requested_bmc_mac}",
            existing.bmc_mac_address,
        )));
    }
    Ok(())
}

async fn lock_configured_fixed_allocations(
    txn: &mut sqlx::PgConnection,
    machines: &[&ExpectedMachine],
) -> Result<(), CarbideError> {
    let mut requested_allocations = Vec::new();
    for machine in machines {
        if let Some(address) = machine.data.bmc_ip_address {
            requested_allocations.push(address);
        }
        requested_allocations.extend(
            machine
                .data
                .host_nics
                .iter()
                .filter_map(|interface| interface.fixed_ip),
        );
    }

    let mut allocations = Vec::with_capacity(requested_allocations.len());
    for address in requested_allocations {
        let segment = db::network_segment::for_static_address(txn, address, None).await?;
        allocations.push((segment.id, address));
    }
    db::machine_interface::lock_static_address_allocations(txn, &allocations).await?;
    Ok(())
}

async fn release_removed_fixed_reservations(
    txn: &mut sqlx::PgConnection,
    previous: &[ExpectedMachine],
    retained: &std::collections::HashSet<(MacAddress, std::net::IpAddr)>,
    transitioned_macs: &std::collections::HashSet<MacAddress>,
    same_owner_transition_macs: &std::collections::HashSet<MacAddress>,
) -> Result<(), CarbideError> {
    let mut removed = previous
        .iter()
        .flat_map(|machine| &machine.data.host_nics)
        .filter_map(|interface| {
            interface
                .fixed_ip
                .map(|fixed_ip| (interface.mac_address, fixed_ip))
        })
        .filter(|reservation| !retained.contains(reservation))
        .collect::<Vec<_>>();
    removed.sort_by_key(|(mac_address, fixed_ip)| (mac_address.to_string(), *fixed_ip));
    removed.dedup();

    for (mac_address, fixed_ip) in removed {
        if same_owner_transition_macs.contains(&mac_address) {
            db::machine_interface::remove_expected_machine_interface_preallocation_if_never_associated(
                txn,
                mac_address,
                fixed_ip,
            )
            .await?;
        } else if transitioned_macs.contains(&mac_address) {
            db::machine_interface::release_expected_machine_interface_preallocation_or_legacy_if_never_associated(
                txn,
                mac_address,
                fixed_ip,
            )
            .await?;
        } else {
            db::machine_interface::release_expected_machine_interface_preallocation_if_never_associated(
                txn,
                mac_address,
                fixed_ip,
            )
            .await?;
        }
    }

    clear_removed_expected_interface_snapshots(txn, previous, transitioned_macs).await?;

    Ok(())
}

async fn clear_removed_expected_interface_snapshots(
    txn: &mut sqlx::PgConnection,
    previous: &[ExpectedMachine],
    retained_macs: &std::collections::HashSet<MacAddress>,
) -> Result<(), CarbideError> {
    let mut removed_macs = previous
        .iter()
        .flat_map(|machine| &machine.data.host_nics)
        .map(|interface| interface.mac_address)
        .filter(|mac_address| !retained_macs.contains(mac_address))
        .collect::<Vec<_>>();
    removed_macs.sort_by_key(ToString::to_string);
    removed_macs.dedup();
    for mac_address in removed_macs {
        db::machine_interface::clear_expected_interface_if_never_associated(txn, mac_address)
            .await?;
    }
    Ok(())
}

async fn reconcile_configured_interfaces(
    txn: &mut sqlx::PgConnection,
    previous: Option<&ExpectedMachine>,
    machine: &ExpectedMachine,
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    // Lock top-level BMCs with nested declarations so tolerated legacy
    // cross-identity MACs cannot invert the order during BMC preallocation.
    let interface_macs = previous
        .into_iter()
        .flat_map(|machine| &machine.data.host_nics)
        .chain(&machine.data.host_nics)
        .map(|interface| interface.mac_address)
        .chain(previous.into_iter().map(|machine| machine.bmc_mac_address))
        .chain(std::iter::once(machine.bmc_mac_address))
        .collect::<Vec<_>>();
    db::machine_interface::lock_expected_machine_interface_macs(txn, interface_macs).await?;
    let configured_machines = previous
        .into_iter()
        .chain(std::iter::once(machine))
        .collect::<Vec<_>>();
    lock_configured_fixed_allocations(txn, &configured_machines).await?;

    if let Some(previous) = previous {
        let mut previous_fixed_interfaces = previous
            .data
            .host_nics
            .iter()
            .filter_map(|interface| interface.fixed_ip.map(|ip| (interface.mac_address, ip)))
            .collect::<Vec<_>>();
        previous_fixed_interfaces.sort_by_key(|(mac_address, _)| mac_address.to_string());
        for (mac_address, old_fixed_ip) in previous_fixed_interfaces {
            let new_interface = machine
                .data
                .host_nics
                .iter()
                .find(|interface| interface.mac_address == mac_address);
            match new_interface {
                Some(interface) if interface.fixed_ip != Some(old_fixed_ip) => {
                    db::machine_interface::remove_expected_machine_interface_preallocation_if_never_associated(
                        txn,
                        mac_address,
                        old_fixed_ip,
                    )
                    .await?;
                }
                None => {
                    db::machine_interface::release_expected_machine_interface_preallocation_if_never_associated(
                        txn,
                        mac_address,
                        old_fixed_ip,
                    )
                    .await?;
                }
                _ => {}
            }
        }

        let retained_macs = machine
            .data
            .host_nics
            .iter()
            .map(|interface| interface.mac_address)
            .collect();
        clear_removed_expected_interface_snapshots(
            txn,
            std::slice::from_ref(previous),
            &retained_macs,
        )
        .await?;
    }

    apply_configured_interface_reservations(txn, machine, retained_window).await
}

async fn apply_configured_interface_reservations(
    txn: &mut sqlx::PgConnection,
    machine: &ExpectedMachine,
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    if let Some(bmc_ip) = machine.data.bmc_ip_address {
        update_preallocated_machine_interface(
            txn,
            machine.bmc_mac_address,
            bmc_ip,
            InterfaceType::Bmc,
            retained_window,
        )
        .await?;
    }

    let mut interfaces = machine
        .data
        .host_nics
        .iter()
        .map(|interface| {
            machine
                .data
                .expected_interface_for_mac(interface.mac_address)
                .expect("an ExpectedMachine interface must resolve by its own MAC")
        })
        .collect::<Vec<_>>();
    interfaces.sort_by_key(|interface| interface.mac_address.to_string());
    for interface in &interfaces {
        db::machine_interface::preallocate_expected_machine_interface_if_never_associated(
            txn,
            interface,
            retained_window,
        )
        .await?;
    }
    Ok(())
}

/// Helper function to sanitize expected machine and return parsed IDs (ID+MAC)
fn sanitize_expected_machine_and_get_ids(
    _api: &Api,
    request: rpc::ExpectedMachine,
    _is_update: bool,
) -> Result<(Uuid, MacAddress), CarbideError> {
    // Validate id is present
    let id = match &request.id {
        Some(uuid_val) => Uuid::parse_str(&uuid_val.value).map_err(|_| {
            CarbideError::InvalidArgument("invalid expected_machine id".to_string())
        })?,
        None => {
            return Err(CarbideError::InvalidArgument(
                "id is mandatory for batch operations".to_string(),
            ));
        }
    };

    // Validate bmc_mac_address is present and parseable
    if request.bmc_mac_address.is_empty() {
        return Err(CarbideError::InvalidArgument(
            "bmc_mac_address is mandatory".to_string(),
        ));
    }

    let parsed_mac: MacAddress = request
        .bmc_mac_address
        .parse::<MacAddress>()
        .map_err(CarbideError::from)?;

    // Validate duplicates in fallback DPU serial numbers
    if carbide_utils::has_duplicates(&request.fallback_dpu_serial_numbers) {
        return Err(CarbideError::InvalidArgument(
            "duplicate dpu serial number found".to_string(),
        ));
    }

    // Validate chassis serial format
    if !CHASSIS_SERIAL_REGEX.is_match(&request.chassis_serial_number) {
        return Err(CarbideError::InvalidArgument(format!(
            "chassis serial is not formatted properly {}",
            request.chassis_serial_number
        )));
    }

    Ok((id, parsed_mac))
}

/// Validates and creates one expected machine inside an existing transaction.
async fn create_expected_machine(
    txn: &mut sqlx::PgConnection,
    machine: rpc::ExpectedMachine,
    id: Uuid,
    parsed_mac: MacAddress,
) -> Result<(), CarbideError> {
    create_expected_machine_records(txn, &[(machine, id, parsed_mac)]).await
}

/// Updates one expected machine inside an existing transaction, including the
/// same configured-interface reconciliation as [`update`].
async fn update_expected_machine(
    txn: &mut sqlx::PgConnection,
    machine: rpc::ExpectedMachine,
    id: Uuid,
    parsed_mac: MacAddress,
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    let unspecified_interface_fields = unspecified_interface_fields(&machine);
    let data: ExpectedMachineData = machine.try_into()?;

    let mut expected_machine = ExpectedMachine {
        id: Some(id),
        bmc_mac_address: parsed_mac,
        data,
    };

    let existing = db::expected_machine::find_for_update(
        txn,
        &ExpectedMachineRequest {
            id: expected_machine.id,
            bmc_mac_address: Some(expected_machine.bmc_mac_address),
        },
    )
    .await?;
    reject_bmc_mac_change(existing.as_ref(), expected_machine.bmc_mac_address)?;
    preserve_unspecified_interface_fields(
        &mut expected_machine,
        &unspecified_interface_fields,
        existing.as_ref(),
    );
    validate_expected_machine(&expected_machine)?;
    let locked_identity_macs = existing
        .iter()
        .flat_map(identity_macs)
        .chain(identity_macs(&expected_machine))
        .collect::<Vec<_>>();
    db::expected_machine::lock_identity_macs(txn, locked_identity_macs).await?;
    if existing
        .as_ref()
        .is_none_or(|existing| identity_macs(existing) != identity_macs(&expected_machine))
    {
        db::expected_machine::validate_identity_macs_available(
            txn,
            &expected_machine,
            existing.as_ref().map(|machine| machine.bmc_mac_address),
        )
        .await?;
    }
    reconcile_configured_interfaces(txn, existing.as_ref(), &expected_machine, retained_window)
        .await?;

    db::expected_machine::update(txn, &expected_machine).await?;

    Ok(())
}

#[derive(Copy, Clone)]
enum BatchOperation {
    Create,
    Update,
}

impl BatchOperation {
    fn is_update(&self) -> bool {
        matches!(self, BatchOperation::Update)
    }
}

fn build_success_result(machine: rpc::ExpectedMachine) -> rpc::ExpectedMachineOperationResult {
    // Ensure the id is set in the returned machine payload.
    let id = machine
        .id
        .as_ref()
        .and_then(|u| Uuid::parse_str(&u.value).ok());

    rpc::ExpectedMachineOperationResult {
        id: id.map(|value| ::rpc::common::Uuid {
            value: value.to_string(),
        }),
        success: true,
        error_message: None,
        expected_machine: Some(machine),
    }
}

fn build_failure_result(id: Uuid, error_message: String) -> rpc::ExpectedMachineOperationResult {
    rpc::ExpectedMachineOperationResult {
        id: Some(::rpc::common::Uuid {
            value: id.to_string(),
        }),
        success: false,
        error_message: Some(error_message),
        expected_machine: None,
    }
}

async fn apply_operation(
    op: BatchOperation,
    txn: &mut sqlx::PgConnection,
    machine: rpc::ExpectedMachine,
    id: Uuid,
    parsed_mac: MacAddress,
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    match op {
        BatchOperation::Create => create_expected_machine(txn, machine, id, parsed_mac).await,
        BatchOperation::Update => {
            update_expected_machine(txn, machine, id, parsed_mac, retained_window).await
        }
    }
}

async fn create_expected_machine_records(
    txn: &mut sqlx::PgConnection,
    prepared: &[(rpc::ExpectedMachine, Uuid, MacAddress)],
) -> Result<(), CarbideError> {
    let mut models = Vec::with_capacity(prepared.len());
    for (machine, id, parsed_mac) in prepared {
        let expected_machine = ExpectedMachine {
            id: Some(*id),
            bmc_mac_address: *parsed_mac,
            data: ExpectedMachineData::try_from(machine.clone())?,
        };
        validate_expected_machine(&expected_machine)?;
        models.push(expected_machine);
    }
    validate_identity_macs_unique(&models)?;
    let locked_identity_macs = models.iter().flat_map(identity_macs).collect::<Vec<_>>();
    db::expected_machine::lock_identity_macs(&mut *txn, locked_identity_macs).await?;
    let reservation_macs = models
        .iter()
        .flat_map(|machine| {
            machine
                .data
                .host_nics
                .iter()
                .map(|interface| interface.mac_address)
                .chain(std::iter::once(machine.bmc_mac_address))
        })
        .collect::<Vec<_>>();
    db::machine_interface::lock_expected_machine_interface_macs(&mut *txn, reservation_macs)
        .await?;

    for expected_machine in models {
        db::expected_machine::validate_identity_macs_available(&mut *txn, &expected_machine, None)
            .await?;
        db::expected_machine::create(&mut *txn, expected_machine).await?;
    }
    Ok(())
}

async fn apply_atomic_batch_updates(
    txn: &mut sqlx::PgConnection,
    prepared: &[(rpc::ExpectedMachine, Uuid, MacAddress)],
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    let existing_machines = db::expected_machine::find_all_for_update(&mut *txn).await?;
    let mut updates = Vec::with_capacity(prepared.len());

    for (machine, id, parsed_mac) in prepared {
        let existing = existing_machines
            .iter()
            .find(|existing| existing.id == Some(*id))
            .ok_or_else(|| CarbideError::NotFoundError {
                kind: "expected_machine",
                id: id.to_string(),
            })?;
        reject_bmc_mac_change(Some(existing), *parsed_mac)?;

        let unspecified_fields = unspecified_interface_fields(machine);
        let mut expected_machine = ExpectedMachine {
            id: Some(*id),
            bmc_mac_address: *parsed_mac,
            data: ExpectedMachineData::try_from(machine.clone())?,
        };
        preserve_unspecified_interface_fields(
            &mut expected_machine,
            &unspecified_fields,
            Some(existing),
        );
        validate_expected_machine(&expected_machine)?;
        updates.push((existing.clone(), expected_machine));
    }

    let mut projected_machines = existing_machines;
    for (_, update) in &updates {
        let stored = projected_machines
            .iter_mut()
            .find(|machine| machine.id == update.id)
            .expect("each prepared update was matched above");
        *stored = update.clone();
    }
    let changed_identity_machines = updates
        .iter()
        .filter(|(previous, update)| identity_macs(previous) != identity_macs(update))
        .map(|(_, update)| update.bmc_mac_address)
        .collect::<std::collections::HashSet<_>>();
    validate_changed_identity_macs_unique(&projected_machines, &changed_identity_machines)?;

    let locked_identity_macs = updates
        .iter()
        .flat_map(|(previous, update)| {
            identity_macs(previous)
                .into_iter()
                .chain(identity_macs(update))
        })
        .collect::<Vec<_>>();
    db::expected_machine::lock_identity_macs(&mut *txn, locked_identity_macs).await?;

    let reservation_macs = updates
        .iter()
        .flat_map(|(previous, update)| {
            previous
                .data
                .host_nics
                .iter()
                .chain(&update.data.host_nics)
                .map(|interface| interface.mac_address)
                .chain([previous.bmc_mac_address, update.bmc_mac_address])
        })
        .collect::<Vec<_>>();
    db::machine_interface::lock_expected_machine_interface_macs(&mut *txn, reservation_macs)
        .await?;
    let configured_machines = updates
        .iter()
        .flat_map(|(previous, update)| [previous, update])
        .collect::<Vec<_>>();
    lock_configured_fixed_allocations(&mut *txn, &configured_machines).await?;

    let retained_reservations = fixed_reservations_from_models(&projected_machines);
    let transitioned_macs = projected_machines
        .iter()
        .flat_map(|machine| &machine.data.host_nics)
        .map(|interface| interface.mac_address)
        .collect();
    let same_owner_transition_macs = updates
        .iter()
        .flat_map(|(previous, update)| {
            previous
                .data
                .host_nics
                .iter()
                .filter(|interface| interface.fixed_ip.is_some())
                .filter(|interface| {
                    update
                        .data
                        .host_nics
                        .iter()
                        .any(|updated| updated.mac_address == interface.mac_address)
                })
                .map(|interface| interface.mac_address)
        })
        .collect();
    let previous_machines = updates
        .iter()
        .map(|(previous, _)| previous.clone())
        .collect::<Vec<_>>();
    release_removed_fixed_reservations(
        &mut *txn,
        &previous_machines,
        &retained_reservations,
        &transitioned_macs,
        &same_owner_transition_macs,
    )
    .await?;

    let mut updates_in_lock_order = updates.iter().map(|(_, update)| update).collect::<Vec<_>>();
    updates_in_lock_order.sort_by_key(|machine| machine.bmc_mac_address.to_string());
    for update in updates_in_lock_order {
        apply_configured_interface_reservations(&mut *txn, update, retained_window).await?;
    }

    for (_, update) in &updates {
        db::expected_machine::update(&mut *txn, update).await?;
    }
    for (_, update) in &updates {
        if changed_identity_machines.contains(&update.bmc_mac_address) {
            db::expected_machine::validate_identity_macs_available(
                &mut *txn,
                update,
                Some(update.bmc_mac_address),
            )
            .await?;
        }
    }

    Ok(())
}

async fn process_batch_operations(
    api: &Api,
    machines: Vec<rpc::ExpectedMachine>,
    accept_partial: bool,
    op: BatchOperation,
) -> Result<Vec<rpc::ExpectedMachineOperationResult>, CarbideError> {
    let mut results = Vec::new();

    if accept_partial {
        for machine in machines {
            let request_id = machine
                .id
                .as_ref()
                .and_then(|u| Uuid::parse_str(&u.value).ok())
                .unwrap_or_else(Uuid::nil);

            let (id, parsed_mac) =
                match sanitize_expected_machine_and_get_ids(api, machine.clone(), op.is_update()) {
                    Ok(ids) => ids,
                    Err(e) => {
                        results.push(build_failure_result(
                            request_id,
                            format!("Validation failed: {}", e),
                        ));
                        continue;
                    }
                };

            let mut machine_for_result = machine.clone();
            machine_for_result.id = Some(::rpc::common::Uuid {
                value: id.to_string(),
            });

            let mut txn = match api.txn_begin().await {
                Ok(txn) => txn,
                Err(e) => {
                    results.push(build_failure_result(
                        id,
                        format!("Failed to begin transaction: {}", e),
                    ));
                    continue;
                }
            };

            let operation_result =
                match db::expected_machine::lock_config_mutations_shared(txn.as_pgconn()).await {
                    Ok(()) => {
                        apply_operation(
                            op,
                            txn.as_pgconn(),
                            machine,
                            id,
                            parsed_mac,
                            api.runtime_config.retained_boot_interface_window,
                        )
                        .await
                    }
                    Err(error) => Err(error.into()),
                };

            match operation_result {
                Ok(_) => match txn.commit().await {
                    Ok(_) => results.push(build_success_result(machine_for_result)),
                    Err(e) => {
                        results.push(build_failure_result(id, format!("Failed to commit: {}", e)))
                    }
                },
                Err(e) => {
                    txn.rollback_or_log("expected-machine write after operation failure")
                        .await;
                    results.push(build_failure_result(id, format!("Operation failed: {}", e)));
                }
            }
        }

        return Ok(results);
    }

    let mut prepared = Vec::with_capacity(machines.len());
    let mut target_ids = std::collections::HashSet::new();
    for machine in machines {
        let (id, parsed_mac) =
            sanitize_expected_machine_and_get_ids(api, machine.clone(), op.is_update())?;
        if !target_ids.insert(id) {
            return Err(CarbideError::InvalidArgument(format!(
                "duplicate expected_machine id {id} in batch request",
            )));
        }
        prepared.push((machine, id, parsed_mac));
    }

    let mut txn = api.txn_begin().await?;
    let operation_result =
        match db::expected_machine::lock_config_mutations_shared(txn.as_pgconn()).await {
            Ok(()) => match op {
                BatchOperation::Create => {
                    create_expected_machine_records(txn.as_pgconn(), &prepared).await
                }
                BatchOperation::Update => {
                    apply_atomic_batch_updates(
                        txn.as_pgconn(),
                        &prepared,
                        api.runtime_config.retained_boot_interface_window,
                    )
                    .await
                }
            },
            Err(error) => Err(error.into()),
        };
    if let Err(error) = operation_result {
        txn.rollback_or_log("expected-machine atomic batch failure")
            .await;
        return Err(error);
    }

    for (mut machine, id, _) in prepared {
        machine.id = Some(::rpc::common::Uuid {
            value: id.to_string(),
        });
        results.push(build_success_result(machine));
    }

    txn.commit().await?;

    Ok(results)
}

/// Batch-create expected machines. Static BMC IP handling matches single [`add`] for each row.
pub(crate) async fn create_expected_machines(
    api: &Api,
    request: tonic::Request<rpc::BatchExpectedMachineOperationRequest>,
) -> Result<tonic::Response<rpc::BatchExpectedMachineOperationResponse>, tonic::Status> {
    log_request_data(&request);

    let request = request.into_inner();
    let accept_partial = request.accept_partial_results;
    let machines = request
        .expected_machines
        .ok_or_else(|| CarbideError::InvalidArgument("expected_machines is required".to_string()))?
        .expected_machines;

    let results =
        process_batch_operations(api, machines, accept_partial, BatchOperation::Create).await?;

    Ok(tonic::Response::new(
        rpc::BatchExpectedMachineOperationResponse { results },
    ))
}

/// Batch-update expected machines. Static BMC IP handling matches single [`update`] for each row.
pub(crate) async fn update_expected_machines(
    api: &Api,
    request: tonic::Request<rpc::BatchExpectedMachineOperationRequest>,
) -> Result<tonic::Response<rpc::BatchExpectedMachineOperationResponse>, tonic::Status> {
    log_request_data(&request);

    let request = request.into_inner();
    let accept_partial = request.accept_partial_results;
    let machines = request
        .expected_machines
        .ok_or_else(|| CarbideError::InvalidArgument("expected_machines is required".to_string()))?
        .expected_machines;

    let results =
        process_batch_operations(api, machines, accept_partial, BatchOperation::Update).await?;

    Ok(tonic::Response::new(
        rpc::BatchExpectedMachineOperationResponse { results },
    ))
}

// Utility method called by `explore`. Not a grpc handler.
pub(crate) async fn query(
    api: &Api,
    mac: MacAddress,
) -> Result<Option<ExpectedMachine>, CarbideError> {
    let mut txn = api.txn_begin().await?;

    let mut expected = db::expected_machine::find_many_by_bmc_mac_address(&mut txn, &[mac]).await?;

    txn.commit().await?;

    Ok(expected.remove(&mac))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chassis_serial_regex() {
        assert!(CHASSIS_SERIAL_REGEX.is_match("ABC123"));
        assert!(CHASSIS_SERIAL_REGEX.is_match("ABC-123"));
        assert!(CHASSIS_SERIAL_REGEX.is_match("ABC_123"));
        assert!(CHASSIS_SERIAL_REGEX.is_match("DELL-R740-12345"));
        assert!(CHASSIS_SERIAL_REGEX.is_match("A495122X5503847"));

        assert!(!CHASSIS_SERIAL_REGEX.is_match("ABC"));
        assert!(!CHASSIS_SERIAL_REGEX.is_match("ABC 123"));
        assert!(!CHASSIS_SERIAL_REGEX.is_match("A495122X5503847\r"));
        assert!(!CHASSIS_SERIAL_REGEX.is_match("ABC.123"));

        let too_long = "A".repeat(65);
        assert!(!CHASSIS_SERIAL_REGEX.is_match(&too_long));
    }
}
