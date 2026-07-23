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

use mac_address::MacAddress;
use model::address_selection_strategy::AddressSelectionStrategy;
use model::allocation_type::AllocationType;
use model::machine_interface::InterfaceType;
use rpc::forge as rpc;
use tonic::{Request, Response, Status};

use crate::api::Api;
use crate::errors::CarbideError;

/// Resolve the correct segment for a static IP. If the IP is within a
/// managed network prefix, use that segment. Otherwise use the
/// static-assignments anchor segment.
async fn resolve_segment_for_static_ip(
    txn: &mut sqlx::PgConnection,
    ip: std::net::IpAddr,
) -> Result<model::network_segment::NetworkSegment, CarbideError> {
    Ok(db::network_segment::for_static_address(txn, ip, None).await?)
}

/// Update or create a machine_interface with a static address.
///
/// If no interface exists for this MAC, creates a new one. If an
/// interface exists but has no addresses, assigns the static IP.
/// If an interface already has addresses, it remains unchanged because
/// expected-device updates are decoupled from managed-device state.
/// The expected data is updated in the database by the caller.
/// To change the IP on a live interface, operators should use
/// 'machine-interfaces assign-address' or 'remove-address'.
pub async fn update_preallocated_machine_interface(
    txn: &mut sqlx::PgConnection,
    mac_address: MacAddress,
    ip_address: std::net::IpAddr,
    interface_type: InterfaceType,
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    db::machine_interface::lock_expected_machine_interface_macs(txn, std::iter::once(mac_address))
        .await?;

    // Validate the declared segment guard even when an existing address
    // allocation remains operator-managed below.
    let target_segment = resolve_segment_for_static_ip(txn, ip_address).await?;
    db::machine_interface::lock_static_address_allocation(txn, &target_segment, ip_address).await?;

    update_preallocated_machine_interface_after_locks(
        txn,
        mac_address,
        ip_address,
        interface_type,
        &target_segment,
        retained_window,
    )
    .await
}

/// Update or create a preallocated interface after the caller has locked its
/// MAC and every static-address target in the surrounding batch.
///
/// `target_segment` must be the segment resolved for `ip_address`.
pub(crate) async fn update_preallocated_machine_interface_after_locks(
    txn: &mut sqlx::PgConnection,
    mac_address: MacAddress,
    ip_address: std::net::IpAddr,
    interface_type: InterfaceType,
    target_segment: &model::network_segment::NetworkSegment,
    retained_window: Option<chrono::Duration>,
) -> Result<(), CarbideError> {
    let existing = db::machine_interface::find_by_mac_address(&mut *txn, mac_address).await?;

    if let Some(iface) = existing.first() {
        if iface.addresses.is_empty() {
            db::machine_interface_address::ensure_available_for_interface(
                txn, iface.id, ip_address,
            )
            .await?;
            if iface.interface_type != interface_type {
                db::machine_interface::set_interface_type(&iface.id, interface_type, &mut *txn)
                    .await?;
            }
            if interface_type == InterfaceType::Bmc && iface.primary_interface {
                db::machine_interface::set_primary_interface(&iface.id, false, &mut *txn).await?;
            }

            // No addresses -- safe to assign the static IP.
            db::machine_interface_address::assign_static(txn, iface.id, ip_address).await?;

            if iface.segment_id != target_segment.id {
                db::machine_interface::update_segment_id(
                    txn,
                    iface.id,
                    target_segment.id,
                    target_segment.config.subdomain_id,
                )
                .await?;
            }

            db::machine_interface::sync_hostname_after_address_assignment(
                txn,
                iface.id,
                target_segment.config.subdomain_id,
            )
            .await?;

            tracing::info!(
                %mac_address,
                %ip_address,
                machine_interface_id = %iface.id,
                "Assigned static address to existing interface without addresses"
            );
        } else {
            tracing::info!(
                %mac_address,
                %ip_address,
                existing_addresses = ?iface.addresses,
                "Interface already has addresses, left address allocation unchanged"
            );
        }
    } else {
        // No interface yet -- create a new one.
        // Anonymous Data rows retain the legacy primary default. Explicit host
        // primary declarations are applied when DHCP first discovers the NIC;
        // a DPU OS Data row is primary for its own machine.
        db::machine_interface::create_with_type(
            txn,
            std::slice::from_ref(target_segment),
            &mac_address,
            interface_type != InterfaceType::Bmc,
            AddressSelectionStrategy::StaticAddress(ip_address),
            interface_type,
            retained_window,
        )
        .await?;

        tracing::info!(
            %mac_address,
            %ip_address,
            network_segment_id = %target_segment.id,
            "Pre-allocated static machine interface"
        );
    }

    Ok(())
}

pub async fn assign_static_address(
    api: &Api,
    request: Request<rpc::AssignStaticAddressRequest>,
) -> Result<Response<rpc::AssignStaticAddressResponse>, CarbideError> {
    let req = request.into_inner();
    let interface_id = req.interface_id.ok_or(CarbideError::InvalidArgument(
        "interface_id is required".into(),
    ))?;
    let ip_address: std::net::IpAddr = req.ip_address.parse()?;

    let mut txn = api.txn_begin().await?;
    let interface_preview = db::machine_interface::find_one(txn.as_pgconn(), interface_id).await?;
    db::machine_interface::lock_expected_machine_interface_macs(
        txn.as_pgconn(),
        std::iter::once(interface_preview.mac_address),
    )
    .await?;

    // Resolve and lock before reading ownership. Dynamic allocators use the
    // same address lock, so the post-lock check sees any preceding commit.
    let target_segment = resolve_segment_for_static_ip(txn.as_pgconn(), ip_address).await?;
    db::machine_interface::lock_static_address_allocation(
        txn.as_pgconn(),
        &target_segment,
        ip_address,
    )
    .await?;
    let current_iface =
        db::machine_interface::find_one_for_update(txn.as_pgconn(), interface_id).await?;
    if current_iface.mac_address != interface_preview.mac_address {
        return Err(CarbideError::FailedPrecondition(format!(
            "machine interface {interface_id} MAC address changed from {} to {} while static assignment was waiting for allocator locks; retry the assignment",
            interface_preview.mac_address, current_iface.mac_address,
        )));
    }
    db::machine_interface_address::ensure_available_for_interface(
        txn.as_pgconn(),
        interface_id,
        ip_address,
    )
    .await?;
    let result =
        db::machine_interface_address::assign_static(&mut txn, interface_id, ip_address).await?;

    if current_iface.segment_id != target_segment.id {
        db::machine_interface::update_segment_id(
            &mut txn,
            interface_id,
            target_segment.id,
            target_segment.config.subdomain_id,
        )
        .await?;
        tracing::info!(
            machine_interface_id = %interface_id,
            %ip_address,
            previous_network_segment_id = %current_iface.segment_id,
            next_network_segment_id = %target_segment.id,
            "Moved interface to correct segment for static address"
        );
    }

    // Keep the interface's IP-derived hostname/domain consistent with the new
    // address, matching every other assignment path. Skipping this leaves a
    // stale hostname that encodes a now-freed IP and collides with the
    // fqdn_must_be_unique constraint when the allocator reuses that IP.
    db::machine_interface::sync_hostname_after_address_assignment(
        &mut txn,
        interface_id,
        target_segment.config.subdomain_id,
    )
    .await?;

    txn.commit().await?;

    let status: rpc::AssignStaticAddressStatus = result.into();
    tracing::info!(machine_interface_id = %interface_id, %ip_address, assignment_status = ?status, "Static address assignment");

    Ok(Response::new(rpc::AssignStaticAddressResponse {
        interface_id: Some(interface_id),
        ip_address: ip_address.to_string(),
        status: status.into(),
    }))
}

pub async fn remove_static_address(
    api: &Api,
    request: Request<rpc::RemoveStaticAddressRequest>,
) -> Result<Response<rpc::RemoveStaticAddressResponse>, CarbideError> {
    let req = request.into_inner();
    let interface_id = req.interface_id.ok_or(CarbideError::InvalidArgument(
        "interface_id is required".into(),
    ))?;
    let ip_address: std::net::IpAddr = req.ip_address.parse()?;

    let mut txn = api.txn_begin().await?;
    // Scope the delete to the caller's interface so remove-address only ever
    // removes that interface's own address, matching the command's contract
    // ("remove the address from a machine interface"). A mismatched interface_id
    // deletes nothing and returns NotFound rather than removing another
    // interface's row that happens to hold the same IP.
    let deleted = db::machine_interface_address::delete_by_interface_and_address(
        &mut txn,
        interface_id,
        ip_address,
        AllocationType::Static,
    )
    .await?;

    // Re-derive the interface's hostname/domain now that its address is gone,
    // matching the DHCP lease-expiry path. Without this the interface keeps a
    // hostname pinned to the removed IP.
    if deleted {
        db::machine_interface::sync_hostname_after_address_change(&mut txn, interface_id).await?;
    }

    txn.commit().await?;

    let status = if deleted {
        tracing::info!(machine_interface_id = %interface_id, %ip_address, "Removed static address");
        rpc::RemoveStaticAddressStatus::Removed
    } else {
        tracing::info!(machine_interface_id = %interface_id, %ip_address, "Static address not found");
        rpc::RemoveStaticAddressStatus::NotFound
    };

    Ok(Response::new(rpc::RemoveStaticAddressResponse {
        interface_id: Some(interface_id),
        ip_address: ip_address.to_string(),
        status: status.into(),
    }))
}

pub async fn find_interface_addresses(
    api: &Api,
    request: Request<rpc::FindInterfaceAddressesRequest>,
) -> Result<Response<rpc::FindInterfaceAddressesResponse>, Status> {
    let req = request.into_inner();
    let interface_id = req.interface_id.ok_or(CarbideError::InvalidArgument(
        "interface_id is required".into(),
    ))?;

    let mut txn = api.txn_begin().await?;
    let addresses =
        db::machine_interface_address::find_for_interface(&mut txn, interface_id).await?;
    txn.commit().await?;

    let proto_addresses = addresses
        .into_iter()
        .map(|a| rpc::InterfaceAddress {
            address: a.address.to_string(),
            allocation_type: match a.allocation_type {
                AllocationType::Dhcp => "dhcp".to_string(),
                AllocationType::Static => "static".to_string(),
                AllocationType::Slaac => "slaac".to_string(),
            },
        })
        .collect();

    Ok(Response::new(rpc::FindInterfaceAddressesResponse {
        interface_id: Some(interface_id),
        addresses: proto_addresses,
    }))
}
