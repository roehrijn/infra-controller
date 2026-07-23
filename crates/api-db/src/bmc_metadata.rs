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

use std::net::IpAddr;

use carbide_uuid::machine::{MachineId, MachineInterfaceId};
use carbide_uuid::network::NetworkSegmentId;
use mac_address::MacAddress;
use model::bmc_info::BmcInfo;
use model::expected_machine::ExpectedHostNic;
use serde_json::json;
use sqlx::PgConnection;

use crate::{DatabaseError, DatabaseResult};

#[cfg(test)]
mod tests;

/// Interface identity needed to stabilize BMC-address ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct BmcInterfaceOwner {
    /// Interface that owns the BMC address.
    pub interface_id: MachineInterfaceId,
    /// MAC used for ExpectedMachine selection and ingestion serialization.
    pub mac_address: MacAddress,
    /// Segment that must be locked before the interface and address rows.
    pub segment_id: NetworkSegmentId,
}

async fn find_interface_by_bmc_ip_with_query(
    txn: &mut PgConnection,
    bmc_ip: IpAddr,
    query: &'static str,
) -> DatabaseResult<Option<BmcInterfaceOwner>> {
    let mut owners = sqlx::query_as::<_, BmcInterfaceOwner>(query)
        .bind(bmc_ip)
        .fetch_all(&mut *txn)
        .await
        .map_err(|error| DatabaseError::query(query, error))?;

    match owners.len() {
        0 => Ok(None),
        1 => Ok(owners.pop()),
        _ => Err(DatabaseError::internal(format!(
            "multiple machine interfaces own BMC IP address {bmc_ip}",
        ))),
    }
}

/// Find the single machine interface that owns a BMC address.
///
/// Unlike the general IP lookup, this rejects ambiguous ownership instead of
/// selecting whichever row PostgreSQL returns first.
pub async fn find_interface_by_bmc_ip(
    txn: &mut PgConnection,
    bmc_ip: IpAddr,
) -> DatabaseResult<Option<BmcInterfaceOwner>> {
    let query = "
        SELECT
            mi.id AS interface_id,
            mi.mac_address,
            mi.segment_id
        FROM machine_interface_addresses mia
        JOIN machine_interfaces mi ON mi.id = mia.interface_id
        WHERE mia.address = $1::inet
        ORDER BY mi.id, mia.id
    ";
    find_interface_by_bmc_ip_with_query(txn, bmc_ip, query).await
}

/// Find and lock the single machine interface that owns a BMC address.
///
/// Both the interface and address rows stay locked for the caller's
/// transaction.
pub async fn find_interface_by_bmc_ip_for_update(
    txn: &mut PgConnection,
    bmc_ip: IpAddr,
) -> DatabaseResult<Option<BmcInterfaceOwner>> {
    let query = "
        SELECT
            mi.id AS interface_id,
            mi.mac_address,
            mi.segment_id
        FROM machine_interface_addresses mia
        JOIN machine_interfaces mi ON mi.id = mia.interface_id
        WHERE mia.address = $1::inet
        ORDER BY mi.id, mia.id
        FOR UPDATE OF mia, mi
    ";
    find_interface_by_bmc_ip_with_query(txn, bmc_ip, query).await
}

async fn update_bmc_network_into_topologies(
    txn: &mut PgConnection,
    machine_id: &MachineId,
    bmc_info: &BmcInfo,
) -> DatabaseResult<()> {
    if bmc_info.mac.is_none() {
        return Err(DatabaseError::internal(format!(
            "BMC Info in machine_topologies does not have a MAC address for machine {machine_id}"
        )));
    }
    tracing::info!(
        bmc_info = ?bmc_info,
        "Updating BMC info",
    );

    // A entry with same machine id is already created by discover_machine call.
    // Just update json by adding a ipmi_ip entry.
    let query = "UPDATE machine_topologies SET topology = jsonb_set(topology, '{bmc_info}', $1, true) WHERE machine_id=$2 RETURNING machine_id";
    sqlx::query_as::<_, MachineId>(query)
        .bind(json!(bmc_info))
        .bind(machine_id)
        .fetch_optional(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?
        .ok_or(DatabaseError::NotFoundError {
            kind: "machine_topologies.machine_id",
            id: machine_id.to_string(),
        })?;
    Ok(())
}

/// Associate a discovered BMC interface after saving the ExpectedMachine
/// declaration selected for this ingestion attempt.
///
/// The caller must select `expected_interface` while holding the matching
/// ExpectedMachine row and acquire the interface MAC lock before segment locks.
pub async fn update_bmc_network_into_machine_interfaces(
    txn: &mut PgConnection,
    machine_id: &MachineId,
    bmc_info: &mut BmcInfo,
    expected_interface: Option<&ExpectedHostNic>,
) -> DatabaseResult<()> {
    let Some(bmc_mac_address) = bmc_info.mac else {
        return Err(DatabaseError::internal(format!(
            "BMC Info does not have a MAC address for machine {machine_id}"
        )));
    };

    let (interface_id, interface_mac_address) = if let Some(interface_id) =
        bmc_info.machine_interface_id
    {
        let interface = crate::machine_interface::find_one(&mut *txn, interface_id).await?;
        (interface.id, interface.mac_address)
    } else if let Some(bmc_ip) = bmc_info.ip.as_ref() {
        let owner = find_interface_by_bmc_ip_for_update(&mut *txn, *bmc_ip)
            .await?
            .ok_or_else(|| DatabaseError::NotFoundError {
                kind: "machine_interfaces.address",
                id: bmc_ip.to_string(),
            })?;
        (owner.interface_id, owner.mac_address)
    } else {
        let interface = crate::machine_interface::find_by_mac_address(&mut *txn, bmc_mac_address)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| DatabaseError::NotFoundError {
                kind: "machine_interfaces.mac_address",
                id: bmc_mac_address.to_string(),
            })?;
        (interface.id, interface.mac_address)
    };

    if interface_mac_address != bmc_mac_address {
        return Err(DatabaseError::internal(format!(
            "BMC interface {} MAC {} does not match BMC Info MAC {} for machine {machine_id}",
            interface_id, interface_mac_address, bmc_mac_address
        )));
    }

    crate::machine_interface::capture_expected_interface_before_association(
        txn,
        interface_id,
        expected_interface,
    )
    .await?;
    crate::machine_interface::associate_bmc_interface(
        &interface_id,
        model::machine_interface_address::MachineInterfaceAssociation::Machine(*machine_id),
        txn,
    )
    .await?;
    bmc_info.machine_interface_id = Some(interface_id);

    update_bmc_network_into_topologies(txn, machine_id, bmc_info).await
}

// enrich_mac_address queries the MachineInterfaces table to populate the BMC mac address of the BmcMetaDataInfo structure in memory if it does not exist
// If this function populates the BMC mac address, and persist is speciifed as true, the function will update the machine_topologies table
// with the mac address for that BMC
pub async fn enrich_mac_address(
    bmc_info: &mut BmcInfo,
    caller: String,
    txn: &mut PgConnection,
    machine_id: &MachineId,
    persist: bool,
) -> DatabaseResult<()> {
    if bmc_info.ip.is_none() {
        return Err(DatabaseError::internal(format!(
            "{caller} cannot enrich BMC Info without a valid BMC IP address for machine {machine_id}: {bmc_info:#?}"
        )));
    }

    let bmc_ip_address = bmc_info.ip.unwrap();
    if bmc_info.mac.is_none() {
        if let Some(bmc_interface_owner) =
            find_interface_by_bmc_ip(&mut *txn, bmc_ip_address).await?
        {
            let bmc_mac_address = bmc_interface_owner.mac_address;

            tracing::info!(
                caller = %caller,
                machine_id = %machine_id,
                mac_address = ?bmc_interface_owner.mac_address,
                "Enriching BMC information",
            );
            bmc_info.mac = Some(bmc_mac_address);
            bmc_info.machine_interface_id = Some(bmc_interface_owner.interface_id);
            if persist {
                update_bmc_network_into_topologies(txn, machine_id, bmc_info).await?;
            }
        } else {
            // This should never happen. Should we return an error here?
            tracing::info!(
                caller = %caller,
                machine_id = %machine_id,
                bmc_ip_address = %bmc_ip_address,
                "Failed to enrich BMC information: machine interface not found",
            );
        }
    }
    Ok(())
}
