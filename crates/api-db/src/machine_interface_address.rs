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

use carbide_network::ip::{IdentifyAddressFamily, IpAddressFamily};
use carbide_uuid::machine::{MachineId, MachineInterfaceId};
use carbide_uuid::network::NetworkSegmentId;
use mac_address::MacAddress;
use model::allocation_type::{AllocationType, AssignStaticResult};
use model::network_segment::NetworkSegmentType;
use sqlx::{FromRow, PgConnection};

use super::DatabaseError;
use crate::db_read::DbReader;

#[cfg(test)]
mod test_find_by_address;

/// Returned by allocation paths with `AddressSelectionStrategy::StaticAddress`
/// when the target IP is already held by some other interface.
#[derive(thiserror::Error, Debug)]
#[error("address already in use: {0} by {1} in network segment {2} (interface: {3})")]
pub struct AddressAlreadyInUseError(
    pub IpAddr,
    pub MacAddress,
    pub NetworkSegmentId,
    pub MachineInterfaceId,
);

#[derive(Debug, FromRow, Clone)]
pub struct MachineInterfaceAddress {
    pub address: IpAddr,
}

#[derive(Debug, FromRow, Clone, Eq, PartialEq)]
pub struct MachineInterfaceAddressWithType {
    pub address: IpAddr,
    pub allocation_type: AllocationType,
}

pub async fn find_ipv4_for_interface(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
) -> Result<MachineInterfaceAddress, DatabaseError> {
    let query =
        "SELECT * FROM machine_interface_addresses WHERE interface_id = $1 AND family(address) = 4";
    sqlx::query_as(query)
        .bind(interface_id)
        .fetch_one(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}

/// Looks up which machine interface owns an IP, with segment metadata and **allocation type**.
///
/// `allocation_type` is used by the IP finder to classify operator static assignments
/// (`AllocationType::Static` or addresses on the `static-assignments` segment) as
/// `IpTypeStaticBmcIp` where appropriate.
pub async fn find_by_address(
    txn: impl DbReader<'_>,
    address: IpAddr,
) -> Result<Option<MachineInterfaceSearchResult>, DatabaseError> {
    let query = "SELECT mi.id, mi.machine_id, ns.name, ns.network_segment_type, mia.allocation_type
            FROM machine_interface_addresses mia
            INNER JOIN machine_interfaces mi ON mi.id = mia.interface_id
            INNER JOIN network_segments ns ON ns.id = mi.segment_id
            WHERE mia.address = $1::inet
        ";
    sqlx::query_as(query)
        .bind(address)
        .fetch_optional(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}

/// Reject an address already owned by another interface.
///
/// Callers that allocate a static address take the allocator's address lock
/// first, then use this re-read before changing any rows.
pub async fn ensure_available_for_interface(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    address: IpAddr,
) -> Result<(), DatabaseError> {
    let query = "SELECT mi.id, ns.name
        FROM machine_interface_addresses mia
        INNER JOIN machine_interfaces mi ON mi.id = mia.interface_id
        INNER JOIN network_segments ns ON ns.id = mi.segment_id
        WHERE mia.address = $1::inet
          AND mi.id <> $2
        LIMIT 1";
    let existing = sqlx::query_as::<_, (MachineInterfaceId, String)>(query)
        .bind(address)
        .bind(interface_id)
        .fetch_optional(txn)
        .await
        .map_err(|error| DatabaseError::query(query, error))?;
    if let Some((existing_interface_id, segment_name)) = existing {
        return Err(DatabaseError::InvalidArgument(format!(
            "IP address {address} is already allocated to interface {} on segment {}",
            existing_interface_id, segment_name,
        )));
    }
    Ok(())
}

pub async fn delete(
    txn: &mut PgConnection,
    interface_id: &MachineInterfaceId,
) -> Result<(), DatabaseError> {
    let query = "DELETE FROM machine_interface_addresses WHERE interface_id = $1";
    sqlx::query(query)
        .bind(interface_id)
        .execute(txn)
        .await
        .map(|_| ())
        .map_err(|e| DatabaseError::query(query, e))
}

/// Find all addresses for an interface, including their allocation type.
pub async fn find_for_interface(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
) -> Result<Vec<MachineInterfaceAddressWithType>, DatabaseError> {
    let query =
        "SELECT address, allocation_type FROM machine_interface_addresses WHERE interface_id = $1";
    sqlx::query_as(query)
        .bind(interface_id)
        .fetch_all(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}

/// Lock and return all address rows for one already-locked interface.
///
/// Callers take MAC, segment, address-key, and interface-row locks first.
pub(crate) async fn find_for_interface_for_update(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
) -> Result<Vec<MachineInterfaceAddressWithType>, DatabaseError> {
    let query = "SELECT address, allocation_type
        FROM machine_interface_addresses
        WHERE interface_id = $1
        ORDER BY address
        FOR UPDATE";
    sqlx::query_as(query)
        .bind(interface_id)
        .fetch_all(txn)
        .await
        .map_err(|error| DatabaseError::query(query, error))
}

/// Find the allocation type of the existing address for a given
/// interface and address family, if one exists.
pub async fn find_allocation_type_for_family(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    family: IpAddressFamily,
) -> Result<Option<AllocationType>, DatabaseError> {
    let query = "SELECT allocation_type FROM machine_interface_addresses WHERE interface_id = $1 AND family(address) = $2";
    let result: Option<(AllocationType,)> = sqlx::query_as(query)
        .bind(interface_id)
        .bind(family.pg_family())
        .fetch_optional(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))?;
    Ok(result.map(|(t,)| t))
}

/// Retain one interface's DHCP address for an address family by promoting it
/// to the existing non-expiring Static representation. Returns true when a
/// DHCP row was promoted and false when the interface has no DHCP allocation
/// for that family.
pub async fn retain_dhcp_address_for_family(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    family: IpAddressFamily,
) -> Result<bool, DatabaseError> {
    let query = "UPDATE machine_interface_addresses \
                 SET allocation_type = $3 \
                 WHERE interface_id = $1 \
                   AND family(address) = $2 \
                   AND allocation_type = $4";
    sqlx::query(query)
        .bind(interface_id)
        .bind(family.pg_family())
        .bind(AllocationType::Static)
        .bind(AllocationType::Dhcp)
        .execute(txn)
        .await
        .map(|result| result.rows_affected() > 0)
        .map_err(|error| DatabaseError::query(query, error))
}

/// Retain a DHCP address on behalf of an ExpectedMachine declaration.
///
/// The ownership marker lets a later anonymous `Retained` to `Dynamic`
/// transition restore the address to DHCP without touching operator-owned
/// static assignments.
pub async fn retain_expected_machine_dhcp_address_for_family(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    family: IpAddressFamily,
) -> Result<bool, DatabaseError> {
    let query = "UPDATE machine_interface_addresses
                 SET allocation_type = $3,
                     expected_machine_preallocation = true
                 WHERE interface_id = $1
                   AND family(address) = $2
                   AND allocation_type = $4";
    sqlx::query(query)
        .bind(interface_id)
        .bind(family.pg_family())
        .bind(AllocationType::Static)
        .bind(AllocationType::Dhcp)
        .execute(txn)
        .await
        .map(|result| result.rows_affected() > 0)
        .map_err(|error| DatabaseError::query(query, error))
}

/// Restore ExpectedMachine-retained addresses to normal DHCP ownership.
///
/// Callers must first prove the interface is still anonymous and that its
/// saved policy is changing away from `Retained`.
pub async fn release_expected_machine_retained_addresses(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
) -> Result<bool, DatabaseError> {
    let query = "UPDATE machine_interface_addresses
                 SET allocation_type = $2,
                     expected_machine_preallocation = false
                 WHERE interface_id = $1
                   AND allocation_type = $3
                   AND expected_machine_preallocation";
    sqlx::query(query)
        .bind(interface_id)
        .bind(AllocationType::Dhcp)
        .bind(AllocationType::Static)
        .execute(txn)
        .await
        .map(|result| result.rows_affected() > 0)
        .map_err(|error| DatabaseError::query(query, error))
}

/// Delete the address for a given interface, address family, and
/// allocation type. Returns true if a row was deleted.
pub async fn delete_by_interface_family(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    family: IpAddressFamily,
    allocation_type: AllocationType,
) -> Result<bool, DatabaseError> {
    let query = "DELETE FROM machine_interface_addresses WHERE interface_id = $1 AND family(address) = $2 AND allocation_type = $3";
    sqlx::query(query)
        .bind(interface_id)
        .bind(family.pg_family())
        .bind(allocation_type)
        .execute(txn)
        .await
        .map(|r| r.rows_affected() > 0)
        .map_err(|e| DatabaseError::query(query, e))
}

#[derive(Clone, Copy)]
enum AddressRemovalScope {
    Interface(MachineInterfaceId),
    Mac(MacAddress),
    Any,
}

#[derive(Clone, Debug, Eq, FromRow, PartialEq)]
struct AddressRemovalMatch {
    interface_id: MachineInterfaceId,
    mac_address: MacAddress,
    segment_id: NetworkSegmentId,
    address: IpAddr,
    allocation_type: AllocationType,
}

#[derive(Clone, Copy, Debug, Eq, FromRow, PartialEq)]
struct AddressRemovalInterface {
    interface_id: MachineInterfaceId,
    mac_address: MacAddress,
    segment_id: NetworkSegmentId,
}

async fn find_address_removal_matches(
    txn: &mut PgConnection,
    address: IpAddr,
    allocation_type: AllocationType,
    scope: AddressRemovalScope,
) -> Result<Vec<AddressRemovalMatch>, DatabaseError> {
    match scope {
        AddressRemovalScope::Interface(interface_id) => {
            let query = "SELECT
                    mia.interface_id,
                    mi.mac_address,
                    mi.segment_id,
                    mia.address,
                    mia.allocation_type
                FROM machine_interface_addresses mia
                INNER JOIN machine_interfaces mi ON mi.id = mia.interface_id
                WHERE mia.interface_id = $1
                  AND mia.address = $2::inet
                  AND mia.allocation_type = $3
                ORDER BY mia.interface_id, mia.address";
            sqlx::query_as(query)
                .bind(interface_id)
                .bind(address)
                .bind(allocation_type)
                .fetch_all(&mut *txn)
                .await
                .map_err(|error| DatabaseError::query(query, error))
        }
        AddressRemovalScope::Mac(mac_address) => {
            let query = "SELECT
                    mia.interface_id,
                    mi.mac_address,
                    mi.segment_id,
                    mia.address,
                    mia.allocation_type
                FROM machine_interface_addresses mia
                INNER JOIN machine_interfaces mi ON mi.id = mia.interface_id
                WHERE mi.mac_address = $1
                  AND mia.address = $2::inet
                  AND mia.allocation_type = $3
                ORDER BY mia.interface_id, mia.address";
            sqlx::query_as(query)
                .bind(mac_address)
                .bind(address)
                .bind(allocation_type)
                .fetch_all(&mut *txn)
                .await
                .map_err(|error| DatabaseError::query(query, error))
        }
        AddressRemovalScope::Any => {
            let query = "SELECT
                    mia.interface_id,
                    mi.mac_address,
                    mi.segment_id,
                    mia.address,
                    mia.allocation_type
                FROM machine_interface_addresses mia
                INNER JOIN machine_interfaces mi ON mi.id = mia.interface_id
                WHERE mia.address = $1::inet
                  AND mia.allocation_type = $2
                ORDER BY mia.interface_id, mia.address";
            sqlx::query_as(query)
                .bind(address)
                .bind(allocation_type)
                .fetch_all(&mut *txn)
                .await
                .map_err(|error| DatabaseError::query(query, error))
        }
    }
}

async fn find_address_removal_interfaces(
    txn: &mut PgConnection,
    scope: AddressRemovalScope,
    matches: &[AddressRemovalMatch],
) -> Result<Vec<AddressRemovalInterface>, DatabaseError> {
    match scope {
        AddressRemovalScope::Interface(interface_id) => {
            let query = "SELECT
                    id AS interface_id,
                    mac_address,
                    segment_id
                FROM machine_interfaces
                WHERE id = $1";
            sqlx::query_as(query)
                .bind(interface_id)
                .fetch_optional(txn)
                .await
                .map(|interface| interface.into_iter().collect())
                .map_err(|error| DatabaseError::query(query, error))
        }
        AddressRemovalScope::Mac(mac_address) => {
            let query = "SELECT
                    id AS interface_id,
                    mac_address,
                    segment_id
                FROM machine_interfaces
                WHERE mac_address = $1
                ORDER BY id";
            sqlx::query_as(query)
                .bind(mac_address)
                .fetch_all(txn)
                .await
                .map_err(|error| DatabaseError::query(query, error))
        }
        AddressRemovalScope::Any => {
            let mut interfaces = matches
                .iter()
                .map(|address_match| AddressRemovalInterface {
                    interface_id: address_match.interface_id,
                    mac_address: address_match.mac_address,
                    segment_id: address_match.segment_id,
                })
                .collect::<Vec<_>>();
            interfaces.sort_by_key(|interface| interface.interface_id);
            interfaces.dedup();
            Ok(interfaces)
        }
    }
}

async fn delete_address_allocations_with_lock_order(
    txn: &mut PgConnection,
    address: IpAddr,
    allocation_type: AllocationType,
    scope: AddressRemovalScope,
) -> Result<Vec<MachineInterfaceId>, DatabaseError> {
    let expected_matches =
        find_address_removal_matches(&mut *txn, address, allocation_type, scope).await?;
    let interfaces = find_address_removal_interfaces(&mut *txn, scope, &expected_matches).await?;
    if interfaces.is_empty() {
        return Ok(Vec::new());
    }

    let lock_targets = interfaces
        .iter()
        .map(
            |interface| crate::machine_interface::InterfaceAddressMutationTarget {
                interface_id: interface.interface_id,
                mac_address: interface.mac_address,
                segment_id: interface.segment_id,
                addresses: vec![address],
            },
        )
        .collect::<Vec<_>>();
    crate::machine_interface::lock_interface_address_mutation_targets(txn, &lock_targets).await?;

    let locked_matches =
        find_address_removal_matches(&mut *txn, address, allocation_type, scope).await?;
    if locked_matches != expected_matches {
        return Err(DatabaseError::FailedPrecondition(format!(
            "address {address} ownership changed while preparing its removal; retry the request",
        )));
    }

    let lock_query = "SELECT 1
        FROM machine_interface_addresses
        WHERE interface_id = $1
          AND address = $2::inet
          AND allocation_type = $3
        FOR UPDATE";
    for address_match in &expected_matches {
        let locked = sqlx::query_scalar::<_, i32>(lock_query)
            .bind(address_match.interface_id)
            .bind(address_match.address)
            .bind(address_match.allocation_type)
            .fetch_optional(&mut *txn)
            .await
            .map_err(|error| DatabaseError::query(lock_query, error))?;
        if locked.is_none() {
            return Err(DatabaseError::FailedPrecondition(format!(
                "address {address} ownership changed while preparing its removal; retry the request",
            )));
        }
    }

    let delete_query = "DELETE FROM machine_interface_addresses
        WHERE interface_id = $1
          AND address = $2::inet
          AND allocation_type = $3";
    let mut removed_interfaces = Vec::with_capacity(expected_matches.len());
    for address_match in expected_matches {
        let deleted = sqlx::query(delete_query)
            .bind(address_match.interface_id)
            .bind(address_match.address)
            .bind(address_match.allocation_type)
            .execute(&mut *txn)
            .await
            .map_err(|error| DatabaseError::query(delete_query, error))?;
        if deleted.rows_affected() != 1 {
            return Err(DatabaseError::FailedPrecondition(format!(
                "address {address} ownership changed while preparing its removal; retry the request",
            )));
        }
        removed_interfaces.push(address_match.interface_id);
    }
    removed_interfaces.sort_unstable();
    removed_interfaces.dedup();
    Ok(removed_interfaces)
}

/// Delete a specific address from a specific interface. Returns true if a
/// matching row was deleted. Scoping by `interface_id` ensures an operator
/// remove-address call only removes the caller's own address, never another
/// interface's row that happens to hold the same IP.
pub async fn delete_by_interface_and_address(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    address: IpAddr,
    allocation_type: AllocationType,
) -> Result<bool, DatabaseError> {
    delete_address_allocations_with_lock_order(
        txn,
        address,
        allocation_type,
        AddressRemovalScope::Interface(interface_id),
    )
    .await
    .map(|removed| !removed.is_empty())
}

/// Delete a static address only when ExpectedMachine reconciliation created it.
pub async fn delete_expected_machine_preallocation_by_interface_and_address(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    address: IpAddr,
) -> Result<bool, DatabaseError> {
    let query = "DELETE FROM machine_interface_addresses
        WHERE interface_id = $1
          AND address = $2::inet
          AND allocation_type = $3
          AND expected_machine_preallocation
        ";
    sqlx::query(query)
        .bind(interface_id)
        .bind(address)
        .bind(AllocationType::Static)
        .execute(txn)
        .await
        .map(|result| result.rows_affected() > 0)
        .map_err(|error| DatabaseError::query(query, error))
}

/// Delete a reservation created by ExpectedMachine reconciliation or one
/// created before reservation ownership was tracked. Callers use this only for
/// an explicit fixed-address policy transition, never for configuration
/// deletion.
pub async fn delete_expected_machine_preallocation_or_legacy_by_interface_and_address(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    address: IpAddr,
) -> Result<bool, DatabaseError> {
    let query = "DELETE FROM machine_interface_addresses
        WHERE interface_id = $1
          AND address = $2::inet
          AND allocation_type = $3
          AND expected_machine_preallocation IS DISTINCT FROM false
        ";
    sqlx::query(query)
        .bind(interface_id)
        .bind(address)
        .bind(AllocationType::Static)
        .execute(txn)
        .await
        .map(|result| result.rows_affected() > 0)
        .map_err(|error| DatabaseError::query(query, error))
}

/// Mark a static reservation as created by ExpectedMachine reconciliation.
/// Existing operator-created static assignments are never adopted by this
/// helper's callers.
pub async fn mark_expected_machine_preallocation(
    txn: &mut PgConnection,
    mac_address: MacAddress,
    address: IpAddr,
) -> Result<(), DatabaseError> {
    let query = "UPDATE machine_interface_addresses mia
        SET expected_machine_preallocation = true
        FROM machine_interfaces mi
        WHERE mia.interface_id = mi.id
          AND mi.mac_address = $1
          AND mia.address = $2::inet
          AND mia.allocation_type = $3
        ";
    let result = sqlx::query(query)
        .bind(mac_address)
        .bind(address)
        .bind(AllocationType::Static)
        .execute(txn)
        .await
        .map_err(|error| DatabaseError::query(query, error))?;
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(DatabaseError::internal(format!(
            "expected one static reservation for MAC {mac_address} and IP {address}, updated {}",
            result.rows_affected(),
        )))
    }
}

/// Insert a new address for an interface with the given allocation type.
pub async fn insert(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    address: IpAddr,
    allocation_type: AllocationType,
) -> Result<(), DatabaseError> {
    let query = "INSERT INTO machine_interface_addresses (
            interface_id,
            address,
            allocation_type,
            expected_machine_preallocation
        )
        VALUES ($1::uuid, $2::inet, $3, false)";
    sqlx::query(query)
        .bind(interface_id)
        .bind(address)
        .bind(allocation_type)
        .execute(txn)
        .await
        .map(|_| ())
        .map_err(|e| DatabaseError::query(query, e))
}

/// Assign a static address to an interface. If the interface already
/// has an address for the same family, the behavior depends on its
/// allocation type:
///
/// - `Static`: the old static address is replaced.
/// - `Dhcp` or `Slaac`: the managed allocation is removed and
///   replaced with the static assignment.
pub async fn assign_static(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    address: IpAddr,
) -> Result<AssignStaticResult, DatabaseError> {
    let family = address.address_family();

    let existing = find_allocation_type_for_family(&mut *txn, interface_id, family).await?;

    let result = match existing {
        Some(allocation_type @ (AllocationType::Dhcp | AllocationType::Slaac)) => {
            delete_by_interface_family(&mut *txn, interface_id, family, allocation_type).await?;
            AssignStaticResult::ReplacedDhcp
        }
        Some(AllocationType::Static) => {
            delete_by_interface_family(&mut *txn, interface_id, family, AllocationType::Static)
                .await?;
            AssignStaticResult::ReplacedStatic
        }
        None => AssignStaticResult::Assigned,
    };

    insert(txn, interface_id, address, AllocationType::Static).await?;

    Ok(result)
}

/// Delete an address allocation of the given type. Returns the interfaces that
/// owned the deleted addresses (normally one, empty if nothing matched) so
/// callers can resync each one's hostname rather than guessing the owner. The
/// delete removes every matching row, so all owners are returned — not just the
/// first — since `(address, allocation_type)` is not unique on its own.
pub async fn delete_by_address(
    txn: &mut PgConnection,
    address: IpAddr,
    allocation_type: AllocationType,
) -> Result<Vec<MachineInterfaceId>, DatabaseError> {
    delete_address_allocations_with_lock_order(
        txn,
        address,
        allocation_type,
        AddressRemovalScope::Any,
    )
    .await
}

/// Delete an address allocation for a given (ip, mac) pair, which
/// of course only actually deletes when the pair matches.
///
/// Returns the interfaces that owned the deleted allocations (normally one,
/// empty if the pair matched nothing) so callers can resync each one's hostname
/// against the authoritative deleted rows rather than a separate lookup.
pub async fn delete_by_address_and_mac(
    txn: &mut PgConnection,
    address: IpAddr,
    mac_address: mac_address::MacAddress,
    allocation_type: AllocationType,
) -> Result<Vec<MachineInterfaceId>, DatabaseError> {
    delete_address_allocations_with_lock_order(
        txn,
        address,
        allocation_type,
        AddressRemovalScope::Mac(mac_address),
    )
    .await
}

/// Check whether an interface has any address assigned for the
/// given address family.
///
/// This is used by the DHCPDISCOVER flow to decide whether to
/// re-allocate after a lease expiration. If the interface still
/// has an address for the family (static or DHCP), no re-allocation
// is needed.
pub async fn has_address_for_family(
    txn: &mut PgConnection,
    interface_id: MachineInterfaceId,
    family: IpAddressFamily,
) -> Result<bool, DatabaseError> {
    let query = "SELECT EXISTS(SELECT 1 FROM machine_interface_addresses WHERE interface_id = $1 AND family(address) = $2)";
    sqlx::query_scalar(query)
        .bind(interface_id)
        .bind(family.pg_family())
        .fetch_one(txn)
        .await
        .map_err(|e| DatabaseError::query(query, e))
}

/// Row shape for [`find_by_address`]: interface identity, owning segment, and how the address was
/// assigned (DHCP vs static / operator-configured).
#[derive(Debug, FromRow)]
pub struct MachineInterfaceSearchResult {
    pub id: MachineInterfaceId,
    pub machine_id: Option<MachineId>,
    pub name: String,
    pub network_segment_type: NetworkSegmentType,
    pub allocation_type: AllocationType,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the new SLAAC allocation type survives a database round trip.
    #[crate::sqlx_test]
    async fn slaac_allocation_type_round_trips(
        pool: sqlx::PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut txn = pool.begin().await?;

        // Create the minimal segment and interface rows needed to own an address.
        let segment_id: NetworkSegmentId = sqlx::query_scalar(
            "INSERT INTO network_segments (name, version) VALUES ($1, 'V1-T0') RETURNING id",
        )
        .bind("slaac-roundtrip")
        .fetch_one(txn.as_mut())
        .await?;
        let interface_id: MachineInterfaceId = sqlx::query_scalar(
            "INSERT INTO machine_interfaces (segment_id, mac_address, primary_interface, hostname)
             VALUES ($1, $2::macaddr, true, 'slaac-roundtrip') RETURNING id",
        )
        .bind(segment_id)
        .bind("02:00:00:00:00:01")
        .fetch_one(txn.as_mut())
        .await?;

        // Insert a SLAAC allocation through the public helper and read it back.
        insert(
            txn.as_mut(),
            interface_id,
            "2001:db8::10".parse()?,
            AllocationType::Slaac,
        )
        .await?;
        let addresses = find_for_interface(txn.as_mut(), interface_id).await?;

        // Verify the persisted row preserved the new allocation type.
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].allocation_type, AllocationType::Slaac);

        txn.rollback().await?;
        Ok(())
    }

    /// Retaining one address family promotes only its DHCP allocation. Other
    /// families and non-DHCP allocations keep their original allocation types.
    #[crate::sqlx_test]
    async fn retain_dhcp_address_for_family_is_scoped_and_idempotent(
        pool: sqlx::PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut txn = pool.begin().await?;
        let segment_id: NetworkSegmentId = sqlx::query_scalar(
            "INSERT INTO network_segments (name, version) VALUES ($1, 'V1-T0') RETURNING id",
        )
        .bind("retain-family")
        .fetch_one(txn.as_mut())
        .await?;
        let interface_id: MachineInterfaceId = sqlx::query_scalar(
            "INSERT INTO machine_interfaces (segment_id, mac_address, primary_interface, hostname)
             VALUES ($1, $2::macaddr, true, 'retain-family') RETURNING id",
        )
        .bind(segment_id)
        .bind("02:00:00:00:00:02")
        .fetch_one(txn.as_mut())
        .await?;

        insert(
            txn.as_mut(),
            interface_id,
            "192.0.2.10".parse()?,
            AllocationType::Dhcp,
        )
        .await?;
        insert(
            txn.as_mut(),
            interface_id,
            "2001:db8::10".parse()?,
            AllocationType::Slaac,
        )
        .await?;

        assert!(
            retain_dhcp_address_for_family(txn.as_mut(), interface_id, IpAddressFamily::Ipv4,)
                .await?
        );
        assert!(
            !retain_dhcp_address_for_family(txn.as_mut(), interface_id, IpAddressFamily::Ipv4,)
                .await?,
            "an already retained address should be an idempotent no-op"
        );

        let mut addresses = find_for_interface(txn.as_mut(), interface_id).await?;
        addresses.sort_by_key(|address| address.address);
        assert_eq!(addresses.len(), 2);
        assert_eq!(addresses[0].allocation_type, AllocationType::Static);
        assert_eq!(addresses[1].allocation_type, AllocationType::Slaac);

        txn.rollback().await?;
        Ok(())
    }
}
