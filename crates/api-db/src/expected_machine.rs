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
use std::collections::HashMap;
use std::net::IpAddr;

use carbide_uuid::machine::MachineId;
use carbide_uuid::rack::RackId;
use itertools::Itertools;
use mac_address::MacAddress;
use model::expected_machine::{
    ExpectedMachine, ExpectedMachineRequest, LinkedExpectedMachine, UnexpectedMachine,
};
use model::site_explorer::EndpointExplorationReport;
use sqlx::{FromRow, PgConnection};
use uuid::Uuid;

use crate::db_read::DbReader;
use crate::{DatabaseError, DatabaseResult};

const SQL_VIOLATION_DUPLICATE_MAC: &str = "expected_machines_bmc_mac_address_key";
const CONFIG_MUTATION_LOCK_KEY: &str = "expected_machines:config_mutations";

pub async fn find_by_bmc_mac_address(
    txn: impl DbReader<'_>,
    bmc_mac_address: MacAddress,
) -> Result<Option<ExpectedMachine>, DatabaseError> {
    let sql = "SELECT * FROM expected_machines WHERE bmc_mac_address=$1";
    sqlx::query_as(sql)
        .bind(bmc_mac_address)
        .fetch_optional(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))
}

pub async fn find_by_id(
    txn: impl DbReader<'_>,
    id: Uuid,
) -> Result<Option<ExpectedMachine>, DatabaseError> {
    let sql = "SELECT * FROM expected_machines WHERE id=$1";
    sqlx::query_as(sql)
        .bind(id)
        .fetch_optional(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))
}

pub async fn find_many_by_bmc_mac_address(
    txn: &mut PgConnection,
    bmc_mac_addresses: &[MacAddress],
) -> DatabaseResult<HashMap<MacAddress, ExpectedMachine>> {
    let sql = "SELECT * FROM expected_machines WHERE bmc_mac_address=ANY($1)";
    let v: Vec<ExpectedMachine> = sqlx::query_as(sql)
        .bind(bmc_mac_addresses)
        .fetch_all(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))?;

    // expected_machines has a unique constraint on bmc_mac_address,
    // but if the constraint gets dropped and we have multiple mac addresses,
    // we want this code to generate an Err and not silently drop values
    // and/or return nothing.
    v.into_iter()
        .into_group_map_by(|exp| exp.bmc_mac_address)
        .drain()
        .map(|(k, mut v)| {
            if v.len() > 1 {
                Err(DatabaseError::AlreadyFoundError {
                    kind: "ExpectedMachine",
                    id: k.to_string(),
                })
            } else {
                Ok((k, v.pop().unwrap()))
            }
        })
        .collect()
}

// the expected machines table needs host mac addresses to control dhcp vending of ip's
// since the carbide dhcp server in some cases is not authoritative on a large network.
// Search and lock the host_nics field before vending an IP. Holding this row
// lock through interface preallocation serializes readers with ExpectedMachine
// updates and deletion.
pub async fn find_by_host_mac_address(
    txn: &mut PgConnection,
    host_mac_address: MacAddress,
) -> DatabaseResult<Option<ExpectedMachine>> {
    let query = "SELECT *
        FROM expected_machines
        WHERE bmc_mac_address = $1
           OR host_nics @> $2::jsonb
        ORDER BY bmc_mac_address
        FOR SHARE";
    find_by_host_mac_address_with_query(txn, host_mac_address, query).await
}

/// Repeat an initially empty nested-interface lookup after locking its MAC.
///
/// The caller must hold the transaction-scoped
/// `expected_machine_interface.<mac>` advisory lock. This deliberately uses a
/// plain `SELECT`: taking an ExpectedMachine row lock after the MAC lock would
/// invert the row-then-MAC order used by configuration updates.
pub async fn find_by_host_mac_address_after_interface_lock(
    txn: &mut PgConnection,
    host_mac_address: MacAddress,
) -> DatabaseResult<Option<ExpectedMachine>> {
    let query = "SELECT *
        FROM expected_machines
        WHERE bmc_mac_address = $1
           OR host_nics @> $2::jsonb
        ORDER BY bmc_mac_address";
    find_by_host_mac_address_with_query(txn, host_mac_address, query).await
}

async fn find_by_host_mac_address_with_query(
    txn: &mut PgConnection,
    host_mac_address: MacAddress,
    query: &'static str,
) -> DatabaseResult<Option<ExpectedMachine>> {
    let mac_address = serde_json::json!([{ "mac_address": host_mac_address.to_string() }]);
    let mut machines: Vec<ExpectedMachine> = sqlx::query_as(query)
        .bind(host_mac_address)
        .bind(sqlx::types::Json(mac_address))
        .fetch_all(txn)
        .await
        .map_err(|err| DatabaseError::query(query, err))?;
    let top_level_matches = machines
        .iter()
        .filter(|machine| machine.bmc_mac_address == host_mac_address)
        .count();
    let nested_matches = machines
        .iter()
        .flat_map(|machine| &machine.data.host_nics)
        .filter(|interface| interface.mac_address == host_mac_address)
        .count();
    if top_level_matches > 0 && nested_matches > 0 {
        return Err(DatabaseError::InvalidArgument(format!(
            "mac address {host_mac_address} is declared as both an ExpectedMachine BMC and nested interface",
        )));
    }
    match nested_matches {
        0 => Ok(None),
        1 => Ok(machines.drain(..).find(|machine| {
            machine
                .data
                .host_nics
                .iter()
                .any(|interface| interface.mac_address == host_mac_address)
        })),
        _ => Err(DatabaseError::InvalidArgument(format!(
            "multiple ExpectedMachine interface declarations use MAC address {host_mac_address}",
        ))),
    }
}

/// `lock_config_mutations_shared` lets ordinary ExpectedMachine writes run
/// together while serializing them with `ReplaceAll` and `DeleteAll`.
///
/// Callers take this before any ExpectedMachine lookup or lock. The complete
/// order is this set lock, ExpectedMachine rows and identity MACs,
/// interface-MAC advisory locks, and all relevant network-segment locks in
/// sorted order before machine-interface or address rows. Shared-segment
/// static and IPv4 flows also take per-address locks before conflicting row
/// writes. Stateful IPv6 is serialized by its exclusive segment lock and may
/// take address locks later. Taking the set lock inside `create`, `update`, or
/// `delete` would be too late for callers that already performed a lookup.
pub async fn lock_config_mutations_shared(txn: &mut PgConnection) -> DatabaseResult<()> {
    let query = "SELECT pg_advisory_xact_lock_shared(hashtextextended($1::text, 0))";
    sqlx::query(query)
        .bind(CONFIG_MUTATION_LOCK_KEY)
        .execute(txn)
        .await
        .map(|_| ())
        .map_err(|error| DatabaseError::query(query, error))
}

/// `lock_config_mutations_exclusive` keeps a set-wide replacement or deletion
/// stable from its first ExpectedMachine lookup through commit.
pub async fn lock_config_mutations_exclusive(txn: &mut PgConnection) -> DatabaseResult<()> {
    let query = "SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))";
    sqlx::query(query)
        .bind(CONFIG_MUTATION_LOCK_KEY)
        .execute(txn)
        .await
        .map(|_| ())
        .map_err(|error| DatabaseError::query(query, error))
}

/// Serialize ExpectedMachine identity changes by MAC in a stable order.
pub async fn lock_identity_macs(
    txn: &mut PgConnection,
    identity_macs: impl IntoIterator<Item = MacAddress>,
) -> DatabaseResult<()> {
    let mut identity_macs = identity_macs.into_iter().collect::<Vec<_>>();
    identity_macs.sort_by_key(ToString::to_string);
    identity_macs.dedup();

    let lock_query = "SELECT pg_advisory_xact_lock(
        hashtextextended('expected_machine_identity.' || $1::text, 0)
    )";
    for mac_address in &identity_macs {
        sqlx::query(lock_query)
            .bind(mac_address)
            .execute(&mut *txn)
            .await
            .map_err(|error| DatabaseError::query(lock_query, error))?;
    }
    Ok(())
}

/// Reject a new or identity-changing ExpectedMachine when any of its BMC or
/// nested interface MACs is already declared by another ExpectedMachine.
/// `current_bmc_mac_address` identifies the update target even when its legacy
/// database row has no ID.
pub async fn validate_identity_macs_available(
    txn: &mut PgConnection,
    machine: &ExpectedMachine,
    current_bmc_mac_address: Option<MacAddress>,
) -> DatabaseResult<()> {
    let identity_macs = machine
        .data
        .host_nics
        .iter()
        .map(|interface| interface.mac_address)
        .chain(std::iter::once(machine.bmc_mac_address))
        .collect::<Vec<_>>();
    lock_identity_macs(&mut *txn, identity_macs.iter().copied()).await?;

    let query = "SELECT bmc_mac_address
        FROM expected_machines
        WHERE bmc_mac_address IS DISTINCT FROM $1::macaddr
          AND (
              bmc_mac_address = ANY($2::macaddr[])
              OR EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(host_nics) AS declared_interface
                  WHERE (declared_interface->>'mac_address')::macaddr = ANY($2::macaddr[])
              )
          )
        ORDER BY bmc_mac_address
        LIMIT 1";
    if let Some(conflicting_bmc_mac) = sqlx::query_scalar::<_, MacAddress>(query)
        .bind(current_bmc_mac_address)
        .bind(&identity_macs)
        .fetch_optional(txn)
        .await
        .map_err(|error| DatabaseError::query(query, error))?
    {
        return Err(DatabaseError::InvalidArgument(format!(
            "expected machine identity MAC conflicts with the machine whose BMC MAC is {conflicting_bmc_mac}",
        )));
    }

    Ok(())
}

pub async fn find_one_linked(
    txn: &mut PgConnection,
    bmc_mac_address: MacAddress,
) -> DatabaseResult<Option<LinkedExpectedMachine>> {
    let sql = r#"
 SELECT
 em.serial_number,
 em.bmc_mac_address,
 mi.id AS interface_id,
 ee.address AS address,
 mi.machine_id,
 em.id AS expected_machine_id
FROM expected_machines em
 LEFT JOIN machine_interfaces mi ON em.bmc_mac_address = mi.mac_address
 LEFT JOIN machine_interface_addresses mia ON mi.id = mia.interface_id
 LEFT JOIN explored_endpoints ee ON mia.address = ee.address
 WHERE em.bmc_mac_address = $1
 ORDER BY em.bmc_mac_address
 "#;
    sqlx::query_as(sql)
        .bind(bmc_mac_address)
        .fetch_optional(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))
}

pub async fn find_all(txn: impl DbReader<'_>) -> DatabaseResult<Vec<ExpectedMachine>> {
    let sql = "SELECT * FROM expected_machines";
    sqlx::query_as(sql)
        .fetch_all(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))
}

/// find_all_by_rack_id returns all expected machines for a given rack_id.
pub async fn find_all_by_rack_id(
    txn: &mut PgConnection,
    rack_id: &RackId,
) -> DatabaseResult<Vec<ExpectedMachine>> {
    let sql = "SELECT * FROM expected_machines WHERE rack_id=$1";
    sqlx::query_as(sql)
        .bind(rack_id)
        .fetch_all(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))
}

pub async fn find_all_linked(txn: impl DbReader<'_>) -> DatabaseResult<Vec<LinkedExpectedMachine>> {
    let sql = r#"
 SELECT
 em.serial_number,
 em.bmc_mac_address,
 mi.id AS interface_id,
 ee.address AS address,
 mi.machine_id,
 em.id AS expected_machine_id
FROM expected_machines em
 LEFT JOIN machine_interfaces mi ON em.bmc_mac_address = mi.mac_address
 LEFT JOIN machine_interface_addresses mia ON mi.id = mia.interface_id
 LEFT JOIN explored_endpoints ee ON mia.address = ee.address
 ORDER BY em.bmc_mac_address
 "#;
    sqlx::query_as(sql)
        .fetch_all(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))
}

/// Returns host BMC endpoints that Site Explorer has explored but whose MAC is
/// not listed in any of `expected_machines`, `expected_power_shelves`, or
/// `expected_switches`. DPUs, power shelves, and switches are filtered out so
/// the result only contains actual host BMCs. Rows with `machine_id = Some`
/// are orphans (already ingested before the `expected_machines` entry was
/// removed).
pub async fn find_all_unexpected(txn: impl DbReader<'_>) -> DatabaseResult<Vec<UnexpectedMachine>> {
    #[derive(FromRow)]
    struct UnexpectedRow {
        address: IpAddr,
        bmc_mac_address: MacAddress,
        exploration_report: sqlx::types::Json<EndpointExplorationReport>,
        machine_id: Option<MachineId>,
    }

    let sql = r#"
SELECT
    ee.address,
    mi.mac_address AS bmc_mac_address,
    ee.exploration_report,
    mi.machine_id
FROM explored_endpoints ee
    LEFT JOIN machine_interface_addresses mia ON ee.address = mia.address
    LEFT JOIN machine_interfaces mi ON mia.interface_id = mi.id
WHERE mi.mac_address IS NOT NULL
  AND ee.exploration_report->>'EndpointType' = 'Bmc'
  AND mi.mac_address NOT IN (SELECT bmc_mac_address FROM expected_machines)
  AND mi.mac_address NOT IN (SELECT bmc_mac_address FROM expected_power_shelves)
  AND mi.mac_address NOT IN (SELECT bmc_mac_address FROM expected_switches)
ORDER BY ee.address
    "#;

    let rows: Vec<UnexpectedRow> = sqlx::query_as(sql)
        .fetch_all(txn)
        .await
        .map_err(|err| DatabaseError::query(sql, err))?;

    Ok(rows
        .into_iter()
        .filter(|row| {
            !row.exploration_report.0.is_dpu()
                && !row.exploration_report.0.is_power_shelf()
                && !row.exploration_report.0.is_switch()
        })
        .map(|row| UnexpectedMachine {
            address: row.address,
            bmc_mac_address: row.bmc_mac_address,
            machine_id: row.machine_id,
        })
        .collect())
}

pub async fn update_bmc_credentials<'a>(
    value: &'a mut ExpectedMachine,
    txn: &mut PgConnection,
    bmc_username: String,
    bmc_password: String,
) -> DatabaseResult<&'a mut ExpectedMachine> {
    let query = "UPDATE expected_machines SET bmc_username=$1, bmc_password=$2 WHERE bmc_mac_address=$3 RETURNING bmc_mac_address";

    sqlx::query_as::<_, ()>(query)
        .bind(&bmc_username)
        .bind(&bmc_password)
        .bind(value.bmc_mac_address)
        .fetch_one(txn)
        .await
        .map_err(|err: sqlx::Error| match err {
            sqlx::Error::RowNotFound => DatabaseError::NotFoundError {
                kind: "expected_machine",
                id: value.bmc_mac_address.to_string(),
            },
            _ => DatabaseError::query(query, err),
        })?;

    value.data.bmc_username = bmc_username;
    value.data.bmc_password = bmc_password;

    Ok(value)
}

/// Inserts a new expected machine row. If the id field is None, a new UUID is generated.
///
/// Persisted `bmc_ip_address` is the configured static BMC IP; the API layer is responsible for
/// creating/updating the matching `machine_interface` rows before calling this (see
/// `preallocate_machine_interface` / `update_preallocated_machine_interface`).
pub async fn create(
    txn: &mut PgConnection,
    machine: ExpectedMachine,
) -> DatabaseResult<ExpectedMachine> {
    let id = machine.id.unwrap_or_else(Uuid::new_v4);
    let query = "INSERT INTO expected_machines
            (id, bmc_mac_address, bmc_username, bmc_password, serial_number, fallback_dpu_serial_numbers, metadata_name, metadata_description, metadata_labels, sku_id, host_nics, rack_id, default_pause_ingestion_and_poweron, dpf_enabled, bmc_ip_address, bmc_retain_credentials, dpu_mode, bmc_ip_allocation, host_lifecycle_profile)
            VALUES
            ($1::uuid, $2::macaddr, $3::varchar, $4::varchar, $5::varchar, $6::text[], $7, $8, $9::jsonb, $10::varchar, $11::jsonb, $12, $13, $14, $15::inet, $16, $17, $18, $19::jsonb) RETURNING *";

    sqlx::query_as(query)
        .bind(id)
        .bind(machine.bmc_mac_address)
        .bind(&machine.data.bmc_username)
        .bind(&machine.data.bmc_password)
        .bind(&machine.data.serial_number)
        .bind(&machine.data.fallback_dpu_serial_numbers)
        .bind(&machine.data.metadata.name)
        .bind(&machine.data.metadata.description)
        .bind(sqlx::types::Json(&machine.data.metadata.labels))
        .bind(&machine.data.sku_id)
        .bind(sqlx::types::Json(&machine.data.host_nics))
        .bind(&machine.data.rack_id)
        .bind(
            machine
                .data
                .default_pause_ingestion_and_poweron
                .unwrap_or(false),
        )
        .bind(machine.data.dpf_enabled.unwrap_or(true))
        .bind(machine.data.bmc_ip_address)
        .bind(machine.data.bmc_retain_credentials.unwrap_or(false))
        .bind(machine.data.dpu_policy)
        .bind(machine.data.bmc_ip_allocation)
        .bind(sqlx::types::Json(&machine.data.host_lifecycle_profile))
        .fetch_one(txn)
        .await
        .map_err(|err: sqlx::Error| match err {
            sqlx::Error::Database(e) if e.constraint() == Some(SQL_VIOLATION_DUPLICATE_MAC) => {
                DatabaseError::ExpectedHostDuplicateMacAddress(machine.bmc_mac_address)
            }
            _ => DatabaseError::query(query, err),
        })
}

/// find returns an expected machine by id if provided, otherwise by bmc_mac_address.
pub async fn find(
    txn: impl DbReader<'_>,
    req: &ExpectedMachineRequest,
) -> DatabaseResult<Option<ExpectedMachine>> {
    if let Some(id) = req.id {
        find_by_id(txn, id).await
    } else if let Some(mac) = req.bmc_mac_address {
        find_by_bmc_mac_address(txn, mac).await
    } else {
        Err(DatabaseError::InvalidArgument(
            "either id or bmc_mac_address must be provided".into(),
        ))
    }
}

/// Find and lock an ExpectedMachine for an update transaction.
pub async fn find_for_update(
    txn: &mut PgConnection,
    req: &ExpectedMachineRequest,
) -> DatabaseResult<Option<ExpectedMachine>> {
    let (sql, id, mac) = if let Some(id) = req.id {
        (
            "SELECT * FROM expected_machines WHERE id=$1 FOR UPDATE",
            Some(id),
            None,
        )
    } else if let Some(mac) = req.bmc_mac_address {
        (
            "SELECT * FROM expected_machines WHERE bmc_mac_address=$1 FOR UPDATE",
            None,
            Some(mac),
        )
    } else {
        return Err(DatabaseError::InvalidArgument(
            "either id or bmc_mac_address must be provided".into(),
        ));
    };

    let mut query = sqlx::query_as(sql);
    if let Some(id) = id {
        query = query.bind(id);
    } else if let Some(mac) = mac {
        query = query.bind(mac);
    }
    query
        .fetch_optional(txn)
        .await
        .map_err(|error| DatabaseError::query(sql, error))
}

/// Find and lock every ExpectedMachine in stable MAC-address order.
pub async fn find_all_for_update(txn: &mut PgConnection) -> DatabaseResult<Vec<ExpectedMachine>> {
    let sql = "SELECT * FROM expected_machines ORDER BY bmc_mac_address FOR UPDATE";
    sqlx::query_as(sql)
        .fetch_all(txn)
        .await
        .map_err(|error| DatabaseError::query(sql, error))
}

/// delete deletes an expected machine by id if provided, otherwise by bmc_mac_address.
pub async fn delete(txn: &mut PgConnection, req: &ExpectedMachineRequest) -> DatabaseResult<()> {
    if let Some(id) = req.id {
        delete_by_id(txn, id).await
    } else if let Some(mac) = req.bmc_mac_address {
        delete_by_mac(txn, mac).await
    } else {
        Err(DatabaseError::InvalidArgument(
            "either id or bmc_mac_address must be provided".into(),
        ))
    }
}

/// delete_by_mac deletes an expected machine by bmc_mac_address.
pub async fn delete_by_mac(
    txn: &mut PgConnection,
    bmc_mac_address: MacAddress,
) -> DatabaseResult<()> {
    let query = "DELETE FROM expected_machines WHERE bmc_mac_address=$1";

    let result = sqlx::query(query)
        .bind(bmc_mac_address)
        .execute(txn)
        .await
        .map_err(|err| DatabaseError::query(query, err))?;

    if result.rows_affected() == 0 {
        return Err(DatabaseError::NotFoundError {
            kind: "expected_machine",
            id: bmc_mac_address.to_string(),
        });
    }

    Ok(())
}

pub async fn delete_by_id(txn: &mut PgConnection, id: Uuid) -> DatabaseResult<()> {
    let query = "DELETE FROM expected_machines WHERE id=$1";

    let result = sqlx::query(query)
        .bind(id)
        .execute(txn)
        .await
        .map_err(|err| DatabaseError::query(query, err))?;

    if result.rows_affected() == 0 {
        return Err(DatabaseError::NotFoundError {
            kind: "expected_machine",
            id: id.to_string(),
        });
    }

    Ok(())
}

pub async fn clear(txn: &mut PgConnection) -> Result<(), DatabaseError> {
    let query = "DELETE FROM expected_machines";

    sqlx::query(query)
        .execute(txn)
        .await
        .map(|_| ())
        .map_err(|e| DatabaseError::query(query, e))
}

/// Updates an existing expected machine. If id is set, matches by ID; otherwise matches by
/// `bmc_mac_address`. Includes `bmc_ip_address` when the operator configures a static BMC IP.
pub async fn update(txn: &mut PgConnection, machine: &ExpectedMachine) -> DatabaseResult<()> {
    macro_rules! update_expected_machine_query {
        ($where_clause:literal) => {
            concat!(
                "UPDATE expected_machines \
                 SET bmc_username=$1, bmc_password=$2, serial_number=$3, \
                     fallback_dpu_serial_numbers=$4, metadata_name=$5, metadata_description=$6, \
                     metadata_labels=$7, sku_id=$8, host_nics=$9::jsonb, rack_id=$10, \
                     default_pause_ingestion_and_poweron=COALESCE($11, default_pause_ingestion_and_poweron), \
                     dpf_enabled=COALESCE($12, dpf_enabled), \
                     bmc_ip_address=$13, \
                     bmc_retain_credentials=COALESCE($14, bmc_retain_credentials), \
                     dpu_mode=$15, \
                     bmc_ip_allocation=$16, \
                     host_lifecycle_profile=COALESCE($17, host_lifecycle_profile) \
                 WHERE ",
                $where_clause,
            )
        };
    }

    let (query, target_id) = match machine.id {
        Some(id) => (
            update_expected_machine_query!("id=$18::uuid"),
            id.to_string(),
        ),
        None => (
            update_expected_machine_query!("bmc_mac_address=$18::macaddr"),
            machine.bmc_mac_address.to_string(),
        ),
    };

    let result = sqlx::query(query)
        .bind(&machine.data.bmc_username)
        .bind(&machine.data.bmc_password)
        .bind(&machine.data.serial_number)
        .bind(&machine.data.fallback_dpu_serial_numbers)
        .bind(&machine.data.metadata.name)
        .bind(&machine.data.metadata.description)
        .bind(sqlx::types::Json(&machine.data.metadata.labels))
        .bind(&machine.data.sku_id)
        .bind(sqlx::types::Json(&machine.data.host_nics))
        .bind(&machine.data.rack_id)
        .bind(machine.data.default_pause_ingestion_and_poweron)
        .bind(machine.data.dpf_enabled)
        .bind(machine.data.bmc_ip_address)
        .bind(machine.data.bmc_retain_credentials)
        .bind(machine.data.dpu_policy)
        .bind(machine.data.bmc_ip_allocation)
        .bind(
            (!machine.data.host_lifecycle_profile.is_empty())
                .then_some(sqlx::types::Json(&machine.data.host_lifecycle_profile)),
        )
        .bind(&target_id)
        .execute(&mut *txn)
        .await
        .map_err(|err| DatabaseError::query(query, err))?;

    if result.rows_affected() == 0 {
        return Err(DatabaseError::NotFoundError {
            kind: "expected_machine",
            id: target_id,
        });
    }
    Ok(())
}

#[cfg(test)]
mod test_allocation_state_migration;

#[cfg(test)]
mod tests;
