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

use carbide_uuid::machine::{MachineIdSource, MachineType};

use super::*;

async fn insert_bmc_owner(
    txn: &mut PgConnection,
    name: &str,
    mac_address: MacAddress,
    address: IpAddr,
) -> Result<BmcInterfaceOwner, sqlx::Error> {
    let segment_id: NetworkSegmentId = sqlx::query_scalar(
        "INSERT INTO network_segments (name, version)
         VALUES ($1, 'bmc-owner-test')
         RETURNING id",
    )
    .bind(format!("{name}-segment"))
    .fetch_one(&mut *txn)
    .await?;
    let interface_id: MachineInterfaceId = sqlx::query_scalar(
        "INSERT INTO machine_interfaces
             (segment_id, mac_address, primary_interface, hostname, interface_type)
         VALUES ($1, $2, false, $3, 'Bmc')
         RETURNING id",
    )
    .bind(segment_id)
    .bind(mac_address)
    .bind(name)
    .fetch_one(&mut *txn)
    .await?;
    sqlx::query(
        "INSERT INTO machine_interface_addresses (interface_id, address)
         VALUES ($1, $2)",
    )
    .bind(interface_id)
    .bind(address)
    .execute(txn)
    .await?;

    Ok(BmcInterfaceOwner {
        interface_id,
        mac_address,
        segment_id,
    })
}

#[crate::sqlx_test]
async fn test_find_interface_by_bmc_ip_returns_none(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = pool.begin().await?;

    let owner = find_interface_by_bmc_ip(&mut txn, "192.0.2.10".parse()?).await?;

    assert_eq!(owner, None);
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_find_interface_by_bmc_ip_returns_owner_identity(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = "192.0.2.11".parse()?;
    let mut txn = pool.begin().await?;
    let expected = insert_bmc_owner(
        &mut txn,
        "bmc-owner-one",
        "02:00:00:00:10:01".parse()?,
        address,
    )
    .await?;

    let owner = find_interface_by_bmc_ip(&mut txn, address).await?;

    assert_eq!(owner, Some(expected));
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_find_interface_by_bmc_ip_rejects_ambiguous_ownership(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = "192.0.2.12".parse()?;
    let mut txn = pool.begin().await?;
    insert_bmc_owner(
        &mut txn,
        "bmc-owner-first",
        "02:00:00:00:10:02".parse()?,
        address,
    )
    .await?;
    insert_bmc_owner(
        &mut txn,
        "bmc-owner-second",
        "02:00:00:00:10:03".parse()?,
        address,
    )
    .await?;

    let error = find_interface_by_bmc_ip(&mut txn, address)
        .await
        .expect_err("ambiguous BMC address ownership must be rejected");

    assert!(matches!(
        error,
        DatabaseError::Internal { message }
            if message == format!("multiple machine interfaces own BMC IP address {address}")
    ));
    txn.rollback().await?;
    Ok(())
}

async fn assert_update_waits_for_lock(
    pool: &sqlx::PgPool,
    query: &'static str,
    interface_id: MachineInterfaceId,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut contender = pool.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '250ms'")
        .execute(&mut *contender)
        .await?;
    let error = sqlx::query(query)
        .bind(interface_id)
        .execute(&mut *contender)
        .await
        .expect_err("row update must wait for the BMC owner lookup");
    let code = error
        .as_database_error()
        .and_then(|error| error.code())
        .map(|code| code.into_owned());
    assert_eq!(code.as_deref(), Some("55P03"));
    contender.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test probes locks held by another transaction.
async fn test_find_interface_by_bmc_ip_for_update_locks_owner_rows(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = "192.0.2.13".parse()?;
    let mut setup = pool.begin().await?;
    let expected = insert_bmc_owner(
        &mut setup,
        "bmc-owner-locked",
        "02:00:00:00:10:04".parse()?,
        address,
    )
    .await?;
    setup.commit().await?;

    let mut owner = pool.begin().await?;
    let locked = find_interface_by_bmc_ip_for_update(&mut owner, address).await?;
    assert_eq!(locked, Some(expected));

    assert_update_waits_for_lock(
        &pool,
        "UPDATE machine_interfaces SET id = id WHERE id = $1",
        expected.interface_id,
    )
    .await?;
    assert_update_waits_for_lock(
        &pool,
        "UPDATE machine_interface_addresses SET id = id WHERE interface_id = $1",
        expected.interface_id,
    )
    .await?;

    owner.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test probes locks held by another transaction.
async fn test_update_bmc_network_locks_address_owner_before_association(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = "192.0.2.14".parse()?;
    let mac_address = "02:00:00:00:10:05".parse()?;
    let machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x14; 32],
        MachineType::Host,
    );
    let mut setup = pool.begin().await?;
    let expected =
        insert_bmc_owner(&mut setup, "bmc-owner-associated", mac_address, address).await?;
    sqlx::query("INSERT INTO machines (id, dpf) VALUES ($1, '{}'::jsonb)")
        .bind(machine_id)
        .execute(&mut *setup)
        .await?;
    sqlx::query(
        "INSERT INTO machine_topologies (machine_id, topology)
         VALUES ($1, '{}'::jsonb)",
    )
    .bind(machine_id)
    .execute(&mut *setup)
    .await?;
    setup.commit().await?;

    let mut owner = pool.begin().await?;
    crate::machine_interface::lock_expected_machine_interface_macs(&mut owner, [mac_address])
        .await?;
    let mut bmc_info = BmcInfo {
        ip: Some(address),
        mac: Some(mac_address),
        ..Default::default()
    };
    update_bmc_network_into_machine_interfaces(&mut owner, &machine_id, &mut bmc_info, None)
        .await?;
    assert_eq!(bmc_info.machine_interface_id, Some(expected.interface_id));

    assert_update_waits_for_lock(
        &pool,
        "UPDATE machine_interface_addresses SET address = address WHERE interface_id = $1",
        expected.interface_id,
    )
    .await?;

    owner.rollback().await?;
    Ok(())
}
