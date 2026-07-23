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

use carbide_uuid::machine::{MachineId, MachineIdSource, MachineInterfaceId, MachineType};
use carbide_uuid::network::NetworkSegmentId;
use model::allocation_type::AllocationType;
use model::expected_machine::{
    ExpectedHostNic, ExpectedInterfaceIpAllocation, ExpectedInterfaceRole, ExpectedMachine,
    ExpectedMachineData,
};
use model::machine::ManagedHostState;
use model::machine_interface::InterfaceType;
use model::machine_interface_address::InterfaceAssociationType;
use model::network_prefix::NewNetworkPrefix;
use model::network_segment::{
    AllocationStrategy, NetworkSegmentControllerState, NetworkSegmentType, NewNetworkSegment,
};
use model::predicted_machine_interface::NewPredictedMachineInterface;

use super::*;
use crate as db;

async fn create_static_assignments_segment(
    pool: &sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = db::Transaction::begin(pool).await?;
    db::network_segment::persist(
        NewNetworkSegment {
            id: uuid::Uuid::new_v4().into(),
            name: db::network_segment::STATIC_ASSIGNMENTS_SEGMENT_NAME.to_string(),
            subdomain_id: None,
            vpc_id: None,
            mtu: 1500,
            prefixes: vec![NewNetworkPrefix {
                prefix: "169.254.254.254/32".parse().unwrap(),
                gateway: None,
                dhcpv6_link_address: None,
                num_reserved: 1,
            }],
            vlan_id: None,
            vni: None,
            segment_type: NetworkSegmentType::Underlay,
            can_stretch: Some(false),
            allocation_strategy: AllocationStrategy::Reserved,
        },
        txn.as_pgconn(),
        NetworkSegmentControllerState::Ready,
    )
    .await?;
    txn.commit().await?;

    Ok(())
}

async fn create_test_segment(
    pool: &sqlx::PgPool,
    name: &str,
) -> Result<NetworkSegmentId, Box<dyn std::error::Error>> {
    let segment_id = NetworkSegmentId::new();
    let mut txn = db::Transaction::begin(pool).await?;
    db::network_segment::persist(
        NewNetworkSegment {
            id: segment_id,
            name: name.to_string(),
            subdomain_id: None,
            vpc_id: None,
            mtu: 1500,
            prefixes: Vec::new(),
            vlan_id: None,
            vni: None,
            segment_type: NetworkSegmentType::HostInband,
            can_stretch: Some(false),
            allocation_strategy: AllocationStrategy::Reserved,
        },
        txn.as_pgconn(),
        NetworkSegmentControllerState::Ready,
    )
    .await?;
    txn.commit().await?;

    Ok(segment_id)
}

/// A MAC identifies one physical interface even when stale or transitional
/// rows represent it on more than one segment. Site Explorer learns one
/// vendor-native Redfish id for that interface, so `set_boot_interface_id`
/// updates every row for the MAC rather than whichever segment happened to
/// report first.
#[crate::sqlx_test]
async fn set_boot_interface_id_updates_every_segment_row_for_mac(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let segment_a = create_test_segment(&pool, "boot-id-segment-a").await?;
    let segment_b = create_test_segment(&pool, "boot-id-segment-b").await?;
    let boot_mac: MacAddress = "7A:7B:7C:7D:7E:41".parse()?;
    let other_mac: MacAddress = "7A:7B:7C:7D:7E:42".parse()?;

    let mut txn = db::Transaction::begin(&pool).await?;
    let query = "
INSERT INTO machine_interfaces
    (segment_id, mac_address, primary_interface, hostname)
VALUES
    ($1, $2, false, 'boot-a'),
    ($3, $2, false, 'boot-b'),
    ($1, $4, false, 'other')";
    sqlx::query(query)
        .bind(segment_a)
        .bind(boot_mac)
        .bind(segment_b)
        .bind(other_mac)
        .execute(txn.as_pgconn())
        .await?;

    set_boot_interface_id(boot_mac, "NIC.Slot.7-1-1", txn.as_pgconn()).await?;

    let boot_ids: Vec<Option<String>> = sqlx::query_scalar(
        "SELECT boot_interface_id FROM machine_interfaces WHERE mac_address=$1 ORDER BY hostname",
    )
    .bind(boot_mac)
    .fetch_all(txn.as_pgconn())
    .await?;
    let other_id: Option<String> =
        sqlx::query_scalar("SELECT boot_interface_id FROM machine_interfaces WHERE mac_address=$1")
            .bind(other_mac)
            .fetch_one(txn.as_pgconn())
            .await?;

    assert_eq!(
        boot_ids,
        vec![
            Some("NIC.Slot.7-1-1".to_string()),
            Some("NIC.Slot.7-1-1".to_string())
        ]
    );
    assert_eq!(other_id, None, "a different MAC must remain unchanged");

    Ok(())
}

async fn create_managed_segment(
    pool: &sqlx::PgPool,
    name: &str,
    prefix: &str,
    segment_type: NetworkSegmentType,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = db::Transaction::begin(pool).await?;
    db::network_segment::persist(
        NewNetworkSegment {
            id: uuid::Uuid::new_v4().into(),
            name: name.to_string(),
            subdomain_id: None,
            vpc_id: None,
            mtu: 1500,
            prefixes: vec![NewNetworkPrefix {
                prefix: prefix.parse().unwrap(),
                gateway: None,
                dhcpv6_link_address: None,
                num_reserved: 0,
            }],
            vlan_id: None,
            vni: None,
            segment_type,
            can_stretch: Some(false),
            allocation_strategy: AllocationStrategy::Reserved,
        },
        txn.as_pgconn(),
        NetworkSegmentControllerState::Ready,
    )
    .await?;
    txn.commit().await?;

    Ok(())
}

async fn insert_anonymous_interface(
    txn: &mut sqlx::PgConnection,
    mac_address: MacAddress,
    primary_interface: bool,
) -> Result<MachineInterfaceId, Box<dyn std::error::Error>> {
    let segment_id = NetworkSegmentId::from(uuid::Uuid::new_v4());
    sqlx::query(
        "INSERT INTO network_segments (id, name, version)
         VALUES ($1, $2, 'marker-test')",
    )
    .bind(segment_id)
    .bind(format!("marker-test-{segment_id}"))
    .execute(&mut *txn)
    .await?;

    Ok(sqlx::query_scalar(
        "INSERT INTO machine_interfaces
             (segment_id, mac_address, primary_interface, hostname)
         VALUES ($1, $2, $3, $4)
         RETURNING id",
    )
    .bind(segment_id)
    .bind(mac_address)
    .bind(primary_interface)
    .bind(format!(
        "marker-{}",
        mac_address.to_string().replace(':', "")
    ))
    .fetch_one(txn)
    .await?)
}

async fn expected_interface_capture(
    txn: &mut sqlx::PgConnection,
    interface_id: MachineInterfaceId,
) -> Result<(Option<sqlx::types::Json<ExpectedHostNic>>, bool), sqlx::Error> {
    sqlx::query_as(
        "SELECT expected_interface, expected_interface_captured
         FROM machine_interfaces
         WHERE id = $1",
    )
    .bind(interface_id)
    .fetch_one(txn)
    .await
}

async fn wait_for_advisory_lock_wait(pool: &sqlx::PgPool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*)
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND wait_event = 'advisory'
                   AND query LIKE '%pg_advisory_xact_lock%'",
            )
            .fetch_one(pool)
            .await
            .expect("inspect advisory lock wait");
            if waiting > 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request did not reach the advisory-lock queue");
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test changes a segment while reconciliation waits on its lock.
async fn admin_segment_snapshot_is_loaded_after_exclusive_lock(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let segment_id = NetworkSegmentId::new();
    let mut setup = db::Transaction::begin(&pool).await?;
    db::network_segment::persist(
        NewNetworkSegment {
            id: segment_id,
            name: "admin-lock-snapshot".to_string(),
            subdomain_id: None,
            vpc_id: None,
            mtu: 1500,
            prefixes: Vec::new(),
            vlan_id: None,
            vni: None,
            segment_type: NetworkSegmentType::Admin,
            can_stretch: Some(false),
            allocation_strategy: AllocationStrategy::Reserved,
        },
        setup.as_pgconn(),
        NetworkSegmentControllerState::Ready,
    )
    .await?;
    setup.commit().await?;

    let mut lock_owner = db::Transaction::begin(&pool).await?;
    lock_network_segments_exclusive(lock_owner.as_pgconn(), std::slice::from_ref(&segment_id))
        .await?;

    let loader_pool = pool.clone();
    let loading = tokio::spawn(async move {
        let mut txn = db::Transaction::begin(&loader_pool).await?;
        let result = load_and_lock_all_admin_segments(&mut txn).await;
        txn.rollback().await?;
        result
    });
    wait_for_advisory_lock_wait(&pool).await;

    let mut writer = db::Transaction::begin(&pool).await?;
    let updated = sqlx::query(
        "UPDATE network_segments
         SET network_segment_type = 'underlay'
         WHERE id = $1",
    )
    .bind(segment_id)
    .execute(writer.as_pgconn())
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    writer.commit().await?;

    lock_owner.commit().await?;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), loading)
        .await
        .expect("Admin segment load did not finish after releasing its lock")
        .expect("Admin segment load task panicked")
        .expect_err("segment changes while locking must reject the stale snapshot");
    assert!(matches!(error, DatabaseError::FailedPrecondition(_)));

    Ok(())
}

#[crate::sqlx_test]
async fn test_force_deleting_machine_rejects_interface_associations(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = db::Transaction::begin(&pool).await?;
    let machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x82; 32],
        MachineType::Host,
    );
    sqlx::query(
        "INSERT INTO machines (id, dpf, controller_state)
         VALUES ($1, '{}'::jsonb, $2)",
    )
    .bind(machine_id)
    .bind(sqlx::types::Json(ManagedHostState::ForceDeletion))
    .execute(txn.as_pgconn())
    .await?;
    let interface_macs = [
        "7A:7B:7C:7D:82:01".parse()?,
        "7A:7B:7C:7D:82:02".parse()?,
        "7A:7B:7C:7D:82:03".parse()?,
    ];
    let mut interface_ids = Vec::with_capacity(interface_macs.len());
    for mac_address in interface_macs {
        interface_ids.push(insert_anonymous_interface(txn.as_pgconn(), mac_address, false).await?);
    }

    let results = [
        associate_interface_with_machine(
            &interface_ids[0],
            MachineInterfaceAssociation::Machine(machine_id),
            txn.as_pgconn(),
        )
        .await,
        associate_bmc_interface(
            &interface_ids[1],
            MachineInterfaceAssociation::Machine(machine_id),
            txn.as_pgconn(),
        )
        .await,
        associate_interface_with_dpu_machine(&interface_ids[2], &machine_id, txn.as_pgconn()).await,
    ];
    for result in results {
        assert!(
            matches!(
                &result,
                Err(DatabaseError::FailedPrecondition(message))
                    if message.contains("force-deleted")
            ),
            "association must reject a force-deleting machine: {result:?}",
        );
    }

    interface_ids.sort_unstable();
    let stored_associations: Vec<(MachineInterfaceId, Option<MachineId>, Option<MachineId>)> =
        sqlx::query_as(
            "SELECT id, machine_id, attached_dpu_machine_id
             FROM machine_interfaces
             WHERE id = ANY($1)
             ORDER BY id",
        )
        .bind(&interface_ids)
        .fetch_all(txn.as_pgconn())
        .await?;
    assert_eq!(stored_associations.len(), interface_ids.len());
    assert!(
        stored_associations
            .iter()
            .all(|(_, machine_id, attached_dpu_machine_id)| {
                machine_id.is_none() && attached_dpu_machine_id.is_none()
            }),
        "rejected associations must not change interface ownership",
    );

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_dpu_association_rejects_force_deleting_interface_owner(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let host_machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x84; 32],
        MachineType::Host,
    );
    let dpu_machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x85; 32],
        MachineType::Dpu,
    );
    let interface_id = {
        let mut txn = db::Transaction::begin(&pool).await?;
        sqlx::query(
            "INSERT INTO machines (id, dpf, controller_state)
             VALUES
                 ($1, '{}'::jsonb, $3),
                 ($2, '{}'::jsonb, $3)",
        )
        .bind(host_machine_id)
        .bind(dpu_machine_id)
        .bind(sqlx::types::Json(ManagedHostState::Created))
        .execute(txn.as_pgconn())
        .await?;
        let interface_id =
            insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:84:01".parse()?, false)
                .await?;
        associate_interface_with_machine(
            &interface_id,
            MachineInterfaceAssociation::Machine(host_machine_id),
            txn.as_pgconn(),
        )
        .await?;
        txn.commit().await?;
        interface_id
    };

    let mut txn = db::Transaction::begin(&pool).await?;
    sqlx::query("UPDATE machines SET controller_state = $2 WHERE id = $1")
        .bind(host_machine_id)
        .bind(sqlx::types::Json(ManagedHostState::ForceDeletion))
        .execute(txn.as_pgconn())
        .await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    let result =
        associate_interface_with_dpu_machine(&interface_id, &dpu_machine_id, txn.as_pgconn()).await;
    assert!(
        matches!(
            &result,
            Err(DatabaseError::FailedPrecondition(message))
                if message.contains(&host_machine_id.to_string())
                    && message.contains("force-deleted")
        ),
        "DPU association must reject a force-deleting interface owner: {result:?}",
    );
    let attached_dpu_machine_id: Option<MachineId> =
        sqlx::query_scalar("SELECT attached_dpu_machine_id FROM machine_interfaces WHERE id = $1")
            .bind(interface_id)
            .fetch_one(txn.as_pgconn())
            .await?;
    assert!(
        attached_dpu_machine_id.is_none(),
        "rejected DPU association must not change the interface",
    );
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_associations_reject_reassignment_away_from_force_deleting_machine(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let old_host_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x86; 32],
        MachineType::Host,
    );
    let new_host_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x87; 32],
        MachineType::Host,
    );
    let old_dpu_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x88; 32],
        MachineType::Dpu,
    );
    let new_dpu_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x89; 32],
        MachineType::Dpu,
    );

    let mut txn = db::Transaction::begin(&pool).await?;
    sqlx::query(
        "INSERT INTO machines (id, dpf, controller_state)
         VALUES
             ($1, '{}'::jsonb, $5),
             ($2, '{}'::jsonb, $5),
             ($3, '{}'::jsonb, $5),
             ($4, '{}'::jsonb, $5)",
    )
    .bind(old_host_id)
    .bind(new_host_id)
    .bind(old_dpu_id)
    .bind(new_dpu_id)
    .bind(sqlx::types::Json(ManagedHostState::Created))
    .execute(txn.as_pgconn())
    .await?;
    let data_interface =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:86:01".parse()?, false).await?;
    let bmc_interface =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:86:02".parse()?, false).await?;
    let dpu_interface =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:86:03".parse()?, false).await?;
    associate_interface_with_machine(
        &data_interface,
        MachineInterfaceAssociation::Machine(old_host_id),
        txn.as_pgconn(),
    )
    .await?;
    associate_bmc_interface(
        &bmc_interface,
        MachineInterfaceAssociation::Machine(old_host_id),
        txn.as_pgconn(),
    )
    .await?;
    associate_interface_with_machine(
        &dpu_interface,
        MachineInterfaceAssociation::Machine(new_host_id),
        txn.as_pgconn(),
    )
    .await?;
    associate_interface_with_dpu_machine(&dpu_interface, &old_dpu_id, txn.as_pgconn()).await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    sqlx::query(
        "UPDATE machines
         SET controller_state = $3
         WHERE id = $1 OR id = $2",
    )
    .bind(old_host_id)
    .bind(old_dpu_id)
    .bind(sqlx::types::Json(ManagedHostState::ForceDeletion))
    .execute(txn.as_pgconn())
    .await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    let data_result = associate_interface_with_machine(
        &data_interface,
        MachineInterfaceAssociation::Machine(new_host_id),
        txn.as_pgconn(),
    )
    .await;
    let bmc_result = associate_bmc_interface(
        &bmc_interface,
        MachineInterfaceAssociation::Machine(new_host_id),
        txn.as_pgconn(),
    )
    .await;
    let dpu_result =
        associate_interface_with_dpu_machine(&dpu_interface, &new_dpu_id, txn.as_pgconn()).await;
    let type_result = set_interface_type(&dpu_interface, InterfaceType::Bmc, txn.as_pgconn()).await;
    for (result, deleted_machine_id) in [
        (data_result, old_host_id),
        (bmc_result, old_host_id),
        (dpu_result, old_dpu_id),
        (type_result, old_dpu_id),
    ] {
        assert!(
            matches!(
                &result,
                Err(DatabaseError::FailedPrecondition(message))
                    if message.contains(&deleted_machine_id.to_string())
                        && message.contains("force-deleted")
            ),
            "association must not be reassigned away from a force-deleting machine: {result:?}",
        );
    }

    let stored: Vec<(MachineInterfaceId, Option<MachineId>, Option<MachineId>)> = sqlx::query_as(
        "SELECT id, machine_id, attached_dpu_machine_id
         FROM machine_interfaces
         WHERE id = ANY($1)
         ORDER BY id",
    )
    .bind([data_interface, bmc_interface, dpu_interface])
    .fetch_all(txn.as_pgconn())
    .await?;
    assert_eq!(stored.len(), 3);
    assert!(stored.contains(&(data_interface, Some(old_host_id), None)));
    assert!(stored.contains(&(bmc_interface, Some(old_host_id), None)));
    assert!(stored.contains(&(dpu_interface, Some(new_host_id), Some(old_dpu_id),)));
    let dpu_interface_type: InterfaceType =
        sqlx::query_scalar("SELECT interface_type FROM machine_interfaces WHERE id = $1")
            .bind(dpu_interface)
            .fetch_one(txn.as_pgconn())
            .await?;
    assert_eq!(dpu_interface_type, InterfaceType::Data);
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test probes locks held by another transaction.
async fn test_machine_association_and_force_deletion_locks_do_not_wait(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x83; 32],
        MachineType::Host,
    );
    let interface_id = {
        let mut txn = db::Transaction::begin(&pool).await?;
        sqlx::query(
            "INSERT INTO machines (id, dpf, controller_state)
             VALUES ($1, '{}'::jsonb, $2)",
        )
        .bind(machine_id)
        .bind(sqlx::types::Json(ManagedHostState::Created))
        .execute(txn.as_pgconn())
        .await?;
        let interface_id =
            insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:83:01".parse()?, false)
                .await?;
        txn.commit().await?;
        interface_id
    };

    let mut ordinary_update = db::Transaction::begin(&pool).await?;
    sqlx::query("UPDATE machines SET controller_state = controller_state WHERE id = $1")
        .bind(machine_id)
        .execute(ordinary_update.as_pgconn())
        .await?;
    let mut association = db::Transaction::begin(&pool).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        associate_interface_with_machine(
            &interface_id,
            MachineInterfaceAssociation::Machine(machine_id),
            association.as_pgconn(),
        ),
    )
    .await
    .expect("association must not wait for an ordinary machine update")?;
    association.rollback().await?;
    ordinary_update.rollback().await?;

    let mut force_delete = db::Transaction::begin(&pool).await?;
    db::machine::lock_force_deletion_targets(
        force_delete.as_pgconn(),
        std::slice::from_ref(&machine_id),
    )
    .await?;

    let mut association = db::Transaction::begin(&pool).await?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        associate_interface_with_machine(
            &interface_id,
            MachineInterfaceAssociation::Machine(machine_id),
            association.as_pgconn(),
        ),
    )
    .await
    .expect("association must not wait for a machine lifecycle row lock");
    assert!(
        matches!(
            &result,
            Err(DatabaseError::FailedPrecondition(message))
                if message.contains("machines are changing")
        ),
        "association must retry rather than wait while force-deletion is being published: {result:?}",
    );
    association.rollback().await?;
    force_delete.rollback().await?;

    let mut association = db::Transaction::begin(&pool).await?;
    associate_interface_with_machine(
        &interface_id,
        MachineInterfaceAssociation::Machine(machine_id),
        association.as_pgconn(),
    )
    .await?;
    let mut force_delete = db::Transaction::begin(&pool).await?;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        db::machine::lock_force_deletion_targets(
            force_delete.as_pgconn(),
            std::slice::from_ref(&machine_id),
        ),
    )
    .await
    .expect("force-deletion must not wait for an interface association");
    assert!(
        matches!(
            &result,
            Err(DatabaseError::FailedPrecondition(message))
                if message.contains("machines changed")
        ),
        "force-deletion must retry rather than wait for an interface association: {result:?}",
    );
    force_delete.rollback().await?;
    association.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_interface_capture_marker_tracks_writes_and_associations(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = db::Transaction::begin(&pool).await?;
    let saved_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:01".parse()?, false).await?;
    let absent_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:02".parse()?, false).await?;
    let machine_interface_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:03".parse()?, false).await?;
    let bmc_interface_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:04".parse()?, false).await?;
    let dpu_interface_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:05".parse()?, false).await?;

    for interface_id in [
        saved_id,
        absent_id,
        machine_interface_id,
        bmc_interface_id,
        dpu_interface_id,
    ] {
        let (expected_interface, captured) =
            expected_interface_capture(txn.as_pgconn(), interface_id).await?;
        assert!(expected_interface.is_none());
        assert!(
            !captured,
            "a new anonymous interface must remain configuration-controlled"
        );
    }

    let saved_interface = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:80:01".parse()?,
        role: ExpectedInterfaceRole::DpuBmc,
        ..Default::default()
    };
    persist_expected_interface_if_missing(txn.as_pgconn(), saved_id, Some(&saved_interface))
        .await?;
    persist_expected_interface_if_missing(txn.as_pgconn(), absent_id, None).await?;

    let host = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x80; 32],
        MachineType::Host,
    );
    let dpu = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x81; 32],
        MachineType::Dpu,
    );
    sqlx::query(
        "INSERT INTO machines (id, dpf)
         VALUES ($1, '{}'::jsonb), ($2, '{}'::jsonb)",
    )
    .bind(host.to_string())
    .bind(dpu.to_string())
    .execute(txn.as_pgconn())
    .await?;

    associate_interface_with_machine(
        &machine_interface_id,
        MachineInterfaceAssociation::Machine(host),
        txn.as_pgconn(),
    )
    .await?;
    associate_bmc_interface(
        &bmc_interface_id,
        MachineInterfaceAssociation::Machine(host),
        txn.as_pgconn(),
    )
    .await?;
    associate_interface_with_dpu_machine(&dpu_interface_id, &dpu, txn.as_pgconn()).await?;

    let (saved, saved_captured) = expected_interface_capture(txn.as_pgconn(), saved_id).await?;
    let saved = saved.expect("the declaration should be saved");
    assert_eq!(saved.role, ExpectedInterfaceRole::DpuBmc);
    assert!(saved_captured);

    let (absent, absent_captured) = expected_interface_capture(txn.as_pgconn(), absent_id).await?;
    assert!(absent.is_none());
    assert!(absent_captured);

    for interface_id in [machine_interface_id, bmc_interface_id, dpu_interface_id] {
        let (_, captured) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
        assert!(captured, "association must finalize the captured state");
    }

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_address_ownership_default_distinguishes_rolling_writers(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = db::Transaction::begin(&pool).await?;
    let old_writer_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:06".parse()?, false).await?;
    let current_writer_id =
        insert_anonymous_interface(txn.as_pgconn(), "7A:7B:7C:7D:80:07".parse()?, false).await?;

    // A binary from before the migration omits the ownership column.
    sqlx::query(
        "INSERT INTO machine_interface_addresses (interface_id, address, allocation_type)
         VALUES ($1, '192.0.2.206'::inet, 'static')",
    )
    .bind(old_writer_id)
    .execute(txn.as_pgconn())
    .await?;
    db::machine_interface_address::insert(
        txn.as_pgconn(),
        current_writer_id,
        "192.0.2.207".parse()?,
        AllocationType::Static,
    )
    .await?;

    let old_writer_marker: Option<bool> = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1",
    )
    .bind(old_writer_id)
    .fetch_one(txn.as_pgconn())
    .await?;
    let current_writer_marker: Option<bool> = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1",
    )
    .bind(current_writer_id)
    .fetch_one(txn.as_pgconn())
    .await?;
    assert_eq!(old_writer_marker, None);
    assert_eq!(current_writer_marker, Some(false));

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_old_writer_association_does_not_apply_later_expected_interface_policy(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:10".parse()?;
    let original_ip = "192.0.2.210".parse()?;
    let later_fixed_ip = "192.0.2.211".parse()?;
    let mut txn = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(txn.as_pgconn(), mac_address, true).await?;
    db::machine_interface_address::insert(
        txn.as_pgconn(),
        interface_id,
        original_ip,
        AllocationType::Dhcp,
    )
    .await?;

    // Reproduce a rolling-upgrade writer that knows how to associate the row
    // but predates ExpectedInterface snapshots and their capture marker.
    sqlx::query(
        "UPDATE machine_interfaces
         SET association_type = 'Machine'::association_type
         WHERE id = $1",
    )
    .bind(interface_id)
    .execute(txn.as_pgconn())
    .await?;
    let (_, captured_before) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
    assert!(!captured_before);

    let later_configuration = ExpectedHostNic {
        mac_address,
        role: ExpectedInterfaceRole::DpuBmc,
        ip_allocation: Some(ExpectedInterfaceIpAllocation::Fixed),
        fixed_ip: Some(later_fixed_ip),
        ..Default::default()
    };
    let captured = capture_expected_interface_for_discovery(
        txn.as_pgconn(),
        mac_address,
        Some(&later_configuration),
    )
    .await?;
    assert!(
        captured.is_none(),
        "current configuration must not be applied retroactively"
    );
    preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &later_configuration,
        None,
    )
    .await?;

    let interface = find_one(txn.as_pgconn(), interface_id).await?;
    let addresses =
        db::machine_interface_address::find_for_interface(txn.as_pgconn(), interface_id).await?;
    let (saved, captured_after) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
    assert_eq!(interface.interface_type, InterfaceType::Data);
    assert!(interface.primary_interface);
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].address, original_ip);
    assert_eq!(addresses[0].allocation_type, AllocationType::Dhcp);
    assert!(saved.is_none());
    assert!(captured_after);

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_removed_expected_interface_is_cleared_before_association(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:15".parse()?;
    let mut txn = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(txn.as_pgconn(), mac_address, true).await?;
    let stale_interface = ExpectedHostNic {
        mac_address,
        role: ExpectedInterfaceRole::DpuBmc,
        ip_allocation: Some(ExpectedInterfaceIpAllocation::Retained),
        ..Default::default()
    };
    persist_expected_interface_if_missing(txn.as_pgconn(), interface_id, Some(&stale_interface))
        .await?;

    let mut expected_machine = db::expected_machine::create(
        txn.as_pgconn(),
        ExpectedMachine {
            id: None,
            bmc_mac_address: "7A:7B:7C:7D:80:16".parse()?,
            data: ExpectedMachineData {
                host_nics: vec![stale_interface.clone()],
                ..Default::default()
            },
        },
    )
    .await?;
    expected_machine.data.host_nics.clear();
    db::expected_machine::update(txn.as_pgconn(), &expected_machine).await?;

    capture_expected_interface_before_association(txn.as_pgconn(), interface_id, None).await?;
    let (saved, captured) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
    assert!(
        saved.is_none(),
        "a removed declaration must not survive association"
    );
    assert!(captured);

    let host = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x82; 32],
        MachineType::Host,
    );
    sqlx::query("INSERT INTO machines (id, dpf) VALUES ($1, '{}'::jsonb)")
        .bind(host.to_string())
        .execute(txn.as_pgconn())
        .await?;
    associate_interface_with_machine(
        &interface_id,
        MachineInterfaceAssociation::Machine(host),
        txn.as_pgconn(),
    )
    .await?;

    expected_machine
        .data
        .host_nics
        .push(stale_interface.clone());
    db::expected_machine::update(txn.as_pgconn(), &expected_machine).await?;
    capture_expected_interface_before_association(
        txn.as_pgconn(),
        interface_id,
        Some(&stale_interface),
    )
    .await?;
    let (saved, captured) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
    assert!(
        saved.is_none(),
        "later configuration must not rewrite an associated interface"
    );
    assert!(captured);

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_anonymous_retained_state_is_released_when_old_writer_downgrades_declaration(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:18".parse()?;
    let retained_address = "192.0.2.218".parse()?;
    let operator_address = "2001:db8::218".parse()?;
    let mut txn = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(txn.as_pgconn(), mac_address, true).await?;
    db::machine_interface_address::insert(
        txn.as_pgconn(),
        interface_id,
        retained_address,
        AllocationType::Dhcp,
    )
    .await?;
    db::machine_interface_address::insert(
        txn.as_pgconn(),
        interface_id,
        operator_address,
        AllocationType::Static,
    )
    .await?;

    let retained_interface = ExpectedHostNic {
        mac_address,
        role: ExpectedInterfaceRole::DpuBmc,
        ip_allocation: Some(ExpectedInterfaceIpAllocation::Retained),
        ..Default::default()
    };
    capture_expected_interface_before_association(
        txn.as_pgconn(),
        interface_id,
        Some(&retained_interface),
    )
    .await?;

    let downgraded_interface = ExpectedHostNic {
        mac_address,
        ..Default::default()
    };
    capture_expected_interface_before_association(
        txn.as_pgconn(),
        interface_id,
        Some(&downgraded_interface),
    )
    .await?;

    let interface = find_one(txn.as_pgconn(), interface_id).await?;
    assert_eq!(interface.interface_type, InterfaceType::Data);
    assert!(interface.primary_interface);
    let (saved, captured) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
    let saved = saved.expect("the downgraded declaration should be saved");
    assert_eq!(saved.role, ExpectedInterfaceRole::Host);
    assert_eq!(
        saved.resolved_ip_allocation(),
        ExpectedInterfaceIpAllocation::Dynamic
    );
    assert!(captured);

    let addresses: Vec<(IpAddr, AllocationType, Option<bool>)> = sqlx::query_as(
        "SELECT address, allocation_type, expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1
         ORDER BY address",
    )
    .bind(interface_id)
    .fetch_all(txn.as_pgconn())
    .await?;
    assert_eq!(
        addresses,
        vec![
            (retained_address, AllocationType::Dhcp, Some(false)),
            (operator_address, AllocationType::Static, Some(false)),
        ]
    );

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_anonymous_fixed_state_is_released_when_old_writer_downgrades_declaration(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    create_managed_segment(
        &pool,
        "fixed-policy-rewrite",
        "192.0.2.0/24",
        NetworkSegmentType::Underlay,
    )
    .await?;

    let cases = [
        ("7A:7B:7C:7D:80:19", "192.0.2.219", Some(true), true),
        ("7A:7B:7C:7D:80:1A", "192.0.2.220", None, true),
        ("7A:7B:7C:7D:80:1B", "192.0.2.221", Some(false), false),
    ];

    for (mac_address, fixed_ip, ownership_marker, should_be_deleted) in cases {
        let mac_address: MacAddress = mac_address.parse()?;
        let fixed_ip: IpAddr = fixed_ip.parse()?;
        let fixed_interface = ExpectedHostNic {
            mac_address,
            ip_allocation: Some(ExpectedInterfaceIpAllocation::Fixed),
            fixed_ip: Some(fixed_ip),
            ..Default::default()
        };

        let mut setup = db::Transaction::begin(&pool).await?;
        preallocate_expected_machine_interface_if_never_associated(
            setup.as_pgconn(),
            &fixed_interface,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE machine_interface_addresses
             SET expected_machine_preallocation = $2
             WHERE address = $1::inet",
        )
        .bind(fixed_ip)
        .bind(ownership_marker)
        .execute(setup.as_pgconn())
        .await?;
        setup.commit().await?;

        // Simulate discovery after an older writer saved the same interface
        // without the newer fixed-policy fields.
        let downgraded_interface = ExpectedHostNic {
            mac_address,
            ..Default::default()
        };
        let mut discovery = db::Transaction::begin(&pool).await?;
        let captured = capture_expected_interface_for_discovery(
            discovery.as_pgconn(),
            mac_address,
            Some(&downgraded_interface),
        )
        .await?
        .expect("the replacement declaration should be captured");
        assert_eq!(
            captured.resolved_ip_allocation(),
            ExpectedInterfaceIpAllocation::Dynamic
        );
        discovery.commit().await?;

        let mut check = db::Transaction::begin(&pool).await?;
        let interfaces = find_by_mac_address(check.as_pgconn(), mac_address).await?;
        let [interface] = interfaces.as_slice() else {
            panic!("expected one interface for {mac_address}");
        };
        let address_state: Option<(AllocationType, Option<bool>)> = sqlx::query_as(
            "SELECT allocation_type, expected_machine_preallocation
             FROM machine_interface_addresses
             WHERE interface_id = $1
               AND address = $2::inet",
        )
        .bind(interface.id)
        .bind(fixed_ip)
        .fetch_optional(check.as_pgconn())
        .await?;
        if should_be_deleted {
            assert!(
                address_state.is_none(),
                "ExpectedMachine-owned fixed address {fixed_ip} should be released",
            );
        } else {
            assert_eq!(
                address_state,
                Some((AllocationType::Static, Some(false))),
                "operator-owned fixed address {fixed_ip} must remain",
            );
        }
        let (saved, captured) = expected_interface_capture(check.as_pgconn(), interface.id).await?;
        assert_eq!(
            saved
                .expect("the replacement declaration should be saved")
                .resolved_ip_allocation(),
            ExpectedInterfaceIpAllocation::Dynamic
        );
        assert!(captured);
        check.rollback().await?;
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_removed_anonymous_fixed_declaration_preserves_untracked_reservation(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    create_managed_segment(
        &pool,
        "fixed-policy-removal",
        "192.0.2.0/24",
        NetworkSegmentType::Underlay,
    )
    .await?;

    let cases = [
        ("7A:7B:7C:7D:80:1C", "192.0.2.222", Some(true), true),
        ("7A:7B:7C:7D:80:1D", "192.0.2.223", None, false),
        ("7A:7B:7C:7D:80:1E", "192.0.2.224", Some(false), false),
    ];
    for (mac_address, fixed_ip, ownership_marker, should_be_deleted) in cases {
        let mac_address: MacAddress = mac_address.parse()?;
        let fixed_ip: IpAddr = fixed_ip.parse()?;
        let fixed_interface = ExpectedHostNic {
            mac_address,
            ip_allocation: Some(ExpectedInterfaceIpAllocation::Fixed),
            fixed_ip: Some(fixed_ip),
            ..Default::default()
        };

        let mut setup = db::Transaction::begin(&pool).await?;
        preallocate_expected_machine_interface_if_never_associated(
            setup.as_pgconn(),
            &fixed_interface,
            None,
        )
        .await?;
        sqlx::query(
            "UPDATE machine_interface_addresses
             SET expected_machine_preallocation = $2
             WHERE address = $1::inet",
        )
        .bind(fixed_ip)
        .bind(ownership_marker)
        .execute(setup.as_pgconn())
        .await?;
        setup.commit().await?;

        let mut discovery = db::Transaction::begin(&pool).await?;
        assert!(
            capture_expected_interface_for_discovery(discovery.as_pgconn(), mac_address, None,)
                .await?
                .is_none()
        );
        discovery.commit().await?;

        let mut check = db::Transaction::begin(&pool).await?;
        let interfaces = find_by_mac_address(check.as_pgconn(), mac_address).await?;
        let [interface] = interfaces.as_slice() else {
            panic!("expected one interface for {mac_address}");
        };
        let address_state: Option<Option<bool>> = sqlx::query_scalar(
            "SELECT expected_machine_preallocation
             FROM machine_interface_addresses
             WHERE interface_id = $1
               AND address = $2::inet",
        )
        .bind(interface.id)
        .bind(fixed_ip)
        .fetch_optional(check.as_pgconn())
        .await?;
        if should_be_deleted {
            assert!(address_state.is_none());
        } else {
            assert_eq!(address_state, Some(ownership_marker));
        }
        let (saved, captured) = expected_interface_capture(check.as_pgconn(), interface.id).await?;
        assert!(saved.is_none());
        assert!(captured);
        check.rollback().await?;
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_association_uses_prediction_when_current_declaration_is_absent(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:17".parse()?;
    let mut txn = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(txn.as_pgconn(), mac_address, true).await?;
    let predicted_machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x83; 32],
        MachineType::PredictedHost,
    );
    sqlx::query("INSERT INTO machines (id, dpf) VALUES ($1, '{}'::jsonb)")
        .bind(predicted_machine_id.to_string())
        .execute(txn.as_pgconn())
        .await?;

    let predicted_interface = ExpectedHostNic {
        mac_address,
        role: ExpectedInterfaceRole::DpuBmc,
        ip_allocation: Some(ExpectedInterfaceIpAllocation::Retained),
        ..Default::default()
    };
    db::predicted_machine_interface::create(
        NewPredictedMachineInterface {
            machine_id: &predicted_machine_id,
            mac_address,
            expected_network_segment_type: NetworkSegmentType::Underlay,
            boot_interface_id: None,
            primary_interface: false,
            expected_interface: Some(&predicted_interface),
        },
        txn.as_pgconn(),
    )
    .await?;

    capture_expected_interface_before_association(txn.as_pgconn(), interface_id, None).await?;
    associate_interface_with_machine(
        &interface_id,
        MachineInterfaceAssociation::Machine(predicted_machine_id),
        txn.as_pgconn(),
    )
    .await?;

    let interface = find_one(txn.as_pgconn(), interface_id).await?;
    assert_eq!(interface.interface_type, InterfaceType::Bmc);
    assert!(!interface.primary_interface);
    let (saved, captured) = expected_interface_capture(txn.as_pgconn(), interface_id).await?;
    let saved = saved.expect("the prediction declaration should be saved");
    assert_eq!(saved.role, ExpectedInterfaceRole::DpuBmc);
    assert_eq!(
        saved.resolved_ip_allocation(),
        ExpectedInterfaceIpAllocation::Retained
    );
    assert!(captured);

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test probes locks held by another transaction.
async fn test_undeclared_capture_does_not_lock_interface_row(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:20".parse()?;
    let mut setup = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(setup.as_pgconn(), mac_address, true).await?;
    setup.commit().await?;

    let mut discovery = db::Transaction::begin(&pool).await?;
    let captured =
        capture_expected_interface_for_discovery(discovery.as_pgconn(), mac_address, None).await?;
    assert!(captured.is_none());

    let mut writer = db::Transaction::begin(&pool).await?;
    sqlx::query("SET LOCAL lock_timeout = '250ms'")
        .execute(writer.as_pgconn())
        .await?;
    let updated = sqlx::query("UPDATE machine_interfaces SET id = id WHERE id = $1")
        .bind(interface_id)
        .execute(writer.as_pgconn())
        .await?;
    assert_eq!(updated.rows_affected(), 1);

    writer.rollback().await?;
    discovery.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_delete_missing_interface_records_invalidation(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = db::Transaction::begin(&pool).await?;
    sqlx::query(
        "UPDATE machine_interfaces_deletion
         SET last_deletion = '-infinity'::timestamptz
         WHERE id = 1",
    )
    .execute(txn.as_pgconn())
    .await?;

    delete(&MachineInterfaceId::new(), txn.as_pgconn()).await?;

    let deletion_recorded: bool = sqlx::query_scalar(
        "SELECT last_deletion > '-infinity'::timestamptz
         FROM machine_interfaces_deletion
         WHERE id = 1",
    )
    .fetch_one(txn.as_pgconn())
    .await?;
    assert!(deletion_recorded);

    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test probes locks held by another transaction.
async fn test_interface_delete_locks_address_before_interface_row(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:21".parse()?;
    let address: IpAddr = "192.0.2.221".parse()?;
    let mut setup = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(setup.as_pgconn(), mac_address, true).await?;
    let segment_id: NetworkSegmentId =
        sqlx::query_scalar("SELECT segment_id FROM machine_interfaces WHERE id = $1")
            .bind(interface_id)
            .fetch_one(setup.as_pgconn())
            .await?;
    db::machine_interface_address::insert(
        setup.as_pgconn(),
        interface_id,
        address,
        AllocationType::Static,
    )
    .await?;
    setup.commit().await?;

    let mut address_owner = db::Transaction::begin(&pool).await?;
    lock_static_address_allocations(address_owner.as_pgconn(), &[(segment_id, address)]).await?;

    let delete_pool = pool.clone();
    let deletion = tokio::spawn(async move {
        let mut txn = db::Transaction::begin(&delete_pool).await?;
        delete(&interface_id, txn.as_pgconn()).await?;
        txn.commit().await
    });
    wait_for_advisory_lock_wait(&pool).await;

    // The delete is waiting for the address key, so a row writer must still
    // complete. A row-first delete would time out here and invert the allocator
    // order.
    let mut row_writer = db::Transaction::begin(&pool).await?;
    sqlx::query("SET LOCAL lock_timeout = '250ms'")
        .execute(row_writer.as_pgconn())
        .await?;
    let updated = sqlx::query("UPDATE machine_interfaces SET id = id WHERE id = $1")
        .bind(interface_id)
        .execute(row_writer.as_pgconn())
        .await?;
    assert_eq!(updated.rows_affected(), 1);
    row_writer.commit().await?;

    address_owner.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), deletion)
        .await
        .expect("interface delete did not finish after releasing the address lock")
        .expect("interface delete task panicked")?;

    let mut txn = db::Transaction::begin(&pool).await?;
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM machine_interfaces WHERE id = $1)",
    )
    .bind(interface_id)
    .fetch_one(txn.as_pgconn())
    .await?;
    assert!(!exists);
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test changes state while deletion waits on a lock.
async fn test_interface_delete_rejects_address_change_after_preview(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:22".parse()?;
    let original_address: IpAddr = "192.0.2.222".parse()?;
    let concurrent_address: IpAddr = "2001:db8::222".parse()?;
    let mut setup = db::Transaction::begin(&pool).await?;
    let interface_id = insert_anonymous_interface(setup.as_pgconn(), mac_address, true).await?;
    let segment_id: NetworkSegmentId =
        sqlx::query_scalar("SELECT segment_id FROM machine_interfaces WHERE id = $1")
            .bind(interface_id)
            .fetch_one(setup.as_pgconn())
            .await?;
    db::machine_interface_address::insert(
        setup.as_pgconn(),
        interface_id,
        original_address,
        AllocationType::Static,
    )
    .await?;
    setup.commit().await?;

    let mut segment_owner = db::Transaction::begin(&pool).await?;
    lock_network_segments_exclusive(segment_owner.as_pgconn(), std::slice::from_ref(&segment_id))
        .await?;

    let delete_pool = pool.clone();
    let deletion = tokio::spawn(async move {
        let mut txn = db::Transaction::begin(&delete_pool).await?;
        let result = delete(&interface_id, txn.as_pgconn()).await;
        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(error) => {
                txn.rollback().await?;
                Err(error)
            }
        }
    });
    wait_for_advisory_lock_wait(&pool).await;

    // Change the child-row set while deletion is stopped before its interface
    // row lock. Revalidation must reject instead of deleting the later state.
    let mut writer = db::Transaction::begin(&pool).await?;
    db::machine_interface_address::insert(
        writer.as_pgconn(),
        interface_id,
        concurrent_address,
        AllocationType::Static,
    )
    .await?;
    writer.commit().await?;

    segment_owner.commit().await?;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), deletion)
        .await
        .expect("interface delete did not finish after releasing the segment lock")
        .expect("interface delete task panicked")
        .expect_err("address-state change must reject deletion");
    assert!(matches!(error, DatabaseError::FailedPrecondition(_)));

    let mut txn = db::Transaction::begin(&pool).await?;
    let addresses =
        db::machine_interface_address::find_for_interface(txn.as_pgconn(), interface_id).await?;
    assert_eq!(addresses.len(), 2);
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test changes state while deletion waits on a lock.
async fn test_interface_delete_rejects_owner_change_after_preview(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mac_address: MacAddress = "7A:7B:7C:7D:80:23".parse()?;
    let address: IpAddr = "192.0.2.223".parse()?;
    let original_owner = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x83; 32],
        MachineType::Host,
    );
    let replacement_owner = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x84; 32],
        MachineType::Host,
    );
    let mut setup = db::Transaction::begin(&pool).await?;
    sqlx::query(
        "INSERT INTO machines (id, dpf)
         VALUES ($1, '{}'::jsonb), ($2, '{}'::jsonb)",
    )
    .bind(original_owner.to_string())
    .bind(replacement_owner.to_string())
    .execute(setup.as_pgconn())
    .await?;
    let interface_id = insert_anonymous_interface(setup.as_pgconn(), mac_address, true).await?;
    associate_interface_with_machine(
        &interface_id,
        MachineInterfaceAssociation::Machine(original_owner),
        setup.as_pgconn(),
    )
    .await?;
    let segment_id: NetworkSegmentId =
        sqlx::query_scalar("SELECT segment_id FROM machine_interfaces WHERE id = $1")
            .bind(interface_id)
            .fetch_one(setup.as_pgconn())
            .await?;
    db::machine_interface_address::insert(
        setup.as_pgconn(),
        interface_id,
        address,
        AllocationType::Static,
    )
    .await?;
    setup.commit().await?;

    let mut segment_owner = db::Transaction::begin(&pool).await?;
    lock_network_segments_exclusive(segment_owner.as_pgconn(), std::slice::from_ref(&segment_id))
        .await?;

    let delete_pool = pool.clone();
    let deletion = tokio::spawn(async move {
        let mut txn = db::Transaction::begin(&delete_pool).await?;
        let result = delete(&interface_id, txn.as_pgconn()).await;
        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(error) => {
                txn.rollback().await?;
                Err(error)
            }
        }
    });
    wait_for_advisory_lock_wait(&pool).await;

    let mut writer = db::Transaction::begin(&pool).await?;
    sqlx::query("UPDATE machine_interfaces SET machine_id = $1 WHERE id = $2")
        .bind(replacement_owner)
        .bind(interface_id)
        .execute(writer.as_pgconn())
        .await?;
    writer.commit().await?;

    segment_owner.commit().await?;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), deletion)
        .await
        .expect("interface delete did not finish after releasing the segment lock")
        .expect("interface delete task panicked")
        .expect_err("owner change must reject deletion");
    assert!(matches!(error, DatabaseError::FailedPrecondition(_)));

    let mut txn = db::Transaction::begin(&pool).await?;
    let interface = find_one(txn.as_pgconn(), interface_id).await?;
    assert_eq!(interface.machine_id, Some(replacement_owner));
    assert_eq!(interface.addresses, vec![address]);
    txn.rollback().await?;
    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test probes locks held by another transaction.
async fn test_static_preallocation_uses_allocator_address_lock(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    create_managed_segment(
        &pool,
        "static-lock-underlay",
        "192.0.2.0/24",
        NetworkSegmentType::Underlay,
    )
    .await?;
    let fixed_ip: IpAddr = "192.0.2.208".parse()?;
    let first = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:80:08".parse()?,
        role: ExpectedInterfaceRole::DpuBmc,
        ip_allocation: Some(ExpectedInterfaceIpAllocation::Fixed),
        fixed_ip: Some(fixed_ip),
        ..Default::default()
    };
    let second = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:80:09".parse()?,
        ..first.clone()
    };

    let mut owner = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface_with_type(
        owner.as_pgconn(),
        first.mac_address,
        fixed_ip,
        InterfaceType::Bmc,
        None,
    )
    .await?;
    let segment =
        db::network_segment::for_static_address(owner.as_pgconn(), fixed_ip, None).await?;

    let mut contender = pool.begin().await?;
    let mac_lock_available = sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_xact_lock(
            hashtextextended('expected_machine_interface.' || $1::text, 0)
        )",
    )
    .bind(first.mac_address)
    .fetch_one(&mut *contender)
    .await?;
    assert!(
        !mac_lock_available,
        "static preallocation must lock the MAC before its allocator address",
    );
    let segment_exclusive_available = sqlx::query_scalar::<_, bool>(
        "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0))",
    )
    .bind(format!("network_segment.{}", segment.id))
    .fetch_one(&mut *contender)
    .await?;
    assert!(
        !segment_exclusive_available,
        "static preallocation must hold a shared segment lock before its allocator address",
    );
    assert!(
        !try_lock_ip_candidate(&mut contender, &segment, fixed_ip).await?,
        "the fixed reservation must hold the allocator's candidate lock",
    );
    contender.rollback().await?;
    owner.commit().await?;

    let mut conflict = db::Transaction::begin(&pool).await?;
    let error = preallocate_expected_machine_interface_if_never_associated(
        conflict.as_pgconn(),
        &second,
        None,
    )
    .await
    .expect_err("a second fixed reservation must observe the committed owner");
    assert!(matches!(error, DatabaseError::InvalidArgument(_)));
    conflict.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_interface_role_and_segment_guard_drive_preallocation(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    create_managed_segment(
        &pool,
        "managed-underlay",
        "192.0.2.0/24",
        NetworkSegmentType::Underlay,
    )
    .await?;

    let dpu_bmc = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:7E:50".parse().unwrap(),
        role: ExpectedInterfaceRole::DpuBmc,
        fixed_ip: Some("192.0.2.50".parse().unwrap()),
        network_segment_type: Some(NetworkSegmentType::Underlay),
        ..Default::default()
    };
    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_expected_machine_interface_if_never_associated(txn.as_pgconn(), &dpu_bmc, None)
        .await?;
    preallocate_expected_machine_interface_if_never_associated(txn.as_pgconn(), &dpu_bmc, None)
        .await?;
    let interfaces = find_by_mac_address(txn.as_pgconn(), dpu_bmc.mac_address).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(interfaces[0].interface_type, InterfaceType::Bmc);
    assert!(!interfaces[0].primary_interface);
    txn.commit().await?;

    let dpu_os_from_bmc = ExpectedHostNic {
        role: ExpectedInterfaceRole::DpuOs,
        ..dpu_bmc.clone()
    };
    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &dpu_os_from_bmc,
        None,
    )
    .await?;
    let interfaces = find_by_mac_address(txn.as_pgconn(), dpu_bmc.mac_address).await?;
    assert_eq!(interfaces[0].interface_type, InterfaceType::Data);
    assert!(interfaces[0].primary_interface);
    txn.commit().await?;

    let dpu_os = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:7E:53".parse().unwrap(),
        role: ExpectedInterfaceRole::DpuOs,
        fixed_ip: Some("192.0.2.53".parse().unwrap()),
        ..Default::default()
    };
    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_expected_machine_interface_if_never_associated(txn.as_pgconn(), &dpu_os, None)
        .await?;
    let interfaces = find_by_mac_address(txn.as_pgconn(), dpu_os.mac_address).await?;
    let containing_segment = db::network_segment::for_prefix_containing_address(
        txn.as_pgconn(),
        dpu_os.fixed_ip.unwrap(),
    )
    .await?
    .unwrap();
    assert_eq!(interfaces.len(), 1);
    assert_eq!(interfaces[0].interface_type, InterfaceType::Data);
    assert_eq!(interfaces[0].segment_id, containing_segment.id);
    txn.commit().await?;

    let wrong_type = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:7E:51".parse().unwrap(),
        role: ExpectedInterfaceRole::DpuOs,
        fixed_ip: Some("192.0.2.51".parse().unwrap()),
        network_segment_type: Some(NetworkSegmentType::Admin),
        ..Default::default()
    };
    let mut txn = db::Transaction::begin(&pool).await?;
    let result = preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &wrong_type,
        None,
    )
    .await;
    assert!(matches!(result, Err(DatabaseError::InvalidArgument(_))));
    txn.rollback().await?;

    let external_with_guard = ExpectedHostNic {
        mac_address: "7A:7B:7C:7D:7E:52".parse().unwrap(),
        role: ExpectedInterfaceRole::DpuOs,
        fixed_ip: Some("198.51.100.52".parse().unwrap()),
        network_segment_type: Some(NetworkSegmentType::Underlay),
        ..Default::default()
    };
    let mut txn = db::Transaction::begin(&pool).await?;
    let result = preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &external_with_guard,
        None,
    )
    .await;
    assert!(matches!(result, Err(DatabaseError::InvalidArgument(_))));

    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_interface_preallocation_leaves_associated_interface_unchanged(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    create_managed_segment(
        &pool,
        "managed-underlay",
        "192.0.2.0/24",
        NetworkSegmentType::Underlay,
    )
    .await?;

    let mac_address = "7A:7B:7C:7D:7E:54".parse().unwrap();
    let original_ip = "192.0.2.54".parse().unwrap();
    let expected_interface = ExpectedHostNic {
        mac_address,
        fixed_ip: Some(original_ip),
        network_segment_type: Some(NetworkSegmentType::Underlay),
        ..Default::default()
    };

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &expected_interface,
        None,
    )
    .await?;
    let interface = find_by_mac_address(txn.as_pgconn(), mac_address)
        .await?
        .pop()
        .unwrap();
    let machine_id = MachineId::new(
        MachineIdSource::ProductBoardChassisSerial,
        [0x54; 32],
        MachineType::Host,
    );
    db::machine::get_or_create(txn.as_pgconn(), None, &machine_id, &interface).await?;

    let changed_configuration = ExpectedHostNic {
        mac_address,
        role: ExpectedInterfaceRole::DpuBmc,
        fixed_ip: Some("192.0.2.55".parse().unwrap()),
        network_segment_type: Some(NetworkSegmentType::Underlay),
        ..Default::default()
    };
    preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &changed_configuration,
        None,
    )
    .await?;

    let interfaces = find_by_mac_address(txn.as_pgconn(), mac_address).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(interfaces[0].machine_id, Some(machine_id));
    assert_eq!(interfaces[0].interface_type, InterfaceType::Data);
    assert_eq!(interfaces[0].addresses, vec![original_ip]);

    db::machine::force_cleanup(txn.as_pgconn(), &machine_id).await?;
    let preserved = find_by_mac_address(txn.as_pgconn(), mac_address)
        .await?
        .pop()
        .unwrap();
    assert_eq!(preserved.machine_id, None);
    assert_eq!(
        preserved.association_type,
        Some(InterfaceAssociationType::Machine),
    );

    preallocate_expected_machine_interface_if_never_associated(
        txn.as_pgconn(),
        &changed_configuration,
        None,
    )
    .await?;
    let preserved = find_by_mac_address(txn.as_pgconn(), mac_address)
        .await?
        .pop()
        .unwrap();
    assert_eq!(preserved.interface_type, InterfaceType::Data);
    assert_eq!(preserved.addresses, vec![original_ip]);
    txn.commit().await?;

    Ok(())
}

/// Verify `preallocate_machine_interface` is idempotent.
/// AddExpectedMachine, expected_machines.json, and the DHCP discover() flow can
/// all fire against the same (ip, mac) pair, including after state has already
/// converged, which is both on purpose and to help flexibly adjust where we
/// find these calls fit best.
///
/// A repeat call must be Ok without changing rows.
#[crate::sqlx_test]
async fn test_preallocate_machine_interface_is_idempotent(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    let mac: MacAddress = "7A:7B:7C:7D:7E:31".parse().unwrap();
    let ip: std::net::IpAddr = "192.0.2.241".parse().unwrap();

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    let interfaces = find_by_mac_address(&mut txn, mac).await?;
    txn.commit().await?;

    assert_eq!(
        interfaces.len(),
        1,
        "second preallocate should be a no-op, not create a duplicate row"
    );
    assert!(
        interfaces[0].addresses.contains(&ip),
        "interface should still carry the static IP"
    );

    Ok(())
}

/// Pre-allocating a different IP for an existing MAC must error, rather than
/// silently reassigning. If an `expected_machine.bmc_ip_address` (or a host_nic
/// fixed_ip) drifts from its `machine_interface` row, operators should see the
/// conflict instead of an automatic rewrite.
#[crate::sqlx_test]
async fn test_preallocate_machine_interface_rejects_conflicting_ip(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    let mac: MacAddress = "7A:7B:7C:7D:7E:32".parse().unwrap();
    let ip1: std::net::IpAddr = "192.0.2.242".parse().unwrap();
    let ip2: std::net::IpAddr = "192.0.2.243".parse().unwrap();

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac, ip1, None).await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    let result = preallocate_machine_interface(txn.as_pgconn(), mac, ip2, None).await;
    assert!(
        matches!(result, Err(DatabaseError::InvalidArgument(_))),
        "preallocating a different IP for the same MAC should be rejected, got {result:?}"
    );

    Ok(())
}

/// Symmetric to `test_preallocate_machine_interface_rejects_conflicting_ip`: pre-allocating
/// an IP that another MAC already owns must error rather than silently reassigning. Covers
/// the `find_by_address`-branch in `preallocate_machine_interface_with_type`.
#[crate::sqlx_test]
async fn test_preallocate_machine_interface_rejects_ip_owned_by_different_mac(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    let mac_a: MacAddress = "7A:7B:7C:7D:7E:35".parse().unwrap();
    let mac_b: MacAddress = "7A:7B:7C:7D:7E:36".parse().unwrap();
    let ip: std::net::IpAddr = "192.0.2.248".parse().unwrap();

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac_a, ip, None).await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    let result = preallocate_machine_interface(txn.as_pgconn(), mac_b, ip, None).await;
    assert!(
        matches!(result, Err(DatabaseError::InvalidArgument(_))),
        "preallocating an IP owned by a different MAC should be rejected, got {result:?}"
    );

    Ok(())
}

/// After a `machine_interface` row gets deleted (e.g. force-delete
/// --delete-interfaces), a subsequent `preallocate_machine_interface` call
/// must successfully recreate it with the same static IP. This is the
/// deferred-allocation flow that we rely on with DHCP discover(...).
#[crate::sqlx_test]
async fn test_preallocate_machine_interface_recreates_after_deletion(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    let mac: MacAddress = "7A:7B:7C:7D:7E:33".parse().unwrap();
    let ip: std::net::IpAddr = "192.0.2.244".parse().unwrap();

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    let interfaces_before = find_by_mac_address(&mut txn, mac).await?;
    let interface_id = interfaces_before[0].id;
    delete(&interface_id, txn.as_pgconn()).await?;
    txn.commit().await?;

    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    let interfaces_after = find_by_mac_address(&mut txn, mac).await?;
    txn.commit().await?;

    assert_eq!(
        interfaces_after.len(),
        1,
        "interface should be re-created after deletion"
    );
    assert!(
        interfaces_after[0].addresses.contains(&ip),
        "re-created interface should carry the same static IP"
    );

    Ok(())
}

/// When an interface row already exists for the right (MAC, IP) but with the wrong
/// `interface_type`, a subsequent preallocate call should promote the type rather than
/// erroring or creating a duplicate. Covers the case where a host NIC initially DHCPs in as
/// `InterfaceType::Data`, then the operator's expected_machine config later marks the same
/// MAC as the BMC (or vice versa), and the next reconciliation pass (or discover hook)
/// reconciles.
#[crate::sqlx_test]
async fn test_preallocate_machine_interface_promotes_interface_type(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    let mac: MacAddress = "7A:7B:7C:7D:7E:34".parse().unwrap();
    let ip: std::net::IpAddr = "192.0.2.247".parse().unwrap();

    // Initial preallocation lands as InterfaceType::Data.
    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    let before = find_by_mac_address(&mut txn, mac).await?;
    assert_eq!(
        before[0].interface_type,
        InterfaceType::Data,
        "Data-variant preallocate should start as InterfaceType::Data"
    );
    txn.commit().await?;

    // Re-preallocate the same (MAC, IP) but as the BMC variant. Helper should promote
    // the existing row's interface_type rather than erroring or creating a duplicate.
    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_bmc_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    let after = find_by_mac_address(&mut txn, mac).await?;
    txn.commit().await?;

    assert_eq!(after.len(), 1, "no duplicate row should have been created");
    assert_eq!(
        after[0].interface_type,
        InterfaceType::Bmc,
        "Bmc-variant preallocate should promote the existing row to InterfaceType::Bmc"
    );
    assert!(
        after[0].addresses.contains(&ip),
        "promoted row should still carry the same IP"
    );

    Ok(())
}

/// `retain_bmc_address_by_mac` promotes a BMC interface's DHCP address to
/// `Static` so DHCP lease expiry can't reap it, is a no-op on a second call, and
/// the promoted address then survives the DHCP-scoped expiry delete path.
#[crate::sqlx_test]
async fn test_retain_bmc_address_pins_dhcp_and_survives_expiry(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    use model::allocation_type::AllocationType;

    create_static_assignments_segment(&pool).await?;
    let mac: MacAddress = "7A:7B:7C:7D:7E:37".parse().unwrap();
    let ip: std::net::IpAddr = "192.0.2.250".parse().unwrap();

    // Create a BMC interface (preallocate lands a Static address), then swap that
    // address for a Dhcp one so we have a BMC interface holding a DHCP lease --
    // the state a BMC reaches when it auto-allocates over DHCP.
    let mut txn = db::Transaction::begin(&pool).await?;
    preallocate_bmc_machine_interface(txn.as_pgconn(), mac, ip, None).await?;
    let interfaces = find_by_mac_address(&mut txn, mac).await?;
    let interface_id = interfaces[0].id;
    assert_eq!(
        interfaces[0].interface_type,
        InterfaceType::Bmc,
        "preallocated interface should be the BMC type"
    );
    crate::machine_interface_address::delete(txn.as_pgconn(), &interface_id).await?;
    crate::machine_interface_address::insert(
        txn.as_pgconn(),
        interface_id,
        ip,
        AllocationType::Dhcp,
    )
    .await?;
    txn.commit().await?;

    // Retain: the DHCP address is promoted to Static.
    let mut txn = db::Transaction::begin(&pool).await?;
    retain_bmc_address_by_mac(txn.as_pgconn(), mac).await?;
    let addrs =
        crate::machine_interface_address::find_for_interface(txn.as_pgconn(), interface_id).await?;
    txn.commit().await?;
    assert_eq!(addrs.len(), 1, "retain must not duplicate the address row");
    assert_eq!(
        addrs[0].allocation_type,
        AllocationType::Static,
        "retain should promote the DHCP address to Static"
    );

    // Idempotent: a second retain is a no-op (the row is already Static).
    let mut txn = db::Transaction::begin(&pool).await?;
    retain_bmc_address_by_mac(txn.as_pgconn(), mac).await?;
    let addrs =
        crate::machine_interface_address::find_for_interface(txn.as_pgconn(), interface_id).await?;
    txn.commit().await?;
    assert_eq!(addrs.len(), 1, "second retain must remain a single row");
    assert_eq!(
        addrs[0].allocation_type,
        AllocationType::Static,
        "second retain should leave the address Static"
    );

    // The promoted Static address survives the DHCP-scoped expiry delete path:
    // delete_by_address(.., Dhcp) finds nothing to delete and the row remains.
    let mut txn = db::Transaction::begin(&pool).await?;
    let deleted = crate::machine_interface_address::delete_by_address(
        txn.as_pgconn(),
        ip,
        AllocationType::Dhcp,
    )
    .await?;
    let addrs =
        crate::machine_interface_address::find_for_interface(txn.as_pgconn(), interface_id).await?;
    txn.commit().await?;
    assert!(
        deleted.is_empty(),
        "DHCP-scoped expiry delete should not match a Static address"
    );
    assert_eq!(
        addrs.len(),
        1,
        "the retained Static address should survive DHCP lease expiry"
    );
    assert_eq!(addrs[0].allocation_type, AllocationType::Static);

    Ok(())
}

#[crate::sqlx_test]
#[allow(txn_held_across_await)] // Intentional: this test changes state while retention waits on a lock.
async fn test_retain_bmc_address_rejects_interface_type_change(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    create_static_assignments_segment(&pool).await?;
    let mac: MacAddress = "7A:7B:7C:7D:7E:38".parse()?;
    let ip: IpAddr = "192.0.2.251".parse()?;

    let mut setup = db::Transaction::begin(&pool).await?;
    preallocate_bmc_machine_interface(setup.as_pgconn(), mac, ip, None).await?;
    let interface = find_by_mac_address(&mut setup, mac).await?.remove(0);
    db::machine_interface_address::delete(setup.as_pgconn(), &interface.id).await?;
    db::machine_interface_address::insert(
        setup.as_pgconn(),
        interface.id,
        ip,
        AllocationType::Dhcp,
    )
    .await?;
    setup.commit().await?;

    let mut segment_owner = db::Transaction::begin(&pool).await?;
    lock_network_segments_exclusive(
        segment_owner.as_pgconn(),
        std::slice::from_ref(&interface.segment_id),
    )
    .await?;

    let retain_pool = pool.clone();
    let retaining = tokio::spawn(async move {
        let mut txn = db::Transaction::begin(&retain_pool).await?;
        let result = retain_bmc_address_by_mac(txn.as_pgconn(), mac).await;
        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(error) => {
                txn.rollback().await?;
                Err(error)
            }
        }
    });
    wait_for_advisory_lock_wait(&pool).await;

    let mut writer = db::Transaction::begin(&pool).await?;
    sqlx::query("UPDATE machine_interfaces SET interface_type = 'Data' WHERE id = $1")
        .bind(interface.id)
        .execute(writer.as_pgconn())
        .await?;
    writer.commit().await?;

    segment_owner.commit().await?;
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), retaining)
        .await
        .expect("BMC address retention did not finish after releasing the segment lock")
        .expect("BMC address retention task panicked")
        .expect_err("interface type change must reject BMC address retention");
    assert!(matches!(error, DatabaseError::FailedPrecondition(_)));

    let mut txn = db::Transaction::begin(&pool).await?;
    let addresses =
        db::machine_interface_address::find_for_interface(txn.as_pgconn(), interface.id).await?;
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].allocation_type, AllocationType::Dhcp);
    txn.rollback().await?;
    Ok(())
}
