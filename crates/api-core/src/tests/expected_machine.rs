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
use std::default::Default;
use std::net::IpAddr;

use common::api_fixtures::{
    TestEnvOverrides, create_test_env, create_test_env_with_overrides, get_config,
};
use db::{self};
use mac_address::MacAddress;
use model::expected_machine::{ExpectedMachine, ExpectedMachineData};
use rpc::forge::forge_server::Forge;
use rpc::forge::{ExpectedMachineList, ExpectedMachineRequest};
use uuid::Uuid;

use crate::CarbideError;
use crate::test_support::fixture_config::FixtureDefault as _;
use crate::tests::common;

async fn create_fixture_expected_machines(pool: &sqlx::PgPool) {
    let mut txn = pool.begin().await.unwrap();
    for (bmc_mac_address, serial_number, fallback_dpu_serial_numbers) in [
        ("0a:0b:0c:0d:0e:0f", "VVG121GG", vec![]),
        ("1a:1b:1c:1d:1e:1f", "VVG121GH", vec![]),
        ("2a:2b:2c:2d:2e:2f", "VVG121GI", vec![]),
        ("3a:3b:3c:3d:3e:3f", "VVG121GJ", vec!["dpu_serial1"]),
        (
            "4a:4b:4c:4d:4e:4f",
            "VVG121GK",
            vec!["dpu_serial2", "dpu_serial3"],
        ),
        ("5a:5b:5c:5d:5e:5f", "VVG121GL", vec![]),
    ] {
        db::expected_machine::create(
            &mut txn,
            ExpectedMachine {
                id: None,
                bmc_mac_address: bmc_mac_address.parse().unwrap(),
                data: ExpectedMachineData {
                    bmc_username: "ADMIN".into(),
                    bmc_password: "Pwd2023x0x0x0x0x7".into(),
                    serial_number: serial_number.into(),
                    fallback_dpu_serial_numbers: fallback_dpu_serial_numbers
                        .into_iter()
                        .map(ToString::to_string)
                        .collect(),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();
}

fn expected_machine_with_fixed_interface(
    bmc_mac_address: &str,
    serial_number: &str,
    interface_mac_address: &str,
    fixed_ip: &str,
) -> rpc::forge::ExpectedMachine {
    rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac_address.into(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: serial_number.into(),
        host_nics: vec![rpc::forge::ExpectedHostNic {
            mac_address: interface_mac_address.into(),
            fixed_ip: Some(fixed_ip.into()),
            ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn clear_expected_machine_id(
    pool: &sqlx::PgPool,
    bmc_mac_address: MacAddress,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut txn = pool.begin().await?;
    let result = sqlx::query(
        "UPDATE expected_machines
         SET id = NULL
         WHERE bmc_mac_address = $1",
    )
    .bind(bmc_mac_address)
    .execute(&mut *txn)
    .await?;
    assert_eq!(result.rows_affected(), 1);
    txn.commit().await?;
    Ok(())
}

async fn wait_for_expected_machine_advisory_lock(pool: &sqlx::PgPool, blocker_pid: i32) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND wait_event = 'advisory'
                      AND $1::integer = ANY(pg_blocking_pids(pid))
                )",
            )
            .bind(blocker_pid)
            .fetch_one(pool)
            .await
            .expect("inspect ExpectedMachine advisory lock wait");
            if waiting {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ExpectedMachine mutation did not reach the advisory lock wait");
}

async fn wait_for_advisory_lock_wait(pool: &sqlx::PgPool, waiting_pid: i32) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE pid = $1
                      AND datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND wait_event = 'advisory'
                )",
            )
            .bind(waiting_pid)
            .fetch_one(pool)
            .await
            .expect("inspect ExpectedMachine advisory lock wait");
            if waiting {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ExpectedMachine transaction did not reach an advisory lock wait");
}

#[crate::sqlx_test]
async fn test_add_expected_machine_serializes_with_interface_discovery(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let interface_mac: MacAddress = "AA:BB:CC:DD:F1:01".parse()?;

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
            hashtextextended('expected_machine_interface.' || $1::text, 0)
        )",
    )
    .bind(interface_mac)
    .execute(&mut *blocker)
    .await?;

    let api = env.api.clone();
    let add = tokio::spawn(async move {
        api.add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: "AA:BB:CC:DD:F1:00".into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-LOCK-INTERFACE".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: interface_mac.to_string(),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
    });

    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;
    assert!(!add.is_finished());

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), add)
        .await
        .expect("ExpectedMachine add did not resume after releasing the lock")
        .expect("ExpectedMachine add task panicked")?;

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "AA:BB:CC:DD:F1:00".into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(stored.host_nics[0].mac_address, interface_mac.to_string());

    Ok(())
}

#[crate::sqlx_test]
async fn test_concurrent_adds_revalidate_duplicate_interface_mac(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let interface_mac: MacAddress = "AA:BB:CC:DD:F1:03".parse()?;

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
            hashtextextended('expected_machine_interface.' || $1::text, 0)
        )",
    )
    .bind(interface_mac)
    .execute(&mut *blocker)
    .await?;

    let first_api = env.api.clone();
    let first = tokio::spawn(async move {
        first_api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                bmc_mac_address: "AA:BB:CC:DD:F1:04".into(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: "EM-CONCURRENT-FIRST".into(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac.to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
    });
    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;

    let second_api = env.api.clone();
    let second = tokio::spawn(async move {
        second_api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                bmc_mac_address: "AA:BB:CC:DD:F1:05".into(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: "EM-CONCURRENT-SECOND".into(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac.to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
    });

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), first)
        .await
        .expect("first ExpectedMachine add did not complete")
        .expect("first ExpectedMachine add task panicked")?;
    let second_error = tokio::time::timeout(std::time::Duration::from_secs(5), second)
        .await
        .expect("second ExpectedMachine add did not complete")
        .expect("second ExpectedMachine add task panicked")
        .expect_err("second ExpectedMachine add unexpectedly succeeded");
    assert_eq!(second_error.code(), tonic::Code::InvalidArgument);
    assert!(
        second_error
            .message()
            .contains("expected machine identity MAC"),
        "unexpected error: {second_error}",
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_update_and_add_serialize_identity_changes(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let owner_bmc = "AA:BB:CC:DD:F1:06";
    let interface_mac: MacAddress = "AA:BB:CC:DD:F1:07".parse()?;

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: owner_bmc.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-IDENTITY-UPDATE".into(),
            ..Default::default()
        }))
        .await?;
    let mut update = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: owner_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    update.host_nics = vec![rpc::forge::ExpectedHostNic {
        mac_address: interface_mac.to_string(),
        ..Default::default()
    }];

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    db::machine_interface::lock_expected_machine_interface_macs(&mut blocker, [interface_mac])
        .await?;

    let update_api = env.api.clone();
    let update_task = tokio::spawn(async move {
        update_api
            .update_expected_machine(tonic::Request::new(update))
            .await
    });
    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;

    let add_api = env.api.clone();
    let add_task = tokio::spawn(async move {
        add_api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                bmc_mac_address: "AA:BB:CC:DD:F1:08".into(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: "EM-IDENTITY-COMPETITOR".into(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac.to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
    });

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), update_task)
        .await
        .expect("ExpectedMachine update did not resume")
        .expect("ExpectedMachine update task panicked")?;
    let add_error = tokio::time::timeout(std::time::Duration::from_secs(5), add_task)
        .await
        .expect("competing ExpectedMachine add did not complete")
        .expect("competing ExpectedMachine add task panicked")
        .expect_err("competing ExpectedMachine add unexpectedly succeeded");
    assert_eq!(add_error.code(), tonic::Code::InvalidArgument);
    assert!(
        add_error
            .message()
            .contains("expected machine identity MAC"),
        "unexpected error: {add_error}",
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_add_expected_machine_locks_top_level_bmc_mac(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let bmc_mac: MacAddress = "AA:BB:CC:DD:F1:02".parse()?;

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(
            hashtextextended('expected_machine_interface.' || $1::text, 0)
        )",
    )
    .bind(bmc_mac)
    .execute(&mut *blocker)
    .await?;

    let api = env.api.clone();
    let add = tokio::spawn(async move {
        api.add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-LOCK-BMC".into(),
            ..Default::default()
        }))
        .await
    });

    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;
    assert!(!add.is_finished());

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), add)
        .await
        .expect("ExpectedMachine add did not resume after releasing the BMC MAC lock")
        .expect("ExpectedMachine add task panicked")?;

    let mut txn = env.db_txn().await;
    assert!(
        db::expected_machine::find_by_bmc_mac_address(txn.as_mut(), bmc_mac)
            .await?
            .is_some(),
    );
    txn.commit().await?;
    Ok(())
}

#[crate::sqlx_test]
async fn test_add_expected_machine_waits_for_exclusive_config_mutation_lock(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let bmc_mac: MacAddress = "AA:BB:CC:DD:F2:00".parse()?;

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    db::expected_machine::lock_config_mutations_exclusive(&mut blocker).await?;

    let api = env.api.clone();
    let add = tokio::spawn(async move {
        api.add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-SET-LOCK-ADD".into(),
            ..Default::default()
        }))
        .await
    });

    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;
    assert!(!add.is_finished());

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), add)
        .await
        .expect("ExpectedMachine add did not resume after releasing the set lock")
        .expect("ExpectedMachine add task panicked")?;

    assert!(
        db::expected_machine::find_by_bmc_mac_address(&pool, bmc_mac)
            .await?
            .is_some()
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_replace_all_expected_machines_waits_for_shared_config_mutation_lock(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: "AA:BB:CC:DD:F3:00".into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-SET-LOCK-REPLACE".into(),
            ..Default::default()
        }))
        .await?;

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    db::expected_machine::lock_config_mutations_shared(&mut blocker).await?;

    let api = env.api.clone();
    let replace_all = tokio::spawn(async move {
        api.replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: Vec::new(),
        }))
        .await
    });

    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;
    assert!(!replace_all.is_finished());

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), replace_all)
        .await
        .expect("ReplaceAll did not resume after releasing the set lock")
        .expect("ReplaceAll task panicked")?;

    let remaining = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await?
        .into_inner();
    assert!(remaining.expected_machines.is_empty());

    Ok(())
}

// Test API functionality
/*
  // Expected Machine Management
  // Replace all expected machines in site
  rpc ReplaceAllExpectedMachines(ExpectedMachineList) returns (google.protobuf.Empty);
*/
#[crate::sqlx_test()]
async fn test_add_expected_machine(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    for (idx, expected_machine) in [
        rpc::forge::ExpectedMachine {
            bmc_mac_address: "3A:3B:3C:3D:3E:3F".to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "VVG121GI".into(),
            metadata: None,
            sku_id: None,
            id: Some(::rpc::common::Uuid {
                value: Uuid::new_v4().to_string(),
            }),
            default_pause_ingestion_and_poweron: Some(true),
            is_dpf_enabled: Some(false),
            ..Default::default()
        },
        rpc::forge::ExpectedMachine {
            bmc_mac_address: "3A:3B:3C:3D:3E:40".to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "VVG121GI".into(),
            metadata: Some(rpc::forge::Metadata::default()),
            sku_id: Some("sku_id".to_string()),
            id: Some(::rpc::common::Uuid {
                value: Uuid::new_v4().to_string(),
            }),
            default_pause_ingestion_and_poweron: Some(false),
            is_dpf_enabled: Some(true),
            #[allow(deprecated)]
            dpf_enabled: true,
            ..Default::default()
        },
        rpc::forge::ExpectedMachine {
            bmc_mac_address: "3A:3B:3C:3D:3E:41".to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "VVG121GI".into(),
            metadata: Some(rpc::forge::Metadata {
                name: "a".to_string(),
                description: "desc".to_string(),
                labels: vec![
                    rpc::forge::Label {
                        key: "k1".to_string(),
                        value: None,
                    },
                    rpc::forge::Label {
                        key: "k2".to_string(),
                        value: Some("v2".to_string()),
                    },
                ],
            }),
            id: Some(::rpc::common::Uuid {
                value: Uuid::new_v4().to_string(),
            }),
            sku_id: Some("sku_id".to_string()),
            default_pause_ingestion_and_poweron: None,
            is_dpf_enabled: Some(false),
            ..Default::default()
        },
    ]
    .iter_mut()
    .enumerate()
    {
        env.api
            .add_expected_machine(tonic::Request::new(expected_machine.clone()))
            .await
            .expect("unable to add expected machine ");

        let expected_machine_query = rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: expected_machine.bmc_mac_address.clone(),
            id: None,
        };

        let mut retrieved_expected_machine = env
            .api
            .get_expected_machine(tonic::Request::new(expected_machine_query))
            .await
            .expect("unable to retrieve expected machine ")
            .into_inner();
        retrieved_expected_machine
            .metadata
            .as_mut()
            .unwrap()
            .labels
            .sort_by(|l1, l2| l1.key.cmp(&l2.key));
        if expected_machine.metadata.is_none() {
            expected_machine.metadata = Some(Default::default());
        }
        if expected_machine
            .default_pause_ingestion_and_poweron
            .is_none()
        {
            expected_machine.default_pause_ingestion_and_poweron = Some(false);
        }
        assert_eq!(retrieved_expected_machine, expected_machine.clone());

        if idx != 1 {
            assert!(
                !retrieved_expected_machine
                    .is_dpf_enabled
                    .unwrap_or_default()
            );
        } else {
            assert!(
                retrieved_expected_machine
                    .is_dpf_enabled
                    .unwrap_or_default()
            );
        }
    }
}

#[crate::sqlx_test]
async fn test_delete_expected_machine(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;

    let expected_machine_count = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner()
        .expected_machines
        .len();

    let expected_machine_query = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "2A:2B:2C:2D:2E:2F".into(),
        id: None,
    };
    env.api
        .delete_expected_machine(tonic::Request::new(expected_machine_query))
        .await
        .expect("unable to delete expected machine ")
        .into_inner();

    let new_expected_machine_count = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner()
        .expected_machines
        .len();

    assert_eq!(new_expected_machine_count, expected_machine_count - 1);
}

#[crate::sqlx_test()]
async fn test_delete_expected_machine_error(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address: MacAddress = "2A:2B:2C:2D:2E:2F".parse().unwrap();
    let expected_machine_request = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: bmc_mac_address.to_string(),
        id: None,
    };

    let err = env
        .api
        .delete_expected_machine(tonic::Request::new(expected_machine_request))
        .await
        .unwrap_err();

    assert_eq!(
        err.message().to_string(),
        CarbideError::NotFoundError {
            kind: "expected_machine",
            id: bmc_mac_address.to_string(),
        }
        .to_string()
    );
}

#[crate::sqlx_test]
async fn test_update_expected_machine(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;

    let bmc_mac_address: MacAddress = "2A:2B:2C:2D:2E:2F".parse().unwrap();
    for mut updated_machine in [
        rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN_UPDATE".into(),
            bmc_password: "PASS_UPDATE".into(),
            chassis_serial_number: "VVG121GI".into(),
            metadata: None,
            default_pause_ingestion_and_poweron: Some(true),
            is_dpf_enabled: Some(false),
            ..Default::default()
        },
        rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN_UPDATE".into(),
            bmc_password: "PASS_UPDATE".into(),
            chassis_serial_number: "VVG121GJ".into(),
            metadata: Some(Default::default()),
            default_pause_ingestion_and_poweron: Some(false),
            is_dpf_enabled: Some(false),
            ..Default::default()
        },
        rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN_UPDATE1".into(),
            bmc_password: "PASS_UPDATE1".into(),
            chassis_serial_number: "VVG121GN".into(),
            metadata: Some(rpc::forge::Metadata {
                name: "a".to_string(),
                description: "desc".to_string(),
                labels: vec![
                    rpc::forge::Label {
                        key: "k1".to_string(),
                        value: None,
                    },
                    rpc::forge::Label {
                        key: "k2".to_string(),
                        value: Some("v2".to_string()),
                    },
                ],
            }),
            default_pause_ingestion_and_poweron: None,
            is_dpf_enabled: Some(false),
            ..Default::default()
        },
    ] {
        // ensure MAC-based update; id is ignored by update path
        updated_machine.id = None;
        env.api
            .update_expected_machine(tonic::Request::new(updated_machine.clone()))
            .await
            .expect("unable to update expected machine ")
            .into_inner();

        let mut retrieved_expected_machine = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: bmc_mac_address.to_string(),
                id: None,
            }))
            .await
            .expect("unable to fetch expected machine ")
            .into_inner();
        retrieved_expected_machine
            .metadata
            .as_mut()
            .unwrap()
            .labels
            .sort_by(|l1, l2| l1.key.cmp(&l2.key));
        // Ignore id field in comparison; MAC-based update path doesn't care about id
        retrieved_expected_machine.id = None;
        if updated_machine.metadata.is_none() {
            updated_machine.metadata = Some(Default::default());
        }

        if updated_machine
            .default_pause_ingestion_and_poweron
            .is_none()
        {
            updated_machine.default_pause_ingestion_and_poweron = Some(false);
        }

        assert_eq!(retrieved_expected_machine, updated_machine);
    }
}

#[crate::sqlx_test()]
async fn test_update_expected_machine_error(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address: MacAddress = "2A:2B:2C:2D:2E:2F".parse().unwrap();
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac_address.to_string(),
        bmc_username: "ADMIN_UPDATE".into(),
        bmc_password: "PASS_UPDATE".into(),
        chassis_serial_number: "VVG121GI".into(),
        ..Default::default()
    };

    let err = env
        .api
        .update_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .unwrap_err();

    assert_eq!(
        err.message().to_string(),
        CarbideError::NotFoundError {
            kind: "expected_machine",
            id: bmc_mac_address.to_string(),
        }
        .to_string()
    );
}

#[crate::sqlx_test]
async fn test_delete_all_expected_machines(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;
    let mut expected_machine_count = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner()
        .expected_machines
        .len();

    assert_eq!(expected_machine_count, 6);

    env.api
        .delete_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner();

    expected_machine_count = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner()
        .expected_machines
        .len();

    assert_eq!(expected_machine_count, 0);
}

#[crate::sqlx_test]
async fn test_replace_all_expected_machines(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;
    let expected_machine_count = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner()
        .expected_machines
        .len();

    assert_eq!(expected_machine_count, 6);

    let mut expected_machine_list = ExpectedMachineList {
        expected_machines: Vec::new(),
    };

    let expected_machine_1 = rpc::forge::ExpectedMachine {
        bmc_mac_address: "4A:4B:4C:4D:4E:4F".into(),
        bmc_username: "ADMIN_NEW".into(),
        bmc_password: "PASS_NEW".into(),
        chassis_serial_number: "SERIAL_NEW".into(),
        metadata: Some(rpc::Metadata::default()),
        default_pause_ingestion_and_poweron: Some(true),
        is_dpf_enabled: Some(false),
        ..Default::default()
    };

    let expected_machine_2 = rpc::forge::ExpectedMachine {
        bmc_mac_address: "5A:5B:5C:5D:5E:5F".into(),
        bmc_username: "ADMIN_NEW".into(),
        bmc_password: "PASS_NEW".into(),
        chassis_serial_number: "SERIAL_NEW".into(),
        metadata: Some(rpc::Metadata::default()),
        default_pause_ingestion_and_poweron: Some(false),
        is_dpf_enabled: Some(false),
        ..Default::default()
    };

    let expected_machine_3 = rpc::forge::ExpectedMachine {
        bmc_mac_address: "6A:6B:6C:6D:6E:6F".into(),
        bmc_username: "ADMIN_NEW".into(),
        bmc_password: "PASS_NEW".into(),
        chassis_serial_number: "SERIAL_NEW".into(),
        metadata: Some(rpc::Metadata::default()),
        default_pause_ingestion_and_poweron: None,
        is_dpf_enabled: Some(false),
        ..Default::default()
    };

    expected_machine_list
        .expected_machines
        .push(expected_machine_1.clone());
    expected_machine_list
        .expected_machines
        .push(expected_machine_2.clone());
    expected_machine_list
        .expected_machines
        .push(expected_machine_3.clone());

    env.api
        .replace_all_expected_machines(tonic::Request::new(expected_machine_list))
        .await
        .expect("unable to get all expected machines")
        .into_inner();

    let mut expected_machines = env
        .api
        .get_all_expected_machines(tonic::Request::new(()))
        .await
        .expect("unable to get all expected machines")
        .into_inner()
        .expected_machines;
    expected_machines.sort_by_key(|e| e.bmc_mac_address.clone());

    assert_eq!(expected_machines.len(), 3);
    let mut resulting_machine_1 = expected_machines[0].clone();
    resulting_machine_1.id = None;
    let mut resulting_machine_2 = expected_machines[1].clone();
    resulting_machine_2.id = None;
    let mut resulting_machine_3 = expected_machines[2].clone();
    resulting_machine_3.id = None;

    // None will become Some(false), so we have to make the adjustment
    let mut expected_machine_3_clone = expected_machine_3.clone();
    expected_machine_3_clone.default_pause_ingestion_and_poweron = Some(false);

    assert_eq!(expected_machine_1, resulting_machine_1);
    assert_eq!(expected_machine_2, resulting_machine_2);
    assert_eq!(expected_machine_3_clone, resulting_machine_3);
}

#[crate::sqlx_test]
async fn test_expected_machine_deletion_paths_release_unassociated_fixed_reservations(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    let single_interface_mac: MacAddress = "7A:7B:7C:7D:7E:71".parse()?;
    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            "7A:7B:7C:7D:7E:70",
            "DELETE-SINGLE",
            &single_interface_mac.to_string(),
            "192.0.2.231",
        )))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                single_interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;
    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "7A:7B:7C:7D:7E:70".into(),
            id: None,
        }))
        .await?;
    let mut txn = env.pool.begin().await?;
    assert!(
        db::machine_interface::find_by_mac_address(&mut *txn, single_interface_mac)
            .await?
            .is_empty(),
    );
    txn.rollback().await?;

    let delete_all_interface_mac: MacAddress = "7A:7B:7C:7D:7E:73".parse()?;
    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            "7A:7B:7C:7D:7E:72",
            "DELETE-ALL",
            &delete_all_interface_mac.to_string(),
            "192.0.2.232",
        )))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                delete_all_interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;
    env.api
        .delete_all_expected_machines(tonic::Request::new(()))
        .await?;
    let mut txn = env.pool.begin().await?;
    assert!(
        db::machine_interface::find_by_mac_address(&mut *txn, delete_all_interface_mac)
            .await?
            .is_empty(),
    );
    txn.rollback().await?;

    let replaced_interface_mac: MacAddress = "7A:7B:7C:7D:7E:75".parse()?;
    let previous = expected_machine_with_fixed_interface(
        "7A:7B:7C:7D:7E:74",
        "REPLACE-OLD",
        &replaced_interface_mac.to_string(),
        "192.0.2.233",
    );
    env.api
        .add_expected_machine(tonic::Request::new(previous))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                replaced_interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    let replacement = expected_machine_with_fixed_interface(
        "7A:7B:7C:7D:7E:74",
        "REPLACE-NEW",
        &replaced_interface_mac.to_string(),
        "192.0.2.234",
    );
    env.api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: vec![replacement],
        }))
        .await?;

    let response = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                replaced_interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner();
    assert_eq!(response.address, "192.0.2.234");

    Ok(())
}

#[crate::sqlx_test]
async fn test_delete_legacy_expected_machine_preserves_shared_interface_reservation(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let deleted_bmc: MacAddress = "7A:7B:7C:7D:7E:6A".parse()?;
    let surviving_bmc: MacAddress = "7A:7B:7C:7D:7E:6B".parse()?;
    let shared_interface_mac: MacAddress = "7A:7B:7C:7D:7E:6C".parse()?;
    let fixed_ip: IpAddr = "192.0.2.226".parse()?;

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            &surviving_bmc.to_string(),
            "LEGACY-SHARED-SURVIVOR",
            &shared_interface_mac.to_string(),
            &fixed_ip.to_string(),
        )))
        .await?;
    let interface_id = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                shared_interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner()
        .machine_interface_id
        .expect("DHCP should return the preallocated interface");
    clear_expected_machine_id(&env.pool, surviving_bmc).await?;

    let shared_interface = model::expected_machine::ExpectedHostNic {
        mac_address: shared_interface_mac,
        ip_allocation: Some(model::expected_machine::ExpectedInterfaceIpAllocation::Fixed),
        fixed_ip: Some(fixed_ip),
        ..Default::default()
    };
    let mut txn = env.pool.begin().await?;
    db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: deleted_bmc,
            data: ExpectedMachineData {
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                serial_number: "LEGACY-SHARED-DELETED".into(),
                host_nics: vec![shared_interface],
                ..Default::default()
            },
        },
    )
    .await?;
    txn.commit().await?;
    clear_expected_machine_id(&env.pool, deleted_bmc).await?;

    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: deleted_bmc.to_string(),
            id: None,
        }))
        .await?;

    let surviving = db::expected_machine::find_by_bmc_mac_address(&env.pool, surviving_bmc)
        .await?
        .expect("the other legacy ExpectedMachine should remain");
    assert_eq!(surviving.id, None);

    let mut txn = env.pool.begin().await?;
    let interfaces =
        db::machine_interface::find_by_mac_address(&mut *txn, shared_interface_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert_eq!(interfaces[0].id, interface_id);
    assert!(interfaces[0].addresses.contains(&fixed_ip));

    let (saved_interface, expected_machine_owned): (
        Option<sqlx::types::Json<model::expected_machine::ExpectedHostNic>>,
        Option<bool>,
    ) = sqlx::query_as(
        "SELECT mi.expected_interface, mia.expected_machine_preallocation
         FROM machine_interfaces mi
         JOIN machine_interface_addresses mia ON mia.interface_id = mi.id
         WHERE mi.id = $1 AND mia.address = $2::inet",
    )
    .bind(interface_id)
    .bind(fixed_ip)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(
        saved_interface
            .expect("the surviving declaration snapshot should remain")
            .fixed_ip,
        Some(fixed_ip),
    );
    assert_eq!(expected_machine_owned, Some(true));
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_replace_all_releases_retained_dual_stack_addresses(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:81".parse()?;
    let interface_mac: MacAddress = "7A:7B:7C:7D:7E:82".parse()?;

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "REPLACE-RETAINED".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: interface_mac.to_string(),
                role: Some(rpc::forge::ExpectedInterfaceRole::Host as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    let interface_id = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner()
        .machine_interface_id
        .expect("DHCP should return the anonymous interface");

    let mut txn = pool.begin().await?;
    db::machine_interface_address::insert(
        &mut txn,
        interface_id,
        "2001:db8::82".parse()?,
        model::allocation_type::AllocationType::Dhcp,
    )
    .await?;
    txn.commit().await?;

    let mut retained = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    retained.host_nics[0].ip_allocation =
        Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32);
    env.api
        .update_expected_machine(tonic::Request::new(retained))
        .await?;

    let mut txn = pool.begin().await?;
    let retained_addresses: Vec<(model::allocation_type::AllocationType, Option<bool>)> =
        sqlx::query_as(
            "SELECT allocation_type, expected_machine_preallocation
             FROM machine_interface_addresses
             WHERE interface_id = $1",
        )
        .bind(interface_id)
        .fetch_all(&mut *txn)
        .await?;
    assert_eq!(retained_addresses.len(), 2);
    assert!(retained_addresses.iter().all(|(allocation_type, marker)| {
        *allocation_type == model::allocation_type::AllocationType::Static && *marker == Some(true)
    }));
    txn.rollback().await?;

    let mut replacement = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    replacement.host_nics[0].ip_allocation =
        Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32);
    env.api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: vec![replacement],
        }))
        .await?;

    let mut txn = pool.begin().await?;
    let addresses: Vec<(IpAddr, model::allocation_type::AllocationType, Option<bool>)> =
        sqlx::query_as(
            "SELECT address, allocation_type, expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1
         ORDER BY family(address)",
        )
        .bind(interface_id)
        .fetch_all(&mut *txn)
        .await?;
    assert_eq!(addresses.len(), 2);
    assert!(addresses.iter().all(|(_, allocation_type, marker)| {
        *allocation_type == model::allocation_type::AllocationType::Dhcp && *marker == Some(false)
    }));
    let saved_interface = sqlx::query_scalar::<
        _,
        sqlx::types::Json<model::expected_machine::ExpectedHostNic>,
    >("SELECT expected_interface FROM machine_interfaces WHERE id = $1")
    .bind(interface_id)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(
        saved_interface.resolved_ip_allocation(),
        model::expected_machine::ExpectedInterfaceIpAllocation::Dynamic,
    );
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_replace_all_reconciles_transferred_retained_interface(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let source_bmc: MacAddress = "7A:7B:7C:7D:7E:83".parse()?;
    let target_bmc: MacAddress = "7A:7B:7C:7D:7E:84".parse()?;
    let interface_mac: MacAddress = "7A:7B:7C:7D:7E:85".parse()?;

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: source_bmc.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "REPLACE-SOURCE".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: interface_mac.to_string(),
                role: Some(rpc::forge::ExpectedInterfaceRole::Host as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    let interface_id = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner()
        .machine_interface_id
        .expect("DHCP should return the anonymous interface");

    let mut txn = pool.begin().await?;
    let addresses =
        db::machine_interface_address::find_for_interface(&mut txn, interface_id).await?;
    assert_eq!(addresses.len(), 1);
    assert_eq!(
        addresses[0].allocation_type,
        model::allocation_type::AllocationType::Static,
    );
    let retained_marker: Option<bool> = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1",
    )
    .bind(interface_id)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(retained_marker, Some(true));
    txn.rollback().await?;

    env.api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: vec![rpc::forge::ExpectedMachine {
                bmc_mac_address: target_bmc.to_string(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: "REPLACE-TARGET".into(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac.to_string(),
                    role: Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }))
        .await?;

    let mut txn = pool.begin().await?;
    let interface = db::machine_interface::find_one(&mut *txn, interface_id).await?;
    assert_eq!(
        interface.interface_type,
        model::machine_interface::InterfaceType::Bmc
    );
    assert!(!interface.primary_interface);
    let addresses =
        db::machine_interface_address::find_for_interface(&mut txn, interface_id).await?;
    assert_eq!(addresses.len(), 1);
    assert_eq!(
        addresses[0].allocation_type,
        model::allocation_type::AllocationType::Dhcp,
    );
    let retained_marker: Option<bool> = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1",
    )
    .bind(interface_id)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(retained_marker, Some(false));
    let saved_interface = sqlx::query_scalar::<
        _,
        sqlx::types::Json<model::expected_machine::ExpectedHostNic>,
    >("SELECT expected_interface FROM machine_interfaces WHERE id = $1")
    .bind(interface_id)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(
        saved_interface.role,
        model::expected_machine::ExpectedInterfaceRole::DpuBmc,
    );
    assert_eq!(
        saved_interface.resolved_ip_allocation(),
        model::expected_machine::ExpectedInterfaceIpAllocation::Dynamic,
    );
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_removing_fixed_declaration_resets_anonymous_dual_stack_interface(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:86".parse()?;
    let interface_mac: MacAddress = "7A:7B:7C:7D:7E:87".parse()?;
    let fixed_ip: IpAddr = "192.0.2.241".parse()?;
    let other_family_ip: IpAddr = "2001:db8::87".parse()?;

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "REMOVE-FIXED-DPU".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: interface_mac.to_string(),
                fixed_ip: Some(fixed_ip.to_string()),
                role: Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    let interface_id = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner()
        .machine_interface_id
        .expect("DHCP should return the preallocated interface");

    let mut txn = pool.begin().await?;
    db::machine_interface_address::insert(
        &mut txn,
        interface_id,
        other_family_ip,
        model::allocation_type::AllocationType::Dhcp,
    )
    .await?;
    txn.commit().await?;

    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.to_string(),
            id: None,
        }))
        .await?;

    let mut txn = pool.begin().await?;
    let interface = db::machine_interface::find_one(&mut *txn, interface_id).await?;
    assert_eq!(
        interface.interface_type,
        model::machine_interface::InterfaceType::Data,
    );
    assert!(interface.primary_interface);
    let addresses =
        db::machine_interface_address::find_for_interface(&mut txn, interface_id).await?;
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].address, other_family_ip);
    assert_eq!(
        addresses[0].allocation_type,
        model::allocation_type::AllocationType::Dhcp,
    );
    let (saved_interface, captured): (
        Option<sqlx::types::Json<model::expected_machine::ExpectedHostNic>>,
        bool,
    ) = sqlx::query_as(
        "SELECT expected_interface, expected_interface_captured
         FROM machine_interfaces
         WHERE id = $1",
    )
    .bind(interface_id)
    .fetch_one(&mut *txn)
    .await?;
    assert!(saved_interface.is_none());
    assert!(captured);
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_machine_delete_preserves_operator_static_reservations(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    // An exact anonymous static assignment may predate the ExpectedMachine.
    // Matching configuration can use it, but must not claim ownership.
    let existing_mac: MacAddress = "7A:7B:7C:7D:7E:77".parse()?;
    let existing_ip: IpAddr = "192.0.2.235".parse()?;
    let mut txn = env.pool.begin().await?;
    db::machine_interface::preallocate_machine_interface(&mut txn, existing_mac, existing_ip, None)
        .await?;
    txn.commit().await?;

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            "7A:7B:7C:7D:7E:76",
            "OPERATOR-EXACT",
            &existing_mac.to_string(),
            &existing_ip.to_string(),
        )))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                existing_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    let mut dynamic = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "7A:7B:7C:7D:7E:76".into(),
            id: None,
        }))
        .await?
        .into_inner();
    dynamic.host_nics[0].fixed_ip = None;
    dynamic.host_nics[0].ip_allocation =
        Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32);
    let error = env
        .api
        .update_expected_machine(tonic::Request::new(dynamic))
        .await
        .expect_err("operator-owned static assignment must block a policy transition");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("operator-owned static address"));

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "7A:7B:7C:7D:7E:76".into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(stored.host_nics[0].fixed_ip.as_deref(), Some("192.0.2.235"));

    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "7A:7B:7C:7D:7E:76".into(),
            id: None,
        }))
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, existing_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert!(interfaces[0].addresses.contains(&existing_ip));
    let expected_machine_owned: bool = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1 AND address = $2::inet",
    )
    .bind(interfaces[0].id)
    .bind(existing_ip)
    .fetch_one(&mut *txn)
    .await?;
    assert!(!expected_machine_owned);
    txn.rollback().await?;

    // Replacing a config-created reservation through the operator API also
    // clears its ownership marker. Deleting the old declaration must leave
    // the replacement address untouched.
    let replaced_mac: MacAddress = "7A:7B:7C:7D:7E:79".parse()?;
    let configured_ip: IpAddr = "192.0.2.236".parse()?;
    let operator_ip: IpAddr = "192.0.2.237".parse()?;
    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            "7A:7B:7C:7D:7E:78",
            "OPERATOR-REPLACED",
            &replaced_mac.to_string(),
            &configured_ip.to_string(),
        )))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                replaced_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, replaced_mac).await?;
    assert_eq!(interfaces.len(), 1);
    db::machine_interface_address::assign_static(&mut txn, interfaces[0].id, operator_ip).await?;
    txn.commit().await?;

    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "7A:7B:7C:7D:7E:78".into(),
            id: None,
        }))
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, replaced_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert!(interfaces[0].addresses.contains(&operator_ip));
    assert!(!interfaces[0].addresses.contains(&configured_ip));
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_legacy_expected_machine_preallocation_transitions_safely(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let interface_mac: MacAddress = "7A:7B:7C:7D:7E:7D".parse()?;
    let old_ip: IpAddr = "192.0.2.239".parse()?;
    let new_ip: IpAddr = "192.0.2.240".parse()?;
    let bmc_mac = "7A:7B:7C:7D:7E:7C";

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            bmc_mac,
            "LEGACY-TRANSITION",
            &interface_mac.to_string(),
            &old_ip.to_string(),
        )))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    // Rows created before ownership tracking are NULL after migration.
    let mut txn = env.pool.begin().await?;
    sqlx::query(
        "UPDATE machine_interface_addresses mia
         SET expected_machine_preallocation = NULL
         FROM machine_interfaces mi
         WHERE mia.interface_id = mi.id AND mi.mac_address = $1",
    )
    .bind(interface_mac)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;

    let mut expected_machine = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.into(),
            id: None,
        }))
        .await?
        .into_inner();
    expected_machine.host_nics[0].fixed_ip = Some(new_ip.to_string());
    env.api
        .update_expected_machine(tonic::Request::new(expected_machine))
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert!(!interfaces[0].addresses.contains(&old_ip));
    assert!(interfaces[0].addresses.contains(&new_ip));
    let expected_machine_owned: Option<bool> = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1 AND address = $2::inet",
    )
    .bind(interfaces[0].id)
    .bind(new_ip)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(expected_machine_owned, Some(true));
    txn.rollback().await?;

    // Configuration deletion remains conservative for an unclassified legacy
    // row because it could have originated from an operator assignment.
    let mut txn = env.pool.begin().await?;
    sqlx::query(
        "UPDATE machine_interface_addresses
         SET expected_machine_preallocation = NULL
         WHERE interface_id = $1 AND address = $2::inet",
    )
    .bind(interfaces[0].id)
    .bind(new_ip)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;
    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.into(),
            id: None,
        }))
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert!(interfaces[0].addresses.contains(&new_ip));
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_machine_delete_preserves_force_cleaned_interface(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let interface_mac: MacAddress = "7A:7B:7C:7D:7E:7B".parse()?;
    let fixed_ip: IpAddr = "192.0.2.238".parse()?;

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine_with_fixed_interface(
            "7A:7B:7C:7D:7E:7A",
            "FORCE-CLEANED",
            &interface_mac.to_string(),
            &fixed_ip.to_string(),
        )))
        .await?;
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                interface_mac,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac).await?;
    assert_eq!(interfaces.len(), 1);
    sqlx::query(
        "UPDATE machine_interfaces
         SET association_type = 'Machine'::association_type
         WHERE id = $1",
    )
    .bind(interfaces[0].id)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;

    env.api
        .delete_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: "7A:7B:7C:7D:7E:7A".into(),
            id: None,
        }))
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac).await?;
    assert_eq!(interfaces.len(), 1);
    assert!(interfaces[0].addresses.contains(&fixed_ip));
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test()]
async fn test_get_expected_machine_error(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address: MacAddress = "2A:2B:2C:2D:2E:2F".parse().unwrap();
    let expected_machine_query = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: bmc_mac_address.to_string(),
        id: None,
    };

    let err = env
        .api
        .get_expected_machine(tonic::Request::new(expected_machine_query))
        .await
        .unwrap_err();

    assert_eq!(
        err.message().to_string(),
        CarbideError::NotFoundError {
            kind: "expected_machine",
            id: bmc_mac_address.to_string(),
        }
        .to_string()
    );
}

#[crate::sqlx_test]
async fn test_get_linked_expected_machines_unseen(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;
    let out = env
        .api
        .get_all_expected_machines_linked(tonic::Request::new(()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.expected_machines.len(), 6);
    // They are sorted by MAC server-side
    let em = out.expected_machines.first().unwrap();
    assert_eq!(em.chassis_serial_number, "VVG121GG");
    assert!(
        em.interface_id.is_none(),
        "expected_machines fixture should have no linked interface"
    );
    assert!(
        em.explored_endpoint_address.is_none(),
        "expected_machines fixture should have no linked explored endpoint"
    );
    assert!(
        em.machine_id.is_none(),
        "expected_machines fixture should have no machine"
    );
    assert!(
        em.expected_machine_id.is_some(),
        "expected_machine_id should be populated from the expected_machines table"
    );
}

#[crate::sqlx_test]
async fn test_get_linked_expected_machines_completed(pool: sqlx::PgPool) {
    // Prep the data

    let env = create_test_env(pool.clone()).await;
    let host_config = model::test_support::ManagedHostConfig::default();
    let bmc_mac = host_config.bmc_mac_address;

    let provided_id = Uuid::new_v4();
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac.to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "GKTEST".into(),
        id: Some(::rpc::common::Uuid {
            value: provided_id.to_string(),
        }),
        ..Default::default()
    };
    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine");

    let (host_machine_id, _dpu_machine_id) =
        common::api_fixtures::create_managed_host_with_config(&env, host_config)
            .await
            .into();
    let host_machine = env.find_machine(host_machine_id).await.remove(0);
    let bmc_ip = host_machine.bmc_info.as_ref().unwrap().ip();

    // The test

    let mut out = env
        .api
        .get_all_expected_machines_linked(tonic::Request::new(()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(out.expected_machines.len(), 1);

    let mut em = out.expected_machines.remove(0);
    assert_eq!(em.chassis_serial_number, "GKTEST");
    assert!(em.interface_id.is_some(), "interface not found");
    assert_eq!(
        em.explored_endpoint_address.take().unwrap(),
        bmc_ip,
        "BMC MAC should match"
    );
    assert_eq!(
        em.machine_id.take().unwrap().to_string(),
        host_machine_id.to_string(),
        "machine id should match via bmc_mac"
    );
    assert!(
        em.expected_machine_id.is_some(),
        "expected_machine_id should be populated"
    );
    assert_eq!(
        em.expected_machine_id.unwrap().value,
        provided_id.to_string(),
        "expected_machine_id should match the ID we provided"
    );
}

#[crate::sqlx_test()]
async fn test_add_expected_machine_dpu_serials(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address: MacAddress = "3A:3B:3C:3D:3E:3F".parse().unwrap();
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac_address.to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "VVG121GI".into(),
        fallback_dpu_serial_numbers: vec!["dpu_serial1".to_string()],
        metadata: Some(rpc::Metadata::default()),
        sku_id: None,
        id: None,
        default_pause_ingestion_and_poweron: Some(true),
        host_nics: vec![],
        rack_id: None,
        is_dpf_enabled: Some(true),
        bmc_ip_address: None,
        bmc_retain_credentials: None,
        dpu_mode: None,
        bmc_ip_allocation: None,
        host_lifecycle_profile: None,
        #[allow(deprecated)]
        dpf_enabled: true,
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine ");

    let expected_machine_query = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: bmc_mac_address.to_string(),
        id: None,
    };

    let mut retrieved_expected_machine = env
        .api
        .get_expected_machine(tonic::Request::new(expected_machine_query))
        .await
        .expect("unable to retrieve expected machine ")
        .into_inner();
    // Zero id for equality test
    retrieved_expected_machine.id = None;
    assert_eq!(retrieved_expected_machine, expected_machine);
}

#[crate::sqlx_test()]
async fn test_add_and_update_expected_machine_with_invalid_metadata(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address: MacAddress = "3A:3B:3C:3D:3E:3F".parse().unwrap();
    // Start adding an expected-machine with invalid metadata
    for (invalid_metadata, expected_err) in common::metadata::invalid_metadata_testcases(false) {
        let expected_machine = rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "VVG121GI".into(),
            fallback_dpu_serial_numbers: vec![],
            metadata: Some(invalid_metadata.clone()),
            sku_id: None,
            id: None,
            default_pause_ingestion_and_poweron: None,
            host_nics: vec![],
            rack_id: None,
            is_dpf_enabled: Some(true),
            ..Default::default()
        };

        let err = env
            .api
            .add_expected_machine(tonic::Request::new(expected_machine.clone()))
            .await
            .expect_err(&format!(
                "Invalid metadata of type should not be accepted: {invalid_metadata:?}"
            ));
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains(&expected_err),
            "Testcase: {:?}\nMessage is \"{}\".\nMessage should contain: \"{}\"",
            invalid_metadata,
            err.message(),
            expected_err
        );
    }

    // Create one with valid metadata, and try to update it to invalid
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac_address.to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "VVG121GI".into(),
        fallback_dpu_serial_numbers: vec![],
        metadata: None,
        sku_id: None,
        id: None,
        default_pause_ingestion_and_poweron: None,
        host_nics: vec![],
        rack_id: None,
        is_dpf_enabled: Some(true),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("Expected addition to succeed");

    for (invalid_metadata, expected_err) in common::metadata::invalid_metadata_testcases(false) {
        let expected_machine = rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "VVG121GI".into(),
            fallback_dpu_serial_numbers: vec![],
            metadata: Some(invalid_metadata.clone()),
            sku_id: None,
            id: None,
            default_pause_ingestion_and_poweron: None,
            host_nics: vec![],
            rack_id: None,
            is_dpf_enabled: Some(true),
            ..Default::default()
        };

        let err = env
            .api
            .update_expected_machine(tonic::Request::new(expected_machine.clone()))
            .await
            .expect_err(&format!(
                "Invalid metadata of type should not be accepted: {invalid_metadata:?}"
            ));
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains(&expected_err),
            "Testcase: {:?}\nMessage is \"{}\".\nMessage should contain: \"{}\"",
            invalid_metadata,
            err.message(),
            expected_err
        );
    }
}

#[crate::sqlx_test()]
async fn test_add_expected_machine_duplicate_dpu_serials(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address: MacAddress = "3A:3B:3C:3D:3E:3F".parse().unwrap();
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac_address.to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "VVG121GI".into(),
        fallback_dpu_serial_numbers: vec!["dpu_serial1".to_string(), "dpu_serial1".to_string()],
        metadata: None,
        sku_id: None,
        id: None,
        default_pause_ingestion_and_poweron: None,
        host_nics: vec![],
        rack_id: None,
        is_dpf_enabled: Some(true),
        ..Default::default()
    };

    assert!(
        env.api
            .add_expected_machine(tonic::Request::new(expected_machine.clone()))
            .await
            .is_err()
    );
}

#[crate::sqlx_test]
async fn test_update_expected_machine_add_dpu_serial(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;

    let env = create_test_env(pool).await;

    let mut ee1 = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "2A:2B:2C:2D:2E:2F".into(),
            id: None,
        }))
        .await
        .expect("unable to get")
        .into_inner();

    ee1.fallback_dpu_serial_numbers = vec!["dpu_serial".to_string()];

    env.api
        .update_expected_machine(tonic::Request::new(ee1.clone()))
        .await
        .expect("unable to update")
        .into_inner();

    let ee2 = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "2A:2B:2C:2D:2E:2F".into(),
            id: None,
        }))
        .await
        .expect("unable to get")
        .into_inner();

    assert_eq!(ee1, ee2);
}
#[crate::sqlx_test]
async fn test_update_expected_machine_add_duplicate_dpu_serial(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;

    let mut ee1 = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "2A:2B:2C:2D:2E:2F".into(),
            id: None,
        }))
        .await
        .expect("unable to get")
        .into_inner();

    ee1.fallback_dpu_serial_numbers = vec![
        "dpu_serial1".to_string(),
        "dpu_serial2".to_string(),
        "dpu_serial1".to_string(),
    ];

    assert!(
        env.api
            .update_expected_machine(tonic::Request::new(ee1.clone()))
            .await
            .is_err()
    );
}

#[crate::sqlx_test]
async fn test_update_expected_machine_add_sku(pool: sqlx::PgPool) {
    create_fixture_expected_machines(&pool).await;
    let env = create_test_env(pool).await;

    let mut ee1 = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "2A:2B:2C:2D:2E:2F".into(),
            id: None,
        }))
        .await
        .expect("unable to get")
        .into_inner();

    ee1.sku_id = Some("sku_id".to_string());

    env.api
        .update_expected_machine(tonic::Request::new(ee1.clone()))
        .await
        .expect("unable to update")
        .into_inner();

    let ee2 = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "2A:2B:2C:2D:2E:2F".into(),
            id: None,
        }))
        .await
        .expect("unable to get")
        .into_inner();

    assert_eq!(ee1, ee2);
}

#[crate::sqlx_test()]
async fn test_add_expected_machine_with_id_and_get_by_id(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let provided_id = Uuid::new_v4().to_string();
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "AA:BB:CC:DD:EE:01".to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "SERIAL-ID".into(),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine with id");

    // Get by id
    let get_req = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
    };
    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(get_req))
        .await
        .expect("unable to retrieve by id")
        .into_inner();

    assert_eq!(
        retrieved.id,
        Some(::rpc::common::Uuid { value: provided_id })
    );
    assert_eq!(retrieved.bmc_mac_address, "AA:BB:CC:DD:EE:01");
}

#[crate::sqlx_test()]
async fn test_update_expected_machine_by_id(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    // Create with id
    let provided_id = Uuid::new_v4().to_string();
    let mut expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "AA:BB:CC:DD:EE:02".to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "SERIAL-1".into(),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("add with id");

    // Update by id (change username)
    expected_machine.bmc_username = "ADMIN_UPDATED".into();
    env.api
        .update_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("update by id");

    // Fetch by id and verify
    let get_req = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
    };
    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(get_req))
        .await
        .expect("get after update by id")
        .into_inner();

    assert_eq!(
        retrieved.id,
        Some(::rpc::common::Uuid { value: provided_id })
    );
    assert_eq!(retrieved.bmc_username, "ADMIN_UPDATED");
}

#[crate::sqlx_test()]
async fn test_delete_expected_machine_by_id(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    // Create with id
    let provided_id = Uuid::new_v4().to_string();
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "AA:BB:CC:DD:EE:03".to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "SERIAL-DEL".into(),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("add with id");

    // Delete by id
    let del_req = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
    };
    env.api
        .delete_expected_machine(tonic::Request::new(del_req))
        .await
        .expect("delete by id");

    // Verify NotFound by id
    let get_req = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: provided_id.clone(),
        }),
    };
    let err = env
        .api
        .get_expected_machine(tonic::Request::new(get_req))
        .await
        .unwrap_err();
    assert_eq!(
        err.message().to_string(),
        CarbideError::NotFoundError {
            kind: "expected_machine",
            id: provided_id
        }
        .to_string()
    );
}

#[crate::sqlx_test()]
async fn test_batch_create_expected_machines_all_or_nothing_success(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    let request = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:01".to_string(),
                    bmc_username: "admin1".to_string(),
                    bmc_password: "pass1".to_string(),
                    chassis_serial_number: "SERIAL-001".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:02".to_string(),
                    bmc_username: "admin2".to_string(),
                    bmc_password: "pass2".to_string(),
                    chassis_serial_number: "SERIAL-002".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: false,
    };

    let response = env
        .api
        .create_expected_machines(tonic::Request::new(request))
        .await
        .expect("batch create should succeed");

    let results = response.into_inner().results;
    assert_eq!(results.len(), 2);
    assert!(results[0].success);
    assert!(results[1].success);

    // Verify both machines were created
    let get_req1 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id1.to_string(),
        }),
    };
    let machine1 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req1))
        .await
        .expect("should find machine 1");
    assert_eq!(machine1.into_inner().bmc_username, "admin1");

    let get_req2 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id2.to_string(),
        }),
    };
    let machine2 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req2))
        .await
        .expect("should find machine 2");
    assert_eq!(machine2.into_inner().bmc_username, "admin2");
}

#[crate::sqlx_test()]
async fn test_batch_create_expected_machines_all_or_nothing_failure(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    let request = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:03".to_string(),
                    bmc_username: "admin1".to_string(),
                    bmc_password: "pass1".to_string(),
                    chassis_serial_number: "SERIAL-003".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:03".to_string(), // Duplicate MAC
                    bmc_username: "admin2".to_string(),
                    bmc_password: "pass2".to_string(),
                    chassis_serial_number: "SERIAL-004".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: false,
    };

    let result = env
        .api
        .create_expected_machines(tonic::Request::new(request))
        .await;

    // Should fail due to duplicate MAC
    assert!(result.is_err());

    // Verify neither machine was created (transaction rollback)
    let get_req1 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id1.to_string(),
        }),
    };
    let result1 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req1))
        .await;
    assert!(result1.is_err());
}

#[crate::sqlx_test()]
async fn test_batch_create_expected_machines_partial_results(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let id3 = Uuid::new_v4();

    let request = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:05".to_string(),
                    bmc_username: "admin1".to_string(),
                    bmc_password: "pass1".to_string(),
                    chassis_serial_number: "SERIAL-005".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(),
                    }),
                    bmc_mac_address: "INVALID-MAC".to_string(), // Invalid MAC
                    bmc_username: "admin2".to_string(),
                    bmc_password: "pass2".to_string(),
                    chassis_serial_number: "SERIAL-006".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id3.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:07".to_string(),
                    bmc_username: "admin3".to_string(),
                    bmc_password: "pass3".to_string(),
                    chassis_serial_number: "SERIAL-007".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: true,
    };

    let response = env
        .api
        .create_expected_machines(tonic::Request::new(request))
        .await
        .expect("batch create should succeed with partial results");

    let results = response.into_inner().results;
    assert_eq!(results.len(), 3);
    assert!(results[0].success, "First machine should succeed");
    assert!(!results[1].success, "Second machine should fail");
    assert!(results[2].success, "Third machine should succeed");

    // Verify first machine was created
    let get_req1 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id1.to_string(),
        }),
    };
    let machine1 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req1))
        .await
        .expect("should find machine 1");
    assert_eq!(machine1.into_inner().bmc_username, "admin1");

    // Verify second machine was NOT created
    let get_req2 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id2.to_string(),
        }),
    };
    let result2 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req2))
        .await;
    assert!(result2.is_err());

    // Verify third machine was created
    let get_req3 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id3.to_string(),
        }),
    };
    let machine3 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req3))
        .await
        .expect("should find machine 3");
    assert_eq!(machine3.into_inner().bmc_username, "admin3");
}

#[crate::sqlx_test()]
async fn test_batch_create_missing_id(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let request = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![rpc::forge::ExpectedMachine {
                id: None, // Missing ID
                bmc_mac_address: "AA:BB:CC:DD:EE:08".to_string(),
                bmc_username: "admin".to_string(),
                bmc_password: "pass".to_string(),
                chassis_serial_number: "SERIAL-008".to_string(),
                metadata: Some(rpc::forge::Metadata::default()),
                ..Default::default()
            }],
        }),
        accept_partial_results: false,
    };

    let result = env
        .api
        .create_expected_machines(tonic::Request::new(request))
        .await;

    assert!(result.is_err(), "Should fail when id is missing");
}

#[crate::sqlx_test()]
async fn test_batch_update_expected_machines_all_or_nothing_success(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    // Create initial machines
    let create_req = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:10".to_string(),
                    bmc_username: "admin1".to_string(),
                    bmc_password: "pass1".to_string(),
                    chassis_serial_number: "SERIAL-010".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:11".to_string(),
                    bmc_username: "admin2".to_string(),
                    bmc_password: "pass2".to_string(),
                    chassis_serial_number: "SERIAL-011".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: false,
    };

    env.api
        .create_expected_machines(tonic::Request::new(create_req))
        .await
        .expect("create should succeed");

    // Update both machines
    let update_req = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:10".to_string(),
                    bmc_username: "admin1_updated".to_string(),
                    bmc_password: "pass1_updated".to_string(),
                    chassis_serial_number: "SERIAL-010".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:11".to_string(),
                    bmc_username: "admin2_updated".to_string(),
                    bmc_password: "pass2_updated".to_string(),
                    chassis_serial_number: "SERIAL-011".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: false,
    };

    let response = env
        .api
        .update_expected_machines(tonic::Request::new(update_req))
        .await
        .expect("batch update should succeed");

    let results = response.into_inner().results;
    assert_eq!(results.len(), 2);
    assert!(results[0].success);
    assert!(results[1].success);

    // Verify both machines were updated
    let get_req1 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id1.to_string(),
        }),
    };
    let machine1 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req1))
        .await
        .expect("should find machine 1");
    assert_eq!(machine1.into_inner().bmc_username, "admin1_updated");

    let get_req2 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id2.to_string(),
        }),
    };
    let machine2 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req2))
        .await
        .expect("should find machine 2");
    assert_eq!(machine2.into_inner().bmc_username, "admin2_updated");
}

#[crate::sqlx_test()]
async fn test_batch_update_expected_machines_all_or_nothing_failure(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    // Create initial machines
    let create_req = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: id1.to_string(),
                }),
                bmc_mac_address: "AA:BB:CC:DD:EE:12".to_string(),
                bmc_username: "admin1".to_string(),
                bmc_password: "pass1".to_string(),
                chassis_serial_number: "SERIAL-012".to_string(),
                metadata: Some(rpc::forge::Metadata::default()),
                ..Default::default()
            }],
        }),
        accept_partial_results: false,
    };

    env.api
        .create_expected_machines(tonic::Request::new(create_req))
        .await
        .expect("create should succeed");

    // Try to update with one valid and one invalid (non-existent id)
    let update_req = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:12".to_string(),
                    bmc_username: "admin1_updated".to_string(),
                    bmc_password: "pass1_updated".to_string(),
                    chassis_serial_number: "SERIAL-012".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(), // Non-existent ID
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:13".to_string(),
                    bmc_username: "admin2".to_string(),
                    bmc_password: "pass2".to_string(),
                    chassis_serial_number: "SERIAL-013".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: false,
    };

    let result = env
        .api
        .update_expected_machines(tonic::Request::new(update_req))
        .await;

    // Should fail
    assert!(result.is_err());

    // Verify first machine was NOT updated (transaction rollback)
    let get_req1 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id1.to_string(),
        }),
    };
    let machine1 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req1))
        .await
        .expect("should find machine 1");
    assert_eq!(
        machine1.into_inner().bmc_username,
        "admin1",
        "Should still have original username due to rollback"
    );
}

#[crate::sqlx_test()]
async fn test_batch_update_expected_machines_partial_results(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let id3 = Uuid::new_v4();

    // Create initial machines
    let create_req = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:14".to_string(),
                    bmc_username: "admin1".to_string(),
                    bmc_password: "pass1".to_string(),
                    chassis_serial_number: "SERIAL-014".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id3.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:16".to_string(),
                    bmc_username: "admin3".to_string(),
                    bmc_password: "pass3".to_string(),
                    chassis_serial_number: "SERIAL-016".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: false,
    };

    env.api
        .create_expected_machines(tonic::Request::new(create_req))
        .await
        .expect("create should succeed");

    // Try to update with partial results
    let update_req = rpc::forge::BatchExpectedMachineOperationRequest {
        expected_machines: Some(rpc::forge::ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id1.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:14".to_string(),
                    bmc_username: "admin1_updated".to_string(),
                    bmc_password: "pass1_updated".to_string(),
                    chassis_serial_number: "SERIAL-014".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id2.to_string(), // Non-existent ID
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:15".to_string(),
                    bmc_username: "admin2".to_string(),
                    bmc_password: "pass2".to_string(),
                    chassis_serial_number: "SERIAL-015".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id3.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:EE:16".to_string(),
                    bmc_username: "admin3_updated".to_string(),
                    bmc_password: "pass3_updated".to_string(),
                    chassis_serial_number: "SERIAL-016".to_string(),
                    metadata: Some(rpc::forge::Metadata::default()),
                    ..Default::default()
                },
            ],
        }),
        accept_partial_results: true,
    };

    let response = env
        .api
        .update_expected_machines(tonic::Request::new(update_req))
        .await
        .expect("batch update should succeed with partial results");

    let results = response.into_inner().results;
    assert_eq!(results.len(), 3);
    assert!(results[0].success, "First update should succeed");
    assert!(!results[1].success, "Second update should fail");
    assert!(results[2].success, "Third update should succeed");

    // Verify first machine was updated
    let get_req1 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id1.to_string(),
        }),
    };
    let machine1 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req1))
        .await
        .expect("should find machine 1");
    assert_eq!(machine1.into_inner().bmc_username, "admin1_updated");

    // Verify second machine does not exist
    let get_req2 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id2.to_string(),
        }),
    };
    let result2 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req2))
        .await;
    assert!(result2.is_err());

    // Verify third machine was updated
    let get_req3 = rpc::forge::ExpectedMachineRequest {
        bmc_mac_address: "".to_string(),
        id: Some(::rpc::common::Uuid {
            value: id3.to_string(),
        }),
    };
    let machine3 = env
        .api
        .get_expected_machine(tonic::Request::new(get_req3))
        .await
        .expect("should find machine 3");
    assert_eq!(machine3.into_inner().bmc_username, "admin3_updated");
}

// test_patch_dpf_enabled_none_to_true verifies that when an expected machine is
// added with is_dpf_enabled: None, the value defaults to true on insert, and a
// subsequent update with is_dpf_enabled: None preserves that value.
#[crate::sqlx_test()]
async fn test_patch_dpf_enabled_none_to_true(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address = "AA:BB:CC:DD:EE:F0";

    // Create machine with dpf_enabled = null (is_dpf_enabled: None)
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "SN-DPF-NULL".into(),
            metadata: Some(rpc::forge::Metadata::default()),
            is_dpf_enabled: None,
            ..Default::default()
        }))
        .await
        .expect("unable to add expected machine");

    // Patch (update) with is_dpf_enabled: None — should keep dpf_enabled as NULL
    let mut updated = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: bmc_mac_address.to_string(),
            id: None,
        }))
        .await
        .expect("unable to fetch expected machine")
        .into_inner();

    // default should be true
    assert_eq!(updated.is_dpf_enabled, Some(true),);

    updated.id = None;
    updated.bmc_username = "ADMIN_PATCHED".into();
    updated.is_dpf_enabled = None;

    env.api
        .update_expected_machine(tonic::Request::new(updated))
        .await
        .expect("unable to update expected machine");

    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: bmc_mac_address.to_string(),
            id: None,
        }))
        .await
        .expect("unable to fetch expected machine after update")
        .into_inner();

    assert_eq!(retrieved.is_dpf_enabled, Some(true),);
}

// test_patch_dpf_enabled_true_stays_true_when_patched_with_null verifies that when
// dpf_enabled is true in the DB and an update is applied with is_dpf_enabled: None,
// the value remains true (not overwritten to NULL).
#[crate::sqlx_test()]
async fn test_patch_dpf_enabled_true_stays_true_when_patched_with_null(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac_address = "AA:BB:CC:DD:EE:F1";

    // Create machine with dpf_enabled = true
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "SN-DPF-TRUE".into(),
            metadata: Some(rpc::forge::Metadata::default()),
            is_dpf_enabled: Some(true),
            ..Default::default()
        }))
        .await
        .expect("unable to add expected machine");

    // Patch (update) with is_dpf_enabled: None — should preserve dpf_enabled = true
    let mut updated = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: bmc_mac_address.to_string(),
            id: None,
        }))
        .await
        .expect("unable to fetch expected machine")
        .into_inner();

    assert_eq!(updated.is_dpf_enabled, Some(true),);

    updated.id = None;
    updated.bmc_username = "ADMIN_PATCHED".into();
    updated.is_dpf_enabled = None;

    env.api
        .update_expected_machine(tonic::Request::new(updated))
        .await
        .expect("unable to update expected machine");

    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: bmc_mac_address.to_string(),
            id: None,
        }))
        .await
        .expect("unable to fetch expected machine after update")
        .into_inner();

    assert_eq!(retrieved.is_dpf_enabled, Some(true),);
}

// --- Optional `ExpectedMachine.bmc_ip_address`: persists configured BMC IP and exercises API
// pre-allocation (`preallocate_machine_interface` / `update_preallocated_machine_interface`). ---
#[crate::sqlx_test()]
async fn test_add_expected_machine_with_static_ip(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "5A:5B:5C:5D:5E:60".to_string(),
        bmc_username: "root".into(),
        bmc_password: "testpass".into(),
        chassis_serial_number: "STATIC-IP-TEST".into(),
        bmc_ip_address: Some("10.0.0.100".to_string()),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: uuid::Uuid::new_v4().to_string(),
        }),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine with static IP");

    let retrieved_machine = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "5A:5B:5C:5D:5E:60".to_string(),
            id: None,
        }))
        .await
        .expect("unable to retrieve expected machine")
        .into_inner();

    assert_eq!(
        retrieved_machine.bmc_ip_address,
        Some("10.0.0.100".to_string())
    );
    assert_eq!(retrieved_machine.bmc_username, "root");
}

#[crate::sqlx_test()]
async fn test_update_expected_machine_add_static_ip(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    // Create machine without static IP
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "5A:5B:5C:5D:5E:62".to_string(),
        bmc_username: "root".into(),
        bmc_password: "testpass".into(),
        chassis_serial_number: "UPDATE-STATIC-IP".into(),
        bmc_ip_address: None,
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: uuid::Uuid::new_v4().to_string(),
        }),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine");

    // Update to add static IP
    let mut updated_machine = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "5A:5B:5C:5D:5E:62".to_string(),
            id: None,
        }))
        .await
        .expect("unable to retrieve expected machine")
        .into_inner();

    updated_machine.id = None;
    updated_machine.bmc_ip_address = Some("192.168.1.50".to_string());

    env.api
        .update_expected_machine(tonic::Request::new(updated_machine.clone()))
        .await
        .expect("unable to update expected machine with static IP");

    let retrieved_machine = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "5A:5B:5C:5D:5E:62".to_string(),
            id: None,
        }))
        .await
        .expect("unable to retrieve expected machine after update")
        .into_inner();

    assert_eq!(
        retrieved_machine.bmc_ip_address,
        Some("192.168.1.50".to_string())
    );
}

#[crate::sqlx_test()]
async fn test_update_expected_machine_change_static_ip(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    // Create machine with static IP
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "5A:5B:5C:5D:5E:63".to_string(),
        bmc_username: "root".into(),
        bmc_password: "testpass".into(),
        chassis_serial_number: "CHANGE-STATIC-IP".into(),
        bmc_ip_address: Some("10.0.0.200".to_string()),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: uuid::Uuid::new_v4().to_string(),
        }),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine");

    // Update to change static IP
    let mut updated_machine = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "5A:5B:5C:5D:5E:63".to_string(),
            id: None,
        }))
        .await
        .expect("unable to retrieve expected machine")
        .into_inner();

    updated_machine.id = None;
    updated_machine.bmc_ip_address = Some("10.0.0.201".to_string());

    env.api
        .update_expected_machine(tonic::Request::new(updated_machine.clone()))
        .await
        .expect("unable to update expected machine IP");

    let retrieved_machine = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: "5A:5B:5C:5D:5E:63".to_string(),
            id: None,
        }))
        .await
        .expect("unable to retrieve expected machine after IP change")
        .into_inner();

    assert_eq!(
        retrieved_machine.bmc_ip_address,
        Some("10.0.0.201".to_string())
    );
}

#[crate::sqlx_test()]
async fn test_add_expected_machine_with_invalid_static_ip(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;

    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "5A:5B:5C:5D:5E:64".to_string(),
        bmc_username: "root".into(),
        bmc_password: "testpass".into(),
        chassis_serial_number: "INVALID-IP".into(),
        bmc_ip_address: Some("not-a-valid-ip".to_string()),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: uuid::Uuid::new_v4().to_string(),
        }),
        ..Default::default()
    };

    let result = env
        .api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await;

    assert!(
        result.is_err(),
        "Should fail when adding machine with invalid IP address"
    );
}

/// Adding an expected machine with `host_nics[].fixed_ip` should result in a static
/// `machine_interface` for that NIC. The materialization is deferred: site-explorer's
/// reconciliation pass (or the DHCP discover hook) is what creates the row. The test
/// triggers that reconciliation after add to verify the end-to-end flow.
#[crate::sqlx_test]
async fn test_add_with_host_nic_fixed_ip_creates_interface(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:01".parse().unwrap();
    let nic_mac: MacAddress = "7A:7B:7C:7D:7E:02".parse().unwrap();
    let fixed_ip = "192.0.2.230";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-FIXEDIP-001".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                network_segment_type: None,
                mac_address: nic_mac.to_string(),
                nic_type: Some("onboard".into()),
                fixed_ip: Some(fixed_ip.into()),
                fixed_mask: None,
                fixed_gateway: None,
                primary: None,
                role: None,
                ip_allocation: None,
            }],
            ..Default::default()
        }))
        .await?;

    // Add doesn't preallocate inline; mimic what site-explorer does on the next iteration --
    // materialize the host NIC's static fixed_ip.
    carbide_site_explorer::try_preallocate_expected_interface(&env.pool, nic_mac, None).await;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, nic_mac).await?;
    assert_eq!(
        interfaces.len(),
        1,
        "should have one interface for the host NIC MAC"
    );
    assert!(
        interfaces[0].addresses.contains(&fixed_ip.parse().unwrap()),
        "interface should have the fixed IP"
    );

    let addrs =
        db::machine_interface_address::find_for_interface(&mut txn, interfaces[0].id).await?;
    assert_eq!(addrs.len(), 1);
    assert_eq!(
        addrs[0].allocation_type,
        model::allocation_type::AllocationType::Static
    );
    let expected_machine_owned: Option<bool> = sqlx::query_scalar(
        "SELECT expected_machine_preallocation
         FROM machine_interface_addresses
         WHERE interface_id = $1 AND address = $2::inet",
    )
    .bind(interfaces[0].id)
    .bind(fixed_ip.parse::<IpAddr>()?)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(expected_machine_owned, Some(true));

    Ok(())
}

#[crate::sqlx_test]
async fn test_update_expected_interface_reconciles_unattached_fixed_reservation(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:11".parse()?;
    let interface_mac: MacAddress = "7A:7B:7C:7D:7E:12".parse()?;
    let first_ip = "192.0.2.233";
    let second_ip = "192.0.2.234";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-FIXEDIP-UPDATE-001".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: interface_mac.to_string(),
                fixed_ip: Some(first_ip.into()),
                role: Some(rpc::forge::ExpectedInterfaceRole::Host as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    carbide_site_explorer::try_preallocate_expected_interface(&env.pool, interface_mac, None).await;

    let mut expected_machine = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    expected_machine.id = None;
    expected_machine.host_nics[0].fixed_ip = Some(second_ip.into());
    env.api
        .update_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await?;

    let mut txn = env.pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac).await?;
    assert_eq!(interfaces.len(), 1);
    let addresses =
        db::machine_interface_address::find_for_interface(&mut txn, interfaces[0].id).await?;
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].address, second_ip.parse::<std::net::IpAddr>()?);
    assert_eq!(
        addresses[0].allocation_type,
        model::allocation_type::AllocationType::Static,
    );
    txn.rollback().await?;

    expected_machine.host_nics[0].fixed_ip = None;
    expected_machine.host_nics[0].ip_allocation =
        Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32);
    env.api
        .update_expected_machine(tonic::Request::new(expected_machine))
        .await?;

    let mut txn = env.pool.begin().await?;
    assert!(
        db::machine_interface::find_by_mac_address(&mut *txn, interface_mac)
            .await?
            .is_empty(),
    );
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_fixed_preallocation_normalizes_primary_for_anonymous_interfaces(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let primary_mac: MacAddress = "7A:7B:7C:7D:7E:21".parse()?;
    let other_mac: MacAddress = "7A:7B:7C:7D:7E:22".parse()?;
    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: "7A:7B:7C:7D:7E:20".into(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "EM-FIXED-PRIMARY-UPDATE".into(),
        host_nics: vec![
            rpc::forge::ExpectedHostNic {
                mac_address: primary_mac.to_string(),
                fixed_ip: Some("192.0.2.244".into()),
                primary: Some(true),
                role: Some(rpc::forge::ExpectedInterfaceRole::Host as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                ..Default::default()
            },
            rpc::forge::ExpectedHostNic {
                mac_address: other_mac.to_string(),
                fixed_ip: Some("192.0.2.245".into()),
                primary: None,
                role: Some(rpc::forge::ExpectedInterfaceRole::Host as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await?;
    carbide_site_explorer::try_preallocate_expected_interface(&env.pool, primary_mac, None).await;
    carbide_site_explorer::try_preallocate_expected_interface(&env.pool, other_mac, None).await;

    let mut txn = env.pool.begin().await?;
    let primary_interfaces =
        db::machine_interface::find_by_mac_address(&mut *txn, primary_mac).await?;
    let other_interfaces = db::machine_interface::find_by_mac_address(&mut *txn, other_mac).await?;
    assert_eq!(primary_interfaces.len(), 1);
    assert_eq!(other_interfaces.len(), 1);
    assert!(primary_interfaces[0].primary_interface);
    assert!(
        !other_interfaces[0].primary_interface,
        "Site Explorer must turn off the legacy default for every other Host",
    );
    txn.rollback().await?;

    // Updating the same declarations runs the handler's inline reconciliation
    // against the existing anonymous rows. It must keep the same primary
    // choice instead of restoring the other Host's legacy default.
    env.api
        .update_expected_machine(tonic::Request::new(expected_machine))
        .await?;

    let mut txn = env.pool.begin().await?;
    let primary_interfaces =
        db::machine_interface::find_by_mac_address(&mut *txn, primary_mac).await?;
    let other_interfaces = db::machine_interface::find_by_mac_address(&mut *txn, other_mac).await?;
    assert_eq!(primary_interfaces.len(), 1);
    assert_eq!(other_interfaces.len(), 1);
    assert!(primary_interfaces[0].primary_interface);
    assert!(
        !other_interfaces[0].primary_interface,
        "inline reconciliation must keep every other Host non-primary",
    );

    Ok(())
}

/// When a device DHCPs with a MAC that has a fixed_ip in the expected
/// machine's host_nics, it should get the fixed IP (not a pool allocation).
#[crate::sqlx_test]
async fn test_dhcp_discover_uses_fixed_ip_from_host_nics(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:03".parse().unwrap();
    let nic_mac: MacAddress = "7A:7B:7C:7D:7E:04".parse().unwrap();
    let fixed_ip = "192.0.2.231";

    // Register expected machine with host NIC fixed_ip.
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-DHCP-001".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                network_segment_type: None,
                mac_address: nic_mac.to_string(),
                nic_type: Some("onboard".into()),
                fixed_ip: Some(fixed_ip.into()),
                fixed_mask: None,
                fixed_gateway: None,
                primary: None,
                role: None,
                ip_allocation: None,
            }],
            ..Default::default()
        }))
        .await?;

    // DHCP discover with the host NIC MAC -- should get the fixed IP.
    let nic_mac_str = nic_mac.to_string();
    let response = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                &nic_mac_str,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner();

    assert_eq!(
        response.address, fixed_ip,
        "DHCP should return the fixed IP from host_nics"
    );

    Ok(())
}

/// First DHCPDISCOVER for an `expected_machines` BMC: discover() consults
/// `find_by_bmc_mac_address`, preallocates from `bmc_ip_address`, and the existing
/// find_or_create path serves that static IP. Add-time doesn't preallocate; row materialization
/// is deferred until this hook fires (for in-network MACs that DHCPDISCOVER) or until
/// site-explorer's reconciliation pass runs (for everything, including external
/// static-assignments IPs).
#[crate::sqlx_test]
async fn test_dhcp_discover_preallocates_bmc_ip_for_unknown_mac(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:41".parse().unwrap();
    let bmc_ip = "192.0.2.245";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-RECOVERY-001".into(),
            bmc_ip_address: Some(bmc_ip.into()),
            ..Default::default()
        }))
        .await?;

    // Add no longer preallocates -- the row should be absent until DHCPDISCOVER fires.
    let mut txn = env.db_txn().await;
    let before = db::machine_interface::find_by_mac_address(txn.as_mut(), bmc_mac).await?;
    assert!(
        before.is_empty(),
        "add does not preallocate inline; the interface should only appear after discover()"
    );
    txn.commit().await?;

    let bmc_mac_str = bmc_mac.to_string();
    let response = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                &bmc_mac_str,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner();

    assert_eq!(
        response.address, bmc_ip,
        "BMC DHCP should serve the configured bmc_ip_address, not a dynamic-pool allocation"
    );

    let mut txn = env.db_txn().await;
    let after = db::machine_interface::find_by_mac_address(txn.as_mut(), bmc_mac).await?;
    assert_eq!(after.len(), 1, "interface should be created by discover()");
    assert!(
        after[0].addresses.contains(&bmc_ip.parse().unwrap()),
        "preallocated interface should have the configured static IP"
    );
    assert_eq!(
        after[0].interface_type,
        model::machine_interface::InterfaceType::Bmc,
        "BMC discover hook should mark the interface as InterfaceType::Bmc, not Data"
    );

    Ok(())
}

#[crate::sqlx_test]
async fn test_legacy_bmc_reservation_precedes_ambiguous_nvos_config(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:42".parse()?;
    let bmc_ip = "192.0.2.244";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-BMC-PRECEDENCE".into(),
            bmc_ip_address: Some(bmc_ip.into()),
            ..Default::default()
        }))
        .await?;

    for (index, switch_bmc_mac) in ["7A:7B:7C:7D:7E:43", "7A:7B:7C:7D:7E:44"]
        .into_iter()
        .enumerate()
    {
        env.api
            .add_expected_switch(tonic::Request::new(rpc::forge::ExpectedSwitch {
                bmc_mac_address: switch_bmc_mac.into(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                switch_serial_number: format!("SW-BMC-PRECEDENCE-{index}"),
                nvos_mac_addresses: vec![format!("7A:7B:7C:7D:8E:{index:02X}")],
                metadata: Some(rpc::forge::Metadata::default()),
                ..Default::default()
            }))
            .await?;
    }

    let mut txn = env.db_txn().await;
    sqlx::query("UPDATE expected_switches SET nvos_mac_addresses = $1::macaddr[]")
        .bind(vec![bmc_mac])
        .execute(txn.as_mut())
        .await?;
    txn.commit().await?;

    let response = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                bmc_mac.to_string(),
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner();

    assert_eq!(response.address, bmc_ip);
    Ok(())
}

/// First DHCPDISCOVER for an `ExpectedHostNic.fixed_ip`. discover() passes the matched NIC
/// through to `validate_existing_mac_and_create`, which honors `fixed_ip` via
/// `AddressSelectionStrategy::StaticAddress`. Pins the deferred preallocation path for host NICs.
#[crate::sqlx_test]
async fn test_dhcp_discover_preallocates_host_nic_fixed_ip_for_unknown_mac(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:51".parse().unwrap();
    let nic_mac: MacAddress = "7A:7B:7C:7D:7E:52".parse().unwrap();
    let fixed_ip = "192.0.2.246";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-RECOVERY-002".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                network_segment_type: None,
                mac_address: nic_mac.to_string(),
                nic_type: Some("onboard".into()),
                fixed_ip: Some(fixed_ip.into()),
                fixed_mask: None,
                fixed_gateway: None,
                primary: None,
                role: None,
                ip_allocation: None,
            }],
            ..Default::default()
        }))
        .await?;

    let mut txn = env.db_txn().await;
    let before = db::machine_interface::find_by_mac_address(txn.as_mut(), nic_mac).await?;
    assert!(
        before.is_empty(),
        "add does not preallocate inline; the interface should only appear after discover()"
    );
    txn.commit().await?;

    let nic_mac_str = nic_mac.to_string();
    let response = env
        .api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                &nic_mac_str,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?
        .into_inner();

    assert_eq!(
        response.address, fixed_ip,
        "host NIC re-DHCP should serve the configured fixed_ip"
    );

    let mut txn = env.db_txn().await;
    let after = db::machine_interface::find_by_mac_address(txn.as_mut(), nic_mac).await?;
    assert_eq!(after.len(), 1, "interface should be created by discover()");
    assert_eq!(
        after[0].interface_type,
        model::machine_interface::InterfaceType::Data,
        "host NIC discover hook should mark the interface as InterfaceType::Data, not Bmc"
    );

    Ok(())
}

/// A DPU BMC declared in `host_nics` should receive its fixed IP through DHCP and
/// remain typed as a BMC interface when the reservation is served again.
#[crate::sqlx_test]
async fn test_dhcp_discover_preallocates_dpu_bmc_fixed_ip_with_bmc_type(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "7A:7B:7C:7D:7E:61".parse().unwrap();
    let dpu_bmc_mac: MacAddress = "7A:7B:7C:7D:7E:62".parse().unwrap();
    let fixed_ip = "192.0.2.232";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-DPU-BMC-001".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                network_segment_type: None,
                mac_address: dpu_bmc_mac.to_string(),
                nic_type: None,
                fixed_ip: Some(fixed_ip.into()),
                fixed_mask: None,
                fixed_gateway: None,
                primary: None,
                role: Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
                ip_allocation: None,
            }],
            ..Default::default()
        }))
        .await?;

    let mut txn = env.db_txn().await;
    let before = db::machine_interface::find_by_mac_address(txn.as_mut(), dpu_bmc_mac).await?;
    assert!(
        before.is_empty(),
        "add should defer DPU BMC interface materialization until DHCP discover"
    );
    txn.commit().await?;

    let dpu_bmc_mac_string = dpu_bmc_mac.to_string();
    for attempt in 1..=2 {
        let response = env
            .api
            .discover_dhcp(
                common::rpc_builder::DhcpDiscovery::builder(
                    &dpu_bmc_mac_string,
                    common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
                )
                .tonic_request(),
            )
            .await?
            .into_inner();

        assert_eq!(
            response.address, fixed_ip,
            "DPU BMC DHCP attempt {attempt} should serve the configured fixed IP"
        );

        let mut txn = env.db_txn().await;
        let interfaces =
            db::machine_interface::find_by_mac_address(txn.as_mut(), dpu_bmc_mac).await?;
        assert_eq!(
            interfaces.len(),
            1,
            "DPU BMC DHCP attempt {attempt} should leave one interface"
        );
        assert!(
            interfaces[0].addresses.contains(&fixed_ip.parse().unwrap()),
            "DPU BMC interface should use its configured fixed IP"
        );
        assert_eq!(
            interfaces[0].interface_type,
            model::machine_interface::InterfaceType::Bmc,
            "DPU BMC DHCP attempt {attempt} should retain InterfaceType::Bmc"
        );
        txn.commit().await?;
    }

    Ok(())
}

/// Older clients omit newly-added expected-interface fields or send their
/// Unspecified discriminants. Single and batch updates must preserve stored
/// explicit values, while a newly-added interface keeps normal inference.
#[crate::sqlx_test]
async fn test_update_expected_machine_preserves_interface_fields_omitted_by_older_client(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (case, suffix, omitted_role, omitted_allocation, use_batch) in [
        ("single update, omitted", 0x63, None, None, false),
        (
            "single update, unspecified",
            0x66,
            Some(rpc::forge::ExpectedInterfaceRole::Unspecified as i32),
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Unspecified as i32),
            false,
        ),
        ("batch update, omitted", 0x69, None, None, true),
        (
            "batch update, unspecified",
            0x6c,
            Some(rpc::forge::ExpectedInterfaceRole::Unspecified as i32),
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Unspecified as i32),
            true,
        ),
    ] {
        let id = Uuid::new_v4();
        let bmc_mac: MacAddress = format!("7A:7B:7C:7D:7E:{suffix:02X}").parse()?;
        let dpu_bmc_mac: MacAddress = format!("7A:7B:7C:7D:7E:{:02X}", suffix + 1).parse()?;
        let new_dpu_os_mac: MacAddress = format!("7A:7B:7C:7D:7E:{:02X}", suffix + 2).parse()?;
        let serial = format!("EM-COMPAT-{suffix:02X}");

        env.api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: id.to_string(),
                }),
                bmc_mac_address: bmc_mac.to_string(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: serial.clone(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: dpu_bmc_mac.to_string(),
                    role: Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await?;

        let update = rpc::forge::ExpectedMachine {
            id: Some(::rpc::common::Uuid {
                value: id.to_string(),
            }),
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "UPDATED_ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: serial,
            host_nics: vec![
                rpc::forge::ExpectedHostNic {
                    mac_address: dpu_bmc_mac.to_string(),
                    role: omitted_role,
                    ip_allocation: omitted_allocation,
                    ..Default::default()
                },
                rpc::forge::ExpectedHostNic {
                    mac_address: new_dpu_os_mac.to_string(),
                    role: Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
                    ip_allocation: omitted_allocation,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        if use_batch {
            let response = env
                .api
                .update_expected_machines(tonic::Request::new(
                    rpc::forge::BatchExpectedMachineOperationRequest {
                        expected_machines: Some(rpc::forge::ExpectedMachineList {
                            expected_machines: vec![update],
                        }),
                        accept_partial_results: false,
                    },
                ))
                .await?
                .into_inner();
            assert!(response.results[0].success, "case: {case}");
        } else {
            env.api
                .update_expected_machine(tonic::Request::new(update))
                .await?;
        }

        let retrieved = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: String::new(),
                id: Some(::rpc::common::Uuid {
                    value: id.to_string(),
                }),
            }))
            .await?
            .into_inner();

        assert_eq!(retrieved.bmc_username, "UPDATED_ADMIN", "case: {case}");
        let retained = retrieved
            .host_nics
            .iter()
            .find(|interface| interface.mac_address == dpu_bmc_mac.to_string())
            .expect("stored interface should remain present");
        assert_eq!(
            retained.role,
            Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
            "case {case}: omitted role should preserve the stored role",
        );
        assert_eq!(
            retained.ip_allocation,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32),
            "case {case}: omitted allocation should preserve the stored explicit policy",
        );

        let added = retrieved
            .host_nics
            .iter()
            .find(|interface| interface.mac_address == new_dpu_os_mac.to_string())
            .expect("new interface should be added");
        assert_eq!(
            added.ip_allocation, None,
            "case {case}: a new interface should keep allocation inference",
        );
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_replace_all_preserves_interface_fields_omitted_by_older_client(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (case, suffix, role, allocation, omit_id) in [
        ("omitted fields and id", 0x72, None, None, true),
        (
            "Unspecified fields",
            0x74,
            Some(rpc::forge::ExpectedInterfaceRole::Unspecified as i32),
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Unspecified as i32),
            false,
        ),
    ] {
        let id = Uuid::new_v4();
        let bmc_mac = format!("7A:7B:7C:7D:80:{suffix:02X}");
        let interface_mac = format!("7A:7B:7C:7D:80:{:02X}", suffix + 1);
        let serial = format!("EM-REPLACE-COMPAT-{suffix:02X}");
        env.api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: id.to_string(),
                }),
                bmc_mac_address: bmc_mac.clone(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: serial.clone(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac.clone(),
                    role: Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await?;

        env.api
            .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
                expected_machines: vec![rpc::forge::ExpectedMachine {
                    id: (!omit_id).then(|| ::rpc::common::Uuid {
                        value: id.to_string(),
                    }),
                    bmc_mac_address: bmc_mac.clone(),
                    bmc_username: "UPDATED".into(),
                    bmc_password: "PASS".into(),
                    chassis_serial_number: serial,
                    host_nics: vec![rpc::forge::ExpectedHostNic {
                        mac_address: interface_mac,
                        role,
                        ip_allocation: allocation,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }))
            .await?;

        let stored = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: if omit_id { bmc_mac } else { String::new() },
                id: (!omit_id).then(|| ::rpc::common::Uuid {
                    value: id.to_string(),
                }),
            }))
            .await?
            .into_inner();
        assert_eq!(stored.bmc_username, "UPDATED", "case: {case}");
        assert_eq!(
            stored.host_nics[0].role,
            Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
            "case: {case}",
        );
        assert_eq!(
            stored.host_nics[0].ip_allocation,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32),
            "case: {case}",
        );
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_atomic_batch_rejects_duplicate_target_ids(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let id = Uuid::new_v4();
    let expected_machines = ["AA:BB:CC:DD:F0:10", "AA:BB:CC:DD:F0:11"]
        .into_iter()
        .enumerate()
        .map(|(index, bmc_mac_address)| rpc::forge::ExpectedMachine {
            id: Some(::rpc::common::Uuid {
                value: id.to_string(),
            }),
            bmc_mac_address: bmc_mac_address.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: format!("EM-DUPLICATE-ID-{index}"),
            ..Default::default()
        })
        .collect();

    let error = env
        .api
        .create_expected_machines(tonic::Request::new(
            rpc::forge::BatchExpectedMachineOperationRequest {
                expected_machines: Some(ExpectedMachineList { expected_machines }),
                accept_partial_results: false,
            },
        ))
        .await
        .expect_err("an atomic batch must reject duplicate target ids");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("duplicate expected_machine id"));

    Ok(())
}

#[crate::sqlx_test]
async fn test_atomic_batch_and_replace_all_preserve_single_update_static_conflict_validation(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let id = Uuid::new_v4();
    let bmc_mac = "AA:BB:CC:DD:F0:14";
    let interface_mac: MacAddress = "AA:BB:CC:DD:F0:15".parse()?;
    let configured_ip: IpAddr = "192.0.2.223".parse()?;
    let operator_ip: IpAddr = "192.0.2.224".parse()?;

    let mut txn = env.pool.begin().await?;
    db::machine_interface::preallocate_machine_interface(
        &mut txn,
        interface_mac,
        operator_ip,
        None,
    )
    .await?;
    txn.commit().await?;

    let mut expected_machine = expected_machine_with_fixed_interface(
        bmc_mac,
        "EM-ATOMIC-STATIC-CONFLICT",
        &interface_mac.to_string(),
        &configured_ip.to_string(),
    );
    expected_machine.id = Some(::rpc::common::Uuid {
        value: id.to_string(),
    });
    env.api
        .add_expected_machine(tonic::Request::new(expected_machine))
        .await?;

    let mut update = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.into(),
            id: None,
        }))
        .await?
        .into_inner();
    update.host_nics[0].fixed_ip = None;
    update.host_nics[0].ip_allocation =
        Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32);

    let single_error = env
        .api
        .update_expected_machine(tonic::Request::new(update.clone()))
        .await
        .expect_err("single update must reject a conflicting operator static address");
    assert_eq!(single_error.code(), tonic::Code::InvalidArgument);

    let batch_error = env
        .api
        .update_expected_machines(tonic::Request::new(
            rpc::forge::BatchExpectedMachineOperationRequest {
                expected_machines: Some(ExpectedMachineList {
                    expected_machines: vec![update.clone()],
                }),
                accept_partial_results: false,
            },
        ))
        .await
        .expect_err("atomic update must enforce the same static-address conflict");
    assert_eq!(batch_error.code(), tonic::Code::InvalidArgument);
    assert_eq!(batch_error.message(), single_error.message());

    let replace_error = env
        .api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: vec![update],
        }))
        .await
        .expect_err("ReplaceAll must enforce the same static-address conflict");
    assert_eq!(replace_error.code(), tonic::Code::InvalidArgument);
    assert_eq!(replace_error.message(), single_error.message());

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(stored.host_nics[0].fixed_ip.as_deref(), Some("192.0.2.223"),);

    let mut txn = env.pool.begin().await?;
    let interface = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac)
        .await?
        .pop()
        .expect("operator reservation should remain");
    assert!(interface.addresses.contains(&operator_ip));
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_replace_all_allows_interface_transfer_with_operator_static_address(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let source_id = Uuid::new_v4();
    let target_id = Uuid::new_v4();
    let source_bmc = "AA:BB:CC:DD:F0:20";
    let target_bmc = "AA:BB:CC:DD:F0:21";
    let interface_mac: MacAddress = "AA:BB:CC:DD:F0:22".parse()?;
    let configured_ip: IpAddr = "192.0.2.227".parse()?;
    let operator_ip: IpAddr = "192.0.2.228".parse()?;

    let mut txn = env.pool.begin().await?;
    db::machine_interface::preallocate_machine_interface(
        &mut txn,
        interface_mac,
        operator_ip,
        None,
    )
    .await?;
    txn.commit().await?;

    let mut source = expected_machine_with_fixed_interface(
        source_bmc,
        "EM-REPLACE-TRANSFER-SOURCE",
        &interface_mac.to_string(),
        &configured_ip.to_string(),
    );
    source.id = Some(::rpc::common::Uuid {
        value: source_id.to_string(),
    });
    env.api
        .add_expected_machine(tonic::Request::new(source))
        .await?;
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: Some(::rpc::common::Uuid {
                value: target_id.to_string(),
            }),
            bmc_mac_address: target_bmc.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-REPLACE-TRANSFER-TARGET".into(),
            ..Default::default()
        }))
        .await?;

    env.api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: source_id.to_string(),
                    }),
                    bmc_mac_address: source_bmc.into(),
                    bmc_username: "ADMIN".into(),
                    bmc_password: "PASS".into(),
                    chassis_serial_number: "EM-REPLACE-TRANSFER-SOURCE".into(),
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: target_id.to_string(),
                    }),
                    bmc_mac_address: target_bmc.into(),
                    bmc_username: "ADMIN".into(),
                    bmc_password: "PASS".into(),
                    chassis_serial_number: "EM-REPLACE-TRANSFER-TARGET".into(),
                    host_nics: vec![rpc::forge::ExpectedHostNic {
                        mac_address: interface_mac.to_string(),
                        ip_allocation: Some(
                            rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32,
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        }))
        .await?;

    let source = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: source_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert!(source.host_nics.is_empty());
    let target = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: target_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(target.host_nics[0].mac_address, interface_mac.to_string());

    let mut txn = env.pool.begin().await?;
    let interface = db::machine_interface::find_by_mac_address(&mut *txn, interface_mac)
        .await?
        .pop()
        .expect("operator reservation should remain");
    assert!(interface.addresses.contains(&operator_ip));
    txn.rollback().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_atomic_batch_and_replace_all_allow_non_identity_updates_with_legacy_duplicate_macs(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let duplicate_mac: MacAddress = "AA:BB:CC:DD:F0:19".parse()?;

    let mut txn = env.pool.begin().await?;
    for (id, bmc_mac_address, serial_number) in [
        (first_id, "AA:BB:CC:DD:F0:17", "EM-LEGACY-DUP-A"),
        (second_id, "AA:BB:CC:DD:F0:18", "EM-LEGACY-DUP-B"),
    ] {
        db::expected_machine::create(
            &mut txn,
            ExpectedMachine {
                id: Some(id),
                bmc_mac_address: bmc_mac_address.parse()?,
                data: ExpectedMachineData {
                    bmc_username: "ADMIN".into(),
                    bmc_password: "PASS".into(),
                    serial_number: serial_number.into(),
                    host_nics: vec![model::expected_machine::ExpectedHostNic {
                        mac_address: duplicate_mac,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
        )
        .await?;
    }
    txn.commit().await?;

    let response = env
        .api
        .update_expected_machines(tonic::Request::new(
            rpc::forge::BatchExpectedMachineOperationRequest {
                expected_machines: Some(ExpectedMachineList {
                    expected_machines: vec![rpc::forge::ExpectedMachine {
                        id: Some(::rpc::common::Uuid {
                            value: first_id.to_string(),
                        }),
                        bmc_mac_address: "AA:BB:CC:DD:F0:17".into(),
                        bmc_username: "ADMIN".into(),
                        bmc_password: "PASS".into(),
                        chassis_serial_number: "EM-LEGACY-DUP-A".into(),
                        host_nics: vec![rpc::forge::ExpectedHostNic {
                            mac_address: duplicate_mac.to_string(),
                            role: Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
                            ip_allocation: Some(
                                rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32,
                            ),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                }),
                accept_partial_results: false,
            },
        ))
        .await?
        .into_inner();
    assert!(response.results[0].success);

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: String::new(),
            id: Some(::rpc::common::Uuid {
                value: first_id.to_string(),
            }),
        }))
        .await?
        .into_inner();
    assert_eq!(
        stored.host_nics[0].role,
        Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
    );

    env.api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: vec![
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: first_id.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:F0:17".into(),
                    bmc_username: "UPDATED_ADMIN".into(),
                    bmc_password: "PASS".into(),
                    chassis_serial_number: "EM-LEGACY-DUP-A".into(),
                    host_nics: vec![rpc::forge::ExpectedHostNic {
                        mac_address: duplicate_mac.to_string(),
                        role: Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
                        ip_allocation: Some(
                            rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32,
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: second_id.to_string(),
                    }),
                    bmc_mac_address: "AA:BB:CC:DD:F0:18".into(),
                    bmc_username: "UPDATED_ADMIN".into(),
                    bmc_password: "PASS".into(),
                    chassis_serial_number: "EM-LEGACY-DUP-B".into(),
                    host_nics: vec![rpc::forge::ExpectedHostNic {
                        mac_address: duplicate_mac.to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
        }))
        .await?;

    for id in [first_id, second_id] {
        let stored = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: String::new(),
                id: Some(::rpc::common::Uuid {
                    value: id.to_string(),
                }),
            }))
            .await?
            .into_inner();
        assert_eq!(stored.bmc_username, "UPDATED_ADMIN");
        assert_eq!(stored.host_nics[0].mac_address, duplicate_mac.to_string());
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_atomic_batch_interface_mac_swaps_are_request_order_independent(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (case, suffix, reverse) in [
        ("first machine first", 0x20, false),
        ("second machine first", 0x30, true),
    ] {
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let first_bmc = format!("AA:BB:CC:DD:F1:{suffix:02X}");
        let second_bmc = format!("AA:BB:CC:DD:F1:{:02X}", suffix + 1);
        let first_interface = format!("AA:BB:CC:DD:F1:{:02X}", suffix + 2);
        let second_interface = format!("AA:BB:CC:DD:F1:{:02X}", suffix + 3);

        for (id, bmc_mac_address, interface_mac, serial) in [
            (
                first_id,
                first_bmc.as_str(),
                first_interface.as_str(),
                format!("EM-MAC-SWAP-A-{suffix:02X}"),
            ),
            (
                second_id,
                second_bmc.as_str(),
                second_interface.as_str(),
                format!("EM-MAC-SWAP-B-{suffix:02X}"),
            ),
        ] {
            env.api
                .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                    id: Some(::rpc::common::Uuid {
                        value: id.to_string(),
                    }),
                    bmc_mac_address: bmc_mac_address.into(),
                    bmc_username: "ADMIN".into(),
                    bmc_password: "PASS".into(),
                    chassis_serial_number: serial,
                    host_nics: vec![rpc::forge::ExpectedHostNic {
                        mac_address: interface_mac.into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
                .await?;
        }

        let mut updates = vec![
            rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: first_id.to_string(),
                }),
                bmc_mac_address: first_bmc.clone(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: format!("EM-MAC-SWAP-A-{suffix:02X}"),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: second_interface.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            },
            rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: second_id.to_string(),
                }),
                bmc_mac_address: second_bmc.clone(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: format!("EM-MAC-SWAP-B-{suffix:02X}"),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: first_interface.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ];
        if reverse {
            updates.reverse();
        }

        let response = env
            .api
            .update_expected_machines(tonic::Request::new(
                rpc::forge::BatchExpectedMachineOperationRequest {
                    expected_machines: Some(ExpectedMachineList {
                        expected_machines: updates,
                    }),
                    accept_partial_results: false,
                },
            ))
            .await?
            .into_inner();
        assert!(
            response.results.iter().all(|result| result.success),
            "case: {case}",
        );

        let stored_first = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: String::new(),
                id: Some(::rpc::common::Uuid {
                    value: first_id.to_string(),
                }),
            }))
            .await?
            .into_inner();
        let stored_second = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: String::new(),
                id: Some(::rpc::common::Uuid {
                    value: second_id.to_string(),
                }),
            }))
            .await?
            .into_inner();
        assert_eq!(
            stored_first.host_nics[0].mac_address, second_interface,
            "case: {case}",
        );
        assert_eq!(
            stored_second.host_nics[0].mac_address, first_interface,
            "case: {case}",
        );
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_atomic_batch_fixed_ip_transfers_are_request_order_independent(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (case, suffix, fixed_ip, reverse) in [
        ("release first", 0x40, "192.0.2.221", false),
        ("reserve first", 0x50, "192.0.2.222", true),
    ] {
        let first_id = Uuid::new_v4();
        let second_id = Uuid::new_v4();
        let first_bmc = format!("AA:BB:CC:DD:F2:{suffix:02X}");
        let second_bmc = format!("AA:BB:CC:DD:F2:{:02X}", suffix + 1);
        let first_interface = format!("AA:BB:CC:DD:F2:{:02X}", suffix + 2);
        let second_interface = format!("AA:BB:CC:DD:F2:{:02X}", suffix + 3);

        let mut first_machine = expected_machine_with_fixed_interface(
            &first_bmc,
            &format!("EM-IP-TRANSFER-A-{suffix:02X}"),
            &first_interface,
            fixed_ip,
        );
        first_machine.id = Some(::rpc::common::Uuid {
            value: first_id.to_string(),
        });
        env.api
            .add_expected_machine(tonic::Request::new(first_machine.clone()))
            .await?;

        env.api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: second_id.to_string(),
                }),
                bmc_mac_address: second_bmc.clone(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: format!("EM-IP-TRANSFER-B-{suffix:02X}"),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: second_interface.clone(),
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await?;
        env.api
            .update_expected_machine(tonic::Request::new(first_machine))
            .await?;

        let mut txn = env.pool.begin().await?;
        let source_interfaces = db::machine_interface::find_by_mac_address(
            &mut *txn,
            first_interface.parse::<MacAddress>()?,
        )
        .await?;
        assert_eq!(source_interfaces.len(), 1, "case: {case}");
        let source_interface = &source_interfaces[0];
        assert!(source_interface.machine_id.is_none(), "case: {case}");
        assert!(
            source_interface.attached_dpu_machine_id.is_none(),
            "case: {case}",
        );
        assert!(source_interface.switch_id.is_none(), "case: {case}");
        assert!(source_interface.power_shelf_id.is_none(), "case: {case}");
        assert!(
            matches!(
                source_interface.association_type,
                None | Some(model::machine_interface_address::InterfaceAssociationType::None)
            ),
            "case: {case}",
        );
        txn.rollback().await?;

        let mut updates = vec![
            rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: first_id.to_string(),
                }),
                bmc_mac_address: first_bmc,
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: format!("EM-IP-TRANSFER-A-{suffix:02X}"),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: first_interface,
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32),
                    ..Default::default()
                }],
                ..Default::default()
            },
            expected_machine_with_fixed_interface(
                &second_bmc,
                &format!("EM-IP-TRANSFER-B-{suffix:02X}"),
                &second_interface,
                fixed_ip,
            ),
        ];
        updates[1].id = Some(::rpc::common::Uuid {
            value: second_id.to_string(),
        });
        if reverse {
            updates.reverse();
        }

        let response = env
            .api
            .update_expected_machines(tonic::Request::new(
                rpc::forge::BatchExpectedMachineOperationRequest {
                    expected_machines: Some(ExpectedMachineList {
                        expected_machines: updates,
                    }),
                    accept_partial_results: false,
                },
            ))
            .await?
            .into_inner();
        assert!(
            response.results.iter().all(|result| result.success),
            "case: {case}",
        );
        let discovery = env
            .api
            .discover_dhcp(
                common::rpc_builder::DhcpDiscovery::builder(
                    second_interface.parse::<MacAddress>()?,
                    common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
                )
                .tonic_request(),
            )
            .await?
            .into_inner();
        assert_eq!(discovery.address, fixed_ip, "case: {case}");
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_older_client_fixed_ip_transitions_reinstate_legacy_inference(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    struct Case {
        scenario: &'static str,
        suffix: u8,
        stored_allocation: rpc::forge::ExpectedInterfaceIpAllocation,
        stored_fixed_ip: Option<&'static str>,
        next_fixed_ip: Option<&'static str>,
        omitted_allocation: Option<i32>,
        use_batch: bool,
    }

    let cases = [
        Case {
            scenario: "single update removes fixed IP with omitted policy",
            suffix: 0x80,
            stored_allocation: rpc::forge::ExpectedInterfaceIpAllocation::Fixed,
            stored_fixed_ip: Some("192.0.2.210"),
            next_fixed_ip: None,
            omitted_allocation: None,
            use_batch: false,
        },
        Case {
            scenario: "single update adds fixed IP with Unspecified policy",
            suffix: 0x82,
            stored_allocation: rpc::forge::ExpectedInterfaceIpAllocation::Retained,
            stored_fixed_ip: None,
            next_fixed_ip: Some("192.0.2.211"),
            omitted_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Unspecified as i32),
            use_batch: false,
        },
        Case {
            scenario: "batch update removes fixed IP with Unspecified policy",
            suffix: 0x84,
            stored_allocation: rpc::forge::ExpectedInterfaceIpAllocation::Fixed,
            stored_fixed_ip: Some("192.0.2.212"),
            next_fixed_ip: None,
            omitted_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Unspecified as i32),
            use_batch: true,
        },
        Case {
            scenario: "batch update adds fixed IP with omitted policy",
            suffix: 0x86,
            stored_allocation: rpc::forge::ExpectedInterfaceIpAllocation::Dynamic,
            stored_fixed_ip: None,
            next_fixed_ip: Some("192.0.2.213"),
            omitted_allocation: None,
            use_batch: true,
        },
    ];

    let env = create_test_env(pool).await;
    for case in cases {
        let id = Uuid::new_v4();
        let bmc_mac = format!("7A:7B:7C:7D:7E:{:02X}", case.suffix);
        let interface_mac = format!("7A:7B:7C:7D:7E:{:02X}", case.suffix + 1);
        let serial = format!("EM-LEGACY-FIXED-{:02X}", case.suffix);

        env.api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                id: Some(::rpc::common::Uuid {
                    value: id.to_string(),
                }),
                bmc_mac_address: bmc_mac.clone(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: serial.clone(),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac.clone(),
                    fixed_ip: case.stored_fixed_ip.map(str::to_string),
                    role: Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
                    ip_allocation: Some(case.stored_allocation as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await?;

        let update = rpc::forge::ExpectedMachine {
            id: Some(::rpc::common::Uuid {
                value: id.to_string(),
            }),
            bmc_mac_address: bmc_mac,
            bmc_username: "UPDATED".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: serial,
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: interface_mac,
                fixed_ip: case.next_fixed_ip.map(str::to_string),
                role: None,
                ip_allocation: case.omitted_allocation,
                ..Default::default()
            }],
            ..Default::default()
        };

        if case.use_batch {
            let response = env
                .api
                .update_expected_machines(tonic::Request::new(
                    rpc::forge::BatchExpectedMachineOperationRequest {
                        expected_machines: Some(ExpectedMachineList {
                            expected_machines: vec![update],
                        }),
                        accept_partial_results: false,
                    },
                ))
                .await?
                .into_inner();
            assert!(response.results[0].success, "case: {}", case.scenario);
        } else {
            env.api
                .update_expected_machine(tonic::Request::new(update))
                .await
                .unwrap_or_else(|error| panic!("case {}: {error}", case.scenario));
        }

        let retrieved = env
            .api
            .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
                bmc_mac_address: String::new(),
                id: Some(::rpc::common::Uuid {
                    value: id.to_string(),
                }),
            }))
            .await?
            .into_inner();
        assert_eq!(
            retrieved.host_nics[0].fixed_ip.as_deref(),
            case.next_fixed_ip
        );
        assert_eq!(
            retrieved.host_nics[0].ip_allocation, None,
            "case {}: fixed_ip presence should restore legacy policy inference",
            case.scenario,
        );
        assert_eq!(
            retrieved.host_nics[0].role,
            Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
            "case {}: role preservation remains independent",
            case.scenario,
        );
    }

    Ok(())
}

/// When `bmc_retain_credentials` is set to true, the value should persist through
/// add -> get round-trip via the RPC API.
#[crate::sqlx_test()]
async fn test_add_expected_machine_with_bmc_retain_credentials(pool: sqlx::PgPool) {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "5A:5B:5C:5D:5E:70".parse().unwrap();

    let expected_machine = rpc::forge::ExpectedMachine {
        bmc_mac_address: bmc_mac.to_string(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "RETAIN-CREDS-001".into(),
        metadata: Some(rpc::forge::Metadata::default()),
        id: Some(::rpc::common::Uuid {
            value: Uuid::new_v4().to_string(),
        }),
        bmc_retain_credentials: Some(true),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(expected_machine.clone()))
        .await
        .expect("unable to add expected machine");

    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.to_string(),
            id: None,
        }))
        .await
        .expect("unable to retrieve expected machine")
        .into_inner();

    assert_eq!(
        retrieved.bmc_retain_credentials,
        Some(true),
        "bmc_retain_credentials should be true after round-trip"
    );
}

/// Verify that updating an expected machine without specifying `bmc_retain_credentials`
/// preserves the existing value (and making sure COALESCE works).
#[crate::sqlx_test()]
async fn test_update_preserves_bmc_retain_credentials(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "5A:5B:5C:5D:5E:71".parse().unwrap();

    // Create with bmc_retain_credentials = true.
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "RETAIN-UPDATE-001".into(),
            metadata: Some(rpc::forge::Metadata::default()),
            id: Some(::rpc::common::Uuid {
                value: Uuid::new_v4().to_string(),
            }),
            bmc_retain_credentials: Some(true),
            ..Default::default()
        }))
        .await?;

    // Update without setting bmc_retain_credentials (None).
    env.api
        .update_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "NEW-ADMIN".into(),
            bmc_password: "NEW-PASS".into(),
            chassis_serial_number: "RETAIN-UPDATE-001".into(),
            metadata: Some(rpc::forge::Metadata::default()),
            bmc_retain_credentials: None,
            ..Default::default()
        }))
        .await?;

    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: bmc_mac.to_string(),
            id: None,
        }))
        .await?
        .into_inner();

    assert_eq!(
        retrieved.bmc_retain_credentials,
        Some(true),
        "bmc_retain_credentials should be preserved after update with None"
    );

    Ok(())
}

/// When an ExpectedMachine's host_nics entry is flagged `primary: true`,
/// the matching NIC's DHCP should land as `machine_interfaces.primary_interface=true`.
#[crate::sqlx_test]
async fn test_dhcp_honors_primary_host_nic(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    // rack_management_enabled is required for discover_dhcp to consult
    // ExpectedMachine records for unknown MACs -- that's the path that
    // reads the matched host_nic's `primary` flag.
    let env = {
        let mut config = get_config();
        config.rack_management_enabled = true;
        create_test_env_with_overrides(pool, TestEnvOverrides::with_config(config)).await
    };
    let bmc_mac: MacAddress = "9A:9B:9C:9D:9E:01".parse().unwrap();
    let primary_mac: MacAddress = "9A:9B:9C:9D:9E:02".parse().unwrap();

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-PRIMARY-001".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                network_segment_type: None,
                mac_address: primary_mac.to_string(),
                nic_type: Some("onboard".into()),
                fixed_ip: None,
                fixed_mask: None,
                fixed_gateway: None,
                primary: Some(true),
                role: None,
                ip_allocation: None,
            }],
            ..Default::default()
        }))
        .await?;

    // DHCP discover with the declared primary MAC.
    let primary_mac_str = primary_mac.to_string();
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                &primary_mac_str,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    // Verify the created machine_interface is flagged primary=true.
    let mut txn = env.pool.begin().await?;
    let ifaces = db::machine_interface::find_by_mac_address(&mut *txn, primary_mac).await?;
    assert_eq!(ifaces.len(), 1);
    assert!(
        ifaces[0].primary_interface,
        "host_nic primary=true should flow to machine_interfaces.primary_interface"
    );

    Ok(())
}

/// When one host_nics entry is flagged `primary: true`, a DHCP from a
/// *different* MAC on the same host should land as `primary_interface: false`.
/// Verifies the "operator declared some other NIC primary, so this one
/// must not inherit the default primary=true" branch, protecting the DB's
/// one_primary_interface_per_machine unique constraint once the primary
/// MAC's interface eventually lands.
#[crate::sqlx_test]
async fn test_dhcp_marks_non_primary_mac_as_non_primary(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = {
        let mut config = get_config();
        config.rack_management_enabled = true;
        create_test_env_with_overrides(pool, TestEnvOverrides::with_config(config)).await
    };
    let bmc_mac: MacAddress = "9A:9B:9C:9D:9E:10".parse().unwrap();
    let primary_mac: MacAddress = "9A:9B:9C:9D:9E:11".parse().unwrap();
    let other_mac: MacAddress = "9A:9B:9C:9D:9E:12".parse().unwrap();

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-PRIMARY-002".into(),
            host_nics: vec![
                rpc::forge::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: primary_mac.to_string(),
                    nic_type: Some("onboard".into()),
                    fixed_ip: None,
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: Some(true),
                    role: None,
                    ip_allocation: None,
                },
                rpc::forge::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: other_mac.to_string(),
                    nic_type: Some("onboard".into()),
                    fixed_ip: None,
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: None,
                    role: None,
                    ip_allocation: None,
                },
            ],
            ..Default::default()
        }))
        .await?;

    // DHCP for the non-primary MAC on this machine.
    let other_mac_str = other_mac.to_string();
    env.api
        .discover_dhcp(
            common::rpc_builder::DhcpDiscovery::builder(
                &other_mac_str,
                common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
            )
            .tonic_request(),
        )
        .await?;

    let mut txn = env.pool.begin().await?;
    let ifaces = db::machine_interface::find_by_mac_address(&mut *txn, other_mac).await?;
    assert_eq!(ifaces.len(), 1);
    assert!(
        !ifaces[0].primary_interface,
        "a MAC that isn't the declared primary should not land as primary_interface=true"
    );

    Ok(())
}

/// An ExpectedMachine with two host_nics entries both flagged `primary: true`
/// must be rejected at the API boundary -- the handler enforces at most one
/// primary NIC per machine (anchoring the DB's `one_primary_interface_per_machine`
/// unique constraint to a single declaration).
#[crate::sqlx_test]
async fn test_add_rejects_multiple_primary_host_nics(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "9A:9B:9C:9D:9E:20".parse().unwrap();
    let mac_a: MacAddress = "9A:9B:9C:9D:9E:21".parse().unwrap();
    let mac_b: MacAddress = "9A:9B:9C:9D:9E:22".parse().unwrap();

    let result = env
        .api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-DUPLICATE-PRIMARY-001".into(),
            host_nics: vec![
                rpc::forge::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: mac_a.to_string(),
                    nic_type: Some("onboard".into()),
                    fixed_ip: None,
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: Some(true),
                    role: None,
                    ip_allocation: None,
                },
                rpc::forge::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: mac_b.to_string(),
                    nic_type: Some("onboard".into()),
                    fixed_ip: None,
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: Some(true),
                    role: None,
                    ip_allocation: None,
                },
            ],
            ..Default::default()
        }))
        .await;

    let err = result.expect_err("multi-primary ExpectedMachine should be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    Ok(())
}

/// Allocation/address consistency is independent of which endpoint owns the
/// interface. Each role accepts inferred Dynamic, explicit Dynamic/Retained,
/// and Fixed with an address.
#[crate::sqlx_test]
async fn test_add_accepts_expected_interface_allocation_policies_for_every_role(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (case, suffix, role, allocation, fixed_ip) in [
        (
            "Host inferred dynamic",
            0x10,
            rpc::forge::ExpectedInterfaceRole::Host,
            None,
            None,
        ),
        (
            "DPU OS explicit dynamic",
            0x12,
            rpc::forge::ExpectedInterfaceRole::DpuOs,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic),
            None,
        ),
        (
            "DPU BMC inferred dynamic",
            0x14,
            rpc::forge::ExpectedInterfaceRole::DpuBmc,
            None,
            None,
        ),
        (
            "Host retained",
            0x16,
            rpc::forge::ExpectedInterfaceRole::Host,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained),
            None,
        ),
        (
            "DPU OS retained",
            0x18,
            rpc::forge::ExpectedInterfaceRole::DpuOs,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained),
            None,
        ),
        (
            "DPU BMC retained",
            0x1a,
            rpc::forge::ExpectedInterfaceRole::DpuBmc,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained),
            None,
        ),
        (
            "Host fixed",
            0x1c,
            rpc::forge::ExpectedInterfaceRole::Host,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed),
            Some("192.0.2.220"),
        ),
        (
            "DPU OS fixed",
            0x1e,
            rpc::forge::ExpectedInterfaceRole::DpuOs,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed),
            Some("192.0.2.221"),
        ),
        (
            "DPU BMC fixed",
            0x20,
            rpc::forge::ExpectedInterfaceRole::DpuBmc,
            Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed),
            Some("192.0.2.222"),
        ),
    ] {
        let bmc_mac = format!("9A:9B:9C:9D:9E:{suffix:02X}");
        let interface_mac = format!("9A:9B:9C:9D:9E:{:02X}", suffix + 1);
        let result = env
            .api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                bmc_mac_address: bmc_mac,
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: format!("EM-POLICY-{suffix:02X}"),
                host_nics: vec![rpc::forge::ExpectedHostNic {
                    mac_address: interface_mac,
                    fixed_ip: fixed_ip.map(str::to_string),
                    role: Some(role as i32),
                    ip_allocation: allocation.map(|allocation| allocation as i32),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await;

        result.unwrap_or_else(|error| panic!("case {case} should be accepted: {error}"));
    }

    Ok(())
}

/// Invalid allocation/address combinations are rejected for Host, DPU OS, and
/// DPU BMC alike. The host's top-level BMC identity also remains separate from
/// the nested interface list.
#[crate::sqlx_test]
async fn test_add_rejects_invalid_expected_interface_declarations(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (case, bmc_mac, serial_number, host_nics, expected_error) in [
        (
            "Host fixed without an address",
            "9A:9B:9C:9D:9E:30",
            "EM-INVALID-INTERFACE-1",
            vec![rpc::forge::ExpectedHostNic {
                mac_address: "9A:9B:9C:9D:9E:31".into(),
                role: Some(rpc::forge::ExpectedInterfaceRole::Host as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                ..Default::default()
            }],
            "ip_allocation=fixed requires fixed_ip",
        ),
        (
            "DPU OS dynamic with an address",
            "9A:9B:9C:9D:9E:32",
            "EM-INVALID-INTERFACE-2",
            vec![rpc::forge::ExpectedHostNic {
                mac_address: "9A:9B:9C:9D:9E:33".into(),
                fixed_ip: Some("192.0.2.223".into()),
                role: Some(rpc::forge::ExpectedInterfaceRole::DpuOs as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Dynamic as i32),
                ..Default::default()
            }],
            "ip_allocation=dynamic cannot be combined with fixed_ip",
        ),
        (
            "DPU BMC retained with an address",
            "9A:9B:9C:9D:9E:34",
            "EM-INVALID-INTERFACE-3",
            vec![rpc::forge::ExpectedHostNic {
                mac_address: "9A:9B:9C:9D:9E:35".into(),
                fixed_ip: Some("192.0.2.224".into()),
                role: Some(rpc::forge::ExpectedInterfaceRole::DpuBmc as i32),
                ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Retained as i32),
                ..Default::default()
            }],
            "ip_allocation=retained cannot be combined with fixed_ip; use fixed",
        ),
        (
            "nested interface matching top-level BMC",
            "9A:9B:9C:9D:9E:36",
            "EM-INVALID-INTERFACE-4",
            vec![rpc::forge::ExpectedHostNic {
                mac_address: "9A:9B:9C:9D:9E:36".into(),
                ..Default::default()
            }],
            "duplicates the expected machine BMC MAC address",
        ),
        (
            "duplicate fixed addresses",
            "9A:9B:9C:9D:9E:37",
            "EM-INVALID-INTERFACE-5",
            vec![
                rpc::forge::ExpectedHostNic {
                    mac_address: "9A:9B:9C:9D:9E:38".into(),
                    fixed_ip: Some("192.0.2.225".into()),
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                    ..Default::default()
                },
                rpc::forge::ExpectedHostNic {
                    mac_address: "9A:9B:9C:9D:9E:39".into(),
                    fixed_ip: Some("192.0.2.225".into()),
                    ip_allocation: Some(rpc::forge::ExpectedInterfaceIpAllocation::Fixed as i32),
                    ..Default::default()
                },
            ],
            "host_nics contains duplicate fixed_ip 192.0.2.225",
        ),
    ] {
        let result = env
            .api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                id: None,
                bmc_mac_address: bmc_mac.into(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: serial_number.into(),
                host_nics,
                ..Default::default()
            }))
            .await;

        let err = result.expect_err(case);
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "case: {case}");
        assert!(
            err.message().contains(expected_error),
            "case {case}: expected error containing {expected_error:?}, got {:?}",
            err.message(),
        );
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_machine_identity_macs_are_unique_across_records(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let base_bmc = "9A:9B:9C:9D:9E:40";
    let base_interface = "9A:9B:9C:9D:9E:41";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: base_bmc.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-IDENTITY-BASE".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: base_interface.into(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;

    for (index, (case, bmc_mac, nested_mac)) in [
        (
            "nested MAC already declared as a nested interface",
            "9A:9B:9C:9D:9E:42",
            Some(base_interface),
        ),
        (
            "BMC MAC already declared as a nested interface",
            base_interface,
            None,
        ),
        (
            "nested MAC already declared as a BMC",
            "9A:9B:9C:9D:9E:43",
            Some(base_bmc),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let result = env
            .api
            .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                bmc_mac_address: bmc_mac.into(),
                bmc_username: "ADMIN".into(),
                bmc_password: "PASS".into(),
                chassis_serial_number: format!("EM-IDENTITY-CONFLICT-{index}"),
                host_nics: nested_mac
                    .map(|mac_address| {
                        vec![rpc::forge::ExpectedHostNic {
                            mac_address: mac_address.into(),
                            ..Default::default()
                        }]
                    })
                    .unwrap_or_default(),
                ..Default::default()
            }))
            .await;

        let error = result.expect_err(case);
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "case: {case}");
        assert!(
            error.message().contains("identity MAC conflicts"),
            "case {case}: unexpected error: {error}",
        );
    }

    Ok(())
}

#[crate::sqlx_test]
async fn test_modern_expected_machine_update_rejects_legacy_identity_macs(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let legacy_bmc: MacAddress = "9A:9B:9C:9D:9E:50".parse()?;
    let legacy_interface: MacAddress = "9A:9B:9C:9D:9E:51".parse()?;
    let modern_bmc = "9A:9B:9C:9D:9E:52";
    let modern_interface = "9A:9B:9C:9D:9E:53";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: legacy_bmc.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-LEGACY-IDENTITY".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: legacy_interface.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    clear_expected_machine_id(&env.pool, legacy_bmc).await?;

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: modern_bmc.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-MODERN-IDENTITY".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: modern_interface.into(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;

    let modern = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: modern_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert!(modern.id.is_some());

    for (case, conflicting_mac) in [
        ("legacy BMC", legacy_bmc),
        ("legacy nested interface", legacy_interface),
    ] {
        let mut update = modern.clone();
        update.host_nics[0].mac_address = conflicting_mac.to_string();
        let error = env
            .api
            .update_expected_machine(tonic::Request::new(update))
            .await
            .expect_err(case);
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "case: {case}");
        assert!(
            error.message().contains("identity MAC conflicts"),
            "case {case}: unexpected error: {error}",
        );
    }

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: modern_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(stored.host_nics[0].mac_address, modern_interface);

    Ok(())
}

#[crate::sqlx_test]
async fn test_legacy_expected_machine_can_change_its_own_identity_macs(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let legacy_bmc: MacAddress = "9A:9B:9C:9D:9E:60".parse()?;
    let legacy_interface = "9A:9B:9C:9D:9E:61";
    let other_bmc: MacAddress = "9A:9B:9C:9D:9E:62".parse()?;
    let other_interface: MacAddress = "9A:9B:9C:9D:9E:63".parse()?;
    let replacement_interface = "9A:9B:9C:9D:9E:64";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: legacy_bmc.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-LEGACY-UPDATE".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: legacy_interface.into(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    clear_expected_machine_id(&env.pool, legacy_bmc).await?;

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: other_bmc.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-OTHER-OWNER".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: other_interface.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;
    clear_expected_machine_id(&env.pool, other_bmc).await?;

    let mut legacy = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: legacy_bmc.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    assert!(legacy.id.is_none());

    legacy.host_nics[0].mac_address = replacement_interface.into();
    env.api
        .update_expected_machine(tonic::Request::new(legacy))
        .await?;

    let mut legacy = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: legacy_bmc.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(legacy.host_nics[0].mac_address, replacement_interface);

    legacy.host_nics.clear();
    env.api
        .update_expected_machine(tonic::Request::new(legacy))
        .await?;

    let legacy = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: legacy_bmc.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    assert!(legacy.host_nics.is_empty());

    for (case, conflicting_mac) in [
        ("another ExpectedMachine BMC", other_bmc),
        ("another ExpectedMachine interface", other_interface),
    ] {
        let mut update = legacy.clone();
        update.host_nics.push(rpc::forge::ExpectedHostNic {
            mac_address: conflicting_mac.to_string(),
            ..Default::default()
        });
        let error = env
            .api
            .update_expected_machine(tonic::Request::new(update))
            .await
            .expect_err(case);
        assert_eq!(error.code(), tonic::Code::InvalidArgument, "case: {case}");
        assert!(
            error.message().contains("identity MAC conflicts"),
            "case {case}: unexpected error: {error}",
        );
    }

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: legacy_bmc.to_string(),
            id: None,
        }))
        .await?
        .into_inner();
    assert!(stored.host_nics.is_empty());

    Ok(())
}

#[crate::sqlx_test]
async fn test_expected_machine_update_rejects_bmc_mac_change(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let original_bmc = "9A:9B:9C:9D:9E:48";
    let original_interface = "9A:9B:9C:9D:9E:49";

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: original_bmc.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-IMMUTABLE-BMC".into(),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: original_interface.into(),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await?;

    let mut machine = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: original_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert!(machine.id.is_some());

    // Swapping the BMC and nested MAC keeps the same identity set, which used
    // to bypass revalidation while the SQL update silently retained the old
    // BMC column.
    machine.bmc_mac_address = original_interface.into();
    machine.host_nics[0].mac_address = original_bmc.into();
    let error = env
        .api
        .update_expected_machine(tonic::Request::new(machine))
        .await
        .expect_err("BMC MAC changes must be rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("BMC MAC address cannot be changed")
    );

    let stored = env
        .api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: original_bmc.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(stored.bmc_mac_address, original_bmc);
    assert_eq!(stored.host_nics[0].mac_address, original_interface);

    Ok(())
}

#[crate::sqlx_test]
async fn test_replace_all_identity_validation_is_atomic(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let original_bmc = "9A:9B:9C:9D:9E:44";
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: original_bmc.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-REPLACE-ORIGINAL".into(),
            ..Default::default()
        }))
        .await?;

    let shared_interface = "9A:9B:9C:9D:9E:47";
    let replacements = ["9A:9B:9C:9D:9E:45", "9A:9B:9C:9D:9E:46"]
        .into_iter()
        .enumerate()
        .map(|(index, bmc_mac_address)| rpc::forge::ExpectedMachine {
            bmc_mac_address: bmc_mac_address.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: format!("EM-REPLACE-{index}"),
            host_nics: vec![rpc::forge::ExpectedHostNic {
                mac_address: shared_interface.into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .collect();
    let error = env
        .api
        .replace_all_expected_machines(tonic::Request::new(ExpectedMachineList {
            expected_machines: replacements,
        }))
        .await
        .expect_err("duplicate replacement identities should be rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);

    env.api
        .get_expected_machine(tonic::Request::new(ExpectedMachineRequest {
            bmc_mac_address: original_bmc.into(),
            id: None,
        }))
        .await
        .expect("failed replacement must leave the original set intact");

    Ok(())
}

/// The declared primary survives whichever order its NICs DHCP in: leasing the
/// non-primary NIC first, then the declared primary, still lands the declared
/// primary as `primary_interface` and the other as non-primary.
#[crate::sqlx_test]
async fn test_declared_primary_survives_dhcp_arrival_order(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = {
        let mut config = get_config();
        config.rack_management_enabled = true;
        create_test_env_with_overrides(pool, TestEnvOverrides::with_config(config)).await
    };
    let bmc_mac: MacAddress = "9A:9B:9C:9D:9F:10".parse().unwrap();
    let primary_mac: MacAddress = "9A:9B:9C:9D:9F:11".parse().unwrap();
    let other_mac: MacAddress = "9A:9B:9C:9D:9F:12".parse().unwrap();

    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac.to_string(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-PRIMARY-003".into(),
            host_nics: vec![
                rpc::forge::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: primary_mac.to_string(),
                    nic_type: Some("onboard".into()),
                    fixed_ip: None,
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: Some(true),
                    role: None,
                    ip_allocation: None,
                },
                rpc::forge::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: other_mac.to_string(),
                    nic_type: Some("onboard".into()),
                    fixed_ip: None,
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: None,
                    role: None,
                    ip_allocation: None,
                },
            ],
            ..Default::default()
        }))
        .await?;

    // The non-primary NIC leases first, then the declared primary.
    for mac in [other_mac, primary_mac] {
        let mac_str = mac.to_string();
        env.api
            .discover_dhcp(
                common::rpc_builder::DhcpDiscovery::builder(
                    &mac_str,
                    common::api_fixtures::FIXTURE_DHCP_RELAY_ADDRESS,
                )
                .tonic_request(),
            )
            .await?;
    }

    let mut txn = env.pool.begin().await?;
    let primary = db::machine_interface::find_by_mac_address(&mut *txn, primary_mac).await?;
    let other = db::machine_interface::find_by_mac_address(&mut *txn, other_mac).await?;
    assert_eq!(primary.len(), 1);
    assert_eq!(other.len(), 1);
    assert!(
        primary[0].primary_interface,
        "the declared primary NIC should be primary even when it leases last"
    );
    assert!(
        !other[0].primary_interface,
        "the non-declared NIC should not be primary"
    );

    Ok(())
}

/// The stable Forge DPU policy field round-trips through the database.
#[crate::sqlx_test]
async fn test_dpu_mode_round_trip_for_non_default_values(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (idx, mode) in [rpc::forge::DpuMode::NicMode, rpc::forge::DpuMode::NoDpu]
        .iter()
        .enumerate()
    {
        let mac = format!("5A:5B:5C:5D:5E:{idx:02X}");
        let request = rpc::forge::ExpectedMachine {
            bmc_mac_address: mac.clone(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: format!("EM-DPU-MODE-{idx}"),
            dpu_mode: Some(*mode as i32),
            ..Default::default()
        };

        env.api
            .add_expected_machine(tonic::Request::new(request))
            .await?;

        let retrieved = env
            .api
            .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
                bmc_mac_address: mac.clone(),
                id: None,
            }))
            .await?
            .into_inner();

        assert_eq!(
            retrieved.dpu_mode,
            Some(*mode as i32),
            "DPU policy mode {mode:?} should survive DB round-trip unchanged"
        );
    }

    Ok(())
}

/// The default host DPU policy is omitted on the wire, preserving existing
/// clients' absent-field behavior.
#[crate::sqlx_test]
async fn test_dpu_mode_default_value_omitted_on_wire(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    let mac = "5A:5B:5C:5D:5E:FF";
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: mac.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-DPU-DEFAULT".into(),
            ..Default::default()
        }))
        .await?;

    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: mac.into(),
            id: None,
        }))
        .await?
        .into_inner();

    assert_eq!(
        retrieved.dpu_mode, None,
        "default HostDpuPolicy should not be emitted on the Forge compatibility field"
    );

    Ok(())
}

/// Verify the update RPC (for update/patch flows) changes the DPU policy.
#[crate::sqlx_test]
async fn test_update_changes_dpu_mode(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    let mac = "5A:5B:5C:5D:5E:80";
    let base = rpc::forge::ExpectedMachine {
        bmc_mac_address: mac.into(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "EM-DPU-UPDATE".into(),
        metadata: Some(rpc::forge::Metadata::default()),
        ..Default::default()
    };

    env.api
        .add_expected_machine(tonic::Request::new(base.clone()))
        .await?;

    for mode in [
        rpc::forge::DpuMode::NicMode,
        rpc::forge::DpuMode::NoDpu,
        rpc::forge::DpuMode::DpuMode,
    ] {
        env.api
            .update_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                dpu_mode: Some(mode as i32),
                ..base.clone()
            }))
            .await?;

        let retrieved = env
            .api
            .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
                bmc_mac_address: mac.into(),
                id: None,
            }))
            .await?
            .into_inner();

        // Manage is the column default and the wire-default; the model
        // collapses it to `None` on the way out (see `From<ExpectedMachine>
        // for rpc::forge::ExpectedMachine`), so compare accordingly.
        let expected_wire = match mode {
            rpc::forge::DpuMode::DpuMode | rpc::forge::DpuMode::Unspecified => None,
            other => Some(other as i32),
        };
        assert_eq!(
            retrieved.dpu_mode, expected_wire,
            "update to {mode:?} should persist and round-trip on the wire"
        );
    }

    Ok(())
}

/// `ExpectedMachine.bmc_ip_allocation` round-trips through the API: a non-default
/// value (`Dynamic`, `Retained`) set on the wire persists to the DB and reads back
/// unchanged (the default/unset case is covered separately below). `Dynamic`/`Retained`
/// are used here because they're valid with no `bmc_ip_address` (which these requests
/// omit); `Fixed` requires an address and is exercised by the validation tests.
#[crate::sqlx_test]
async fn test_bmc_ip_allocation_round_trip_for_non_default_values(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    for (idx, mode) in [
        rpc::forge::BmcIpAllocationType::Dynamic,
        rpc::forge::BmcIpAllocationType::Retained,
    ]
    .iter()
    .enumerate()
    {
        let mac = format!("5A:5B:5C:5D:5F:{idx:02X}");
        let request = rpc::forge::ExpectedMachine {
            bmc_mac_address: mac.clone(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: format!("EM-BMC-ALLOC-{idx}"),
            bmc_ip_allocation: Some(*mode as i32),
            ..Default::default()
        };

        env.api
            .add_expected_machine(tonic::Request::new(request))
            .await?;

        let retrieved = env
            .api
            .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
                bmc_mac_address: mac.clone(),
                id: None,
            }))
            .await?
            .into_inner();

        assert_eq!(
            retrieved.bmc_ip_allocation,
            Some(*mode as i32),
            "bmc_ip_allocation {mode:?} should survive DB round-trip unchanged"
        );
    }

    Ok(())
}

/// Default-case round-trip for `bmc_ip_allocation`: when the operator omits it on
/// the wire, the server persists the Postgres default (`Auto`) and returns `None`
/// on the wire, so old clients see exactly what they sent.
#[crate::sqlx_test]
async fn test_bmc_ip_allocation_default_value_omitted_on_wire(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;

    let mac = "5A:5B:5C:5D:5F:FF";
    env.api
        .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
            bmc_mac_address: mac.into(),
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            chassis_serial_number: "EM-BMC-ALLOC-DEFAULT".into(),
            bmc_ip_allocation: None,
            ..Default::default()
        }))
        .await?;

    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: mac.into(),
            id: None,
        }))
        .await?
        .into_inner();

    assert_eq!(
        retrieved.bmc_ip_allocation, None,
        "default bmc_ip_allocation should not be emitted on the wire for stable round-trips"
    );

    Ok(())
}

/// Every `bmc_ip_allocation` x `bmc_ip_address` combination driven through the
/// real handlers: `add_expected_machine` accepts the six valid pairings and
/// refuses the three invalid ones, and `update_expected_machine` refuses the
/// same invalid pairings on an existing machine. Rejection is `InvalidArgument`
/// and names `bmc_ip_allocation` so the operator knows which knob to fix, and a
/// rejected update leaves the stored machine untouched. The combination table
/// itself is unit-tested in api-model -- these rows pin the handler wiring that
/// enforces it at the API boundary.
#[crate::sqlx_test]
async fn test_bmc_ip_allocation_combinations_enforced_at_the_api_boundary(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    use carbide_test_support::Outcome::{FailsWith, Yields};
    use carbide_test_support::{Case, check_cases_async};
    use rpc::forge::BmcIpAllocationType::{Auto, Dynamic, Fixed, Retained};

    let env = create_test_env(pool).await;

    /// One `add_expected_machine` request: the allocation/address pairing under
    /// test, plus the unique per-machine identity the API requires.
    struct AddRequest {
        mode: Option<rpc::forge::BmcIpAllocationType>,
        bmc_ip_address: Option<&'static str>,
        mac: &'static str,
        serial: &'static str,
    }

    // Rejections are projected to (status code, "the error names
    // bmc_ip_allocation") so each failing row pins both.
    check_cases_async(
        [
            Case {
                scenario: "add: unset with no address is accepted (defaults to auto)",
                input: AddRequest {
                    mode: None,
                    bmc_ip_address: None,
                    mac: "5A:5B:5C:5D:61:01",
                    serial: "EM-BMC-ALLOC-API-01",
                },
                expect: Yields(()),
            },
            Case {
                scenario: "add: auto with no address is accepted (retains)",
                input: AddRequest {
                    mode: Some(Auto),
                    bmc_ip_address: None,
                    mac: "5A:5B:5C:5D:61:02",
                    serial: "EM-BMC-ALLOC-API-02",
                },
                expect: Yields(()),
            },
            Case {
                scenario: "add: auto with an address is accepted (fixed)",
                input: AddRequest {
                    mode: Some(Auto),
                    bmc_ip_address: Some("192.0.2.61"),
                    mac: "5A:5B:5C:5D:61:03",
                    serial: "EM-BMC-ALLOC-API-03",
                },
                expect: Yields(()),
            },
            Case {
                scenario: "add: dynamic with no address is accepted",
                input: AddRequest {
                    mode: Some(Dynamic),
                    bmc_ip_address: None,
                    mac: "5A:5B:5C:5D:61:04",
                    serial: "EM-BMC-ALLOC-API-04",
                },
                expect: Yields(()),
            },
            Case {
                scenario: "add: fixed with an address is accepted",
                input: AddRequest {
                    mode: Some(Fixed),
                    bmc_ip_address: Some("192.0.2.62"),
                    mac: "5A:5B:5C:5D:61:05",
                    serial: "EM-BMC-ALLOC-API-05",
                },
                expect: Yields(()),
            },
            Case {
                scenario: "add: retained with no address is accepted",
                input: AddRequest {
                    mode: Some(Retained),
                    bmc_ip_address: None,
                    mac: "5A:5B:5C:5D:61:06",
                    serial: "EM-BMC-ALLOC-API-06",
                },
                expect: Yields(()),
            },
            Case {
                scenario: "add: fixed with no address is rejected",
                input: AddRequest {
                    mode: Some(Fixed),
                    bmc_ip_address: None,
                    mac: "5A:5B:5C:5D:61:07",
                    serial: "EM-BMC-ALLOC-API-07",
                },
                expect: FailsWith((tonic::Code::InvalidArgument, true)),
            },
            Case {
                scenario: "add: dynamic with an address is rejected",
                input: AddRequest {
                    mode: Some(Dynamic),
                    bmc_ip_address: Some("192.0.2.63"),
                    mac: "5A:5B:5C:5D:61:08",
                    serial: "EM-BMC-ALLOC-API-08",
                },
                expect: FailsWith((tonic::Code::InvalidArgument, true)),
            },
            Case {
                scenario: "add: retained with an address is rejected",
                input: AddRequest {
                    mode: Some(Retained),
                    bmc_ip_address: Some("192.0.2.64"),
                    mac: "5A:5B:5C:5D:61:09",
                    serial: "EM-BMC-ALLOC-API-09",
                },
                expect: FailsWith((tonic::Code::InvalidArgument, true)),
            },
        ],
        |req| {
            let env = &env;
            async move {
                env.api
                    .add_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                        bmc_mac_address: req.mac.into(),
                        bmc_username: "ADMIN".into(),
                        bmc_password: "PASS".into(),
                        chassis_serial_number: req.serial.into(),
                        bmc_ip_allocation: req.mode.map(|m| m as i32),
                        bmc_ip_address: req.bmc_ip_address.map(Into::into),
                        ..Default::default()
                    }))
                    .await
                    .map(|_| ())
                    .map_err(|status| {
                        (
                            status.code(),
                            status.message().contains("bmc_ip_allocation"),
                        )
                    })
            }
        },
    )
    .await;

    // The same invalid pairings through update_expected_machine, against one
    // existing machine.
    let mac = "5A:5B:5C:5D:61:10";
    let base = rpc::forge::ExpectedMachine {
        bmc_mac_address: mac.into(),
        bmc_username: "ADMIN".into(),
        bmc_password: "PASS".into(),
        chassis_serial_number: "EM-BMC-ALLOC-API-10".into(),
        metadata: Some(rpc::forge::Metadata::default()),
        ..Default::default()
    };
    env.api
        .add_expected_machine(tonic::Request::new(base.clone()))
        .await?;

    /// One `update_expected_machine` request: the invalid pairing sent for the
    /// machine created above.
    struct UpdateRequest {
        mode: rpc::forge::BmcIpAllocationType,
        bmc_ip_address: Option<&'static str>,
    }

    check_cases_async(
        [
            Case {
                scenario: "update: fixed with no address is rejected",
                input: UpdateRequest {
                    mode: Fixed,
                    bmc_ip_address: None,
                },
                expect: FailsWith((tonic::Code::InvalidArgument, true)),
            },
            Case {
                scenario: "update: dynamic with an address is rejected",
                input: UpdateRequest {
                    mode: Dynamic,
                    bmc_ip_address: Some("192.0.2.65"),
                },
                expect: FailsWith((tonic::Code::InvalidArgument, true)),
            },
            Case {
                scenario: "update: retained with an address is rejected",
                input: UpdateRequest {
                    mode: Retained,
                    bmc_ip_address: Some("192.0.2.66"),
                },
                expect: FailsWith((tonic::Code::InvalidArgument, true)),
            },
        ],
        |req| {
            let env = &env;
            let base = &base;
            async move {
                env.api
                    .update_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachine {
                        bmc_ip_allocation: Some(req.mode as i32),
                        bmc_ip_address: req.bmc_ip_address.map(Into::into),
                        ..base.clone()
                    }))
                    .await
                    .map(|_| ())
                    .map_err(|status| {
                        (
                            status.code(),
                            status.message().contains("bmc_ip_allocation"),
                        )
                    })
            }
        },
    )
    .await;

    // Every rejected update happened before any write: the stored machine still
    // has the default allocation and no address.
    let retrieved = env
        .api
        .get_expected_machine(tonic::Request::new(rpc::forge::ExpectedMachineRequest {
            bmc_mac_address: mac.into(),
            id: None,
        }))
        .await?
        .into_inner();
    assert_eq!(
        retrieved.bmc_ip_allocation, None,
        "rejected updates should leave the stored allocation untouched"
    );
    assert_eq!(
        retrieved.bmc_ip_address, None,
        "rejected updates should not store a bmc_ip_address"
    );

    Ok(())
}

/// Make sure expected_machines.json, which uses create_missing_from,
/// follows the shared codepath for handling interface allocation.
#[crate::sqlx_test]
async fn test_create_missing_from_preallocates_interfaces(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool).await;
    let bmc_mac: MacAddress = "AA:BB:CC:DD:EE:01".parse().unwrap();
    let nic_mac: MacAddress = "AA:BB:CC:DD:EE:02".parse().unwrap();
    let bmc_ip: std::net::IpAddr = "192.0.2.240".parse().unwrap();
    let host_ip: std::net::IpAddr = "192.0.2.241".parse().unwrap();

    let machine = ExpectedMachine {
        id: None,
        bmc_mac_address: bmc_mac,
        data: ExpectedMachineData {
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            serial_number: "EM-JSON-SEED-001".into(),
            bmc_ip_address: Some(bmc_ip),
            host_nics: vec![model::expected_machine::ExpectedHostNic {
                network_segment_type: None,
                mac_address: nic_mac,
                nic_type: Some("onboard".into()),
                fixed_ip: Some(host_ip),
                fixed_mask: None,
                fixed_gateway: None,
                primary: Some(true),
                role: Default::default(),
                ip_allocation: None,
            }],
            ..Default::default()
        },
    };

    let mut txn = env.pool.begin().await?;
    crate::handlers::expected_machine::create_missing_from(
        &mut txn,
        std::slice::from_ref(&machine),
    )
    .await?;
    txn.commit().await?;

    // Mimic site-explorer's per-row materialization: one preallocate per static IP on the
    // entity we just inserted.
    carbide_site_explorer::try_preallocate_one(
        &env.pool,
        bmc_mac,
        bmc_ip,
        model::machine_interface::InterfaceType::Bmc,
        "expected_machine BMC",
        None,
    )
    .await;
    carbide_site_explorer::try_preallocate_expected_interface(&env.pool, nic_mac, None).await;

    let mut txn = env.pool.begin().await?;
    for (mac, expected_ip) in [(bmc_mac, bmc_ip), (nic_mac, host_ip)] {
        let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, mac).await?;
        assert_eq!(
            interfaces.len(),
            1,
            "expected one machine_interface for MAC {mac}"
        );
        assert!(
            interfaces[0].addresses.contains(&expected_ip),
            "machine_interface for MAC {mac} should carry static IP {expected_ip}, got {:?}",
            interfaces[0].addresses,
        );
    }
    let expected_machine_owned: Option<bool> = sqlx::query_scalar(
        "SELECT mia.expected_machine_preallocation
         FROM machine_interface_addresses mia
         JOIN machine_interfaces mi ON mi.id = mia.interface_id
         WHERE mi.mac_address = $1 AND mia.address = $2::inet",
    )
    .bind(nic_mac)
    .bind(host_ip)
    .fetch_one(&mut *txn)
    .await?;
    assert_eq!(expected_machine_owned, Some(true));
    txn.rollback().await?;

    // Re-running create_missing_from with the same input must be a no-op (idempotent).
    let mut txn = env.pool.begin().await?;
    crate::handlers::expected_machine::create_missing_from(
        &mut txn,
        std::slice::from_ref(&machine),
    )
    .await?;
    txn.commit().await?;

    Ok(())
}

#[crate::sqlx_test]
async fn test_create_missing_from_is_concurrently_idempotent(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let env = create_test_env(pool.clone()).await;
    let bmc_mac: MacAddress = "AA:BB:CC:DD:EE:10".parse()?;
    let interface_mac: MacAddress = "AA:BB:CC:DD:EE:11".parse()?;
    let machine = ExpectedMachine {
        id: None,
        bmc_mac_address: bmc_mac,
        data: ExpectedMachineData {
            bmc_username: "ADMIN".into(),
            bmc_password: "PASS".into(),
            serial_number: "EM-CONCURRENT-SEED".into(),
            host_nics: vec![model::expected_machine::ExpectedHostNic {
                mac_address: interface_mac,
                ..Default::default()
            }],
            ..Default::default()
        },
    };

    let mut blocker = pool.begin().await?;
    let blocker_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    db::expected_machine::lock_identity_macs(&mut blocker, [bmc_mac, interface_mac]).await?;

    let mut first_txn = pool.begin().await?;
    let first_machine = machine.clone();
    let first = tokio::spawn(async move {
        let result = crate::handlers::expected_machine::create_missing_from(
            &mut first_txn,
            std::slice::from_ref(&first_machine),
        )
        .await;
        match result {
            Ok(()) => {
                first_txn
                    .commit()
                    .await
                    .expect("commit first startup import");
                Ok(())
            }
            Err(error) => {
                first_txn
                    .rollback()
                    .await
                    .expect("roll back first startup import");
                Err(error)
            }
        }
    });
    wait_for_expected_machine_advisory_lock(&pool, blocker_pid).await;

    let mut second_txn = pool.begin().await?;
    let second_pid = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *second_txn)
        .await?;
    let second_machine = machine.clone();
    let second = tokio::spawn(async move {
        let result = crate::handlers::expected_machine::create_missing_from(
            &mut second_txn,
            std::slice::from_ref(&second_machine),
        )
        .await;
        match result {
            Ok(()) => {
                second_txn
                    .commit()
                    .await
                    .expect("commit second startup import");
                Ok(())
            }
            Err(error) => {
                second_txn
                    .rollback()
                    .await
                    .expect("roll back second startup import");
                Err(error)
            }
        }
    });
    wait_for_advisory_lock_wait(&pool, second_pid).await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());

    blocker.commit().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), first)
        .await
        .expect("first startup import did not resume")
        .expect("first startup import task panicked")?;
    tokio::time::timeout(std::time::Duration::from_secs(5), second)
        .await
        .expect("second startup import did not resume")
        .expect("second startup import task panicked")?;

    let stored = db::expected_machine::find_all(&env.pool).await?;
    assert_eq!(
        stored
            .iter()
            .filter(|machine| machine.bmc_mac_address == bmc_mac)
            .count(),
        1
    );

    Ok(())
}
