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

use carbide_test_harness::prelude::*;
use carbide_test_harness::test_support::network_segment::create_static_assignments_segment;
use mac_address::MacAddress;
use model::allocation_type::AllocationType;
use model::expected_machine::{BmcIpAllocationType, ExpectedMachine, ExpectedMachineData};
use model::metadata::Metadata;

async fn init(pool: &PgPool) -> TestHarness {
    let test_harness = TestHarness::builder(pool.clone()).build().await;
    let domain = test_harness.test_domain().await;
    create_static_assignments_segment(test_harness.api(), Some(domain.id)).await;
    test_harness
}

/// Site-explorer reconciles every `expected_*` row's configured static IPs into
/// `machine_interface` rows by calling `try_preallocate_one` per static IP during
/// `update_explored_endpoints`. This test drives the same per-row materialization directly,
/// covering the static-assignments-segment counterpart to the DHCP `discover()` recovery hook:
/// devices whose IP lives outside any Carbide-managed network never reach `discover()`, so this
/// per-row preallocation is what gets their rows onto the books.
#[sqlx_test]
async fn test_site_explorer_reconcile_creates_missing_preallocations(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let machine_bmc_mac: MacAddress = "AA:BB:CC:DD:E0:01".parse().unwrap();
    let machine_bmc_ip: IpAddr = "10.99.0.10".parse().unwrap();
    let switch_bmc_mac: MacAddress = "AA:BB:CC:DD:E0:02".parse().unwrap();
    let switch_bmc_ip: IpAddr = "10.99.0.11".parse().unwrap();
    let power_shelf_bmc_mac: MacAddress = "AA:BB:CC:DD:E0:03".parse().unwrap();
    let power_shelf_bmc_ip: IpAddr = "10.99.0.12".parse().unwrap();

    // Seed each expected_* row WITHOUT a corresponding machine_interface. The gRPC `add`
    // handlers don't preallocate inline; site-explorer's reconciliation pass is what
    // materializes the rows.
    let mut txn = pool.begin().await?;
    db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: machine_bmc_mac,
            data: ExpectedMachineData {
                serial_number: "reconcile-m-001".to_string(),
                bmc_ip_address: Some(machine_bmc_ip),
                ..Default::default()
            },
        },
    )
    .await?;
    db::expected_switch::create(
        &mut txn,
        model::expected_switch::ExpectedSwitch {
            expected_switch_id: None,
            bmc_mac_address: switch_bmc_mac,
            nvos_mac_addresses: vec![],
            bmc_username: "ADMIN".into(),
            serial_number: "reconcile-sw-001".into(),
            bmc_password: "PASS".into(),
            nvos_username: None,
            nvos_password: None,
            bmc_ip_address: Some(switch_bmc_ip),
            nvos_ip_address: None,
            metadata: Metadata::default(),
            rack_id: None,
            bmc_retain_credentials: None,
        },
    )
    .await?;
    db::expected_power_shelf::create(
        &mut txn,
        model::expected_power_shelf::ExpectedPowerShelf {
            expected_power_shelf_id: None,
            bmc_mac_address: power_shelf_bmc_mac,
            bmc_username: "ADMIN".into(),
            serial_number: "reconcile-ps-001".into(),
            bmc_password: "PASS".into(),
            bmc_ip_address: Some(power_shelf_bmc_ip),
            metadata: Metadata::default(),
            rack_id: None,
            bmc_retain_credentials: None,
        },
    )
    .await?;
    txn.commit().await?;

    // Baseline: no interfaces yet.
    let mut txn = pool.begin().await?;
    for mac in [machine_bmc_mac, switch_bmc_mac, power_shelf_bmc_mac] {
        let before = db::machine_interface::find_by_mac_address(&mut *txn, mac).await?;
        assert!(
            before.is_empty(),
            "no machine_interface should exist before site-explorer reconciles for {mac}"
        );
    }
    txn.commit().await?;

    for (mac, ip, kind) in [
        (machine_bmc_mac, machine_bmc_ip, "expected_machine BMC"),
        (switch_bmc_mac, switch_bmc_ip, "expected_switch BMC"),
        (
            power_shelf_bmc_mac,
            power_shelf_bmc_ip,
            "expected_power_shelf BMC",
        ),
    ] {
        carbide_site_explorer::try_preallocate_one(
            &pool,
            mac,
            ip,
            model::machine_interface::InterfaceType::Bmc,
            kind,
            None,
        )
        .await;
    }

    let mut txn = pool.begin().await?;
    for (mac, ip) in [
        (machine_bmc_mac, machine_bmc_ip),
        (switch_bmc_mac, switch_bmc_ip),
        (power_shelf_bmc_mac, power_shelf_bmc_ip),
    ] {
        let after = db::machine_interface::find_by_mac_address(&mut *txn, mac).await?;
        assert_eq!(after.len(), 1, "should be preallocated for {mac}");
        assert!(
            after[0].addresses.contains(&ip),
            "preallocated row for {mac} should carry {ip}, got {:?}",
            after[0].addresses,
        );
        assert_eq!(
            after[0].interface_type,
            model::machine_interface::InterfaceType::Bmc,
            "BMC IPs should be preallocated with InterfaceType::Bmc, not Data ({mac})"
        );
    }
    txn.commit().await?;

    Ok(())
}

/// Running `try_preallocate_one` twice for the same (mac, ip) must be a no-op the second time
/// -- no new rows, no errors. This is the steady-state behavior since site-explorer iterates
/// continuously and re-issues the same calls on every pass.
#[sqlx_test]
async fn test_site_explorer_reconcile_is_idempotent(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E2:01".parse().unwrap();
    let bmc_ip: IpAddr = "10.99.0.30".parse().unwrap();

    let mut txn = pool.begin().await?;
    db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac,
            data: ExpectedMachineData {
                serial_number: "reconcile-idem-001".to_string(),
                bmc_ip_address: Some(bmc_ip),
                ..Default::default()
            },
        },
    )
    .await?;
    txn.commit().await?;

    for _ in 0..2 {
        carbide_site_explorer::try_preallocate_one(
            &pool,
            bmc_mac,
            bmc_ip,
            model::machine_interface::InterfaceType::Bmc,
            "expected_machine BMC",
            None,
        )
        .await;
    }

    let mut txn = pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, bmc_mac).await?;
    txn.commit().await?;
    assert_eq!(
        interfaces.len(),
        1,
        "two preallocate calls should not create duplicate machine_interface rows"
    );
    assert!(interfaces[0].addresses.contains(&bmc_ip));

    Ok(())
}

/// Site-explorer's reconciliation pass must materialize `ExpectedHostNic.fixed_ip`
/// reservations too, not just BMC IPs.
#[sqlx_test]
async fn test_site_explorer_reconcile_preallocates_host_nic_fixed_ip(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E1:01".parse().unwrap();
    let nic_mac: MacAddress = "AA:BB:CC:DD:E1:02".parse().unwrap();
    let fixed_ip = "10.99.0.20";
    let parsed_fixed_ip: IpAddr = fixed_ip.parse().unwrap();

    let mut txn = pool.begin().await?;
    db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac,
            data: ExpectedMachineData {
                serial_number: "reconcile-hostnic-001".to_string(),
                host_nics: vec![model::expected_machine::ExpectedHostNic {
                    network_segment_type: None,
                    mac_address: nic_mac,
                    nic_type: None,
                    fixed_ip: Some(parsed_fixed_ip),
                    fixed_mask: None,
                    fixed_gateway: None,
                    primary: None,
                    role: Default::default(),
                    ip_allocation: None,
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    txn.commit().await?;

    carbide_site_explorer::try_preallocate_expected_interface(&pool, nic_mac, None).await;

    let mut txn = pool.begin().await?;
    let nic_iface = db::machine_interface::find_by_mac_address(&mut *txn, nic_mac).await?;
    txn.commit().await?;
    assert_eq!(
        nic_iface.len(),
        1,
        "host NIC interface should be preallocated"
    );
    assert!(
        nic_iface[0].addresses.contains(&parsed_fixed_ip),
        "preallocated host NIC interface should carry the fixed_ip"
    );
    assert_eq!(
        nic_iface[0].interface_type,
        model::machine_interface::InterfaceType::Data,
        "host NIC preallocation should mark the interface as InterfaceType::Data, not Bmc"
    );

    Ok(())
}

/// A Site Explorer pass can hold a detached ExpectedMachine snapshot while an
/// operator deletes that configuration. The per-interface helper must re-read
/// current state instead of recreating a reservation from the stale snapshot.
#[sqlx_test]
async fn test_site_explorer_reconcile_skips_deleted_expected_interface_snapshot(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E1:11".parse()?;
    let nic_mac: MacAddress = "AA:BB:CC:DD:E1:12".parse()?;
    let fixed_ip: IpAddr = "10.99.0.21".parse()?;

    let mut txn = pool.begin().await?;
    db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac,
            data: ExpectedMachineData {
                serial_number: "reconcile-stale-001".to_string(),
                host_nics: vec![model::expected_machine::ExpectedHostNic {
                    mac_address: nic_mac,
                    fixed_ip: Some(fixed_ip),
                    ..Default::default()
                }],
                ..Default::default()
            },
        },
    )
    .await?;
    txn.commit().await?;

    // The outer loop has already remembered nic_mac, but the configuration is
    // deleted before its delayed per-interface transaction starts.
    let mut txn = pool.begin().await?;
    db::expected_machine::delete_by_mac(&mut txn, bmc_mac).await?;
    txn.commit().await?;

    carbide_site_explorer::try_preallocate_expected_interface(&pool, nic_mac, None).await;

    let mut txn = pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, nic_mac).await?;
    txn.rollback().await?;
    assert!(
        interfaces.is_empty(),
        "a stale Site Explorer snapshot must not recreate a deleted reservation"
    );

    Ok(())
}

/// Legacy BMC reconciliation receives a MAC from the outer Site Explorer
/// snapshot, but its fixed-address decision must come from the current row.
#[sqlx_test]
async fn test_site_explorer_reconcile_skips_removed_expected_machine_bmc_address(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E1:21".parse()?;
    let stale_ip: IpAddr = "10.99.0.22".parse()?;

    let mut txn = pool.begin().await?;
    let mut expected_machine = db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac,
            data: ExpectedMachineData {
                serial_number: "reconcile-stale-bmc-001".to_string(),
                bmc_ip_address: Some(stale_ip),
                ..Default::default()
            },
        },
    )
    .await?;
    txn.commit().await?;

    // The outer loop has remembered the old fixed address, but the operator
    // switches this BMC to Dynamic before its per-source transaction starts.
    expected_machine.data.bmc_ip_address = None;
    expected_machine.data.bmc_ip_allocation = BmcIpAllocationType::Dynamic;
    let mut txn = pool.begin().await?;
    db::expected_machine::update(&mut txn, &expected_machine).await?;
    txn.commit().await?;

    carbide_site_explorer::try_reconcile_expected_machine_bmc(&pool, bmc_mac, None).await;

    let mut txn = pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, bmc_mac).await?;
    txn.rollback().await?;
    assert!(
        interfaces.is_empty(),
        "the stale fixed BMC address must not be materialized after it is removed"
    );

    Ok(())
}

/// A Retained policy from the outer snapshot must not pin a DHCP address after
/// the current ExpectedMachine changes to Dynamic.
#[sqlx_test]
async fn test_site_explorer_reconcile_skips_removed_expected_machine_bmc_retention(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E1:25".parse()?;
    let bmc_ip: IpAddr = "10.99.0.27".parse()?;

    let mut txn = pool.begin().await?;
    let mut expected_machine = db::expected_machine::create(
        &mut txn,
        ExpectedMachine {
            id: None,
            bmc_mac_address: bmc_mac,
            data: ExpectedMachineData {
                serial_number: "reconcile-stale-retained-001".to_string(),
                bmc_ip_allocation: BmcIpAllocationType::Retained,
                ..Default::default()
            },
        },
    )
    .await?;
    txn.commit().await?;

    carbide_site_explorer::try_preallocate_one(
        &pool,
        bmc_mac,
        bmc_ip,
        model::machine_interface::InterfaceType::Bmc,
        "expected_machine BMC test setup",
        None,
    )
    .await;
    let mut txn = pool.begin().await?;
    let interface = db::machine_interface::find_by_mac_address(&mut *txn, bmc_mac)
        .await?
        .remove(0);
    sqlx::query(
        "UPDATE machine_interface_addresses
         SET allocation_type = 'dhcp'
         WHERE interface_id = $1",
    )
    .bind(interface.id)
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;

    expected_machine.data.bmc_ip_allocation = BmcIpAllocationType::Dynamic;
    let mut txn = pool.begin().await?;
    db::expected_machine::update(&mut txn, &expected_machine).await?;
    txn.commit().await?;

    carbide_site_explorer::try_reconcile_expected_machine_bmc(&pool, bmc_mac, None).await;

    let mut txn = pool.begin().await?;
    let addresses =
        db::machine_interface_address::find_for_interface(&mut txn, interface.id).await?;
    txn.rollback().await?;
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].allocation_type, AllocationType::Dhcp);

    Ok(())
}

/// ExpectedSwitch reconciliation must reload updated addresses instead of
/// applying values from the detached Site Explorer snapshot.
#[sqlx_test]
async fn test_site_explorer_reconcile_uses_updated_expected_switch_bmc_address(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E1:31".parse()?;
    let stale_ip: IpAddr = "10.99.0.23".parse()?;
    let current_ip: IpAddr = "10.99.0.24".parse()?;

    let mut txn = pool.begin().await?;
    let mut expected_switch = db::expected_switch::create(
        &mut txn,
        model::expected_switch::ExpectedSwitch {
            expected_switch_id: None,
            bmc_mac_address: bmc_mac,
            nvos_mac_addresses: vec![],
            bmc_username: "ADMIN".into(),
            serial_number: "reconcile-stale-switch-001".into(),
            bmc_password: "PASS".into(),
            nvos_username: None,
            nvos_password: None,
            bmc_ip_address: Some(stale_ip),
            nvos_ip_address: None,
            metadata: Metadata::default(),
            rack_id: None,
            bmc_retain_credentials: None,
        },
    )
    .await?;
    txn.commit().await?;

    expected_switch.bmc_ip_address = Some(current_ip);
    let mut txn = pool.begin().await?;
    db::expected_switch::update(&mut txn, &expected_switch).await?;
    txn.commit().await?;

    carbide_site_explorer::try_reconcile_expected_switch_addresses(&pool, bmc_mac, None).await;

    let mut txn = pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, bmc_mac).await?;
    txn.rollback().await?;
    assert_eq!(interfaces.len(), 1);
    assert!(interfaces[0].addresses.contains(&current_ip));
    assert!(!interfaces[0].addresses.contains(&stale_ip));

    Ok(())
}

/// A malformed legacy NVOS pairing is skipped without hiding an otherwise
/// valid switch BMC reservation.
#[sqlx_test]
async fn test_site_explorer_reconcile_invalid_nvos_pairing_keeps_switch_bmc(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E1:41".parse()?;
    let bmc_ip: IpAddr = "10.99.0.25".parse()?;
    let nvos_ip: IpAddr = "10.99.0.26".parse()?;

    let mut txn = pool.begin().await?;
    db::expected_switch::create(
        &mut txn,
        model::expected_switch::ExpectedSwitch {
            expected_switch_id: None,
            bmc_mac_address: bmc_mac,
            nvos_mac_addresses: vec![],
            bmc_username: "ADMIN".into(),
            serial_number: "reconcile-invalid-nvos-001".into(),
            bmc_password: "PASS".into(),
            nvos_username: None,
            nvos_password: None,
            bmc_ip_address: Some(bmc_ip),
            nvos_ip_address: Some(nvos_ip),
            metadata: Metadata::default(),
            rack_id: None,
            bmc_retain_credentials: None,
        },
    )
    .await?;
    txn.commit().await?;

    carbide_site_explorer::try_reconcile_expected_switch_addresses(&pool, bmc_mac, None).await;

    let mut txn = pool.begin().await?;
    let interfaces = db::machine_interface::find_by_mac_address(&mut *txn, bmc_mac).await?;
    txn.rollback().await?;
    assert_eq!(interfaces.len(), 1);
    assert!(interfaces[0].addresses.contains(&bmc_ip));

    Ok(())
}

/// A per-entry conflict (e.g., two expected_machines configured with the same static IP -- a
/// genuine operator misconfiguration) must not abort the whole reconciliation pass. The
/// conflicting entry gets logged and skipped; the rest of the iteration continues.
#[sqlx_test]
async fn test_site_explorer_reconcile_tolerates_per_entry_conflicts(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let mac_a: MacAddress = "AA:BB:CC:DD:E3:01".parse().unwrap();
    let mac_b: MacAddress = "AA:BB:CC:DD:E3:02".parse().unwrap();
    let mac_c: MacAddress = "AA:BB:CC:DD:E3:03".parse().unwrap();
    let shared_ip: IpAddr = "10.99.0.40".parse().unwrap();
    let ok_ip: IpAddr = "10.99.0.41".parse().unwrap();

    let mut txn = pool.begin().await?;
    // Two machines configured with the SAME bmc_ip_address -- the second will conflict at
    // preallocate time. A third has its own valid IP and should still get preallocated.
    for (mac, ip, sn) in [
        (mac_a, shared_ip, "reconcile-conflict-a"),
        (mac_b, shared_ip, "reconcile-conflict-b"),
        (mac_c, ok_ip, "reconcile-conflict-c"),
    ] {
        db::expected_machine::create(
            &mut txn,
            ExpectedMachine {
                id: None,
                bmc_mac_address: mac,
                data: ExpectedMachineData {
                    serial_number: sn.to_string(),
                    bmc_ip_address: Some(ip),
                    ..Default::default()
                },
            },
        )
        .await?;
    }
    txn.commit().await?;

    // try_preallocate_one swallows per-entry errors -- the conflict between mac_a and mac_b
    // gets logged and the third call still succeeds.
    for (mac, ip) in [(mac_a, shared_ip), (mac_b, shared_ip), (mac_c, ok_ip)] {
        carbide_site_explorer::try_preallocate_one(
            &pool,
            mac,
            ip,
            model::machine_interface::InterfaceType::Bmc,
            "expected_machine BMC",
            None,
        )
        .await;
    }

    let mut txn = pool.begin().await?;
    // Exactly one of {mac_a, mac_b} won the race for the shared IP.
    let a = db::machine_interface::find_by_mac_address(&mut *txn, mac_a).await?;
    let b = db::machine_interface::find_by_mac_address(&mut *txn, mac_b).await?;
    let winners = a.len() + b.len();
    assert_eq!(
        winners, 1,
        "exactly one of the two conflicting MACs should have a preallocated row"
    );
    // The third, non-conflicting entry must still be preallocated.
    let c = db::machine_interface::find_by_mac_address(&mut *txn, mac_c).await?;
    assert_eq!(
        c.len(),
        1,
        "non-conflicting expected_machine should still be preallocated despite the upstream conflict"
    );
    assert!(c[0].addresses.contains(&ok_ip));
    txn.commit().await?;

    Ok(())
}

/// Site-explorer's reconciliation pass must materialize the (nvos_mac, nvos_ip_address)
/// pairing for expected switches, mirroring how it handles `bmc_ip_address` and the
/// host-NIC `fixed_ip` paths. Calls `try_preallocate_one` directly the same way the
/// expected_switches loop does, and verifies the resulting row carries the configured
/// IP with `InterfaceType::Data`.
#[sqlx_test]
async fn test_site_explorer_reconcile_preallocates_nvos_ip(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    init(&pool).await;

    let bmc_mac: MacAddress = "AA:BB:CC:DD:E4:01".parse().unwrap();
    let nvos_mac: MacAddress = "AA:BB:CC:DD:E4:02".parse().unwrap();
    let nvos_ip: IpAddr = "10.99.0.50".parse().unwrap();

    let mut txn = pool.begin().await?;
    db::expected_switch::create(
        &mut txn,
        model::expected_switch::ExpectedSwitch {
            expected_switch_id: None,
            bmc_mac_address: bmc_mac,
            nvos_mac_addresses: vec![nvos_mac],
            bmc_username: "ADMIN".into(),
            serial_number: "reconcile-nvos-001".into(),
            bmc_password: "PASS".into(),
            nvos_username: None,
            nvos_password: None,
            bmc_ip_address: None,
            nvos_ip_address: Some(nvos_ip),
            metadata: Metadata::default(),
            rack_id: None,
            bmc_retain_credentials: None,
        },
    )
    .await?;
    txn.commit().await?;

    // Baseline: no NVOS interface yet.
    let mut txn = pool.begin().await?;
    let before = db::machine_interface::find_by_mac_address(&mut *txn, nvos_mac).await?;
    assert!(
        before.is_empty(),
        "no machine_interface should exist before site-explorer reconciles for {nvos_mac}"
    );
    txn.commit().await?;

    carbide_site_explorer::try_preallocate_one(
        &pool,
        nvos_mac,
        nvos_ip,
        model::machine_interface::InterfaceType::Data,
        "expected_switch NVOS",
        None,
    )
    .await;

    let mut txn = pool.begin().await?;
    let after = db::machine_interface::find_by_mac_address(&mut *txn, nvos_mac).await?;
    assert_eq!(
        after.len(),
        1,
        "expected_switch NVOS should be preallocated for {nvos_mac}"
    );
    assert!(
        after[0].addresses.contains(&nvos_ip),
        "preallocated row should carry {nvos_ip}, got {:?}",
        after[0].addresses,
    );
    assert_eq!(
        after[0].interface_type,
        model::machine_interface::InterfaceType::Data,
        "NVOS IPs should be preallocated with InterfaceType::Data, not Bmc"
    );
    txn.commit().await?;

    Ok(())
}
