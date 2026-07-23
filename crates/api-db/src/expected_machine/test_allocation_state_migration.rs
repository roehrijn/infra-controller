//! Tests for the ExpectedInterface allocation-state data migration.
//!
//! The SQLx test harness applies migrations before the test starts. This test
//! removes only the schema objects created by this migration, inserts rows as
//! an older NICo writer would, and then applies the exact SQL file again.

use serde_json::{Value, json};
use sqlx::PgPool;

const ALLOCATION_STATE_MIGRATION: &str =
    include_str!("../../migrations/20260722120000_expected_interface_allocation_state.sql");

const SEGMENT_ID: &str = "20000000-0000-0000-0000-000000000001";
const HOST_MACHINE_ID: &str = "migration-test-host";
const DUPLICATE_MACHINE_ID: &str = "migration-test-duplicate";
const HOST_MAC: &str = "02:00:00:00:01:01";
const DUPLICATE_MAC: &str = "02:00:00:00:02:01";
const CROSS_ROW_BMC_MAC: &str = "02:00:00:00:02:b0";
const ASSOCIATED_MAC: &str = "02:00:00:00:03:01";
const ANONYMOUS_MAC: &str = "02:00:00:00:04:01";

async fn restore_pre_migration_schema(pool: &PgPool) {
    sqlx::raw_sql(
        "DROP INDEX IF EXISTS expected_machines_host_nics_gin_idx;
         ALTER TABLE machine_interface_addresses
             DROP COLUMN IF EXISTS expected_machine_preallocation;
         ALTER TABLE predicted_machine_interfaces
             DROP COLUMN IF EXISTS expected_interface_captured,
             DROP COLUMN IF EXISTS expected_interface;
         ALTER TABLE machine_interfaces
             DROP COLUMN IF EXISTS expected_interface_captured,
             DROP COLUMN IF EXISTS expected_interface;",
    )
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_pre_migration_rows(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO network_segments (id, name, version)
         VALUES ($1::uuid, 'migration-test', 'test')",
    )
    .bind(SEGMENT_ID)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO machines (id, dpf)
         VALUES ($1, '{}'::jsonb), ($2, '{}'::jsonb)",
    )
    .bind(HOST_MACHINE_ID)
    .bind(DUPLICATE_MACHINE_ID)
    .execute(pool)
    .await
    .unwrap();

    let host_nics = json!([
        {
            "mac_address": HOST_MAC,
            "nic_type": "onboard",
            "fixed_ip": "192.0.2.10",
            "fixed_mask": null,
            "fixed_gateway": null,
            "primary": false
        },
        {
            "mac_address": DUPLICATE_MAC,
            "role": "dpu_os",
            "nic_type": null,
            "fixed_ip": null,
            "fixed_mask": null,
            "fixed_gateway": null,
            "primary": null
        },
        {
            "mac_address": CROSS_ROW_BMC_MAC,
            "role": "dpu_bmc",
            "nic_type": null,
            "fixed_ip": "192.0.2.12",
            "fixed_mask": null,
            "fixed_gateway": null,
            "primary": null
        }
    ]);
    let duplicate_host_nics = json!([
        {
            "mac_address": DUPLICATE_MAC,
            "role": "dpu_bmc",
            "nic_type": null,
            "fixed_ip": null,
            "fixed_mask": null,
            "fixed_gateway": null,
            "primary": null
        }
    ]);
    sqlx::query(
        "INSERT INTO expected_machines
             (serial_number, bmc_mac_address, bmc_username, bmc_password, host_nics)
         VALUES
             ('migration-host', '02:00:00:00:01:b0'::macaddr, 'admin', 'pw', $1),
             ('migration-duplicate', $3::macaddr, 'admin', 'pw', $2)",
    )
    .bind(host_nics)
    .bind(duplicate_host_nics)
    .bind(CROSS_ROW_BMC_MAC)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO predicted_machine_interfaces
             (machine_id, mac_address, expected_network_segment_type, primary_interface)
         VALUES
             ($1, $2::macaddr, 'admin', true),
             ($3, $4::macaddr, 'admin', false),
             ($1, $5::macaddr, 'admin', false)",
    )
    .bind(HOST_MACHINE_ID)
    .bind(HOST_MAC)
    .bind(DUPLICATE_MACHINE_ID)
    .bind(DUPLICATE_MAC)
    .bind(CROSS_ROW_BMC_MAC)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO machine_interfaces
             (segment_id, mac_address, primary_interface, hostname, machine_id, association_type)
         VALUES
             ($1::uuid, $2::macaddr, false, 'migration-associated', $3, 'Machine'),
             ($1::uuid, $4::macaddr, false, 'migration-anonymous', NULL, 'None')",
    )
    .bind(SEGMENT_ID)
    .bind(ASSOCIATED_MAC)
    .bind(HOST_MACHINE_ID)
    .bind(ANONYMOUS_MAC)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO machine_interface_addresses (interface_id, address, allocation_type)
         SELECT id, '192.0.2.20'::inet, 'static'
         FROM machine_interfaces
         WHERE mac_address = $1::macaddr",
    )
    .bind(ANONYMOUS_MAC)
    .execute(pool)
    .await
    .unwrap();
}

#[crate::sqlx_test]
async fn allocation_state_migration_backfills_existing_rows(pool: PgPool) {
    restore_pre_migration_schema(&pool).await;
    seed_pre_migration_rows(&pool).await;

    sqlx::raw_sql(ALLOCATION_STATE_MIGRATION)
        .execute(&pool)
        .await
        .unwrap();

    let (host_snapshot, host_captured): (Option<Value>, bool) = sqlx::query_as(
        "SELECT expected_interface, expected_interface_captured
         FROM predicted_machine_interfaces
         WHERE mac_address = $1::macaddr",
    )
    .bind(HOST_MAC)
    .fetch_one(&pool)
    .await
    .unwrap();
    let expected_host_snapshot = json!({
        "mac_address": HOST_MAC,
        "nic_type": "onboard",
        "fixed_ip": "192.0.2.10",
        "fixed_mask": null,
        "fixed_gateway": null,
        "primary": true
    });
    assert_eq!(host_snapshot, Some(expected_host_snapshot));
    assert!(host_captured);

    let (duplicate_snapshot, duplicate_captured): (Option<Value>, bool) = sqlx::query_as(
        "SELECT expected_interface, expected_interface_captured
         FROM predicted_machine_interfaces
         WHERE mac_address = $1::macaddr",
    )
    .bind(DUPLICATE_MAC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(duplicate_snapshot, None);
    assert!(duplicate_captured);

    // A nested interface cannot take another ExpectedMachine's top-level BMC
    // MAC. The backfill leaves that prediction classified but unclaimed.
    let (bmc_collision_snapshot, bmc_collision_captured): (Option<Value>, bool) = sqlx::query_as(
        "SELECT expected_interface, expected_interface_captured
             FROM predicted_machine_interfaces
             WHERE mac_address = $1::macaddr",
    )
    .bind(CROSS_ROW_BMC_MAC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bmc_collision_snapshot, None);
    assert!(bmc_collision_captured);

    let captured_by_mac: Vec<(String, bool)> = sqlx::query_as(
        "SELECT mac_address::text, expected_interface_captured
         FROM machine_interfaces
         WHERE mac_address IN ($1::macaddr, $2::macaddr)
         ORDER BY mac_address",
    )
    .bind(ASSOCIATED_MAC)
    .bind(ANONYMOUS_MAC)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        captured_by_mac,
        vec![
            (ASSOCIATED_MAC.to_owned(), true),
            (ANONYMOUS_MAC.to_owned(), false),
        ]
    );

    let old_writer_marker: Option<bool> = sqlx::query_scalar(
        "SELECT addresses.expected_machine_preallocation
         FROM machine_interface_addresses AS addresses
         JOIN machine_interfaces AS interfaces ON interfaces.id = addresses.interface_id
         WHERE interfaces.mac_address = $1::macaddr
           AND addresses.address = '192.0.2.20'::inet",
    )
    .bind(ANONYMOUS_MAC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(old_writer_marker, None);

    sqlx::query(
        "INSERT INTO machine_interface_addresses
             (interface_id, address, allocation_type, expected_machine_preallocation)
         SELECT id, '2001:db8::21'::inet, 'static', false
         FROM machine_interfaces
         WHERE mac_address = $1::macaddr",
    )
    .bind(ANONYMOUS_MAC)
    .execute(&pool)
    .await
    .unwrap();
    let current_writer_marker: Option<bool> = sqlx::query_scalar(
        "SELECT addresses.expected_machine_preallocation
         FROM machine_interface_addresses AS addresses
         JOIN machine_interfaces AS interfaces ON interfaces.id = addresses.interface_id
         WHERE interfaces.mac_address = $1::macaddr
           AND addresses.address = '2001:db8::21'::inet",
    )
    .bind(ANONYMOUS_MAC)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(current_writer_marker, Some(false));

    let column_default: Option<String> = sqlx::query_scalar(
        "SELECT column_default
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'machine_interface_addresses'
           AND column_name = 'expected_machine_preallocation'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(column_default, None);

    let index_definition: Option<String> = sqlx::query_scalar(
        "SELECT pg_get_indexdef(
             to_regclass('public.expected_machines_host_nics_gin_idx')
         )",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let index_definition = index_definition.expect("host_nics GIN index must exist");
    assert!(index_definition.contains("USING gin"));
    assert!(index_definition.contains("host_nics jsonb_path_ops"));
}
