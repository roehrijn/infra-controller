ALTER TABLE machine_interfaces
ADD COLUMN expected_interface jsonb;

ALTER TABLE machine_interfaces
ADD COLUMN expected_interface_captured boolean NOT NULL DEFAULT false;

ALTER TABLE predicted_machine_interfaces
ADD COLUMN expected_interface jsonb;

ALTER TABLE predicted_machine_interfaces
ADD COLUMN expected_interface_captured boolean NOT NULL DEFAULT false;

WITH declarations AS (
    SELECT
        (interface->>'mac_address')::macaddr AS mac_address,
        interface,
        count(*) OVER (
            PARTITION BY (interface->>'mac_address')::macaddr
        ) AS declaration_count
    FROM expected_machines AS expected
    CROSS JOIN LATERAL jsonb_array_elements(expected.host_nics) AS interface
),
declared_interfaces AS (
    SELECT mac_address, interface
    FROM declarations
    WHERE declaration_count = 1
      AND NOT EXISTS (
          SELECT 1
          FROM expected_machines AS expected
          WHERE expected.bmc_mac_address = declarations.mac_address
      )
)
UPDATE predicted_machine_interfaces AS predicted
SET expected_interface = CASE
    WHEN COALESCE(declared.interface->>'role', 'host') = 'host'
        THEN jsonb_set(
            declared.interface,
            '{primary}',
            to_jsonb(predicted.primary_interface),
            true
        )
    ELSE declared.interface
END
FROM declared_interfaces AS declared
WHERE predicted.mac_address = declared.mac_address;

-- Predictions created before this migration have now been classified. A NULL
-- snapshot is intentional when no single declaration could be identified.
UPDATE predicted_machine_interfaces
SET expected_interface_captured = true;

-- Associated interfaces that predate this migration have completed ingestion,
-- so NULL is their final snapshot when no declaration was saved. Leave
-- anonymous and preallocated rows unchanged; association state, rather than
-- this flag, determines whether configuration may still reconcile them.
UPDATE machine_interfaces
SET expected_interface_captured = true
WHERE COALESCE(association_type <> 'None'::association_type, false)
   OR num_nonnulls(machine_id, switch_id, power_shelf_id) > 0
   OR attached_dpu_machine_id IS NOT NULL;

-- NULL identifies rows created before ownership was tracked; false is
-- reserved for rows known not to come from ExpectedMachine reconciliation.
-- True covers both fixed reservations and DHCP addresses retained by an
-- ExpectedMachine declaration while the interface remains configuration-owned.
-- Leave the database default NULL during rolling upgrades so older writers,
-- which do not name this column, remain distinguishable from current writers.
ALTER TABLE machine_interface_addresses
ADD COLUMN expected_machine_preallocation boolean;

CREATE INDEX expected_machines_host_nics_gin_idx
ON expected_machines USING GIN (host_nics jsonb_path_ops);
