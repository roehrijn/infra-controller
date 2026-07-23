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
use std::sync::Arc;

use carbide_rack::rms_node_type::compute_node_identity_for_profile;
use carbide_secrets::credentials::{
    BmcCredentialType, CredentialKey, CredentialManager, Credentials,
};
use carbide_utils::none_if_empty::NoneIfEmpty;
use carbide_uuid::machine::MachineId;
use carbide_uuid::network::NetworkSegmentId;
use db::{ObjectColumnFilter, Transaction};
use itertools::Itertools;
use librms::RmsApi;
use librms::protos::rack_manager as rms;
use mac_address::MacAddress;
use model::bmc_info::BmcInfo;
use model::expected_machine::{ExpectedMachine, ExpectedMachineData, ExpectedMachineRequest};
use model::hardware_info::HardwareInfo;
use model::machine::machine_id::host_id_from_dpu_hardware_info;
use model::machine::machine_search_config::MachineSearchConfig;
use model::machine::{
    CURRENT_STATE_MODEL_VERSION, DpuDiscoveringState, DpuDiscoveringStates, Machine,
    MachineInterfaceSnapshot, ManagedHostState,
};
use model::machine_interface::InterfaceType;
use model::machine_interface_address::MachineInterfaceAssociation;
use model::network_segment::NetworkSegmentType;
use model::predicted_machine_interface::NewPredictedMachineInterface;
use model::rack_type::RackProfileConfig;
use model::resource_pool::common::CommonPools;
use model::site_explorer::{EndpointExplorationReport, ExploredDpu, ExploredManagedHost};
use sqlx::{PgConnection, PgPool};

use crate::SiteExplorerConfig;
use crate::errors::{SiteExplorerError, SiteExplorerResult};
use crate::explored_endpoint_index::ExploredEndpointIndex;
use crate::managed_host::ManagedHost;
use crate::metrics::SiteExplorationMetrics;

/// Creates machines from site-explorer managed-host reports.
pub struct MachineCreator {
    database_connection: PgPool,
    config: SiteExplorerConfig,
    common_pools: Arc<CommonPools>,
    rack_profiles: Arc<RackProfileConfig>,
    rms_client: Option<Arc<dyn RmsApi>>,
    credential_manager: Arc<dyn CredentialManager>,
}

impl MachineCreator {
    /// Creates a machine creator with site configuration and optional RMS integration.
    pub fn new(
        database_connection: PgPool,
        config: SiteExplorerConfig,
        common_pools: Arc<CommonPools>,
        rack_profiles: Arc<RackProfileConfig>,
        rms_client: Option<Arc<dyn RmsApi>>,
        credential_manager: Arc<dyn CredentialManager>,
    ) -> Self {
        Self {
            database_connection,
            config,
            common_pools,
            rack_profiles,
            rms_client,
            credential_manager,
        }
    }

    /// Creates a new ManagedHost (Host `Machine` and DPU `Machine` pair)
    /// for each ManagedHost that was identified and that doesn't have a corresponding `Machine` yet
    pub(crate) async fn create_machines(
        &self,
        metrics: &mut SiteExplorationMetrics,
        explored_managed_hosts: &mut [(ExploredManagedHost, EndpointExplorationReport)],
        expected_explored_endpoint_index: &ExploredEndpointIndex,
    ) -> SiteExplorerResult<()> {
        // TODO: Improve the efficiency of this method. Right now we perform 3 database transactions
        // for every identified ManagedHost even if we don't create any objects.
        // We can perform a single query upfront to identify which ManagedHosts don't yet have Machines
        for (host, report) in explored_managed_hosts {
            let expected_machine =
                expected_explored_endpoint_index.matched_expected_machine(&host.host_bmc_ip);

            match self
                .create_managed_host(host, report, expected_machine, &self.database_connection)
                .await
            {
                Ok(true) => {
                    metrics.created_machines += 1;
                    if metrics.created_machines as u64 == self.config.machines_created_per_run {
                        break;
                    }
                }
                Ok(false) => {}
                Err(error) => tracing::error!(
                    %error,
                    host = ?host,
                    "Failed to create managed host"
                ),
            }
        }

        Ok(())
    }

    /// Creates a `Machine` objects for an identified `ManagedHost` with initial states
    ///
    /// Returns `true` if new `Machine` objects have been created or `false` otherwise.
    ///
    /// Refuses to create a Managed Host when `expected_machine` is `None`: only hosts
    /// listed in the `expected_machines` table are allowed to become Managed Hosts.
    /// Already ingested hosts are not affected.
    pub async fn create_managed_host(
        &self,
        explored_host: &ExploredManagedHost,
        report: &mut EndpointExplorationReport,
        expected_machine: Option<&ExpectedMachine>,
        pool: &PgPool,
    ) -> SiteExplorerResult<bool> {
        let Some(expected_machine) = expected_machine else {
            tracing::warn!(
                host_bmc_ip_address = %explored_host.host_bmc_ip,
                    "Refusing to create managed host, expected machines entry not found"
            );
            return Ok(false);
        };
        let expected_machine_identity = ExpectedMachineIngestionIdentity::from(expected_machine);
        let expected_machine_request = ExpectedMachineRequest {
            id: expected_machine.id,
            bmc_mac_address: Some(expected_machine.bmc_mac_address),
        };
        let mut managed_host = ManagedHost::init(explored_host);

        let bmc_credentials =
            if expected_machine.data.rack_id.is_some() && self.rms_client.is_some() {
                let key = CredentialKey::BmcCredentials {
                    credential_type: BmcCredentialType::BmcRoot {
                        bmc_mac_address: expected_machine.bmc_mac_address,
                    },
                };
                match self.credential_manager.get_credentials(&key).await {
                    Ok(Some(Credentials::UsernamePassword { username, password })) => {
                        Some((username, password))
                    }
                    _ => None,
                }
            } else {
                None
            };

        let mut txn = Transaction::begin(pool).await?;
        let expected_machine =
            db::expected_machine::find_for_update(txn.as_pgconn(), &expected_machine_request)
                .await?
                .ok_or_else(|| {
                    db::DatabaseError::FailedPrecondition(
                        "ExpectedMachine changed before managed-host ingestion; retry ingestion"
                            .to_string(),
                    )
                })?;
        if !expected_machine_identity.matches(&expected_machine) {
            return Err(db::DatabaseError::FailedPrecondition(
                "ExpectedMachine identity or credential selection changed before managed-host ingestion; retry ingestion"
                    .to_string(),
            )
            .into());
        }
        let machine_data = Some(&expected_machine.data);

        let bmc_interface_owners =
            preview_bmc_interface_owners(txn.as_pgconn(), explored_host).await?;

        // DHCP locks an interface MAC before entering the segment allocator.
        // Cover every interface this transaction may capture or associate so
        // report-only and IP-resolved BMC interfaces use that same order.
        let ingestion_interface_macs = managed_host_ingestion_interface_macs(
            &expected_machine,
            explored_host,
            report,
            bmc_interface_owners
                .iter()
                .filter_map(|preview| preview.owner.as_ref().map(|owner| owner.mac_address)),
        );
        db::machine_interface::lock_expected_machine_interface_macs(
            txn.as_pgconn(),
            ingestion_interface_macs.iter().copied(),
        )
        .await?;

        let interface_lock_inputs = managed_host_interface_discovery_lock_inputs(
            txn.as_pgconn(),
            &expected_machine,
            &ingestion_interface_macs,
        )
        .await?;
        let ingestion_lock_inputs = managed_host_ingestion_lock_inputs(
            txn.as_pgconn(),
            &expected_machine,
            &bmc_interface_owners,
            &interface_lock_inputs,
        )
        .await?;
        // Take one complete segment-lock pass, then every fixed-address key,
        // before any BMC or machine-interface row is locked. Fixed-address
        // helpers may safely re-acquire these transaction-scoped locks later.
        db::machine_interface::lock_network_segments_exclusive(
            txn.as_pgconn(),
            &ingestion_lock_inputs.segment_ids,
        )
        .await?;
        db::machine_interface::lock_static_address_keys_after_segment_locks(
            txn.as_pgconn(),
            &ingestion_lock_inputs.fixed_allocations,
        )
        .await?;
        lock_and_revalidate_bmc_interface_owners(txn.as_pgconn(), &bmc_interface_owners).await?;
        if !host_bmc_owner_matches_expected_machine(
            &bmc_interface_owners,
            explored_host.host_bmc_ip,
            expected_machine.bmc_mac_address,
        ) {
            return Err(db::DatabaseError::FailedPrecondition(format!(
                "BMC interface ownership for {} does not match the locked ExpectedMachine BMC MAC {}; retry ingestion",
                explored_host.host_bmc_ip, expected_machine.bmc_mac_address,
            ))
            .into());
        }

        // Zero-dpu case: If the explored host had no DPUs, we can create the machine now
        if managed_host.explored_host.dpus.is_empty() {
            if let Some(machine_id) = self
                .create_zero_dpu_machine(&mut txn, &managed_host, report, machine_data)
                .await?
            {
                managed_host.machine_id = Some(machine_id);
            } else {
                // Site explorer has already created a machine for this endpoint previously, skip.
                return Ok(false);
            }
            tracing::info!("Created managed_host with zero DPUs");
        }

        let mut dpu_ids = vec![];
        for dpu_report in managed_host.explored_host.dpus.iter() {
            // machine_id_if_valid_report makes sure that all optional fields on dpu_report are
            // actually set (like the machine-id etc) and returns the machine_id if everything
            // is valid.
            let dpu_machine_id = *dpu_report.machine_id_if_valid_report()?;
            dpu_ids.push(dpu_machine_id);
        }

        let existing_hosts_by_dpu_id =
            db::machine::lookup_host_machine_ids_by_dpu_ids(&mut txn, &dpu_ids).await?;

        if !existing_hosts_by_dpu_id.is_empty() {
            // TODO: We run this code for every endpoint on every site explorer run, and it is slow.
            // The call to reconcile_host_admin_addresses below is particularly slow and locks all
            // network segments. We need to find a good way to know when to skip reconciliation in
            // the common case when nothing has changed.

            // Steady state case: DPU's already exist, so site explorer must have already created
            // this managed host (since only site explorer would have created them.) Ensure they're
            // associated with this machine, then return early.

            let existing_dpu_ids = existing_hosts_by_dpu_id
                .keys()
                .copied()
                .sorted()
                .dedup()
                .collect::<Vec<_>>();
            let existing_managed_host_ids = existing_hosts_by_dpu_id
                .values()
                .copied()
                .sorted()
                .dedup()
                .collect::<Vec<_>>();

            if existing_dpu_ids != dpu_ids.iter().copied().sorted().dedup().collect::<Vec<_>>() {
                // This would only happen if somehow a host endpoint gains/loses a DPU from its endpoint report before we
                // get a chance to create a managed host for it.
                let msg = "explored endpoint has a partial number of DPU's already created";
                tracing::error!(
                    dpu_ids = dpu_ids.iter().join(", "),
                    existing_dpu_ids = existing_dpu_ids.iter().join(", "),
                    "{msg}",
                );
                return Err(SiteExplorerError::internal(msg.to_string()));
            }

            let host_machine_id = match existing_managed_host_ids.as_slice() {
                [host_machine_id] => *host_machine_id,
                host_machine_ids => {
                    let existing_dpu_ids = existing_dpu_ids.iter().join(", ");
                    let existing_host_ids = host_machine_ids.iter().join(", ");
                    let msg = "DPU's from exploration report exist but are members of different managed hosts. exploration results are inconsistent";
                    tracing::error!(%existing_dpu_ids, %existing_host_ids, "BUG: {msg}");
                    return Err(SiteExplorerError::internal(msg.to_string()));
                }
            };

            for dpu_report in managed_host.explored_host.dpus.iter() {
                self.configure_dpu_interface(&mut txn, dpu_report, machine_data)
                    .await?;
            }

            self.reconcile_host_admin_addresses(
                &mut txn,
                &host_machine_id,
                &ingestion_lock_inputs.admin_segment_ids,
            )
            .await?;

            txn.commit().await?;
            return Ok(false);
        }

        for (dpu_report, dpu_machine_id) in
            managed_host.explored_host.dpus.iter().zip(dpu_ids.iter())
        {
            let dpu_machine = self
                .create_dpu(&mut txn, dpu_report, machine_data)
                .await?
                .ok_or_else(|| {
                    SiteExplorerError::internal(format!(
                        "BUG: DPU machine {} was already found, but we already verified that it did not exist?",
                        dpu_machine_id,
                    ))
                })?;

            let host_machine_id = self
                .attach_dpu_to_host(&mut txn, &managed_host, dpu_report, machine_data)
                .await?;
            managed_host.machine_id = Some(host_machine_id);

            // Now that the host link exists in machine_interfaces, the
            // machine group syncing in try_update_network_config keeps this
            // DPU verison bump in sync with the host-level version (and any
            // sibling DPUs already linked) network_config_version.
            self.update_dpu_network_config(&mut txn, &dpu_machine)
                .await?;
        }

        // Now since all DPUs are created, update host and DPUs state correctly.
        let host_machine_id =
            managed_host
                .machine_id
                .ok_or(SiteExplorerError::internal(format!(
                    "Failed to get machine ID for host: {managed_host:#?}"
                )))?;

        db::machine::update_state(
            &mut txn,
            &host_machine_id,
            &ManagedHostState::DpuDiscoveringState {
                dpu_states: DpuDiscoveringStates {
                    states: dpu_ids
                        .iter()
                        .copied()
                        .map(|id| (id, DpuDiscoveringState::Initializing))
                        .collect(),
                },
            },
        )
        .await?;

        let mut rack_profile_id = None;
        if let Some(rack_id) = machine_data.and_then(|d| d.rack_id.as_ref()) {
            tracing::info!(%rack_id, %host_machine_id, "Ensuring rack exists for host machine");
            if let Some(rack) = crate::ensure_rack_exists(&mut txn, rack_id).await? {
                tracing::info!(
                    %rack_id,
                    %host_machine_id,
                    rack = ?rack,
                    "Rack exists"
                );
                rack_profile_id = rack.rack_profile_id;
            }
        }

        // Own a declared integrated boot NIC so a managed-DPU host can boot from it
        // while its DPUs stay managed: the NIC becomes the host's HostInband
        // primary and the DPU admin links go dormant in the reconcile below.
        // Only for hosts with explored DPUs -- a zero-DPU host's NICs (including
        // a declared primary) are already owned by `create_zero_dpu_machine`.
        if !managed_host.explored_host.dpus.is_empty() {
            self.own_declared_host_boot_nic(&mut txn, &host_machine_id, report, machine_data)
                .await?;
        }

        // Normalize host admin address ownership after all DPU-backed host
        // interfaces have been attached and primary flags are final.
        self.reconcile_host_admin_addresses(
            &mut txn,
            &host_machine_id,
            &ingestion_lock_inputs.admin_segment_ids,
        )
        .await?;

        let rms_node_identity = if let (Some(rack_id), Some(_)) =
            (&expected_machine.data.rack_id, &self.rms_client)
        {
            let Some(rack_profile_id) = rack_profile_id.as_ref() else {
                return Err(SiteExplorerError::InvalidArgument(format!(
                    "rack {rack_id} has no rack_profile_id for RMS slot and tray lookup for host machine {host_machine_id}"
                )));
            };

            let Some(rack_profile) = self.rack_profiles.get(rack_profile_id.as_str()) else {
                return Err(SiteExplorerError::InvalidArgument(format!(
                    "rack profile {rack_profile_id} is not configured for RMS slot and tray lookup for host machine {host_machine_id}"
                )));
            };

            Some(
                compute_node_identity_for_profile(rack_profile)
                    .map_err(|error| SiteExplorerError::InvalidArgument(error.to_string()))?,
            )
        } else {
            None
        };

        txn.commit().await?;

        if let (Some(rack_id), Some(rms_client), Some(node_identity)) = (
            &expected_machine.data.rack_id,
            &self.rms_client,
            rms_node_identity,
        ) {
            let mut node = rms::NodeInfo {
                node_id: host_machine_id.to_string(),
                rack_id: rack_id.to_string(),
                r#type: None,
                node_descriptor: None,
                bmc_endpoint: Some(rms::Endpoint {
                    interface: Some(rms::NetworkInterface {
                        ip_address: explored_host.host_bmc_ip.to_string(),
                        mac_address: expected_machine.bmc_mac_address.to_string(),
                        host_name: None,
                    }),
                    port: 443,
                    credentials: bmc_credentials.map(|(username, password)| rms::Credentials {
                        auth: Some(rms::credentials::Auth::UserPass(rms::UsernamePassword {
                            username,
                            password,
                        })),
                    }),
                }),
                ..Default::default()
            };

            node_identity.apply_to_node_info(&mut node);

            let request = rms::BatchGetNodeDeviceInfoRequest {
                nodes: Some(rms::NodeSet { nodes: vec![node] }),
            };
            let (slot_number, tray_index) =
                crate::fetch_slot_and_tray(rms_client.as_ref(), request).await;
            let mut update_txn = Transaction::begin(pool).await?;
            if let Err(e) = db::machine::update_slot_and_tray(
                &mut update_txn,
                &host_machine_id,
                slot_number,
                tray_index,
            )
            .await
            {
                tracing::warn!(
                    error = %e,
                    %host_machine_id,
                    "Failed to update slot_number and tray_index for machine"
                );
            }
            update_txn.commit().await?;
        }

        Ok(true)
    }

    // Returns MachineId if machine was created.
    async fn create_zero_dpu_machine(
        &self,
        txn: &mut PgConnection,
        managed_host: &ManagedHost<'_>,
        report: &mut EndpointExplorationReport,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<Option<MachineId>> {
        // If there's already a machine with the same MAC address as this endpoint, return false. We
        // can't rely on matching the machine_id, as it may have migrated to a stable MachineID
        // already.
        let mac_addresses = host_mac_addresses_for_predicted_machine(report, machine_data);

        // Resolve each MAC's Redfish interface id from the live report up
        // front (`generate_machine_id` below takes a mutable borrow of the
        // report that lives for the rest of this function).
        let report_boot_interface_ids: Vec<(MacAddress, String)> = mac_addresses
            .iter()
            .filter_map(|mac| {
                report
                    .find_interface_id_for_mac(*mac)
                    .map(|id| (*mac, id.to_string()))
            })
            .collect();
        for mac_address in &mac_addresses {
            if db::machine::find_by_mac_address(txn, mac_address)
                .await?
                .is_some()
            {
                return Ok(None);
            }

            // If we already minted this machine and it hasn't DHCP'd yet, there will be an
            // predicted_machine_interface with this MAC address. If so, also skip.
            if !db::predicted_machine_interface::find_by(
                txn,
                ObjectColumnFilter::One(
                    db::predicted_machine_interface::MacAddressColumn,
                    mac_address,
                ),
            )
            .await?
            .is_empty()
            {
                return Ok(None);
            }
        }

        let machine_id = match managed_host.machine_id.as_ref() {
            Some(machine_id) => machine_id,
            None => {
                // Mint a predicted-host machine_id from the exploration report
                report.generate_machine_id(true)?.unwrap()
            }
        };

        tracing::info!(%machine_id, "Minted predicted host ID for zero-DPU machine");

        let existing_machine = db::machine::find_one(
            &mut *txn,
            machine_id,
            MachineSearchConfig {
                include_predicted_host: true,
                ..Default::default()
            },
        )
        .await?;

        if let Some(existing_machine) = existing_machine {
            // There's already a machine with this ID, but we already looked above for machines with
            // the same MAC address as this one, so something's weird here. Log this host's mac
            // addresses and the ones from the colliding hosts to help in diagnosis.
            let existing_macs = existing_machine
                .status
                .hardware_info
                .as_ref()
                .map(|hw| hw.all_mac_addresses())
                .unwrap_or_default();
            tracing::warn!(
                %machine_id,
                existing_mac_addresses = ?existing_macs,
                predicted_host_mac_addresses = ?mac_addresses,
                "Predicted host already exists, with different mac addresses from this one. Potentially multiple machines with same serial number?"
            );
            return Ok(None);
        }

        self.create_machine_from_explored_managed_host(txn, managed_host, machine_id, machine_data)
            .await?;

        // Settle this host's single boot interface as we take ownership: the
        // declared `ExpectedHostNic.primary` (if any) is the host's primary, and
        // every other NIC is non-primary. Routing both the already-leased rows and
        // the freshly-minted predictions through the same declaration makes the
        // choice authoritative regardless of DHCP arrival order, and keeps exactly
        // one primary per machine -- so adopting several NICs that leased before
        // ingestion never trips the `one_primary_interface_per_machine` index.
        // The host's primary (boot) interface is a declared `ExpectedHostNic.primary`
        // when set, otherwise the boot interface preserved across `--delete-interfaces`
        // in `retained_boot_interfaces`. The retained fallback lets a host with no
        // declared primary -- a DPU flipped to NIC mode is the common case -- re-ingest
        // with a settled boot interface, so the controller has a boot target to
        // provision from instead of parking with no primary at all.
        let declared_primary = machine_data.and_then(|data| data.declared_primary_mac());
        let primary_mac = match declared_primary {
            Some(declared) => Some(declared),
            None => {
                let mut recovered = None;
                for mac_address in &mac_addresses {
                    if db::retained_boot_interface::find_by_mac(
                        &mut *txn,
                        *mac_address,
                        self.config.retained_boot_interface_window,
                    )
                    .await?
                    .is_some()
                    {
                        recovered = Some(*mac_address);
                        break;
                    }
                }
                recovered
            }
        };

        // Create and attach a non-DPU machine_interface to the host for every MAC address we see in
        // the exploration report
        for mac_address in mac_addresses {
            let is_primary = primary_mac == Some(mac_address);
            if let Some(machine_interface) =
                db::machine_interface::find_by_mac_address(&mut *txn, mac_address)
                    .await?
                    .into_iter()
                    .next()
            {
                let expected_interface =
                    machine_data.and_then(|data| data.expected_interface_for_mac(mac_address));
                db::machine_interface::capture_expected_interface_before_association(
                    txn,
                    machine_interface.id,
                    expected_interface.as_ref(),
                )
                .await?;
                // There's already a machine_interface with this MAC...
                if let Some(existing_machine_id) = machine_interface.machine_id {
                    // Same machine_id means the preallocated BMC interface row we
                    // just attached via update_machine_topology(), not a contradiction.
                    if existing_machine_id == *machine_id {
                        // Reconcile its primary flag like the anonymous path
                        // below, so a stale primary does not collide when the
                        // real primary NIC is adopted. A BMC interface is never
                        // primary, even when it shares the host NIC MAC.
                        let want_primary =
                            is_primary && machine_interface.interface_type != InterfaceType::Bmc;
                        if machine_interface.primary_interface != want_primary {
                            db::machine_interface::set_primary_interface(
                                &machine_interface.id,
                                want_primary,
                                txn,
                            )
                            .await?;
                        }
                        continue;
                    }
                    // Different machine_id contradicts the find_by_mac() above.
                    tracing::error!(
                        %mac_address,
                        %machine_id,
                        %existing_machine_id,
                        "BUG! Found existing machine_interface with this MAC address, we should not have gotten here!"
                    );
                    return Err(SiteExplorerError::AlreadyFoundError {
                        kind: "MachineInterface",
                        id: mac_address.to_string(),
                    });
                } else {
                    // ...If it has no MachineId, the host must have DHCP'd before site-explorer ran.
                    // Reconcile its primary flag to the declaration before adopting it: an anonymous
                    // DHCP row defaults to primary=true, so without this two pre-ingestion leases
                    // would both arrive primary and collide on association.
                    if machine_interface.primary_interface != is_primary {
                        db::machine_interface::set_primary_interface(
                            &machine_interface.id,
                            is_primary,
                            txn,
                        )
                        .await?;
                    }
                    tracing::info!(%mac_address, %machine_id, "Migrating unowned machine_interface to new managed host");
                    db::machine_interface::associate_interface_with_machine(
                        &machine_interface.id,
                        MachineInterfaceAssociation::Machine(*machine_id),
                        txn,
                    )
                    .await?;
                }
            } else {
                // Give the predicted interface its boot interface id when
                // the live report resolves one, so the promoted row starts
                // with the full boot pair. Retained ids are deliberately
                // NOT copied here: a prediction has no recorded_at, so a
                // copy would dodge the `retained_boot_interface_window`
                // check. The retained pair instead lands on the row at
                // creation (see `create_with_type`), where the window is
                // checked at DHCP time.
                let boot_interface_id = report_boot_interface_ids
                    .iter()
                    .find(|(mac, _)| *mac == mac_address)
                    .map(|(_, id)| id.clone());
                let expected_interface = machine_data
                    .and_then(|data| {
                        data.host_nics
                            .iter()
                            .find(|interface| interface.mac_address == mac_address)
                    })
                    .cloned()
                    .map(|mut interface| {
                        if interface.role.is_host() {
                            interface.primary = Some(is_primary);
                        }
                        interface
                    });
                db::predicted_machine_interface::create(
                    NewPredictedMachineInterface {
                        machine_id,
                        mac_address,
                        expected_network_segment_type: NetworkSegmentType::HostInband,
                        boot_interface_id,
                        primary_interface: is_primary,
                        expected_interface: expected_interface.as_ref(),
                    },
                    txn,
                )
                .await?;
            }
        }

        Ok(Some(*machine_id))
    }

    /// Owns a declared integrated (non-DPU) host NIC as a managed-DPU host's
    /// HostInband boot interface, so a host with managed DPUs can still boot from
    /// an integrated NIC. The NIC carries `primary` into `machine_interfaces` on
    /// its first DHCP; the DPUs stay explored and linked, and their admin links
    /// go dormant in `reconcile_admin_addresses_for_host` once this NIC is the
    /// primary.
    ///
    /// Mirrors the host-NIC ownership in `create_zero_dpu_machine`, but for the
    /// one declared NIC reached from the managed-DPU path. No-op when nothing is
    /// declared, or when the declared NIC is already owned (e.g. a declared DPU
    /// host-PF, which `attach_dpu_to_host` already owns).
    async fn own_declared_host_boot_nic(
        &self,
        txn: &mut PgConnection,
        host_machine_id: &MachineId,
        report: &EndpointExplorationReport,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<()> {
        let Some(declared_mac) = machine_data.and_then(|data| data.declared_primary_mac()) else {
            return Ok(());
        };

        if let Some(existing) = db::machine_interface::find_by_mac_address(&mut *txn, declared_mac)
            .await?
            .into_iter()
            .next()
        {
            let expected_interface =
                machine_data.and_then(|data| data.expected_interface_for_mac(declared_mac));
            db::machine_interface::capture_expected_interface_before_association(
                txn,
                existing.id,
                expected_interface.as_ref(),
            )
            .await?;
            if let Some(existing_machine_id) = existing.machine_id {
                // Owned by THIS host already (e.g. a declared DPU host-PF): its
                // primary flag is settled by the DPU attach / promotion paths.
                // Owned by a DIFFERENT machine: the declaration names a MAC that
                // already belongs elsewhere -- surface it rather than silently
                // dropping the declared boot NIC (mirrors create_zero_dpu_machine).
                if existing_machine_id != *host_machine_id {
                    return Err(SiteExplorerError::AlreadyFoundError {
                        kind: "MachineInterface",
                        id: declared_mac.to_string(),
                    });
                }
                return Ok(());
            }
            // The integrated NIC leased before ingestion: adopt its anonymous row
            // as the host's sole primary, demoting the interim DPU primary so the
            // two never collide on `one_primary_interface_per_machine`.
            db::machine_interface::demote_primary_interfaces_for_machine(host_machine_id, txn)
                .await?;
            if !existing.primary_interface {
                db::machine_interface::set_primary_interface(&existing.id, true, txn).await?;
            }
            db::machine_interface::associate_interface_with_machine(
                &existing.id,
                MachineInterfaceAssociation::Machine(*host_machine_id),
                txn,
            )
            .await?;
            tracing::info!(
                declared_mac_address = %declared_mac, %host_machine_id,
                "Adopted declared integrated boot NIC as the managed-DPU host's primary",
            );
            return Ok(());
        }

        // A prediction may already exist from a prior ingestion of this host --
        // don't mint a second one. One owned by a different machine is the same
        // contradiction as the machine_interface case above, so surface it rather
        // than silently dropping the declaration.
        if let Some(existing_prediction) =
            db::predicted_machine_interface::find_by_mac_address(&mut *txn, declared_mac).await?
        {
            if existing_prediction.machine_id != *host_machine_id {
                return Err(SiteExplorerError::AlreadyFoundError {
                    kind: "PredictedMachineInterface",
                    id: declared_mac.to_string(),
                });
            }
            return Ok(());
        }

        // Not yet leased: mint a HostInband prediction carrying primary, so the
        // NIC is adopted and made primary on its first DHCP (the promotion demotes
        // the interim DPU primary).
        let boot_interface_id = report
            .find_interface_id_for_mac(declared_mac)
            .map(|id| id.to_string());
        let expected_interface = machine_data
            .and_then(|data| {
                data.host_nics
                    .iter()
                    .find(|interface| interface.mac_address == declared_mac)
            })
            .cloned()
            .map(|mut interface| {
                interface.primary = Some(true);
                interface
            });
        db::predicted_machine_interface::create(
            NewPredictedMachineInterface {
                machine_id: host_machine_id,
                mac_address: declared_mac,
                expected_network_segment_type: NetworkSegmentType::HostInband,
                boot_interface_id,
                primary_interface: true,
                expected_interface: expected_interface.as_ref(),
            },
            txn,
        )
        .await?;
        tracing::info!(
            declared_mac_address = %declared_mac, %host_machine_id,
            "Minted HostInband boot-NIC prediction for managed-DPU host's declared integrated primary",
        );
        Ok(())
    }

    // create_dpu does everything needed to create a DPU as part of a newly discovered managed host.
    // If the DPU does not exist in the machines table, the function creates a new DPU machine and
    // configures it appropriately, returning the new `Machine`.
    // If the DPU already exists in the machines table, this is a no-op and returns `None`.
    //
    // The DPU's `network_config` is intentionally NOT written here -- the caller writes it after
    // `attach_dpu_to_host` has wired the host link in `machine_interfaces`, so that
    // `try_update_network_config`'s group sync observes both rows as siblings and keeps their
    // versions equal.
    async fn create_dpu(
        &self,
        txn: &mut PgConnection,
        explored_dpu: &ExploredDpu,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<Option<Machine>> {
        if let Some(dpu_machine) = self.create_dpu_machine(txn, explored_dpu).await? {
            self.configure_dpu_interface(txn, explored_dpu, machine_data)
                .await?;
            let dpu_machine_id: &MachineId = explored_dpu.report.machine_id.as_ref().unwrap();
            let dpu_bmc_info = explored_dpu.bmc_info();
            let dpu_hw_info = explored_dpu.hardware_info()?;
            self.update_machine_topology(
                txn,
                dpu_machine_id,
                dpu_bmc_info,
                dpu_hw_info,
                machine_data,
            )
            .await?;
            return Ok(Some(dpu_machine));
        }
        Ok(None)
    }

    // 1) Create a machine for this host using the passed machine_id
    // 2) Update the "machine_topologies" table with the bmc info for this host
    async fn create_machine_from_explored_managed_host(
        &self,
        txn: &mut PgConnection,
        managed_host: &ManagedHost<'_>,
        predicted_machine_id: &MachineId,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<()> {
        _ = db::machine::create(
            txn,
            Some(&self.common_pools),
            predicted_machine_id,
            ManagedHostState::Created,
            machine_data,
            CURRENT_STATE_MODEL_VERSION,
        )
        .await?;
        let hardware_info = HardwareInfo::default();
        self.update_machine_topology(
            txn,
            predicted_machine_id,
            managed_host.explored_host.bmc_info(),
            hardware_info,
            machine_data,
        )
        .await
    }

    // configure_dpu_interface checks the machine_interfaces table to see if the DPU's machine interface has its machine id set.
    // If the machine ID is already configured appropriately for the DPU's machine interface, configure_dpu_interface will return false
    // If the DPU's machine interface was missing the machine ID in the table, configure_dpu_interface will set the machine ID and return true.
    async fn configure_dpu_interface(
        &self,
        txn: &mut PgConnection,
        explored_dpu: &ExploredDpu,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<bool> {
        let dpu_machine_id: &MachineId = explored_dpu.report.machine_id.as_ref().unwrap();
        let oob_net0_mac = dpu_oob_mac_address(explored_dpu);

        // If machine_interface exists for the DPU and machine_id is not updated, do it now.
        if let Some(oob_net0_mac) = oob_net0_mac {
            let mi = db::machine_interface::find_by_mac_address(&mut *txn, oob_net0_mac).await?;

            if let Some(interface) = mi.first()
                && interface.machine_id.is_none()
            {
                let expected_interface = machine_data
                    .and_then(|data| data.expected_interface_for_mac(interface.mac_address));
                db::machine_interface::capture_expected_interface_before_association(
                    txn,
                    interface.id,
                    expected_interface.as_ref(),
                )
                .await?;
                tracing::info!(
                    machine_interface_id = %interface.id,
                    machine_id = %dpu_machine_id,
                    "Associating machine interface with machine"
                );
                db::machine_interface::associate_interface_with_machine(
                    &interface.id,
                    MachineInterfaceAssociation::Machine(*dpu_machine_id),
                    txn,
                )
                .await?;
                db::machine_interface::associate_interface_with_dpu_machine(
                    &interface.id,
                    dpu_machine_id,
                    txn,
                )
                .await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    // create_dpu_machine creates a machine for the DPU as specified by dpu_machine_id. Returns an Optional Machine indicating whether the function created a new machine (returns None if a machine already existed for this DPU).
    // if an entry exists in the machines table with a machine ID which matches dpu_machine_id, a machine has already been created for this DPU. Returns None.
    // if an entry doesnt exist in the machine table, the site explorer will add an entry in the machines table for the DPU and update its network config appropriately (allocating a loop ip address etc). Return the newly created machine.
    async fn create_dpu_machine(
        &self,
        txn: &mut PgConnection,
        explored_dpu: &ExploredDpu,
    ) -> SiteExplorerResult<Option<Machine>> {
        let dpu_machine_id = explored_dpu.report.machine_id.as_ref().unwrap();
        match db::machine::find_one(&mut *txn, dpu_machine_id, MachineSearchConfig::default())
            .await?
        {
            // Do nothing if machine exists. It'll be reprovisioned via redfish
            Some(_existing_machine) => Ok(None),
            None => match db::machine::create(
                txn,
                Some(&self.common_pools),
                dpu_machine_id,
                ManagedHostState::Created,
                None,
                CURRENT_STATE_MODEL_VERSION,
            )
            .await
            {
                Ok(machine) => {
                    tracing::info!(machine_id = %dpu_machine_id, "Created DPU machine");
                    Ok(Some(machine))
                }
                Err(e) => {
                    tracing::error!(error = %e, "Can't create DPU machine");
                    Err(e.into())
                }
            },
        }
    }

    async fn attach_dpu_to_host(
        &self,
        txn: &mut PgConnection,
        explored_host: &ManagedHost<'_>,
        explored_dpu: &ExploredDpu,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<MachineId> {
        let dpu_hw_info = explored_dpu.hardware_info()?;
        let proactive_host_mac = dpu_hw_info.factory_mac_address().map_err(|error| {
            SiteExplorerError::InvalidArgument(format!(
                "DPU hardware info is missing its host factory MAC address: {error}",
            ))
        })?;
        let proactive_expected_interface =
            machine_data.and_then(|data| data.expected_interface_for_mac(proactive_host_mac));
        // Create Host proactively.
        // In case host interface is created, this method will return existing one, instead
        // creating new everytime.
        let host_machine_interface =
            db::machine_interface::find_or_create_host_machine_dpu_interface_proactively(
                txn,
                Some(&dpu_hw_info),
                explored_dpu.report.machine_id.as_ref().unwrap(),
                proactive_expected_interface.as_ref(),
                self.config.retained_boot_interface_window,
            )
            .await?;

        if host_machine_interface.machine_id.is_some() {
            return Err(SiteExplorerError::internal(format!(
                "The host's machine interface for DPU {} already has the machine ID set--something is wrong: {:#?}",
                explored_dpu.report.machine_id.as_ref().unwrap(),
                host_machine_interface
            )));
        }

        db::machine_interface::capture_expected_interface_before_association(
            txn,
            host_machine_interface.id,
            proactive_expected_interface.as_ref(),
        )
        .await?;
        db::machine_interface::associate_interface_with_dpu_machine(
            &host_machine_interface.id,
            explored_dpu.report.machine_id.as_ref().unwrap(),
            txn,
        )
        .await?;
        let host_machine_interface =
            db::machine_interface::find_one(&mut *txn, host_machine_interface.id).await?;

        let host_machine_id = self
            .configure_host_machine(
                txn,
                explored_host,
                &host_machine_interface,
                explored_dpu,
                machine_data,
            )
            .await?;

        db::machine_interface::associate_interface_with_machine(
            &host_machine_interface.id,
            MachineInterfaceAssociation::Machine(host_machine_id),
            txn,
        )
        .await?;

        Ok(host_machine_id)
    }

    async fn update_machine_topology(
        &self,
        txn: &mut PgConnection,
        machine_id: &MachineId,
        mut bmc_info: BmcInfo,
        hardware_info: HardwareInfo,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<()> {
        let _topology =
            db::machine_topology::create_or_update(txn, machine_id, &hardware_info).await?;

        // Forge scout will update this topology with a full information.
        db::machine_topology::set_topology_update_needed(txn, machine_id, true).await?;

        // call enrich_mac_address to fill the MAC address info from the machine_interfaces table
        db::bmc_metadata::enrich_mac_address(
            &mut bmc_info,
            "SiteExplorer::update_machine_topology".to_string(),
            txn,
            machine_id,
            true,
        )
        .await?;

        let expected_interface = bmc_info.mac.and_then(|mac_address| {
            machine_data.and_then(|data| data.expected_interface_for_mac(mac_address))
        });
        db::bmc_metadata::update_bmc_network_into_machine_interfaces(
            txn,
            machine_id,
            &mut bmc_info,
            expected_interface.as_ref(),
        )
        .await?;

        Ok(())
    }

    async fn update_dpu_network_config(
        &self,
        txn: &mut PgConnection,
        dpu_machine: &Machine,
    ) -> SiteExplorerResult<()> {
        let (mut network_config, version) = dpu_machine.network_config.clone().take();
        if network_config.loopback_ip.is_none() {
            let loopback_ip = db::machine::allocate_loopback_ip(
                &self.common_pools,
                txn,
                &dpu_machine.id.to_string(),
            )
            .await?;
            network_config.loopback_ip = Some(loopback_ip);
        }

        if self.config.allocate_secondary_vtep_ip
            && network_config.secondary_overlay_vtep_ip.is_none()
        {
            let secondary_vtep_ip = db::machine::allocate_secondary_vtep_ip(
                &self.common_pools,
                txn,
                &dpu_machine.id.to_string(),
            )
            .await?;
            network_config.secondary_overlay_vtep_ip = Some(secondary_vtep_ip);
        }

        db::machine::try_update_network_config(txn, &dpu_machine.id, version, &network_config)
            .await?;

        Ok(())
    }

    /// Reconciles host admin addresses and bumps visible host network config when needed.
    ///
    /// Returns whether the active admin config changed.
    async fn reconcile_host_admin_addresses(
        &self,
        txn: &mut PgConnection,
        host_machine_id: &MachineId,
        locked_admin_segment_ids: &[NetworkSegmentId],
    ) -> SiteExplorerResult<bool> {
        let active_config_changed =
            db::machine_interface::reconcile_admin_addresses_for_host_with_locked_admin_segments(
                txn,
                host_machine_id,
                locked_admin_segment_ids,
            )
            .await?;
        if active_config_changed {
            let (network_config, network_config_version) =
                db::machine::get_network_config(&mut *txn, host_machine_id)
                    .await?
                    .take();
            db::machine::try_update_network_config(
                txn,
                host_machine_id,
                network_config_version,
                &network_config,
            )
            .await?;
        }
        Ok(active_config_changed)
    }

    // configure_host_machine configures the host's machine with the specific interface. It returns the host's machine ID.
    //
    // Normally, a host will have a single machine interface because the majority of hosts (for now) have a single DPU.
    // If a host has multiple DPUs, the host machine will have a machine interface for each DPU.
    // However, all of the host machine interfaces must be attached to the same host machine (and host machine-id).
    // Until this point, all of these interfaces will be marked as the "primary" interface by default.
    //
    // configure_host_machine handles two cases:
    // 1) host_machine_interface is the primary interface for this host: generate the machine ID for this host and use it to actually create the machine for the host.
    // 2) host_machine_interface is *not* the primary interface for this host: set "primary_interface" to false for this machine interface. Return the host ID generated from (1)
    //
    // The first DPU that we attach to the host is designated as the primary DPU; the associate host machine interface is designated is the primary interface.
    // Therefore, the primary interface is guaranteed to be configured prior to any secondary interface.
    #[allow(clippy::too_many_arguments)]
    async fn configure_host_machine(
        &self,
        txn: &mut PgConnection,
        explored_host: &ManagedHost<'_>,
        host_machine_interface: &MachineInterfaceSnapshot,
        explored_dpu: &ExploredDpu,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<MachineId> {
        match &explored_host.machine_id {
            Some(host_machine_id) => {
                // This is not the primary interface for this host
                // The primary interface *must* have already been created for this host (otherwise something very bad has happened)
                db::machine_interface::set_primary_interface(
                    &host_machine_interface.id,
                    false,
                    txn,
                )
                .await?;
                Ok(*host_machine_id)
            }
            None => {
                // This is the primary interface for the host.
                // 1. Generate the ID for the host from *this* DPU's hw info
                // 2. Add an entry for this host in the machines table (with a machine-id from (1)).
                let host_machine_id = self
                    .create_host_from_dpu_hw_info(
                        txn,
                        explored_host.explored_host,
                        explored_dpu,
                        machine_data,
                    )
                    .await?;

                tracing::info!(
                    ?host_machine_interface.id,
                    machine_id = %host_machine_id,
                    "Created host machine proactively in site-explorer",
                );

                db::machine_interface::set_primary_interface(&host_machine_interface.id, true, txn)
                    .await?;
                Ok(host_machine_id)
            }
        }
    }

    // 1) Generate the host's machine ID from the DPU's hardware info
    // 2) Create a machine for this host using the machine ID from (1)
    // 3) Update the "machine_topologies" table with the bmc info for this host
    async fn create_host_from_dpu_hw_info(
        &self,
        txn: &mut PgConnection,
        explored_host: &ExploredManagedHost,
        explored_dpu: &ExploredDpu,
        machine_data: Option<&ExpectedMachineData>,
    ) -> SiteExplorerResult<MachineId> {
        let dpu_hw_info = explored_dpu.hardware_info()?;
        let predicted_machine_id = host_id_from_dpu_hardware_info(&dpu_hw_info).map_err(|e| {
            SiteExplorerError::InvalidArgument(format!("hardware info missing: {e}"))
        })?;

        let _host_machine = db::machine::create(
            txn,
            Some(&self.common_pools),
            &predicted_machine_id,
            ManagedHostState::Created,
            machine_data,
            CURRENT_STATE_MODEL_VERSION,
        )
        .await?;

        let host_bmc_info = explored_host.bmc_info();
        let host_hardware_info = HardwareInfo::default();
        self.update_machine_topology(
            txn,
            &predicted_machine_id,
            host_bmc_info,
            host_hardware_info,
            machine_data,
        )
        .await?;

        Ok(predicted_machine_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ExpectedMachineIngestionIdentity {
    id: Option<uuid::Uuid>,
    bmc_mac_address: MacAddress,
    has_rack_id: bool,
}

impl From<&ExpectedMachine> for ExpectedMachineIngestionIdentity {
    fn from(expected_machine: &ExpectedMachine) -> Self {
        Self {
            id: expected_machine.id,
            bmc_mac_address: expected_machine.bmc_mac_address,
            has_rack_id: expected_machine.data.rack_id.is_some(),
        }
    }
}

impl ExpectedMachineIngestionIdentity {
    fn matches(&self, expected_machine: &ExpectedMachine) -> bool {
        self == &Self::from(expected_machine)
    }
}

#[derive(Clone, Copy, Debug)]
struct BmcOwnerPreview {
    address: IpAddr,
    owner: Option<db::bmc_metadata::BmcInterfaceOwner>,
}

async fn preview_bmc_interface_owners(
    txn: &mut PgConnection,
    explored_host: &ExploredManagedHost,
) -> SiteExplorerResult<Vec<BmcOwnerPreview>> {
    let mut bmc_ips = std::iter::once(explored_host.host_bmc_ip)
        .chain(explored_host.dpus.iter().map(|dpu| dpu.bmc_ip))
        .collect::<Vec<_>>();
    bmc_ips.sort_unstable();
    bmc_ips.dedup();

    let mut owners = Vec::with_capacity(bmc_ips.len());
    for address in bmc_ips {
        let owner = db::bmc_metadata::find_interface_by_bmc_ip(txn, address).await?;
        owners.push(BmcOwnerPreview { address, owner });
    }
    Ok(owners)
}

#[derive(Debug, PartialEq, Eq)]
struct ManagedHostIngestionLockInputs {
    admin_segment_ids: Vec<NetworkSegmentId>,
    segment_ids: Vec<NetworkSegmentId>,
    fixed_allocations: Vec<(NetworkSegmentId, IpAddr)>,
}

fn merge_managed_host_ingestion_lock_inputs(
    mut admin_segment_ids: Vec<NetworkSegmentId>,
    mut segment_ids: Vec<NetworkSegmentId>,
    interface_lock_inputs: &[db::machine_interface::ExpectedInterfaceDiscoveryLockInputs],
    mut fixed_allocations: Vec<(NetworkSegmentId, IpAddr)>,
) -> ManagedHostIngestionLockInputs {
    admin_segment_ids.sort_unstable();
    admin_segment_ids.dedup();
    segment_ids.extend(admin_segment_ids.iter().copied());
    for inputs in interface_lock_inputs {
        segment_ids.extend(inputs.existing_segment_id);
        fixed_allocations.extend(inputs.fixed_allocations.iter().copied());
    }
    segment_ids.extend(fixed_allocations.iter().map(|(segment_id, _)| *segment_id));
    segment_ids.sort_unstable();
    segment_ids.dedup();
    fixed_allocations.sort_unstable();
    fixed_allocations.dedup();

    ManagedHostIngestionLockInputs {
        admin_segment_ids,
        segment_ids,
        fixed_allocations,
    }
}

async fn managed_host_interface_discovery_lock_inputs(
    txn: &mut PgConnection,
    expected_machine: &ExpectedMachine,
    mac_addresses: &[MacAddress],
) -> SiteExplorerResult<Vec<db::machine_interface::ExpectedInterfaceDiscoveryLockInputs>> {
    let mut lock_inputs = Vec::with_capacity(mac_addresses.len());
    for mac_address in mac_addresses {
        let expected_interface = expected_machine
            .data
            .expected_interface_for_mac(*mac_address);
        lock_inputs.push(
            db::machine_interface::expected_interface_discovery_lock_inputs(
                txn,
                *mac_address,
                expected_interface.as_ref(),
            )
            .await?,
        );
    }
    Ok(lock_inputs)
}

async fn managed_host_ingestion_lock_inputs(
    txn: &mut PgConnection,
    expected_machine: &ExpectedMachine,
    bmc_interface_owners: &[BmcOwnerPreview],
    interface_lock_inputs: &[db::machine_interface::ExpectedInterfaceDiscoveryLockInputs],
) -> SiteExplorerResult<ManagedHostIngestionLockInputs> {
    let admin_segment_ids =
        db::network_segment::list_segment_ids(txn, Some(NetworkSegmentType::Admin)).await?;
    let mut segment_ids = bmc_interface_owners
        .iter()
        .filter_map(|preview| preview.owner.as_ref().map(|owner| owner.segment_id))
        .collect::<Vec<_>>();

    // A missing BMC owner remains compatible with the existing enrichment
    // path, but its address segment must stay locked so an owner cannot appear
    // between revalidation and the later lookup.
    let mut missing_owner_addresses = bmc_interface_owners
        .iter()
        .filter(|preview| preview.owner.is_none())
        .map(|preview| preview.address)
        .collect::<Vec<_>>();
    missing_owner_addresses.sort_unstable();
    missing_owner_addresses.dedup();
    for address in missing_owner_addresses {
        let segment = db::network_segment::for_static_address(txn, address, None).await?;
        segment_ids.push(segment.id);
    }

    let mut fixed_allocations = Vec::new();
    if let Some(address) = expected_machine.data.bmc_ip_address {
        let segment = db::network_segment::for_static_address(txn, address, None).await?;
        fixed_allocations.push((segment.id, address));
    }

    Ok(merge_managed_host_ingestion_lock_inputs(
        admin_segment_ids,
        segment_ids,
        interface_lock_inputs,
        fixed_allocations,
    ))
}

fn bmc_interface_owner_matches(
    preview: Option<&db::bmc_metadata::BmcInterfaceOwner>,
    current: Option<&db::bmc_metadata::BmcInterfaceOwner>,
) -> bool {
    match (preview, current) {
        (None, None) => true,
        (Some(preview), Some(current)) => {
            current.interface_id == preview.interface_id
                && current.mac_address == preview.mac_address
                && current.segment_id == preview.segment_id
        }
        _ => false,
    }
}

async fn lock_and_revalidate_bmc_interface_owners(
    txn: &mut PgConnection,
    previews: &[BmcOwnerPreview],
) -> SiteExplorerResult<()> {
    for preview in previews {
        let current =
            db::bmc_metadata::find_interface_by_bmc_ip_for_update(txn, preview.address).await?;
        if !bmc_interface_owner_matches(preview.owner.as_ref(), current.as_ref()) {
            return Err(db::DatabaseError::FailedPrecondition(format!(
                "BMC interface ownership for {} changed while managed-host ingestion was waiting for network locks; retry ingestion",
                preview.address,
            ))
            .into());
        }
    }
    Ok(())
}

fn host_bmc_owner_matches_expected_machine(
    previews: &[BmcOwnerPreview],
    host_bmc_ip: IpAddr,
    expected_bmc_mac_address: MacAddress,
) -> bool {
    previews
        .iter()
        .find(|preview| preview.address == host_bmc_ip)
        .is_some_and(|preview| {
            preview
                .owner
                .as_ref()
                .is_none_or(|owner| owner.mac_address == expected_bmc_mac_address)
        })
}

fn dpu_oob_mac_address(explored_dpu: &ExploredDpu) -> Option<MacAddress> {
    explored_dpu.report.systems.iter().find_map(|system| {
        system.ethernet_interfaces.iter().find_map(|interface| {
            if interface
                .id
                .as_ref()
                .is_some_and(|id| id.to_lowercase().contains("oob"))
            {
                interface.mac_address
            } else {
                None
            }
        })
    })
}

fn managed_host_ingestion_interface_macs(
    expected_machine: &ExpectedMachine,
    explored_host: &ExploredManagedHost,
    report: &EndpointExplorationReport,
    bmc_owner_macs: impl IntoIterator<Item = MacAddress>,
) -> Vec<MacAddress> {
    let mut mac_addresses = expected_machine
        .data
        .host_nics
        .iter()
        .map(|interface| interface.mac_address)
        .chain(std::iter::once(expected_machine.bmc_mac_address))
        .chain(report.all_mac_addresses())
        .chain(explored_host.dpus.iter().filter_map(dpu_oob_mac_address))
        .chain(
            explored_host
                .dpus
                .iter()
                .filter_map(|dpu| dpu.bmc_info().mac),
        )
        .chain(
            explored_host
                .dpus
                .iter()
                .filter_map(|dpu| dpu.host_pf_mac_address),
        )
        .chain(bmc_owner_macs)
        .collect::<Vec<_>>();
    mac_addresses.sort_unstable();
    mac_addresses.dedup();
    mac_addresses
}

/// Host inband MACs used when minting `predicted_machine_interface` rows for zero-DPU hosts.
/// Prefers Redfish-reported System EthernetInterfaces; falls back to `ExpectedMachine.host_nics`
/// when the BMC omits them from Redfish.
fn host_mac_addresses_for_predicted_machine(
    report: &EndpointExplorationReport,
    machine_data: Option<&ExpectedMachineData>,
) -> Vec<MacAddress> {
    let from_redfish = report.all_mac_addresses();
    match from_redfish.as_slice() {
        [_, ..] => from_redfish,
        [] => machine_data
            .filter(|_| !(report.is_dpu() || report.is_switch() || report.is_power_shelf()))
            .map(|data| {
                data.host_nics
                    .iter()
                    .filter(|interface| interface.role.is_host())
                    .collect::<Vec<_>>()
            })
            .none_if_empty()
            .map(|host_nics| {
                tracing::info!(
                    host_nic_count = host_nics.len(),
                    "System EthernetInterfaces missing from Redfish; using ExpectedMachine.host_nics for predicted machine interfaces"
                );
                host_nics
                    .iter()
                    .map(|nic| nic.mac_address)
                    .dedup()
                    .collect()
            })
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use carbide_test_harness::prelude::{TestHarness, sqlx_test, sqlx_testing};
    use model::allocation_type::AllocationType;
    use model::expected_machine::{ExpectedHostNic, ExpectedInterfaceRole};
    use model::site_explorer::{ComputerSystem, EthernetInterface, Manager};

    use super::*;

    async fn wait_for_advisory_lock_wait(pool: &PgPool) {
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
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("managed-host ingestion did not reach the segment-lock queue");
    }

    #[test]
    fn zero_dpu_fallback_uses_only_host_interface_declarations() {
        let host_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
        let dpu_os_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x02]);
        let dpu_bmc_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x03]);
        let machine_data = ExpectedMachineData {
            host_nics: vec![
                ExpectedHostNic {
                    mac_address: host_mac,
                    role: ExpectedInterfaceRole::Host,
                    ..Default::default()
                },
                ExpectedHostNic {
                    mac_address: dpu_os_mac,
                    role: ExpectedInterfaceRole::DpuOs,
                    ..Default::default()
                },
                ExpectedHostNic {
                    mac_address: dpu_bmc_mac,
                    role: ExpectedInterfaceRole::DpuBmc,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let mac_addresses = host_mac_addresses_for_predicted_machine(
            &EndpointExplorationReport::default(),
            Some(&machine_data),
        );

        assert_eq!(mac_addresses, vec![host_mac]);
    }

    #[test]
    fn ingestion_mac_locks_cover_config_report_and_dpu_interfaces() {
        let configured_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
        let report_host_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x02]);
        let dpu_oob_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x03]);
        let dpu_host_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x04]);
        let unrelated_dpu_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x05]);
        let dpu_bmc_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x06]);
        let resolved_bmc_owner_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x07]);
        let host_bmc_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x10]);
        let expected_machine = ExpectedMachine {
            id: None,
            bmc_mac_address: host_bmc_mac,
            data: ExpectedMachineData {
                host_nics: vec![
                    ExpectedHostNic {
                        mac_address: configured_mac,
                        ..Default::default()
                    },
                    ExpectedHostNic {
                        mac_address: report_host_mac,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        };
        let host_report = EndpointExplorationReport {
            systems: vec![ComputerSystem {
                ethernet_interfaces: vec![EthernetInterface {
                    id: Some("host0".to_string()),
                    mac_address: Some(report_host_mac),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let dpu_report = EndpointExplorationReport {
            managers: vec![Manager {
                ethernet_interfaces: vec![EthernetInterface {
                    mac_address: Some(dpu_bmc_mac),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            systems: vec![ComputerSystem {
                ethernet_interfaces: vec![
                    EthernetInterface {
                        id: Some("OOB_NET0".to_string()),
                        mac_address: Some(dpu_oob_mac),
                        ..Default::default()
                    },
                    EthernetInterface {
                        id: Some("tmfifo_net0".to_string()),
                        mac_address: Some(unrelated_dpu_mac),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let explored_host = ExploredManagedHost {
            host_bmc_ip: "192.0.2.10".parse().unwrap(),
            dpus: vec![ExploredDpu {
                bmc_ip: "192.0.2.11".parse().unwrap(),
                host_pf_mac_address: Some(dpu_host_mac),
                report: Arc::new(dpu_report),
            }],
        };

        let mac_addresses = managed_host_ingestion_interface_macs(
            &expected_machine,
            &explored_host,
            &host_report,
            [resolved_bmc_owner_mac],
        );

        assert_eq!(
            mac_addresses,
            vec![
                configured_mac,
                report_host_mac,
                dpu_oob_mac,
                dpu_host_mac,
                dpu_bmc_mac,
                resolved_bmc_owner_mac,
                host_bmc_mac,
            ]
        );
        assert!(!mac_addresses.contains(&unrelated_dpu_mac));
    }

    #[test]
    fn bmc_owner_revalidation_checks_row_mac_segment_and_presence() {
        let preview_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
        let preview = db::bmc_metadata::BmcInterfaceOwner {
            interface_id: uuid::Uuid::from_u128(1).into(),
            mac_address: preview_mac,
            segment_id: uuid::Uuid::from_u128(2).into(),
        };

        assert!(bmc_interface_owner_matches(Some(&preview), Some(&preview)));
        assert!(bmc_interface_owner_matches(None, None));
        assert!(!bmc_interface_owner_matches(Some(&preview), None));
        assert!(!bmc_interface_owner_matches(None, Some(&preview)));

        let mut changed_id = preview;
        changed_id.interface_id = uuid::Uuid::from_u128(3).into();
        assert!(!bmc_interface_owner_matches(
            Some(&preview),
            Some(&changed_id)
        ));

        let mut changed_mac = preview;
        changed_mac.mac_address = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x04]);
        assert!(!bmc_interface_owner_matches(
            Some(&preview),
            Some(&changed_mac)
        ));

        let mut changed_segment = preview;
        changed_segment.segment_id = uuid::Uuid::from_u128(5).into();
        assert!(!bmc_interface_owner_matches(
            Some(&preview),
            Some(&changed_segment)
        ));
    }

    #[test]
    fn host_bmc_owner_must_match_expected_machine_bmc_mac() {
        let host_bmc_ip = "192.0.2.10".parse().unwrap();
        let expected_bmc_mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
        let owner = db::bmc_metadata::BmcInterfaceOwner {
            interface_id: uuid::Uuid::from_u128(1).into(),
            mac_address: expected_bmc_mac,
            segment_id: uuid::Uuid::from_u128(2).into(),
        };
        let matching = [BmcOwnerPreview {
            address: host_bmc_ip,
            owner: Some(owner),
        }];
        assert!(host_bmc_owner_matches_expected_machine(
            &matching,
            host_bmc_ip,
            expected_bmc_mac,
        ));

        let absent = [BmcOwnerPreview {
            address: host_bmc_ip,
            owner: None,
        }];
        assert!(host_bmc_owner_matches_expected_machine(
            &absent,
            host_bmc_ip,
            expected_bmc_mac,
        ));

        let mismatched = [BmcOwnerPreview {
            address: host_bmc_ip,
            owner: Some(db::bmc_metadata::BmcInterfaceOwner {
                mac_address: MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x03]),
                ..owner
            }),
        }];
        assert!(!host_bmc_owner_matches_expected_machine(
            &mismatched,
            host_bmc_ip,
            expected_bmc_mac,
        ));
        assert!(!host_bmc_owner_matches_expected_machine(
            &[],
            host_bmc_ip,
            expected_bmc_mac,
        ));
    }

    #[test]
    fn expected_machine_ingestion_identity_covers_credentials_lookup_inputs() {
        let expected_machine = ExpectedMachine {
            id: Some(uuid::Uuid::from_u128(1)),
            bmc_mac_address: MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]),
            data: ExpectedMachineData {
                rack_id: Some("rack-a".into()),
                ..Default::default()
            },
        };
        let identity = ExpectedMachineIngestionIdentity::from(&expected_machine);
        assert!(identity.matches(&expected_machine));

        let mut changed_id = expected_machine.clone();
        changed_id.id = Some(uuid::Uuid::from_u128(3));
        assert!(!identity.matches(&changed_id));

        let mut changed_mac = expected_machine.clone();
        changed_mac.bmc_mac_address = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x04]);
        assert!(!identity.matches(&changed_mac));

        let mut changed_credential_path = expected_machine.clone();
        changed_credential_path.data.rack_id = None;
        assert!(!identity.matches(&changed_credential_path));

        let mut same_credential_path = expected_machine;
        same_credential_path.data.rack_id = Some("rack-b".into());
        assert!(identity.matches(&same_credential_path));
    }

    #[test]
    fn ingestion_lock_input_union_includes_existing_and_fixed_segments() {
        let legacy_segment: NetworkSegmentId = uuid::Uuid::from_u128(1).into();
        let fixed_segment: NetworkSegmentId = uuid::Uuid::from_u128(2).into();
        let owner_segment: NetworkSegmentId = uuid::Uuid::from_u128(3).into();
        let existing_segment: NetworkSegmentId = uuid::Uuid::from_u128(4).into();
        let admin_segment: NetworkSegmentId = uuid::Uuid::from_u128(5).into();
        let fixed_address = "192.0.2.10".parse().unwrap();
        let legacy_address = "192.0.2.11".parse().unwrap();
        let interface_lock_inputs = [
            db::machine_interface::ExpectedInterfaceDiscoveryLockInputs {
                existing_segment_id: Some(existing_segment),
                fixed_allocations: vec![
                    (fixed_segment, fixed_address),
                    (fixed_segment, fixed_address),
                ],
            },
            db::machine_interface::ExpectedInterfaceDiscoveryLockInputs {
                existing_segment_id: Some(admin_segment),
                fixed_allocations: Vec::new(),
            },
        ];

        let lock_inputs = merge_managed_host_ingestion_lock_inputs(
            vec![admin_segment, admin_segment],
            vec![owner_segment],
            &interface_lock_inputs,
            vec![(legacy_segment, legacy_address)],
        );

        assert_eq!(lock_inputs.admin_segment_ids, vec![admin_segment]);
        assert_eq!(
            lock_inputs.segment_ids,
            vec![
                legacy_segment,
                fixed_segment,
                owner_segment,
                existing_segment,
                admin_segment,
            ]
        );
        assert_eq!(
            lock_inputs.fixed_allocations,
            vec![
                (legacy_segment, legacy_address),
                (fixed_segment, fixed_address),
            ]
        );
    }

    #[sqlx_test]
    #[allow(txn_held_across_await)] // Intentional: this test changes state while ingestion waits on a lock.
    async fn missing_bmc_owner_inserted_while_waiting_rejects_without_partial_ingestion(
        pool: PgPool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let test_harness = TestHarness::builder(pool.clone()).build().await;
        let domain = test_harness.test_domain().await;
        let underlay_segment = test_harness
            .network_controller()
            .create_underlay_segment(&domain)
            .await;
        let bmc_ip = underlay_segment.relay_address;
        let bmc_mac_address: MacAddress = "02:00:00:00:00:21".parse()?;

        let mut setup = Transaction::begin(&pool).await?;
        let expected_machine = db::expected_machine::create(
            setup.as_pgconn(),
            ExpectedMachine {
                id: None,
                bmc_mac_address,
                data: ExpectedMachineData::default(),
            },
        )
        .await?;
        setup.commit().await?;

        let creator = MachineCreator::new(
            pool.clone(),
            SiteExplorerConfig::default(),
            test_harness.api().common_pools().clone(),
            Arc::new(RackProfileConfig::default()),
            None,
            test_harness.api().credential_manager().clone(),
        );
        let explored_host = ExploredManagedHost {
            host_bmc_ip: bmc_ip,
            dpus: Vec::new(),
        };

        let mut segment_owner = Transaction::begin(&pool).await?;
        db::machine_interface::lock_network_segments_exclusive(
            segment_owner.as_pgconn(),
            std::slice::from_ref(&underlay_segment.id),
        )
        .await?;

        let create_pool = pool.clone();
        let creation = tokio::spawn(async move {
            let mut report = EndpointExplorationReport::default();
            creator
                .create_managed_host(
                    &explored_host,
                    &mut report,
                    Some(&expected_machine),
                    &create_pool,
                )
                .await
        });
        wait_for_advisory_lock_wait(&pool).await;

        let mut writer = Transaction::begin(&pool).await?;
        let interface_id = sqlx::query_scalar(
            "INSERT INTO machine_interfaces
                 (segment_id, mac_address, primary_interface, hostname)
             VALUES ($1, $2, false, 'concurrent-bmc-owner')
             RETURNING id",
        )
        .bind(underlay_segment.id)
        .bind(bmc_mac_address)
        .fetch_one(writer.as_pgconn())
        .await?;
        db::machine_interface_address::insert(
            writer.as_pgconn(),
            interface_id,
            bmc_ip,
            AllocationType::Static,
        )
        .await?;
        writer.commit().await?;

        segment_owner.commit().await?;
        let error = tokio::time::timeout(std::time::Duration::from_secs(5), creation)
            .await
            .expect("managed-host ingestion did not finish after releasing the segment lock")
            .expect("managed-host ingestion task panicked")
            .expect_err("new BMC owner must reject the stale ingestion attempt");
        assert!(matches!(
            error,
            SiteExplorerError::DatabaseError(db::DatabaseError::FailedPrecondition(_))
        ));

        let mut check = Transaction::begin(&pool).await?;
        let owner = db::bmc_metadata::find_interface_by_bmc_ip(check.as_pgconn(), bmc_ip)
            .await?
            .expect("concurrent BMC owner must remain");
        assert_eq!(owner.interface_id, interface_id);
        assert_eq!(owner.mac_address, bmc_mac_address);
        assert_eq!(owner.segment_id, underlay_segment.id);

        let owner_unchanged = sqlx::query_scalar::<_, bool>(
            "SELECT machine_id IS NULL
                 AND switch_id IS NULL
                 AND power_shelf_id IS NULL
                 AND attached_dpu_machine_id IS NULL
                 AND association_type = 'None'::association_type
                 AND expected_interface IS NULL
                 AND expected_interface_captured = false
             FROM machine_interfaces
             WHERE id = $1",
        )
        .bind(interface_id)
        .fetch_one(check.as_pgconn())
        .await?;
        assert!(owner_unchanged);

        let partial_rows = sqlx::query_as::<_, (i64, i64, i64)>(
            "SELECT
                (SELECT COUNT(*) FROM machines),
                (SELECT COUNT(*) FROM machine_topologies),
                (SELECT COUNT(*) FROM predicted_machine_interfaces)",
        )
        .fetch_one(check.as_pgconn())
        .await?;
        assert_eq!(partial_rows, (0, 0, 0));
        check.rollback().await?;

        Ok(())
    }
}
