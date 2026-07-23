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
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub use ::rpc::forge as rpc;
use carbide_utils::none_if_empty::NoneIfEmpty;
use carbide_uuid::machine::MachineIdSource;
use carbide_uuid::nvlink::NvLinkDomainId;
use db::WithTransaction;
use futures_util::FutureExt;
use model::hardware_info::{GpuPlatformInfo, HardwareInfo, MachineNvLinkInfo, NvLinkGpu};
use model::machine::machine_id::{from_hardware_info, host_id_from_dpu_hardware_info};
use model::machine::machine_search_config::MachineSearchConfig;
use model::machine::{DpuInitState, DpuInitStates, ManagedHostState};
use tonic::{Request, Response, Status};

use crate::api::{Api, log_machine_id, log_request_data};
use crate::handlers::utils::convert_and_log_machine_id;
use crate::{CarbideError, attestation as attest};

#[derive(Clone, Copy)]
enum CallerInterfaceLookup {
    Insecure {
        interface_id: carbide_uuid::machine::MachineInterfaceId,
    },
    InterfaceAddress {
        remote_ip: std::net::IpAddr,
    },
    InstanceAddress {
        remote_ip: std::net::IpAddr,
        interface_id: carbide_uuid::machine::MachineInterfaceId,
    },
}

fn initial_caller_interface_lookup_error(
    error: db::machine_interface::DiscoveryInterfaceLookupError,
) -> CarbideError {
    match error {
        db::machine_interface::DiscoveryInterfaceLookupError::Database(error) => error.into(),
        error @ (db::machine_interface::DiscoveryInterfaceLookupError::AmbiguousInterfaceAddress {
            ..
        }
        | db::machine_interface::DiscoveryInterfaceLookupError::AmbiguousInstanceAddress {
            ..
        }) => CarbideError::internal(error.to_string()),
    }
}

fn caller_interface_changed() -> CarbideError {
    CarbideError::FailedPrecondition(
        "machine interface identity changed while discovery was waiting for allocator locks; retry discovery"
            .to_string(),
    )
}

fn locked_caller_interface_lookup_error(
    error: db::machine_interface::DiscoveryInterfaceLookupError,
) -> CarbideError {
    match error {
        db::machine_interface::DiscoveryInterfaceLookupError::Database(error) => error.into(),
        error @ (db::machine_interface::DiscoveryInterfaceLookupError::AmbiguousInterfaceAddress {
            ..
        }
        | db::machine_interface::DiscoveryInterfaceLookupError::AmbiguousInstanceAddress {
            ..
        }) => {
            tracing::warn!(
                %error,
                "machine interface lookup changed while discovery was waiting for allocator locks"
            );
            caller_interface_changed()
        }
    }
}

pub(crate) async fn discover_machine(
    api: &Api,
    request: Request<rpc::MachineDiscoveryInfo>,
) -> Result<Response<rpc::MachineDiscoveryResult>, Status> {
    // We don't log_request_data(&request); here because the hardware info is huge

    // This is set in api/src/listener.rs::listen_and_serve when we `accept` the connection
    // The IP is usually an IPv4-mapped IPv6 addresses (e.g. `::ffff:10.217.133.10`) so
    // we use to_canonical() to convert it to IPv4.
    let remote_ip = request
        .extensions()
        .get::<Arc<carbide_authn::middleware::ConnectionAttributes>>()
        .map(|conn_attrs| conn_attrs.peer_address.ip().to_canonical());

    let machine_discovery_info = request.into_inner();
    let discovery_reporter = machine_discovery_info.discovery_reporter();

    let discovery_data = machine_discovery_info
        .discovery_data
        .map(|data| match data {
            rpc::machine_discovery_info::DiscoveryData::Info(info) => info,
        })
        .ok_or_else(|| {
            CarbideError::InvalidArgument("discovery data is not populated".to_string())
        })?;
    let attest_key_info_opt = discovery_data.attest_key_info.clone();
    let hardware_info = HardwareInfo::try_from(discovery_data).map_err(CarbideError::from)?;

    // this is an early check for certificate creation that happens later on in this method.
    // let's save us the hassle and return immediately if the below condition is not satisfied
    if api.runtime_config.attestation_enabled
        && !hardware_info.is_dpu()
        && attest_key_info_opt.is_none()
    {
        return Err(
            CarbideError::InvalidArgument("AttestKeyInfo is not populated".to_string()).into(),
        );
    }

    // Generate a stable Machine ID based on the hardware information
    let stable_machine_id = from_hardware_info(&hardware_info).map_err(|e| {
            CarbideError::InvalidArgument(
                format!("insufficient HardwareInfo to derive a stable machine ID from DiscoverMachine call by {remote_ip:?}: {e}"),
            )
        })?;
    log_machine_id(&stable_machine_id);

    // Build NVLink info from scout GPU platform info. domain_uuid is backfilled by the
    // NVLink partition monitor from NMX-C hello.
    let gpu_platform_infos: Vec<&GpuPlatformInfo> = hardware_info
        .gpus
        .iter()
        .filter_map(|gpu| gpu.platform_info.as_ref())
        .collect();

    let nvlink_info = if hardware_info.is_mnnvl_capable()
        && !gpu_platform_infos.is_empty()
        && api
            .runtime_config
            .nvlink_config
            .as_ref()
            .is_some_and(|config| config.enabled)
    {
        nvlink_info_from_gpu_platform_infos(&gpu_platform_infos)
    } else {
        None
    };

    let mut txn = api.txn_begin().await?;

    // Resolve the caller without locking interface rows. ExpectedMachine and
    // allocator locks must come first, so this lookup is repeated under row
    // locks before discovery can modify an interface.
    let (caller_interface_lookup, caller_interface_preview) = if api
        .runtime_config
        .allow_insecure_discovery
    {
        let interface_id = machine_discovery_info.machine_interface_id.ok_or_else(|| {
            CarbideError::InvalidArgument(
                "machine_interface_id is required for insecure discovery".to_string(),
            )
        })?;
        (
            CallerInterfaceLookup::Insecure { interface_id },
            db::machine_interface::find_one(&mut txn, interface_id).await?,
        )
    } else {
        let remote_ip = remote_ip.ok_or_else(|| {
            CarbideError::InvalidArgument(
                "could not determine client IP address for discovery".to_string(),
            )
        })?;
        if let Some(interface) =
            db::machine_interface::find_optional_unique_by_ip(&mut txn, remote_ip)
                .await
                .map_err(initial_caller_interface_lookup_error)?
        {
            (
                CallerInterfaceLookup::InterfaceAddress { remote_ip },
                interface,
            )
        } else {
            let interface_id =
                    machine_discovery_info.machine_interface_id.ok_or_else(|| {
                        CarbideError::InvalidArgument(
                            "no machine_interface found for client IP address, and no machine_interface_id was provided".to_string(),
                        )
                    })?;
            let interface = db::machine_interface::find_if_matches_instance_ip(
                    &mut txn,
                    interface_id,
                    remote_ip,
                )
                .await
                .map_err(initial_caller_interface_lookup_error)?
                .ok_or_else(|| {
                    tracing::error!(
                        %interface_id,
                        %remote_ip,
                        "potential machine impersonation attempt: caller provided machine_interface_id does not belong to this remote IP"
                    );
                    CarbideError::PermissionDeniedError(
                        "selected interface and discovery source IP do not belong to the same host"
                            .to_string(),
                    )
                })?;
            (
                CallerInterfaceLookup::InstanceAddress {
                    remote_ip,
                    interface_id,
                },
                interface,
            )
        }
    };

    let proactive_host_mac = hardware_info
        .is_dpu()
        .then(|| {
            hardware_info.factory_mac_address().map_err(|error| {
                CarbideError::InvalidArgument(format!(
                    "hardware info missing host factory MAC address: {error}"
                ))
            })
        })
        .transpose()?;
    let mut expected_interface_lookup_macs = vec![caller_interface_preview.mac_address];
    expected_interface_lookup_macs.extend(proactive_host_mac);
    expected_interface_lookup_macs.sort_by_key(ToString::to_string);
    expected_interface_lookup_macs.dedup();

    db::expected_machine::lock_config_mutations_shared(&mut txn).await?;
    let mut expected_machine_lookups = Vec::with_capacity(expected_interface_lookup_macs.len());
    for mac_address in &expected_interface_lookup_macs {
        expected_machine_lookups.push((
            *mac_address,
            db::expected_machine::find_by_host_mac_address(&mut txn, *mac_address).await?,
        ));
    }
    // The capture helper also consults pending predictions when current
    // configuration has no declaration. Lock both lookup MACs up front so
    // that fallback never takes a MAC lock after allocator segment locks.
    db::machine_interface::lock_expected_machine_interface_macs(
        &mut txn,
        expected_interface_lookup_macs.clone(),
    )
    .await?;

    for (mac_address, expected_machine) in &mut expected_machine_lookups {
        if expected_machine.is_none() {
            // All nested-interface writers hold this MAC before changing
            // host_nics. The plain SELECT is stable without reversing the
            // ExpectedMachine-row-then-interface-MAC lock order.
            *expected_machine =
                db::expected_machine::find_by_host_mac_address_after_interface_lock(
                    &mut txn,
                    *mac_address,
                )
                .await?;
        }
    }

    let mut expected_interfaces_by_mac = HashMap::new();
    let mut expected_machine_owner = None;
    for (mac_address, expected_machine) in expected_machine_lookups {
        let Some(expected_machine) = expected_machine else {
            continue;
        };
        if let Some((owner_mac, owner_lookup_mac)) = expected_machine_owner
            && owner_mac != expected_machine.bmc_mac_address
        {
            return Err(CarbideError::InvalidArgument(format!(
                "discovery interfaces {owner_lookup_mac} and {mac_address} belong to different ExpectedMachines {owner_mac} and {}",
                expected_machine.bmc_mac_address,
            ))
            .into());
        }
        expected_machine_owner = Some((expected_machine.bmc_mac_address, mac_address));
        let expected_interface = expected_machine
            .data
            .expected_interface_for_mac(mac_address)
            .ok_or_else(|| {
                CarbideError::internal(format!(
                    "ExpectedMachine {} no longer declares interface {mac_address}",
                    expected_machine.bmc_mac_address,
                ))
            })?;
        expected_interfaces_by_mac.insert(mac_address, expected_interface);
    }

    // Lock every segment and fixed address this discovery may touch before
    // locking an interface row. The later capture and reconcile helpers can
    // reacquire these transaction-scoped locks without changing their order.
    let admin_segment_ids = if hardware_info.is_dpu() {
        db::network_segment::list_segment_ids(
            &mut txn,
            Some(model::network_segment::NetworkSegmentType::Admin),
        )
        .await?
    } else {
        Vec::new()
    };
    let mut segment_ids = admin_segment_ids.clone();
    let mut fixed_allocations = Vec::new();
    for mac_address in &expected_interface_lookup_macs {
        let lock_inputs = db::machine_interface::expected_interface_discovery_lock_inputs(
            &mut txn,
            *mac_address,
            expected_interfaces_by_mac.get(mac_address),
        )
        .await?;
        if let Some(existing_segment_id) = lock_inputs.existing_segment_id {
            segment_ids.push(existing_segment_id);
        }
        segment_ids.extend(
            lock_inputs
                .fixed_allocations
                .iter()
                .map(|(segment_id, _)| *segment_id),
        );
        fixed_allocations.extend(lock_inputs.fixed_allocations);
    }
    db::machine_interface::lock_network_segments_exclusive(&mut txn, &segment_ids).await?;
    db::machine_interface::lock_static_address_keys_after_segment_locks(
        &mut txn,
        &fixed_allocations,
    )
    .await?;

    tracing::debug!(
        remote_ip_address = ?remote_ip,
        caller_machine_interface_id = ?machine_discovery_info.machine_interface_id,
        "discover_machine loading interface"
    );

    let caller_interface = match caller_interface_lookup {
        CallerInterfaceLookup::Insecure { interface_id } => {
            let interface =
                db::machine_interface::find_optional_one_for_update(&mut txn, interface_id)
                    .await?
                    .ok_or_else(caller_interface_changed)?;
            tracing::warn!(
                machine_interface_id = %interface_id,
                "Allowing insecure discovery: trusting caller-provided machine_interface_id. This is for integration tests only and must not be done in production."
            );
            interface
        }
        CallerInterfaceLookup::InterfaceAddress { remote_ip } => {
            db::machine_interface::find_optional_for_update_by_ip(&mut txn, remote_ip)
                .await
                .map_err(locked_caller_interface_lookup_error)?
                .ok_or_else(caller_interface_changed)?
        }
        CallerInterfaceLookup::InstanceAddress {
            remote_ip,
            interface_id,
        } => {
            let direct_interface =
                db::machine_interface::find_optional_unique_by_ip(&mut txn, remote_ip)
                    .await
                    .map_err(locked_caller_interface_lookup_error)?;
            if direct_interface.is_some() {
                return Err(caller_interface_changed().into());
            }
            db::machine_interface::find_for_update_if_matches_instance_ip(
                &mut txn,
                interface_id,
                remote_ip,
            )
            .await
            .map_err(locked_caller_interface_lookup_error)?
            .ok_or_else(caller_interface_changed)?
        }
    };
    if caller_interface.id != caller_interface_preview.id
        || caller_interface.mac_address != caller_interface_preview.mac_address
    {
        return Err(caller_interface_changed().into());
    }

    let site_explorer_creates_machines = api
        .runtime_config
        .site_explorer
        .create_machines
        .load(Ordering::Relaxed);

    // Now that we know who's submitting this DiscoveryInfo, make sure they're not submitting info
    // for another host in an attempt to get their machine certificate.
    let authorized = match caller_interface.machine_id.as_ref() {
        Some(caller_machine_id) if caller_machine_id == &stable_machine_id => {
            // The stable machine ID generated from the discovery info matches the caller's machine
            // ID, good.
            true
        }
        Some(caller_machine_id)
            if caller_machine_id.machine_type().is_predicted_host()
                && stable_machine_id.machine_type().is_host() =>
        {
            // This is a predicted host being promoted to a known host, which we allow.
            true
        }
        None if hardware_info.is_dpu() && machine_discovery_info.create_machine => {
            // There's no machine ID at all yet, and this is a dpu attempting to create a machine
            // entry from DiscoveryInfo. We only allow this if site explorer isn't configured to
            // create machines.
            !site_explorer_creates_machines
        }
        _ => false,
    };
    if !authorized {
        return Err(CarbideError::PermissionDeniedError(
            "machine discovery is not authorized for the selected interface".to_string(),
        )
        .into());
    }

    if !hardware_info.is_dpu()
        && hardware_info.tpm_ek_certificate.is_none()
        && api.runtime_config.tpm_required
    {
        return Err(CarbideError::InvalidArgument(format!(
            "ignoring DiscoverMachine request for non-tpm enabled host with InterfaceId {:?}",
            caller_interface.id,
        ))
        .into());
    } else if !hardware_info.is_dpu() && hardware_info.tpm_ek_certificate.is_some() {
        // this means we do have an EK cert for a host

        // get the EK cert from incoming message
        let tpm_ek_cert =
            hardware_info
                .tpm_ek_certificate
                .as_ref()
                .ok_or(CarbideError::InvalidArgument(
                    "tpm_ek_cert is empty".to_string(),
                ))?;

        attest::match_insert_new_ek_cert_status_against_ca(
            &mut txn,
            tpm_ek_cert,
            &stable_machine_id,
        )
        .await?;
    }

    if !hardware_info.is_dpu()
        && hardware_info.tpm_ek_certificate.is_none()
        && stable_machine_id.source() == MachineIdSource::ProductBoardChassisSerial
        && let Some(existing_machine_id) = caller_interface.machine_id
        && existing_machine_id.source() == MachineIdSource::Tpm
        && existing_machine_id.machine_type().is_host()
    {
        return Err(CarbideError::FailedPrecondition(format!(
            "TPM EK certificate missing for host discovery on InterfaceId {:?}; refusing to derive serial-based machine id {} for existing TPM-derived machine id {}",
            caller_interface.id, stable_machine_id, existing_machine_id,
        ))
        .into());
    }

    let machine_id = if hardware_info.is_dpu() {
        // if site explorer is creating machine records and there isn't one for this machine return an error
        if site_explorer_creates_machines {
            db::machine::find_one(
                &mut txn,
                &stable_machine_id,
                MachineSearchConfig {
                    include_dpus: true,
                    ..MachineSearchConfig::default()
                },
            )
            .await?
            .ok_or_else(|| {
                CarbideError::InvalidArgument(format!(
                    "machine id {stable_machine_id} was not discovered by site-explorer"
                ))
            })?;
        }

        let db_machine = if machine_discovery_info.create_machine {
            db::machine_interface::capture_expected_interface_before_association(
                &mut txn,
                caller_interface.id,
                expected_interfaces_by_mac.get(&caller_interface.mac_address),
            )
            .await?;
            let machine = db::machine::get_or_create(
                &mut txn,
                Some(&api.common_pools),
                &stable_machine_id,
                &caller_interface,
            )
            .await?;

            // Update the record only when create_machine is enabled.
            // Site-explorer will update if machine is created by site-explorer.
            db::machine_interface::associate_interface_with_dpu_machine(
                &caller_interface.id,
                &stable_machine_id,
                &mut txn,
            )
            .await?;
            machine
        } else {
            db::machine::find_one(
                &mut txn,
                &stable_machine_id,
                MachineSearchConfig {
                    include_dpus: true,
                    ..MachineSearchConfig::default()
                },
            )
            .await?
            .ok_or_else(|| {
                CarbideError::InvalidArgument(format!("machine id {stable_machine_id} not found"))
            })?
        };

        if db_machine.network_config.loopback_ip.is_none() {
            let loopback_ip = db::machine::allocate_loopback_ip(
                &api.common_pools,
                &mut txn,
                &stable_machine_id.to_string(),
            )
            .await?;

            let mut network_config = db_machine.network_config.value.clone();
            network_config.loopback_ip = Some(loopback_ip);
            db::machine::try_update_network_config(
                &mut txn,
                &stable_machine_id,
                db_machine.network_config.version,
                &network_config,
            )
            .await?;
        }

        if api
            .runtime_config
            .vmaas_config
            .as_ref()
            .map(|vc| vc.secondary_overlay_support)
            .unwrap_or_default()
            && db_machine
                .network_config
                .secondary_overlay_vtep_ip
                .is_none()
        {
            let secondary_vtep_ip = db::machine::allocate_secondary_vtep_ip(
                &api.common_pools,
                &mut txn,
                &stable_machine_id.to_string(),
            )
            .await?;

            let mut network_config = db_machine.network_config.value.clone();
            network_config.secondary_overlay_vtep_ip = Some(secondary_vtep_ip);
            db::machine::try_update_network_config(
                &mut txn,
                &stable_machine_id,
                db_machine.network_config.version,
                &network_config,
            )
            .await?;
        }

        db_machine.id
    } else {
        // Now we know stable machine id for host. Let's update it in db.
        db::machine::try_sync_stable_id_with_current_machine_id_for_host(
            &mut txn,
            &caller_interface.machine_id,
            &stable_machine_id,
        )
        .await?
    };

    db::machine_topology::create_or_update_with_bom_validation(
        &mut txn,
        &stable_machine_id,
        &hardware_info,
        api.runtime_config.bom_validation.enabled,
    )
    .await?;

    if hardware_info.is_dpu() {
        // Create Host proactively.
        // In case host interface is created, this method will return existing one, instead
        // creating new everytime.
        let proactive_expected_interface =
            proactive_host_mac.and_then(|mac_address| expected_interfaces_by_mac.get(&mac_address));
        let machine_interface =
            db::machine_interface::find_or_create_host_machine_dpu_interface_proactively(
                &mut txn,
                Some(&hardware_info),
                &machine_id,
                proactive_expected_interface,
                api.runtime_config.retained_boot_interface_window,
            )
            .await?;
        db::machine_interface::capture_expected_interface_before_association(
            &mut txn,
            machine_interface.id,
            expected_interfaces_by_mac.get(&machine_interface.mac_address),
        )
        .await?;
        db::machine_interface::associate_interface_with_dpu_machine(
            &machine_interface.id,
            &machine_id,
            &mut txn,
        )
        .await?;
        let machine_interface =
            db::machine_interface::find_one(&mut txn, machine_interface.id).await?;

        let host_machine_id = if let Some(host_machine_id) = machine_interface.machine_id {
            host_machine_id
        } else {
            // Create host machine with temporary ID if no machine is attached.
            let predicted_machine_id =
                host_id_from_dpu_hardware_info(&hardware_info).map_err(|e| {
                    CarbideError::InvalidArgument(format!("hardware info missing: {e}"))
                })?;

            let host_has_primary = db::machine_interface::find_by_machine_ids(
                &mut txn,
                std::slice::from_ref(&predicted_machine_id),
            )
            .await?
            .get(&predicted_machine_id)
            .is_some_and(|interfaces| {
                interfaces
                    .iter()
                    .any(|interface| interface.primary_interface)
            });
            if host_has_primary && machine_interface.primary_interface {
                db::machine_interface::set_primary_interface(
                    &machine_interface.id,
                    false,
                    &mut txn,
                )
                .await?;
            }

            let mi_id = machine_interface.id;
            let proactive_machine = db::machine::get_or_create(
                &mut txn,
                Some(&api.common_pools),
                &predicted_machine_id,
                &machine_interface,
            )
            .await?;

            // Update host and DPUs state correctly.
            db::machine::update_state(
                &mut txn,
                &proactive_machine.id,
                &ManagedHostState::DPUInit {
                    dpu_states: DpuInitStates {
                        states: HashMap::from([(machine_id, DpuInitState::Init)]),
                    },
                },
            )
            .await?;

            tracing::info!(
                machine_interface_id = ?mi_id,
                machine_id = %proactive_machine.id,
                "Created host machine proactively",
            );

            proactive_machine.id
        };

        // Normalize admin address ownership any time DPU discovery creates
        // or reattaches a DPU-backed host interface.
        let active_config_changed =
            db::machine_interface::reconcile_admin_addresses_for_host_with_locked_admin_segments(
                &mut txn,
                &host_machine_id,
                &admin_segment_ids,
            )
            .await?;
        if active_config_changed {
            let (network_config, network_config_version) =
                db::machine::get_network_config(&mut txn, &host_machine_id)
                    .await?
                    .take();
            db::machine::try_update_network_config(
                &mut txn,
                &host_machine_id,
                network_config_version,
                &network_config,
            )
            .await?;
        }
    }

    // if attestation is enabled and it is not a DPU, then we create a random nonce (auth token)
    // and create a decrypting challenge (make credential) out of it.
    // Whoever was able to decrypt it (activate credential), possesses
    // the TPM that the endorsement key (EK) and the attestation key (AK) that they came from.
    // if attestation is not enabled, or it is a DPU, then issue machine certificates immediately
    let attest_key_challenge = if api.runtime_config.attestation_enabled && !hardware_info.is_dpu()
    {
        let Some(attest_key_info) = attest_key_info_opt else {
            return Err(CarbideError::InvalidArgument(
                "internal error: this should have been handled above! AttestKeyInfo is not populated".into(),
            )
            .into());
        };

        tracing::info!(
            "It is not a DPU and attestation is enabled. Generating Attest Key Bind Challenge ..."
        );
        Some(
            crate::handlers::measured_boot::create_attest_key_bind_challenge(
                &mut txn,
                &attest_key_info,
                &stable_machine_id,
            )
            .await?,
        )
    } else {
        tracing::info!(
            attestation_enabled = api.runtime_config.attestation_enabled,
            is_dpu = hardware_info.is_dpu(),
            %stable_machine_id,
            "Vending attestation certificates",
        );

        None
    };

    if let Some(nvlink_info) = nvlink_info {
        db::machine::update_nvlink_info(&mut txn, &machine_id, nvlink_info).await?;
    }

    if discovery_reporter == rpc::MachineDiscoveryReporter::Scout
        && let Some(scout_version) = machine_discovery_info
            .discovery_reporter_version
            .as_deref()
            .none_if_empty()
    {
        db::machine::update_last_scout_observed_version(
            &stable_machine_id,
            scout_version,
            &mut txn,
        )
        .await?;
    }

    txn.commit().await?;

    let machine_certificate = if attest_key_challenge.is_none() {
        if std::env::var("UNSUPPORTED_CERTIFICATE_PROVIDER").is_ok() {
            Some(rpc::MachineCertificate::default())
        } else {
            Some(
                api.certificate_provider
                    .get_certificate(&stable_machine_id.to_string(), None, None)
                    .await
                    .map_err(|err| CarbideError::ClientCertificateError(err.to_string()))?
                    .into(),
            )
        }
    } else {
        None
    };

    let response = Ok(Response::new(rpc::MachineDiscoveryResult {
        machine_id: Some(stable_machine_id),
        machine_certificate,
        attest_key_challenge,
        machine_interface_id: Some(caller_interface.id),
    }));

    if hardware_info.is_dpu()
        && let Some(dpu_info) = hardware_info.dpu_info.as_ref()
    {
        // WARNING: DONOT REUSE OLD TXN HERE. IT WILL CREATE DEADLOCK.
        //
        // Create a new transaction here for network devices. Inner transaction is not so
        // helpful in postgres and using same transaction creates deadlock with
        // machine_interface table.

        // Create DPU and LLDP Association.
        api.with_txn(|txn| {
            db::network_devices::dpu_to_network_device_map::create_dpu_network_device_association(
                txn,
                &dpu_info.switches,
                &stable_machine_id,
            )
            .boxed()
        })
        .await??;
    }

    response
}

// Host has completed discovery
pub(crate) async fn discovery_completed(
    api: &Api,
    request: Request<rpc::MachineDiscoveryCompletedRequest>,
) -> Result<Response<rpc::MachineDiscoveryCompletedResponse>, Status> {
    log_request_data(&request);

    let req = request.into_inner();
    let machine_id = convert_and_log_machine_id(req.machine_id.as_ref())?;

    let (machine, mut txn) = api
        .load_machine(&machine_id, MachineSearchConfig::default())
        .await?;
    db::machine::update_discovery_time(&machine.id, &mut txn).await?;

    let discovery_result = "Success".to_owned();

    txn.commit().await?;

    tracing::info!(
        %machine_id,
        discovery_result, "discovery_completed",
    );
    Ok(Response::new(rpc::MachineDiscoveryCompletedResponse {}))
}

/// Builds NVLink discovery info from scout `GpuPlatformInfo` for every GPU that reported it.
fn nvlink_info_from_gpu_platform_infos(
    platform_infos: &[&GpuPlatformInfo],
) -> Option<MachineNvLinkInfo> {
    let chassis_serial = platform_infos
        .first()
        .map(|p| p.chassis_serial.clone())
        .unwrap_or_default();
    if chassis_serial.trim().is_empty() {
        return None;
    }

    let gpus = platform_infos
        .iter()
        .map(|platform_info| {
            let guid = {
                let s = platform_info.fabric_guid.trim();
                if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                    u64::from_str_radix(hex, 16).unwrap_or(0)
                } else {
                    s.parse::<u64>().unwrap_or(0)
                }
            };
            NvLinkGpu {
                tray_index: platform_info.tray_index as i32,
                slot_id: platform_info.slot_number as i32,
                device_id: platform_info.module_id as i32,
                guid,
            }
        })
        .collect();

    Some(MachineNvLinkInfo {
        domain_uuid: NvLinkDomainId::nil(),
        chassis_serial,
        gpus,
    })
}
