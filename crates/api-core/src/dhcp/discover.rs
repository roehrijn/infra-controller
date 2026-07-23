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
use std::net::{IpAddr, Ipv4Addr};

use ::rpc::forge as rpc;
use carbide_network::ip::{IdentifyAddressFamily, IpAddressFamily};
use db::dhcp_entry::DhcpEntry;
use db::{self, expected_machine, machine_interface};
use mac_address::MacAddress;
use model::allocation_type::AllocationType;
use model::dpa_interface::DpaInterface;
use model::expected_machine::{ExpectedHostNic, ExpectedInterfaceRole};
use model::machine::MachineInterfaceSnapshot;
use model::machine_interface::InterfaceType;
use model::network_segment::{
    AllocationStrategy, NetworkSegment, NetworkSegmentSearchConfig, NetworkSegmentType,
};
use sqlx::PgConnection;
use tonic::{Request, Response};

use crate::CarbideError;
use crate::api::Api;
use crate::dhcp::v6;

// MTU for both the underlay and overlay networks on
// the E/W Fabric
const SPX_MTU: i32 = 9000;

/// Given a desired IP address, compute the relay address by toggling the LSB.
fn get_relay_from_desired(desired: Ipv4Addr) -> Ipv4Addr {
    let ip_u32 = u32::from(desired);
    let relay_u32 = ip_u32 ^ 1;
    Ipv4Addr::from(relay_u32)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DhcpMessageKind {
    V4Discover,
    V6Solicit,
    V6Request,
    V6InfoRequest,
}

/// Validate DHCP protocol fields and return the internal routing values.
fn parse_discovery_protocol(
    address_family: Option<i32>,
    message_kind: Option<i32>,
    inferred_family: IpAddressFamily,
    duid: Option<&[u8]>,
) -> Result<(IpAddressFamily, Option<DhcpMessageKind>), CarbideError> {
    let (address_family, message_kind) = match (address_family, message_kind) {
        // Legacy callers omit both fields and are IPv4-only.
        (None, None) => {
            if inferred_family == IpAddressFamily::Ipv6 {
                return Err(CarbideError::InvalidArgument(
                    "address_family and message_kind are required for DHCPv6".to_string(),
                ));
            }
            if duid.is_some() {
                return Err(CarbideError::InvalidArgument(
                    "duid is invalid for DHCPv4 requests".to_string(),
                ));
            }
            return Ok((inferred_family, None));
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(CarbideError::InvalidArgument(
                "address_family and message_kind must be provided together".to_string(),
            ));
        }
        (Some(address_family), Some(message_kind)) => (address_family, message_kind),
    };

    let address_family = rpc::AddressFamily::try_from(address_family).map_err(|_| {
        CarbideError::InvalidArgument("unknown address_family or message_kind".to_string())
    })?;
    let message_kind = rpc::MessageKind::try_from(message_kind).map_err(|_| {
        CarbideError::InvalidArgument("unknown address_family or message_kind".to_string())
    })?;

    // Explicit unspecified values are invalid; callers should omit both fields for legacy v4.
    if address_family == rpc::AddressFamily::Unspecified
        || message_kind == rpc::MessageKind::Unspecified
    {
        return Err(CarbideError::InvalidArgument(
            "address_family and message_kind must be specified".to_string(),
        ));
    }

    let declared_family = match address_family {
        rpc::AddressFamily::V4 => IpAddressFamily::Ipv4,
        rpc::AddressFamily::V6 => IpAddressFamily::Ipv6,
        _ => {
            return Err(CarbideError::InvalidArgument(
                "unknown address_family or message_kind".to_string(),
            ));
        }
    };

    if declared_family != inferred_family {
        return Err(CarbideError::InvalidArgument(
            "address_family must match relay/link-address family".to_string(),
        ));
    }

    // Route only compatible family/kind pairs.
    match (address_family, message_kind) {
        (rpc::AddressFamily::V4, rpc::MessageKind::V4Discover) => {
            if duid.is_some() {
                return Err(CarbideError::InvalidArgument(
                    "duid is invalid for DHCPv4 requests".to_string(),
                ));
            }
            Ok((IpAddressFamily::Ipv4, Some(DhcpMessageKind::V4Discover)))
        }
        (rpc::AddressFamily::V4, _) => Err(CarbideError::InvalidArgument(
            "ADDRESS_FAMILY_V4 requires MESSAGE_KIND_V4_DISCOVER".to_string(),
        )),
        (rpc::AddressFamily::V6, rpc::MessageKind::V6Solicit) => {
            require_dhcpv6_duid(duid)?;
            Ok((IpAddressFamily::Ipv6, Some(DhcpMessageKind::V6Solicit)))
        }
        (rpc::AddressFamily::V6, rpc::MessageKind::V6Request) => {
            require_dhcpv6_duid(duid)?;
            Ok((IpAddressFamily::Ipv6, Some(DhcpMessageKind::V6Request)))
        }
        (rpc::AddressFamily::V6, rpc::MessageKind::V6InfoRequest) => {
            require_dhcpv6_duid(duid)?;
            Ok((IpAddressFamily::Ipv6, Some(DhcpMessageKind::V6InfoRequest)))
        }
        (rpc::AddressFamily::V6, _) => Err(CarbideError::InvalidArgument(
            "ADDRESS_FAMILY_V6 requires a DHCPv6 message_kind".to_string(),
        )),
        _ => Err(CarbideError::InvalidArgument(
            "unknown address_family or message_kind".to_string(),
        )),
    }
}

/// Ensure a DHCPv6 request carries a non-empty DUID.
fn require_dhcpv6_duid(duid: Option<&[u8]>) -> Result<(), CarbideError> {
    if duid.is_some_and(|duid| !duid.is_empty()) {
        Ok(())
    } else {
        Err(CarbideError::MissingArgument("duid"))
    }
}

/// Ensure the selected segment is enabled for DHCPv6.
fn ensure_dhcpv6_enabled(segment: &NetworkSegment) -> Result<(), CarbideError> {
    // A segment must carry at least one IPv6 prefix to serve DHCPv6 options.
    if segment
        .prefixes
        .iter()
        .any(|prefix| prefix.prefix.is_ipv6())
    {
        Ok(())
    } else {
        Err(CarbideError::FailedPrecondition(format!(
            "DHCPv6 request received for network segment {} without an IPv6 prefix",
            segment.id
        )))
    }
}

/// Build an options-only DHCPv6 response for a segment without observing an interface.
async fn options_only_dhcpv6_record_from_segment(
    txn: &mut PgConnection,
    mac_address: MacAddress,
    segment: &NetworkSegment,
    ntp_servers: &[Ipv4Addr],
) -> Result<rpc::DhcpRecord, CarbideError> {
    // Preserve the cache invalidation marker returned by normal DHCP records.
    let last_invalidation_time = db::dhcp_record::last_invalidation_time(&mut *txn).await?;

    Ok(rpc::DhcpRecord {
        machine_id: None,
        machine_interface_id: None,
        segment_id: Some(segment.id),
        subdomain_id: segment.config.subdomain_id,
        fqdn: String::new(),
        mac_address: mac_address.to_string(),
        address: String::new(),
        mtu: segment.config.mtu,
        prefix: String::new(),
        gateway: None,
        booturl: None,
        last_invalidation_time: Some(last_invalidation_time.into()),
        ntp_servers: ntp_servers.iter().map(ToString::to_string).collect(),
    })
}

/// Build an options-only DHCPv6 response from interface metadata.
async fn options_only_dhcpv6_record_from_interface(
    txn: &mut PgConnection,
    machine_interface: &MachineInterfaceSnapshot,
    segment: &NetworkSegment,
    ntp_servers: &[Ipv4Addr],
) -> Result<rpc::DhcpRecord, CarbideError> {
    // Resolve FQDN metadata from the interface domain when one is attached.
    let fqdn_domain_id = machine_interface.domain_id.or(segment.config.subdomain_id);
    let fqdn = if let Some(domain_id) = fqdn_domain_id {
        let domain = db::dns::domain::find_by_uuid(&mut *txn, domain_id)
            .await?
            .ok_or_else(|| CarbideError::NotFoundError {
                kind: "domain",
                id: domain_id.to_string(),
            })?;
        format!("{}.{}", machine_interface.hostname, domain.name)
    } else {
        String::new()
    };

    // Preserve the cache invalidation marker returned by normal DHCP records.
    let last_invalidation_time = db::dhcp_record::last_invalidation_time(&mut *txn).await?;

    Ok(rpc::DhcpRecord {
        machine_id: machine_interface.machine_id,
        machine_interface_id: Some(machine_interface.id),
        segment_id: Some(segment.id),
        subdomain_id: segment.config.subdomain_id,
        fqdn,
        mac_address: machine_interface.mac_address.to_string(),
        address: String::new(),
        mtu: segment.config.mtu,
        prefix: String::new(),
        gateway: None,
        booturl: None,
        last_invalidation_time: Some(last_invalidation_time.into()),
        ntp_servers: ntp_servers.iter().map(ToString::to_string).collect(),
    })
}

/// Ensure a stateful DHCP allocation exists for the requested family.
///
/// DHCPv6 stateful allocations are authoritative over prior SLAAC observations:
/// both are IPv6 rows on the same interface, so the existing unique family index
/// requires replacing an observed SLAAC row before allocating the DHCP lease.
async fn ensure_dhcp_address_for_family(
    txn: &mut PgConnection,
    machine_interface: &MachineInterfaceSnapshot,
    segment: &NetworkSegment,
    parsed_mac: MacAddress,
    address_family: IpAddressFamily,
) -> Result<(), CarbideError> {
    let existing_allocation_type = db::machine_interface_address::find_allocation_type_for_family(
        &mut *txn,
        machine_interface.id,
        address_family,
    )
    .await?;

    match existing_allocation_type {
        None => {}
        Some(AllocationType::Slaac) if address_family == IpAddressFamily::Ipv6 => {
            // Take the segment lock before dropping the SLAAC row so the
            // delete-then-allocate pair holds locks in the allocator order
            // (segment advisory lock first, then address rows).
            db::machine_interface::lock_network_segments_exclusive(
                &mut *txn,
                std::slice::from_ref(&segment.id),
            )
            .await?;
            db::machine_interface_address::delete_by_interface_family(
                &mut *txn,
                machine_interface.id,
                address_family,
                AllocationType::Slaac,
            )
            .await?;
        }
        Some(_) => return Ok(()),
    }

    tracing::info!(
        machine_interface_id = %machine_interface.id,
        client_mac_address = %parsed_mac,
        ?address_family,
        "Interface missing DHCP address for family, allocating from segment"
    );

    // If the segment only allows static reservations, don't dynamically
    // allocate. The device has no reservation.
    if segment.config.allocation_strategy == AllocationStrategy::Reserved {
        return Err(CarbideError::internal(format!(
            "segment {} configured for static DHCP leases only; no static reservation for MAC {parsed_mac}",
            segment.config.name,
        )));
    }

    db::machine_interface::allocate_address_for_family(
        txn,
        machine_interface.id,
        segment,
        address_family,
    )
    .await?;

    Ok(())
}

// Overlay IP address request from DPA. DPA tells us
// what IP address it wants (calculated algorithmically
// from the underlay IP address). So we just allocate
// that desired address and update the DB.
async fn handle_overlay_from_dpa(
    txn: &mut PgConnection,
    dpa_if: &mut DpaInterface,
    macaddr: MacAddress,
    desired_addr: IpAddr,
    ntp_servers: &[Ipv4Addr],
) -> Result<Option<Response<rpc::DhcpRecord>>, CarbideError> {
    let IpAddr::V4(ip_v4_addr) = desired_addr else {
        return Err(CarbideError::internal(
            "IPv6 not supported for DPA overlay".to_string(),
        ));
    };

    let relay_addr = get_relay_from_desired(ip_v4_addr);

    let prefix = format!("{relay_addr}/31");

    dpa_if.overlay_ip = Some(desired_addr);

    db::dpa_interface::update_ip(dpa_if.clone(), false, txn).await?;

    Ok(Some(Response::new(rpc::DhcpRecord {
        machine_id: Some(dpa_if.get_machine_id()),
        machine_interface_id: None,
        segment_id: None,
        subdomain_id: None,
        address: desired_addr.to_string(),
        mac_address: macaddr.to_string(),
        booturl: None,
        last_invalidation_time: None,
        gateway: Some(relay_addr.to_string()),
        mtu: SPX_MTU,
        fqdn: String::new(),
        prefix,
        ntp_servers: ntp_servers.iter().map(ToString::to_string).collect(),
    })))
}

// DPA is asking for an underlay IP address. The underlay IP
// address is just the relay address with the LSB toggled.
async fn handle_underlay_from_dpa(
    txn: &mut PgConnection,
    dpa_if: &mut DpaInterface,
    macaddr: MacAddress,
    relay_address: String,
    ntp_servers: &[Ipv4Addr],
) -> Result<Option<Response<rpc::DhcpRecord>>, CarbideError> {
    // The relay address and the mac address should differ only in bit 0
    let relay_addr = relay_address.parse::<Ipv4Addr>()?;

    let ip_u32 = u32::from(relay_addr);

    let retaddr = ip_u32 ^ 1;

    let ret_addr = Ipv4Addr::from(retaddr);

    let prefix = format!("{relay_addr}/31");

    dpa_if.underlay_ip = Some(IpAddr::from(ret_addr));

    db::dpa_interface::update_ip(dpa_if.clone(), true, txn).await?;

    Ok(Some(Response::new(rpc::DhcpRecord {
        machine_id: Some(dpa_if.get_machine_id()),
        machine_interface_id: None,
        segment_id: None,
        subdomain_id: None,
        address: ret_addr.to_string(),
        mac_address: macaddr.to_string(),
        booturl: None,
        last_invalidation_time: None,
        gateway: Some(relay_address),
        mtu: SPX_MTU,
        fqdn: String::new(),
        prefix,
        ntp_servers: ntp_servers.iter().map(ToString::to_string).collect(),
    })))
}

// See if this is a underlay/overlay IP allocation request
// from a DPA. If the specified macaddr belongs to any DPA
// object, we know it's a request from a DPA. And the presence
// of desired ip (option 50) means it's overlay request, and
// the absence of option 50 means it's an underlay request.
async fn handle_dhcp_from_dpa(
    api: &Api,
    txn: &mut PgConnection,
    macaddr: MacAddress,
    relay_address: String,
    desired_address: Option<IpAddr>,
) -> Result<Option<Response<rpc::DhcpRecord>>, CarbideError> {
    if !api.runtime_config.is_dpa_enabled() {
        return Ok(None);
    }

    let mut dpa_ifs = db::dpa_interface::find_by_mac_addr(&mut *txn, &macaddr).await?;

    if dpa_ifs.len() != 1 {
        // If the MAC address does not belong to any DPA object, len will be 0.
        // Log cases where len is neither 0 nor 1.
        if !dpa_ifs.is_empty() {
            tracing::error!(
                mac_address = %macaddr,
                dpa_interface_count = dpa_ifs.len(),
                "Unexpected number of DPA interfaces found",
            );
        }
        return Ok(None);
    }

    let mut dpa_if = dpa_ifs.remove(0);

    if let Some(addr) = desired_address {
        return handle_overlay_from_dpa(
            txn,
            &mut dpa_if,
            macaddr,
            addr,
            &api.runtime_config.ntp_servers,
        )
        .await;
    }

    handle_underlay_from_dpa(
        txn,
        &mut dpa_if,
        macaddr,
        relay_address,
        &api.runtime_config.ntp_servers,
    )
    .await
}

pub async fn discover_dhcp(
    api: &Api,
    request: Request<rpc::DhcpDiscovery>,
) -> Result<Response<rpc::DhcpRecord>, CarbideError> {
    let mut txn = api.txn_begin().await?;

    let rpc::DhcpDiscovery {
        mac_address,
        relay_address,
        link_address,
        vendor_string,
        desired_address,
        address_family,
        message_kind,
        duid,
        ..
    } = request.into_inner();

    // Select the segment lookup key once. DHCPv6 uses Relay-Forward link-address
    // when present, so all segment lookups and predicted promotion use this value.
    let address_to_use_for_dhcp = link_address.as_ref().unwrap_or(&relay_address);
    let parsed_relay: IpAddr = address_to_use_for_dhcp.parse()?;
    let (address_family, message_kind) = parse_discovery_protocol(
        address_family,
        message_kind,
        parsed_relay.address_family(),
        duid.as_deref(),
    )?;
    let is_v6_observation = address_family == IpAddressFamily::Ipv6
        && message_kind == Some(DhcpMessageKind::V6InfoRequest);
    let parsed_mac: MacAddress = mac_address.parse()?;

    let mut retain_new_expected_address = false;
    let mut expected_machine = expected_machine::find_by_host_mac_address(&mut txn, parsed_mac)
        .await
        .map_err(CarbideError::from)?;
    let initial_lookup_was_empty = expected_machine.is_none();
    db::machine_interface::lock_expected_machine_interface_macs(
        &mut txn,
        std::iter::once(parsed_mac),
    )
    .await?;
    if initial_lookup_was_empty {
        // ExpectedMachine creation cannot change this nested declaration while
        // the MAC is locked. Keep the second pass non-locking so an update
        // holding the ExpectedMachine row cannot deadlock on the MAC.
        expected_machine =
            expected_machine::find_by_host_mac_address_after_interface_lock(&mut txn, parsed_mac)
                .await
                .map_err(CarbideError::from)?;
    }
    let predicted_interface =
        db::predicted_machine_interface::find_by_mac_address(&mut txn, parsed_mac).await?;
    let prediction_needs_refresh = predicted_interface
        .as_ref()
        .is_some_and(|interface| !interface.expected_interface_captured);
    let desired_address_ip: Option<IpAddr> = if is_v6_observation {
        None
    } else {
        desired_address.map(|addr| addr.parse()).transpose()?
    };
    let mut existing_machine_id =
        db::machine::find_existing_machine(&mut txn, parsed_mac, parsed_relay).await?;
    let should_check_legacy_reservations =
        existing_machine_id.is_none() && predicted_interface.is_none();
    if should_check_legacy_reservations
        && address_family == IpAddressFamily::Ipv4
        && let Some(response) =
            handle_dhcp_from_dpa(api, &mut txn, parsed_mac, relay_address, desired_address_ip)
                .await?
    {
        txn.commit().await?;
        return Ok(response);
    }

    // Legacy top-level BMC and ExpectedSwitch reservations are still valid
    // when no nested interface declaration applies. Their writers take this
    // same MAC lock before making the lookup visible, so these post-lock reads
    // are stable through the transaction without taking a config-row lock in
    // the opposite order.
    let legacy_expected_machine = if expected_machine.is_none() && should_check_legacy_reservations
    {
        expected_machine::find_by_bmc_mac_address(&mut txn, parsed_mac)
            .await
            .map_err(CarbideError::from)?
    } else {
        None
    };
    let legacy_bmc_ip = legacy_expected_machine
        .as_ref()
        .and_then(|machine| machine.data.bmc_ip_address)
        .filter(|address| address.is_address_family(address_family));
    let legacy_expected_switch = if expected_machine.is_none()
        && should_check_legacy_reservations
        && legacy_bmc_ip.is_none()
    {
        db::expected_switch::find_by_nvos_mac_address(&mut txn, parsed_mac)
            .await
            .map_err(CarbideError::from)?
    } else {
        None
    };
    let legacy_nvos_ip = legacy_expected_switch
        .as_ref()
        .and_then(|switch| switch.nvos_ip_address)
        .filter(|address| address.is_address_family(address_family));
    let mut legacy_fixed_allocations = Vec::new();
    for address in legacy_bmc_ip.into_iter().chain(legacy_nvos_ip) {
        let segment = db::network_segment::for_static_address(&mut txn, address, None).await?;
        legacy_fixed_allocations.push((segment.id, address));
    }
    let current_expected_interface = expected_machine
        .as_ref()
        .and_then(|machine| machine.data.expected_interface_for_mac(parsed_mac));
    let expected_interface_lock_inputs =
        db::machine_interface::expected_interface_discovery_lock_inputs(
            &mut txn,
            parsed_mac,
            current_expected_interface.as_ref(),
        )
        .await?;

    // Segment locks must precede any machine-interface row lock. Admin
    // reconciliation takes an exclusive segment lock before locking those
    // rows, while allocation later takes per-address locks. Lock every relay
    // candidate so the policy filter cannot select an unlocked fallback, and
    // include fixed and computed SLAAC targets before declaration capture. A
    // fixed reservation's address-selected segment may differ from the relay.
    let relay_candidate_segments =
        db::network_segment::for_relay_all(&mut txn, std::slice::from_ref(&parsed_relay)).await?;
    let mut candidate_segment_ids = relay_candidate_segments
        .iter()
        .map(|segment| segment.id)
        .collect::<Vec<_>>();
    let mut address_lock_targets = expected_interface_lock_inputs.fixed_allocations;
    address_lock_targets.extend(legacy_fixed_allocations);
    if is_v6_observation {
        address_lock_targets.extend(relay_candidate_segments.iter().filter_map(|segment| {
            segment
                .slaac_eligible()
                .and_then(|prefix| v6::slaac_gua_from_eui64(prefix, &parsed_mac))
                .map(|address| (segment.id, IpAddr::V6(address)))
        }));
    }
    if let Some(existing_segment_id) = expected_interface_lock_inputs.existing_segment_id {
        candidate_segment_ids.push(existing_segment_id);
    }
    candidate_segment_ids.extend(
        address_lock_targets
            .iter()
            .map(|(segment_id, _)| *segment_id),
    );
    if address_family == IpAddressFamily::Ipv6 && !is_v6_observation {
        // Stateful IPv6 allocation reads the segment's used-address set.
        db::machine_interface::lock_network_segments_exclusive(&mut txn, &candidate_segment_ids)
            .await?;
    } else {
        // IPv4 and DHCPv6 INFORMATION-REQUEST use per-address locking.
        db::machine_interface::lock_network_segments_shared(&mut txn, &candidate_segment_ids)
            .await?;
    }
    db::machine_interface::lock_static_address_keys_after_segment_locks(
        &mut txn,
        &address_lock_targets,
    )
    .await?;

    let host_nic = db::machine_interface::capture_expected_interface_for_discovery(
        &mut txn,
        parsed_mac,
        current_expected_interface.as_ref(),
    )
    .await?;
    let predicted_interface = if prediction_needs_refresh {
        db::predicted_machine_interface::find_by_mac_address(&mut txn, parsed_mac).await?
    } else {
        predicted_interface
    };
    let is_primary_nic = host_nic
        .as_ref()
        .filter(|interface| interface.role == ExpectedInterfaceRole::Host)
        .map(ExpectedHostNic::initial_primary_interface);

    let mut predicted_interface = predicted_interface;

    if let Some(expected_interface) = host_nic.as_ref() {
        retain_new_expected_address = !is_v6_observation
            && expected_interface
                .resolved_ip_allocation()
                .retains_dynamic_ip();

        if expected_interface.fixed_ip.is_some() {
            // A fixed declaration rebuilds its reservation during initial
            // discovery or re-ingestion, even when first contact uses the
            // other address family. Recheck attachment under the DB row lock
            // so a concurrent ingestion cannot turn this into live-state
            // reconciliation.
            if predicted_interface.is_some() {
                db::machine_interface::preallocate_captured_expected_machine_interface(
                    &mut txn,
                    expected_interface,
                    api.runtime_config.retained_boot_interface_window,
                )
                .await?;
            } else {
                db::machine_interface::preallocate_expected_machine_interface_if_never_associated(
                    &mut txn,
                    expected_interface,
                    api.runtime_config.retained_boot_interface_window,
                )
                .await?;
            }
        }
    } else if should_check_legacy_reservations {
        if let Some(bmc_ip) = legacy_bmc_ip {
            db::machine_interface::preallocate_bmc_machine_interface(
                &mut txn,
                parsed_mac,
                bmc_ip,
                api.runtime_config.retained_boot_interface_window,
            )
            .await?;
        } else if let Some(nvos_ip) = legacy_nvos_ip {
            db::machine_interface::preallocate_machine_interface(
                &mut txn,
                parsed_mac,
                nvos_ip,
                api.runtime_config.retained_boot_interface_window,
            )
            .await?;
        }
    }

    if !is_v6_observation && let Some(expected_interface) = predicted_interface.take() {
        // Remember the expected machine id for the later rack update.
        machine_interface::move_predicted_machine_interface_to_machine(
            &mut txn,
            &expected_interface,
            parsed_relay,
            api.runtime_config.retained_boot_interface_window,
        )
        .await?;
        existing_machine_id = Some(expected_interface.machine_id);
    }

    if is_v6_observation {
        let network_segments = db::machine_interface::network_segments_for_dhcp_relays(
            &mut txn,
            std::slice::from_ref(&parsed_relay),
            host_nic.as_ref(),
        )
        .await?;
        let exact_link_address_match = |segment: &NetworkSegment| {
            segment
                .prefixes
                .iter()
                .any(|prefix| prefix.dhcpv6_link_address == Some(parsed_relay))
        };
        let reserved_segment = |segment: &NetworkSegment| {
            segment.config.allocation_strategy == AllocationStrategy::Reserved
        };

        // Exact DHCPv6 link-address matches are authoritative. Only prefer
        // reserved segments within that exact-match subset; prefix candidates
        // are fallback routing context.
        let segment = network_segments
            .iter()
            .filter(|&segment| exact_link_address_match(segment))
            .find(|&segment| reserved_segment(segment))
            .or_else(|| {
                network_segments
                    .iter()
                    .find(|&segment| exact_link_address_match(segment))
            })
            .or_else(|| {
                // Prefix-overlap routing is intentionally not resolved here. If
                // no exact DHCPv6 link-address match exists, prefer a reserved
                // candidate so anonymous INFO_REQUESTs can receive options-only
                // metadata instead of creating an observed row on an ambiguous
                // dynamic prefix.
                network_segments
                    .iter()
                    .find(|&segment| reserved_segment(segment))
            })
            .or_else(|| network_segments.first())
            .ok_or_else(|| {
                CarbideError::internal(format!(
                    "no network segment defined for DHCPv6 relay address {parsed_relay}"
                ))
            })?;
        ensure_dhcpv6_enabled(segment)?;

        let interfaces = db::machine_interface::find_by_mac_address(&mut txn, parsed_mac).await?;
        let has_cross_segment_interface = interfaces
            .iter()
            .any(|interface| interface.segment_id != segment.id);
        if has_cross_segment_interface {
            // Do not turn a wrong-segment known MAC into config-only success.
            // Fall through so the existing global MAC guard rejects or handles
            // static-assignment moves exactly as IPv4/stateful DHCP does.
            tracing::debug!(
                client_mac_address = %parsed_mac,
                network_segment_id = %segment.id,
                "DHCPv6 options request will use global MAC segment reconciliation"
            );
        } else if segment.config.allocation_strategy == AllocationStrategy::Reserved
            && interfaces.is_empty()
            && predicted_interface.is_none()
            && host_nic.is_none()
        {
            // Anonymous reserved requests can receive segment options without
            // creating observed rows. Known or predicted interfaces must
            // continue through common safety checks and DHCP bookkeeping.
            let record = options_only_dhcpv6_record_from_segment(
                &mut txn,
                parsed_mac,
                segment,
                &api.runtime_config.ntp_servers,
            )
            .await?;
            txn.commit().await?;
            return Ok(Response::new(record));
        }
    }

    if is_v6_observation && predicted_interface.is_some() {
        let interfaces = db::machine_interface::find_by_mac_address(&mut txn, parsed_mac).await?;
        if interfaces.is_empty()
            && let Some(predicted_interface) = predicted_interface.take()
        {
            // Reserved segments reject anonymous observed-row creation, but a
            // prediction is explicit host identity. Promote it before the observed
            // helper so the common safety checks and DHCP bookkeeping still run.
            machine_interface::move_predicted_machine_interface_to_machine(
                &mut txn,
                &predicted_interface,
                parsed_relay,
                api.runtime_config.retained_boot_interface_window,
            )
            .await?;
        }
    }

    let mut machine_interface = if is_v6_observation {
        // INFORMATION-REQUEST observes identity only; it must not consume a DHCP lease.
        db::machine_interface::find_or_create_observed_machine_interface(
            &mut txn,
            existing_machine_id,
            parsed_mac,
            std::slice::from_ref(&parsed_relay),
            host_nic.clone(),
            is_primary_nic,
            api.runtime_config.retained_boot_interface_window,
        )
        .await?
    } else {
        // First-contact stateful DHCP needs candidate-segment fallback, but only
        // for the requested address family.
        db::machine_interface::find_or_create_machine_interface_for_family(
            &mut txn,
            existing_machine_id,
            parsed_mac,
            std::slice::from_ref(&parsed_relay),
            machine_interface::FindOrCreateMachineInterfaceOptions {
                host_nic: host_nic.clone(),
                is_primary: is_primary_nic,
                retained_window: api.runtime_config.retained_boot_interface_window,
            },
            address_family,
        )
        .await?
    };
    db::machine_interface::persist_expected_interface_if_missing(
        &mut txn,
        machine_interface.id,
        host_nic.as_ref(),
    )
    .await?;

    if let Some(predicted_interface) = predicted_interface {
        machine_interface::move_predicted_machine_interface_to_machine(
            &mut txn,
            &predicted_interface,
            parsed_relay,
            api.runtime_config.retained_boot_interface_window,
        )
        .await?;
        machine_interface = db::machine_interface::find_one(&mut txn, machine_interface.id).await?;
    }

    // Use the interface's actual segment, not only relay context, so
    // dormant admin interfaces cannot keep serving stale DHCP leases.
    let segment = db::network_segment::find_by(
        &mut txn,
        db::ObjectColumnFilter::One(db::network_segment::IdColumn, &machine_interface.segment_id),
        NetworkSegmentSearchConfig::default(),
    )
    .await?
    .pop()
    .ok_or_else(|| CarbideError::NotFoundError {
        kind: "network_segment",
        id: machine_interface.segment_id.to_string(),
    })?;

    if address_family == IpAddressFamily::Ipv6 {
        ensure_dhcpv6_enabled(&segment)?;
    }

    // Only DPU-backed host admin links are dormant when non-primary. Other non-primary admin
    // interfaces can be valid operator-declared host NICs and must still be allowed to DHCP.
    let is_dpu_backed_host_admin_interface = machine_interface.attached_dpu_machine_id.is_some()
        && machine_interface.attached_dpu_machine_id != machine_interface.machine_id;
    if is_dpu_backed_host_admin_interface
        && !machine_interface.primary_interface
        && segment.config.segment_type == NetworkSegmentType::Admin
    {
        return Err(CarbideError::FailedPrecondition(format!(
            "DHCP request received on dormant non-primary admin interface {}. ignoring",
            machine_interface.id
        )));
    }

    if is_v6_observation {
        v6::observe_slaac_address(&mut txn, machine_interface.id, &segment, &parsed_mac).await?;
    } else {
        ensure_dhcp_address_for_family(
            &mut txn,
            &machine_interface,
            &segment,
            parsed_mac,
            address_family,
        )
        .await?;
        if retain_new_expected_address
            && db::machine_interface_address::retain_expected_machine_dhcp_address_for_family(
                &mut txn,
                machine_interface.id,
                address_family,
            )
            .await?
        {
            tracing::info!(
                machine_interface_id = %machine_interface.id,
                client_mac_address = %parsed_mac,
                ?address_family,
                "Retained newly allocated expected-interface DHCP address"
            );
        }
    }

    if machine_interface.interface_type != InterfaceType::Bmc
        && let Some(machine_id) = machine_interface.machine_id
        && machine_id.machine_type().is_host()
        && let Some(instance_id) =
            db::instance::find_id_by_machine_id(&mut txn, &machine_id).await?
    {
        // An instance is associated with this host. If the host has DPUs,
        // the DPUs proxy DHCP on its behalf, so we reject the host's direct
        // DHCP request. Zero-DPU hosts have no such intermediary, so let
        // their DHCP proceed.
        let dpus = db::machine::find_dpus_by_host_machine_id(&mut txn, &machine_id).await?;
        if !dpus.is_empty() {
            return Err(CarbideError::internal(format!(
                "DHCP request received for instance: {instance_id}. ignoring"
            )));
        }
    }

    // Save vendor string, this is allowed to fail due to dhcp happening more than once on the same machine/vendor string
    if let Some(vendor) = vendor_string {
        let res = db::dhcp_entry::persist(
            DhcpEntry {
                machine_interface_id: machine_interface.id,
                vendor_string: vendor,
            },
            &mut txn,
        )
        .await;
        match res {
            Ok(()) => {} // do nothing on ok result
            Err(error) => {
                tracing::error!(%error, "Could not persist dhcp entry")
            } // This should not fail the discover call, dhcp happens many times
        }
    }

    db::machine_interface::update_last_dhcp(&mut txn, machine_interface.id, None).await?;

    let options_only_record = if is_v6_observation {
        machine_interface = db::machine_interface::find_one(&mut txn, machine_interface.id).await?;
        Some(
            options_only_dhcpv6_record_from_interface(
                &mut txn,
                &machine_interface,
                &segment,
                &api.runtime_config.ntp_servers,
            )
            .await?,
        )
    } else {
        None
    };

    txn.commit().await?;

    if let Some(record) = options_only_record {
        return Ok(Response::new(record));
    }

    let mut txn = api.txn_begin().await?;

    let record = db::dhcp_record::find_by_mac_address(
        &mut txn,
        &parsed_mac,
        &machine_interface.segment_id,
        address_family,
    )
    .await?
    .ok_or_else(|| CarbideError::NotFoundError {
        kind: "DHCP record",
        id: format!(
            "{parsed_mac} (segment {}, {:?})",
            machine_interface.segment_id, address_family
        ),
    })?;
    let mut record: rpc::DhcpRecord = record.into();

    txn.commit().await?;

    record.ntp_servers = api
        .runtime_config
        .ntp_servers
        .iter()
        .map(ToString::to_string)
        .collect();

    Ok(Response::new(record))
}
