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

use std::str::FromStr;

use carbide_redfish::libredfish::test_support::RedfishSim;
use carbide_redfish::libredfish::{RedfishAuth, RedfishClientPool};
use carbide_secrets::credentials::{CredentialKey, CredentialType};
use libredfish::Redfish;

use super::{DpuMachineStateHandler, MachineId, ReachabilityParams, StateHandlerError};

fn dpu_machine_id() -> MachineId {
    MachineId::from_str("fm100dtes3rn1npvbtm5qd57dkilaag7ljugl1llmm7rfuq1ov50i0rpl30").unwrap()
}

fn handler(
    enable_secure_boot: bool,
    secure_boot_reporting_optional: bool,
) -> DpuMachineStateHandler {
    DpuMachineStateHandler::new(
        false,
        Default::default(),
        ReachabilityParams {
            dpu_wait_time: chrono::Duration::zero(),
            power_down_wait: chrono::Duration::zero(),
            failure_retry_time: chrono::Duration::zero(),
            scout_reporting_timeout: chrono::Duration::zero(),
            uefi_boot_wait: chrono::Duration::zero(),
        },
        enable_secure_boot,
        secure_boot_reporting_optional,
        None,
    )
}

async fn secure_boot_client(sim: &RedfishSim) -> Box<dyn Redfish> {
    sim.create_client(
        "dpu-bmc",
        None,
        RedfishAuth::Key(CredentialKey::HostRedfish {
            credential_type: CredentialType::SiteDefault,
        }),
        None,
    )
    .await
    .unwrap()
}

/// A BMC that answers `SecureBoot` with no state at all reads as "disabled"
/// once the operator has declared its reporting optional, so the DPU can leave
/// `DISABLESECUREBOOT` instead of burning the reboot budget forever.
#[tokio::test]
async fn unreported_secure_boot_reads_as_disabled_when_reporting_is_optional() {
    let sim = RedfishSim::default();
    sim.set_secure_boot_unreported();
    let client = secure_boot_client(&sim).await;

    let disabled = handler(false, true)
        .is_secure_boot_disabled(&dpu_machine_id(), client.as_ref())
        .await
        .expect("an unreported secure boot state must not error");

    assert!(disabled);
}

/// Without the flag the state stays unknown, so the caller keeps its
/// reboot-and-retry work-around for the post-POST race.
#[tokio::test]
async fn unreported_secure_boot_is_missing_data_by_default() {
    let sim = RedfishSim::default();
    sim.set_secure_boot_unreported();
    let client = secure_boot_client(&sim).await;

    let error = handler(false, false)
        .is_secure_boot_disabled(&dpu_machine_id(), client.as_ref())
        .await
        .expect_err("an unreported secure boot state must stay an error by default");

    assert!(
        matches!(error, StateHandlerError::MissingData { .. }),
        "expected MissingData, got {error:?}"
    );
}

/// The flag must not weaken the enable path: enabling secure boot cannot be
/// verified on a BMC that reports nothing, so that stays an error.
#[tokio::test]
async fn unreported_secure_boot_still_errors_on_the_enable_path() {
    let sim = RedfishSim::default();
    sim.set_secure_boot_unreported();
    let client = secure_boot_client(&sim).await;

    let error = handler(true, true)
        .is_secure_boot_disabled(&dpu_machine_id(), client.as_ref())
        .await
        .expect_err("the enable path must not accept an unreported secure boot state");

    assert!(
        matches!(error, StateHandlerError::MissingData { .. }),
        "expected MissingData, got {error:?}"
    );
}

/// A BMC that does report its state is unaffected by the flag.
#[tokio::test]
async fn reported_secure_boot_state_is_unchanged_by_the_flag() {
    let sim = RedfishSim::default();
    let client = secure_boot_client(&sim).await;

    let disabled = handler(false, true)
        .is_secure_boot_disabled(&dpu_machine_id(), client.as_ref())
        .await
        .expect("a reported secure boot state must be read");

    assert!(disabled, "the simulator defaults to secure boot disabled");
}
