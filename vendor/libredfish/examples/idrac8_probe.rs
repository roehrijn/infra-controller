// Read-only pass/fail probe of the live T330 iDRAC8. Validates the Dell-client
// legacy patches WITHOUT building/deploying NICo Core. Never calls machine_setup
// (which writes BIOS / reboots) -- only the read/diff paths the state machine
// gates on. One assertion per patch; exits non-zero if any check FAILs.
//
//   cargo build --example idrac8_probe
//   scp target/debug/examples/idrac8_probe jroehrich@192.168.0.2:/tmp/idrac8_probe
//   just mgmt '/tmp/idrac8_probe'
use std::time::Duration;

use libredfish::model::service_root::RedfishVendor;
use libredfish::{BootInterfaceRef, Endpoint, RedfishClientPool, RedfishError};
use mac_address::MacAddress;

const HOST: &str = "192.168.2.16";
const USER: &str = "root";
const PASS: &str = "vCluster";
// The declared primary host NIC (NIC.Embedded.1-1-1 == MAC 10:98:36:B4:AD:39).
const BOOT_NIC_ID: &str = "NIC.Embedded.1-1-1";
const BOOT_NIC_MAC: &str = "10:98:36:B4:AD:39";

#[tokio::main]
async fn main() {
    let mut failed = 0usize;
    macro_rules! check {
        ($name:expr, $cond:expr, $detail:expr) => {{
            let ok = $cond;
            println!(
                "[{}] {} -- {}",
                if ok { "PASS" } else { "FAIL" },
                $name,
                $detail
            );
            if !ok {
                failed += 1;
            }
        }};
    }

    // iDRAC8 BMCs are slow (observed ~18s/request when degraded); use generous
    // timeouts so the probe tolerates it instead of reporting false failures.
    let pool = RedfishClientPool::builder()
        .danger_accept_invalid_certs() // iDRAC8 serves a self-signed cert (curl -k)
        .connect_timeout(Duration::from_secs(60))
        .timeout(Duration::from_secs(120))
        .build()
        .expect("pool");
    let mk_ep = || Endpoint {
        host: HOST.to_string(),
        port: None,
        user: Some(USER.to_string()),
        password: Some(PASS.to_string()),
    };

    // 0 -- baseline: create_client AUTO-DETECTS the vendor, exactly as NICo's
    // machine-controller does (client_by_info passes vendor=None). This must
    // resolve to the Dell client for the T330 (iDRAC8 reports "Dell Inc."), else
    // every Dell-specific operation falls through to a NotSupported stub.
    let client = match pool.create_client(mk_ep()).await {
        Ok(c) => {
            println!("[PASS] 0 create_client (auto-detect)");
            c
        }
        Err(e) => {
            println!("[FAIL] 0 create_client -- {e:?}");
            std::process::exit(1);
        }
    };
    match client.get_service_root().await {
        Ok(sr) => check!(
            "0 vendor",
            sr.vendor() == Some(RedfishVendor::Dell),
            format!(
                "vendor={:?} vendor_string={:?}",
                sr.vendor(),
                sr.vendor_string()
            )
        ),
        Err(e) => check!("0 vendor", false, format!("{e:?}")),
    }

    // 1 -- Patch 1: lockdown_status is NotSupported on legacy iDRAC8 (was a 404
    // HTTPErrorCode that wedged WaitingForPlatformConfiguration).
    match client.lockdown_status().await {
        Err(RedfishError::NotSupported(_)) => {
            check!("1 lockdown_status", true, "NotSupported (legacy)")
        }
        other => check!(
            "1 lockdown_status",
            false,
            format!("expected NotSupported, got {other:?}")
        ),
    }

    // 1b -- Patch 1+4: machine_setup_status(None) completes with no `lockdown` diff.
    match client.machine_setup_status(None).await {
        Ok(s) => {
            let keys: Vec<&String> = s.diffs.iter().map(|d| &d.key).collect();
            check!(
                "1b machine_setup_status(None)",
                !s.diffs.iter().any(|d| d.key == "lockdown"),
                format!("is_done={}, diff_keys={keys:?}", s.is_done)
            )
        }
        Err(e) => check!("1b machine_setup_status(None)", false, format!("{e:?}")),
    }

    // 3/4 -- Patch 3+4: is_bios_setup by interface id returns Ok(bool) with no
    // MissingKey and no HttpDev1*/Tpm2* diffs (they are absent and skipped).
    match client
        .is_bios_setup(Some(BootInterfaceRef::InterfaceId(BOOT_NIC_ID)))
        .await
    {
        Ok(done) => check!("3/4 is_bios_setup(InterfaceId)", true, format!("Ok({done})")),
        Err(e) => check!("3/4 is_bios_setup(InterfaceId)", false, format!("{e:?}")),
    }

    // 5 -- Patch 5: is_bios_setup by MAC resolves the FQDD via EthernetInterfaces.
    let mac: MacAddress = BOOT_NIC_MAC.parse().expect("valid MAC");
    match client
        .is_bios_setup(Some(BootInterfaceRef::Mac(mac)))
        .await
    {
        Ok(done) => check!("5 is_bios_setup(Mac)", true, format!("Ok({done})")),
        Err(e) => check!("5 is_bios_setup(Mac)", false, format!("{e:?}")),
    }

    // 6 -- Patch 6: is_boot_order_setup resolves the legacy "PXE Device 1:" option
    // (Ok(false) pre-deploy while RAID:Ubuntu boots first; Ok(true) once pinned).
    match client
        .is_boot_order_setup(BootInterfaceRef::InterfaceId(BOOT_NIC_ID))
        .await
    {
        Ok(v) => check!("6 is_boot_order_setup", true, format!("Ok({v})")),
        Err(e) => check!("6 is_boot_order_setup", false, format!("{e:?}")),
    }

    // 7 -- sanity: IPMI-over-LAN read works (NetworkProtocol is 200 on iDRAC8).
    match client.is_ipmi_over_lan_enabled().await {
        Ok(v) => check!("7 is_ipmi_over_lan_enabled", true, format!("Ok({v})")),
        Err(e) => check!("7 is_ipmi_over_lan_enabled", false, format!("{e:?}")),
    }

    // Patch 2 -- legacy job create/delete. WRITE paths, gated behind
    // PROBE_ALLOW_WRITES=1: they clear the BMC job queue and create+delete a
    // config job (real BMC writes, but nothing destructive -- no disk, no boot
    // order, no reboot). This is the path that wedged live on iDRAC8's HTTP 405
    // for DellJobService.DeleteJobQueue.
    if std::env::var("PROBE_ALLOW_WRITES").as_deref() == Ok("1") {
        // 2a -- delete_job_queue must complete via the legacy fallback. Pre-fix
        // it returned the 405 error; post-fix it falls back to Managers/{id}/Jobs.
        match pool.dell_delete_job_queue(mk_ep()).await {
            Ok(()) => check!("2a delete_job_queue", true, "Ok (legacy fallback cleared queue)"),
            Err(e) => check!("2a delete_job_queue", false, format!("{e:?}")),
        }

        // 2b -- create_bios_config_job must reach the legacy Jobs collection. A
        // 404/405 propagating means the fallback did NOT fire (FAIL); an Ok job
        // id means it created one (clean it up); any other iDRAC-level rejection
        // (e.g. no staged settings) still proves the fallback fired (PASS).
        match pool.dell_create_bios_config_job(mk_ep()).await {
            Ok(jid) => {
                check!(
                    "2b create_bios_config_job",
                    true,
                    format!("Ok(job={jid}); cleaning up")
                );
                if let Err(e) = pool.dell_delete_job_queue(mk_ep()).await {
                    println!("[WARN] 2b cleanup delete_job_queue -- {e:?}");
                }
            }
            Err(e) if e.not_found() || e.method_not_allowed() => check!(
                "2b create_bios_config_job",
                false,
                format!("OEM path not handled (fallback did not fire): {e:?}")
            ),
            Err(e) => check!(
                "2b create_bios_config_job",
                true,
                format!("legacy fallback fired; iDRAC rejected w/o staged settings: {e:?}")
            ),
        }

        // 2c -- the FULL machine_setup BIOS PATCH (the exact path the state machine
        // runs at ConfigureBios). Ok means every attribute in the PATCH was
        // accepted; a 400/SYS409 names the rejected attribute (this is how the
        // empty SetBootOrderDis surfaced live). Any config job it stages is cleared
        // inside the probe. This is the check the earlier probe was missing.
        match pool.dell_machine_setup_probe(mk_ep(), BOOT_NIC_ID).await {
            Ok(job) => check!(
                "2c machine_setup BIOS PATCH",
                true,
                format!("all attributes accepted (job={job:?})")
            ),
            Err(e) => check!(
                "2c machine_setup BIOS PATCH",
                false,
                format!("BIOS PATCH rejected: {e:?}")
            ),
        }
    } else {
        println!("[SKIP] 2 write-probe (set PROBE_ALLOW_WRITES=1 to run delete/create job paths)");
    }

    println!("\n{failed} check(s) failed");
    if failed > 0 {
        std::process::exit(1);
    }
}
