// Read-only pass/fail probe of the live T330 iDRAC8. Validates the Dell-client
// legacy patches WITHOUT building/deploying NICo Core. Never calls machine_setup
// (which writes BIOS / reboots) -- only the read/diff paths the state machine
// gates on. One assertion per patch; exits non-zero if any check FAILs.
//
//   cargo build --example idrac8_probe
//   scp target/debug/examples/idrac8_probe jroehrich@192.168.0.2:/tmp/idrac8_probe
//   just mgmt '/tmp/idrac8_probe'
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

    let pool = RedfishClientPool::builder().build().expect("pool");
    let ep = Endpoint {
        host: HOST.to_string(),
        port: None,
        user: Some(USER.to_string()),
        password: Some(PASS.to_string()),
    };

    // 0 -- baseline: create_client + vendor detection (Oem fallback -> Dell).
    let client = match pool.create_client(ep).await {
        Ok(c) => {
            println!("[PASS] 0 create_client");
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
            format!("{:?}", sr.vendor()).contains("Dell"),
            format!("vendor={:?}", sr.vendor())
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

    // Patch 2 (legacy job create/delete) is write-mostly and is exercised by the
    // gated write-probe, not this read-only checklist.

    println!("\n{failed} check(s) failed");
    if failed > 0 {
        std::process::exit(1);
    }
}
