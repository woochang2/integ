// narwhal/primary/src/cef_client.rs

use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::mesh::{
    mesh_client::MeshClient, Ack, CommitteeCandidateInfo, CommitData, FinalizedCommittee,
};
use tokio::sync::watch;
use tokio::time::{sleep, Duration};

// Kept for signature compatibility with primary.rs; we don't use it.
use types::Round;

use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Where to write CSV logs.
const CSV_PATH: &str = "/tmp/cef_sig.csv";

/// Small config passed in from Primary::spawn
#[derive(Clone)]
pub struct CefConfig {
    pub server_addr: String, // e.g. "http://127.0.0.1:50051"
    pub channel: String,     // fabric channel name
}

async fn connect(
    cfg: &CefConfig,
) -> Result<MeshClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = Endpoint::from_shared(cfg.server_addr.clone())?;
    let channel = endpoint.connect().await?;
    Ok(MeshClient::new(channel))
}

/// Run the CEF client loop independently of Narwhal rounds:
/// - r=0: RequestCommittee
/// - r=1..=10: RequestAggregatedCommit
/// Each round is separated by CEF_ROUND_PERIOD_SECS (default 1s).
pub async fn run_cef_loop(
    cfg: CefConfig,
    node_id: String,
    public_key_bytes: Vec<u8>,
    // kept only so primary.rs doesn't change; unused here
    _rx_narwhal_round_updates: watch::Receiver<Round>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Ensure CSV dir exists once.
    ensure_csv_parent_dir();

    let mut client = connect(&cfg).await?;
    let period_secs: u64 = std::env::var("CEF_ROUND_PERIOD_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let r0: u64 = 0;
    let seed0 = format!("round-{r0}-node-{node_id}");
    let join_req = CommitteeCandidateInfo {
        round: r0,
        node_id: node_id.clone(),
        seed: seed0.clone(),
        proof: vrf_proof_stub(&seed0, &public_key_bytes),       // demo 64 bytes
        public_key: public_key_bytes.clone(),
        commit: schnorr_commit_stub(&seed0, &public_key_bytes), // demo 32 bytes
        ip_address: "141.223.121.119".into(),
        port: "50052".into(),
        channel: cfg.channel.clone(),
    };

    // Start the server stream (we just log what we receive)
    let mut stream = client.join_network(Request::new(join_req)).await?.into_inner();

    for r in 0u64..=10 {
        let seed = format!("round-{r}-node-{node_id}");
        let commit = schnorr_commit_stub(&seed, &public_key_bytes);

        if r == 0 {
            // Round 0: RequestCommittee
            let req = CommitteeCandidateInfo {
                round: r,
                node_id: node_id.clone(),
                seed: seed.clone(),
                proof: vrf_proof_stub(&seed, &public_key_bytes), // demo 64 bytes
                public_key: public_key_bytes.clone(),
                commit: commit.clone(),                          // demo 32 bytes
                ip_address: "141.223.121.119".into(),
                port: "50052".into(),
                channel: cfg.channel.clone(),
            };
            let Ack { ok } = client.request_committee(Request::new(req)).await?.into_inner();

            // Create a 64-byte demo aggregate signature and log its length.
            let demo_sig = demo_aggregate_sig_64(
                &commit,                                  // pretend aggregated R
                &agg_pub_stub(&seed, &public_key_bytes),  // pretend aggregated A
                b"narwhal-cef-demo-message",
                b"",
            );
            cef_log_line(&format!("{},0,request_committee,{},ok={}", now_ms(), demo_sig.len(), ok));
        } else {
            // r >= 1: RequestAggregatedCommit
            let Ack { ok } = client
                .request_aggregated_commit(Request::new(CommitData {
                    round: r,
                    commit: commit.clone(),
                }))
                .await?
                .into_inner();

            // 64-byte demo “aggregate signature” and CSV
            let demo_sig = demo_aggregate_sig_64(
                &commit,
                &agg_pub_stub(&seed, &public_key_bytes),
                b"narwhal-cef-demo-message",
                b"",
            );
            cef_log_line(&format!(
                "{},{},request_aggregated_commit,{},ok={}",
                now_ms(),
                r,
                demo_sig.len(),
                ok
            ));
        }

        // simple pacing between rounds
        if r < 10 {
            sleep(Duration::from_secs(period_secs)).await;
        }
    }

    Ok(())
}

// ================= Helpers (CSV + simple stubs) =================

fn ensure_csv_parent_dir() {
    if let Some(parent) = Path::new(CSV_PATH).parent() {
        let _ = create_dir_all(parent);
    }
}

/// Write one line into /tmp/cef_sig.csv (creates file with header if missing).
fn cef_log_line(line: &str) {
    let need_header = std::fs::metadata(CSV_PATH).map(|m| m.len() == 0).unwrap_or(true);

    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(CSV_PATH) {
        if need_header {
            let _ = writeln!(f, "timestamp_ms,round,event,cef_sig_len,extra");
        }
        let _ = writeln!(f, "{line}");
    } else {
        println!("[cef_client] {line}");
    }
}

/// If you later want to inspect server push data, you can reuse this.
#[allow(dead_code)]
fn handle_finalized_committee(_msg: &FinalizedCommittee) {
    // intentionally no-op per your request (we don't wait on stream messages)
}

/// Very simple 32-byte commit for the demo (NOT CRYPTOGRAPHIC):
fn schnorr_commit_stub(seed: &str, public_key_bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha512};
    let mut h = Sha512::new();
    h.update(b"schnorr-commit");
    h.update(public_key_bytes);
    h.update(seed.as_bytes());
    let out = h.finalize();
    out[..32].to_vec()
}

/// Fake “aggregated pubkey” 32 bytes for the demo:
fn agg_pub_stub(seed: &str, public_key_bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha512};
    let mut h = Sha512::new();
    h.update(b"agg-pk");
    h.update(public_key_bytes);
    h.update(seed.as_bytes());
    let out = h.finalize();
    out[..32].to_vec()
}

/// Very simple 64-byte VRF proof for the demo (NOT A REAL VRF):
fn vrf_proof_stub(seed: &str, public_key_bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha512};
    let mut h = Sha512::new();
    h.update(b"vrf-proof");
    h.update(public_key_bytes);
    h.update(seed.as_bytes());
    h.finalize().to_vec() // 64 bytes
}

/// Produce a 64-byte “aggregate signature” as R||S for measurement only.
fn demo_aggregate_sig_64(R_agg: &[u8], A_agg: &[u8], message: &[u8], roster_hash: &[u8]) -> [u8; 64] {
    use sha2::{Digest, Sha512};
    let mut h = Sha512::new();
    h.update(R_agg);
    h.update(A_agg);
    h.update(message);
    h.update(roster_hash);
    let s_full = h.finalize(); // 64 bytes
    let mut out = [0u8; 64];

    // R part: copy up to 32 bytes
    let r_bytes = if R_agg.len() >= 32 { &R_agg[..32] } else { R_agg };
    out[..32].fill(0);
    out[..r_bytes.len()].copy_from_slice(r_bytes);

    // S part: 32 bytes
    out[32..].copy_from_slice(&s_full[..32]);
    out
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}
