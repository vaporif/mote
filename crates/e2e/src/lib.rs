pub mod eth_node_handle;
pub mod sidecar_handle;

fn fetch_container_logs(container_id: &str) -> String {
    let output = std::process::Command::new("docker")
        .args(["logs", container_id])
        .output();
    match output {
        Ok(out) => format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ),
        Err(e) => format!("<failed to fetch docker logs: {e}>"),
    }
}
