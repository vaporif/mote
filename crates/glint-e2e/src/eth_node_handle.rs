use std::path::{Path, PathBuf};

use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

pub struct EthNodeHandle {
    _container: ContainerAsync<GenericImage>,
    container_id: String,
    rpc_url: String,
    exex_volume_path: PathBuf,
    _exex_dir: TempDir,
}

impl EthNodeHandle {
    pub async fn spawn() -> eyre::Result<Self> {
        let image_tag = std::env::var("GLINT_IMAGE_TAG").unwrap_or_else(|_| "latest".into());
        let tmpdir_base = std::env::var("GLINT_E2E_TMPDIR").unwrap_or_else(|_| "/tmp".into());
        let exex_dir = tempfile::Builder::new()
            .prefix("glint-e2e-")
            .tempdir_in(tmpdir_base)?;
        let exex_host_path = exex_dir.path().to_path_buf();

        let image = GenericImage::new("eth-glint", &image_tag)
            .with_exposed_port(8545.tcp())
            .with_mount(Mount::bind_mount(exex_host_path.to_str().unwrap(), "/exex"))
            .with_cmd(vec![
                "node",
                "--chain",
                "/etc/glint/genesis.json",
                "--dev",
                "--dev.block-time",
                "1s",
                "--http",
                "--http.addr",
                "0.0.0.0",
                "--http.port",
                "8545",
                "--port",
                "30303",
                "--discovery.port",
                "30303",
                "--authrpc.port",
                "8551",
                "--datadir",
                "/data",
                "--log.file.directory",
                "/data/logs",
                "--glint.exex-socket-path",
                "/exex/glint-exex.sock",
                "-vvvv",
            ]);

        let container = image.start().await?;
        let container_id = container.id().to_string();
        let host_port = container.get_host_port_ipv4(8545.tcp()).await?;
        let rpc_url = format!("http://127.0.0.1:{host_port}");

        let handle = Self {
            _container: container,
            container_id,
            rpc_url,
            exex_volume_path: exex_host_path,
            _exex_dir: exex_dir,
        };

        handle.wait_ready().await?;
        Ok(handle)
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub fn exex_volume_path(&self) -> &Path {
        &self.exex_volume_path
    }

    pub fn logs(&self) -> String {
        crate::fetch_container_logs(&self.container_id)
    }

    async fn wait_ready(&self) -> eyre::Result<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let client = reqwest::Client::new();

        while tokio::time::Instant::now() < deadline {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            });

            if client
                .post(&self.rpc_url)
                .json(&body)
                .send()
                .await
                .ok()
                .is_some()
            {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        eyre::bail!("node did not become ready within 30s")
    }
}

impl Drop for EthNodeHandle {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let logs = crate::fetch_container_logs(&self.container_id);
            let glint_lines: Vec<&str> = logs.lines().filter(|l| l.contains("glint")).collect();
            eprintln!("\n=== GLINT NODE LOGS (filtered) ===");
            if glint_lines.is_empty() {
                let all_lines: Vec<&str> = logs.lines().collect();
                let start = all_lines.len().saturating_sub(50);
                for line in &all_lines[start..] {
                    eprintln!("{line}");
                }
            } else {
                for line in &glint_lines {
                    eprintln!("{line}");
                }
            }
            eprintln!("=== END NODE LOGS ===\n");
        }
    }
}
