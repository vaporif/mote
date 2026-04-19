use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

use crate::Transport;
use crate::eth_node_handle::EthNodeHandle;

const HOST_ALIAS: &str = "host.testcontainers.internal";

pub struct SidecarHandle {
    _container: ContainerAsync<GenericImage>,
    container_id: String,
    flight_url: String,
    health_url: String,
    metrics_url: String,
}

impl SidecarHandle {
    pub async fn spawn(node: &EthNodeHandle, transport: Transport) -> eyre::Result<Self> {
        let image_tag = std::env::var("GLINT_IMAGE_TAG").unwrap_or_else(|_| "latest".into());

        let mut cmd = vec![
            "run".to_string(),
            "--flight-port".to_string(),
            "50051".to_string(),
            "--health-port".to_string(),
            "8080".to_string(),
            "--db-path".to_string(),
            "/data/glint-sidecar.db".to_string(),
            "--genesis".to_string(),
            "/etc/glint/genesis.json".to_string(),
            "--metrics-port".to_string(),
            "9090".to_string(),
        ];

        let mut image = GenericImage::new("glint-sidecar", &image_tag)
            .with_exposed_port(50051.tcp())
            .with_exposed_port(8080.tcp())
            .with_exposed_port(9090.tcp())
            .with_env_var("RUST_LOG", "debug");

        match transport {
            Transport::Ipc => {
                image = image.with_mount(Mount::bind_mount(
                    node.exex_volume_path().to_str().unwrap(),
                    "/exex",
                ));
                cmd.push("--exex-socket".to_string());
                cmd.push("/exex/glint-exex.sock".to_string());
            }
            Transport::Grpc => {
                let grpc_port = node
                    .grpc_host_port()
                    .expect("node must be spawned with Transport::Grpc");
                image = image.with_exposed_host_port(grpc_port);
                cmd.push("--exex-grpc".to_string());
                cmd.push(format!("http://{HOST_ALIAS}:{grpc_port}"));
            }
        }

        let cmd_refs: Vec<&str> = cmd.iter().map(String::as_str).collect();
        let image = image.with_cmd(cmd_refs);

        let container = image.start().await?;
        let container_id = container.id().to_string();
        let flight_port = container.get_host_port_ipv4(50051.tcp()).await?;
        let health_port = container.get_host_port_ipv4(8080.tcp()).await?;
        let flight_url = format!("http://127.0.0.1:{flight_port}");
        let health_url = format!("http://127.0.0.1:{health_port}");
        let metrics_port = container.get_host_port_ipv4(9090.tcp()).await?;
        let metrics_url = format!("http://127.0.0.1:{metrics_port}/metrics");

        let handle = Self {
            _container: container,
            container_id,
            flight_url,
            health_url,
            metrics_url,
        };

        handle.wait_healthy().await?;
        Ok(handle)
    }

    pub fn flight_url(&self) -> &str {
        &self.flight_url
    }

    pub fn health_url(&self) -> &str {
        &self.health_url
    }

    pub fn metrics_url(&self) -> &str {
        &self.metrics_url
    }

    pub fn logs(&self) -> String {
        crate::fetch_container_logs(&self.container_id)
    }

    async fn wait_healthy(&self) -> eyre::Result<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let client = reqwest::Client::new();
        let url = format!("{}/health", self.health_url);

        while tokio::time::Instant::now() < deadline {
            if client
                .get(&url)
                .send()
                .await
                .ok()
                .is_some_and(|r| r.status().is_success())
            {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        let logs = self.logs();
        eyre::bail!("sidecar did not become healthy within 30s\n\n=== SIDECAR LOGS ===\n{logs}")
    }
}

impl Drop for SidecarHandle {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let logs = crate::fetch_container_logs(&self.container_id);
            let all_lines: Vec<&str> = logs.lines().collect();
            let start = all_lines.len().saturating_sub(50);
            eprintln!(
                "\n=== SIDECAR LOGS (last {} lines) ===",
                all_lines.len() - start
            );
            for line in &all_lines[start..] {
                eprintln!("{line}");
            }
            eprintln!("=== END SIDECAR LOGS ===\n");
        }
    }
}
