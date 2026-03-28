use std::path::Path;

use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::{ContainerAsync, GenericImage, ImageExt, runners::AsyncRunner};

pub struct SidecarHandle {
    _container: ContainerAsync<GenericImage>,
    container_id: String,
    flight_url: String,
    health_url: String,
}

impl SidecarHandle {
    pub async fn spawn(exex_volume_path: &Path) -> eyre::Result<Self> {
        let image_tag = std::env::var("GLINT_IMAGE_TAG").unwrap_or_else(|_| "latest".into());

        let image = GenericImage::new("glint-db-sidecar", &image_tag)
            .with_exposed_port(50051.tcp())
            .with_exposed_port(8080.tcp())
            .with_env_var("RUST_LOG", "debug")
            .with_mount(Mount::bind_mount(
                exex_volume_path.to_str().unwrap(),
                "/exex",
            ))
            .with_cmd(vec![
                "run",
                "--exex-socket",
                "/exex/glint-exex.sock",
                "--flight-port",
                "50051",
                "--health-port",
                "8080",
                "--db-path",
                "/data/glint-sidecar.db",
            ]);

        let container = image.start().await?;
        let container_id = container.id().to_string();
        let flight_port = container.get_host_port_ipv4(50051.tcp()).await?;
        let health_port = container.get_host_port_ipv4(8080.tcp()).await?;
        let flight_url = format!("http://127.0.0.1:{flight_port}");
        let health_url = format!("http://127.0.0.1:{health_port}");

        let handle = Self {
            _container: container,
            container_id,
            flight_url,
            health_url,
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
