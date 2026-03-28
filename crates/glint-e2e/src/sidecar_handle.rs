use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;

pub struct SidecarHandle {
    child: Child,
    flight_port: u16,
    health_port: u16,
    log_file: PathBuf,
    _db_dir: TempDir,
}

impl SidecarHandle {
    pub fn spawn(exex_socket: &Path) -> eyre::Result<Self> {
        let bin = Self::resolve_binary()?;
        let flight_port = pick_port()?;
        let health_port = pick_port()?;
        let db_dir = tempfile::tempdir()?;
        let db_path = db_dir.path().join("glint-sidecar.db");

        let log_file = db_dir.path().join("sidecar-output.log");
        let stderr_file = std::fs::File::create(&log_file)?;
        let stdout_file = stderr_file.try_clone()?;

        let child = Command::new(&bin)
            .arg("run")
            .arg("--exex-socket")
            .arg(exex_socket)
            .arg("--flight-port")
            .arg(flight_port.to_string())
            .arg("--health-port")
            .arg(health_port.to_string())
            .arg("--db-path")
            .arg(&db_path)
            .env("RUST_LOG", "debug")
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .spawn()?;

        let mut handle = Self {
            child,
            flight_port,
            health_port,
            log_file,
            _db_dir: db_dir,
        };

        eprintln!("sidecar log file: {}", handle.log_file.display());
        handle.wait_healthy()?;
        Ok(handle)
    }

    pub fn flight_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.flight_port)
    }

    pub fn health_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.health_port)
    }

    pub fn dump_logs(&self) -> String {
        std::fs::read_to_string(&self.log_file).unwrap_or_else(|e| format!("<read error: {e}>"))
    }

    fn resolve_binary() -> eyre::Result<PathBuf> {
        if let Ok(bin) = std::env::var("GLINT_SIDECAR_BIN") {
            return Ok(PathBuf::from(bin));
        }
        let fallback =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/glint-db-sidecar");
        if !fallback.exists() {
            let status = Command::new("cargo")
                .args([
                    "build",
                    "-p",
                    "glint-db-sidecar",
                    "--bin",
                    "glint-db-sidecar",
                ])
                .status()?;
            eyre::ensure!(status.success(), "failed to build glint-db-sidecar");
        }
        Ok(fallback)
    }

    fn wait_healthy(&mut self) -> eyre::Result<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let client = reqwest::blocking::Client::new();
        let url = format!("http://127.0.0.1:{}/health", self.health_port);

        while std::time::Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                eyre::bail!("sidecar exited with {status} before becoming healthy");
            }

            if client
                .get(&url)
                .send()
                .ok()
                .is_some_and(|r| r.status().is_success())
            {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(500));
        }

        self.child.kill().ok();
        eyre::bail!("sidecar did not become healthy within 30s")
    }
}

impl Drop for SidecarHandle {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();

        if std::thread::panicking() {
            let logs = std::fs::read_to_string(&self.log_file).unwrap_or_default();
            let lines: Vec<&str> = logs.lines().collect();
            let start = lines.len().saturating_sub(50);
            eprintln!(
                "\n=== SIDECAR LOGS (last {} lines) ===",
                lines.len() - start
            );
            for line in &lines[start..] {
                eprintln!("{line}");
            }
            eprintln!("=== END SIDECAR LOGS ===\n");
        }
    }
}

fn pick_port() -> eyre::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}
