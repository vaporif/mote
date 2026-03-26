use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;

pub struct EthNodeHandle {
    child: Child,
    rpc_url: String,
    exex_socket: PathBuf,
    _datadir: TempDir,
    log_file: PathBuf,
}

impl EthNodeHandle {
    pub fn spawn() -> eyre::Result<Self> {
        let bin = Self::resolve_binary()?;
        let genesis = Self::resolve_genesis()?;
        let datadir = tempfile::tempdir()?;
        let port = Self::pick_port()?;
        let rpc_url = format!("http://127.0.0.1:{port}");

        let log_file = datadir.path().join("node-output.log");
        let stderr_file = File::create(&log_file)?;
        let stdout_file = stderr_file.try_clone()?;
        let exex_socket = datadir.path().join("glint-exex.sock");

        let child = Command::new(&bin)
            .arg("node")
            .arg("--chain")
            .arg(&genesis)
            .arg("--dev")
            .arg("--dev.block-time")
            .arg("1s")
            .arg("--http")
            .arg("--http.port")
            .arg(port.to_string())
            .arg("--datadir")
            .arg(datadir.path())
            .arg("--log.file.directory")
            .arg(datadir.path().join("logs"))
            .arg("--glint.exex-socket-path")
            .arg(&exex_socket)
            .arg("-vvvv")
            .stdout(Stdio::from(stdout_file))
            .stderr(Stdio::from(stderr_file))
            .spawn()?;

        let mut handle = Self {
            child,
            rpc_url,
            exex_socket,
            log_file,
            _datadir: datadir,
        };

        handle.wait_ready()?;
        Ok(handle)
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub fn exex_socket(&self) -> &Path {
        &self.exex_socket
    }

    pub fn dump_logs(&self) -> String {
        std::fs::read_to_string(&self.log_file).unwrap_or_else(|e| format!("<read error: {e}>"))
    }

    pub fn grep_logs(&self, pattern: &str) -> Vec<String> {
        self.dump_logs()
            .lines()
            .filter(|l| l.contains(pattern))
            .map(String::from)
            .collect()
    }

    fn resolve_binary() -> eyre::Result<PathBuf> {
        if let Ok(bin) = std::env::var("GLINT_BIN") {
            return Ok(PathBuf::from(bin));
        }
        let fallback =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/eth-glint");
        eyre::ensure!(
            fallback.exists(),
            "eth-glint not found at {fallback:?} — run `cargo build --bin eth-glint` or set GLINT_BIN",
        );
        Ok(fallback)
    }

    fn resolve_genesis() -> eyre::Result<PathBuf> {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../etc/genesis.json")
            .canonicalize()
            .map_err(Into::into)
    }

    fn pick_port() -> eyre::Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    }

    fn wait_ready(&mut self) -> eyre::Result<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let client = reqwest::blocking::Client::new();

        while std::time::Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                eyre::bail!("node exited with {status} before becoming ready");
            }

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
                .ok()
                .and_then(|r| r.json::<serde_json::Value>().ok())
                .and_then(|j| j.get("result").cloned())
                .is_some()
            {
                return Ok(());
            }

            std::thread::sleep(Duration::from_millis(500));
        }

        self.child.kill().ok();
        eyre::bail!("node did not become ready within 30s")
    }
}

impl Drop for EthNodeHandle {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();

        if std::thread::panicking() {
            let logs = std::fs::read_to_string(&self.log_file).unwrap_or_default();
            let glint_lines: Vec<&str> = logs.lines().filter(|l| l.contains("glint")).collect();
            eprintln!("\n=== GLINT NODE LOGS (filtered) ===");
            for line in &glint_lines {
                eprintln!("{line}");
            }
            if glint_lines.is_empty() {
                // Show last 50 lines if no glint-specific logs found
                let all_lines: Vec<&str> = logs.lines().collect();
                let start = all_lines.len().saturating_sub(50);
                eprintln!(
                    "(no glint-specific logs found, showing last {} lines)",
                    all_lines.len() - start
                );
                for line in &all_lines[start..] {
                    eprintln!("{line}");
                }
            }
            eprintln!("=== END NODE LOGS ===\n");
        }
    }
}
