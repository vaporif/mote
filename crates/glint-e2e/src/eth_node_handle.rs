use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tempfile::TempDir;

pub struct EthNodeHandle {
    child: Child,
    rpc_url: String,
    _datadir: TempDir,
}

impl EthNodeHandle {
    pub fn spawn() -> eyre::Result<Self> {
        let bin = Self::resolve_binary()?;
        let genesis = Self::resolve_genesis()?;
        let datadir = tempfile::tempdir()?;
        let port = Self::pick_port()?;
        let rpc_url = format!("http://127.0.0.1:{port}");

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
            .arg("--quiet")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let mut handle = Self {
            child,
            rpc_url,
            _datadir: datadir,
        };

        handle.wait_ready()?;
        Ok(handle)
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
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
    }
}
