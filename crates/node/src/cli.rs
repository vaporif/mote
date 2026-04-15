use std::path::PathBuf;

use clap::Args;

#[derive(Debug, Args)]
#[command(next_help_heading = "Glint")]
pub struct GlintArgs {
    /// Unix socket path for `ExEx` -> Analytics IPC
    #[arg(
        long = "glint.exex-socket-path",
        default_value = "/tmp/glint-exex.sock"
    )]
    pub exex_socket_path: PathBuf,

    /// gRPC listen port for remote sidecars. Set to enable gRPC transport.
    #[arg(long = "glint.exex-grpc-port")]
    pub exex_grpc_port: Option<u16>,

    /// Path for expiration index checkpoint file
    #[arg(long = "glint.checkpoint-path", default_value = None)]
    pub checkpoint_path: Option<PathBuf>,

    /// Run without `ExEx` (debug builds only)
    #[cfg(debug_assertions)]
    #[arg(long = "glint.disable-exex")]
    pub disable_exex: bool,
}

impl GlintArgs {
    #[cfg(not(debug_assertions))]
    pub const fn disable_exex(&self) -> bool {
        false
    }

    #[cfg(debug_assertions)]
    pub const fn disable_exex(&self) -> bool {
        self.disable_exex
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(flatten)]
        glint: GlintArgs,
    }

    #[test]
    fn default_socket_path() {
        let cli = TestCli::parse_from(["test"]);
        assert_eq!(
            cli.glint.exex_socket_path,
            PathBuf::from("/tmp/glint-exex.sock")
        );
    }

    #[test]
    fn custom_socket_path() {
        let cli = TestCli::parse_from(["test", "--glint.exex-socket-path", "/custom/path.sock"]);
        assert_eq!(
            cli.glint.exex_socket_path,
            PathBuf::from("/custom/path.sock")
        );
    }

    #[test]
    fn default_checkpoint_path_is_none() {
        let cli = TestCli::parse_from(["test"]);
        assert_eq!(cli.glint.checkpoint_path, None);
    }

    #[test]
    fn custom_checkpoint_path() {
        let cli = TestCli::parse_from([
            "test",
            "--glint.checkpoint-path",
            "/data/glint/expiration-index.bin",
        ]);
        assert_eq!(
            cli.glint.checkpoint_path,
            Some(PathBuf::from("/data/glint/expiration-index.bin"))
        );
    }

    #[test]
    fn grpc_port_not_set_by_default() {
        let cli = TestCli::parse_from(["test"]);
        assert!(cli.glint.exex_grpc_port.is_none());
    }

    #[test]
    fn custom_grpc_port() {
        let cli = TestCli::parse_from(["test", "--glint.exex-grpc-port", "9100"]);
        assert_eq!(cli.glint.exex_grpc_port, Some(9100));
    }

    #[cfg(debug_assertions)]
    #[test]
    fn disable_exex_flag_exists_in_debug() {
        let cli = TestCli::parse_from(["test", "--glint.disable-exex"]);
        assert!(cli.glint.disable_exex());
    }
}
