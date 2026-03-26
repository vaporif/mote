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

    // Wrapper struct since GlintArgs uses Args trait (flatten), not Parser
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

    #[cfg(debug_assertions)]
    #[test]
    fn disable_exex_flag_exists_in_debug() {
        let cli = TestCli::parse_from(["test", "--glint.disable-exex"]);
        assert!(cli.glint.disable_exex());
    }
}
