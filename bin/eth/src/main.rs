use std::sync::{Arc, Mutex};

use clap::Parser;
use mote_engine::executor::MoteEvmConfig;
use reth_ethereum::cli::Cli;
use reth_ethereum::cli::chainspec::EthereumChainSpecParser;
use reth_ethereum::evm::EthEvmConfig;
use reth_ethereum::node::{EthereumAddOns, EthereumNode};
use tracing::info;

use mote_node::cli::MoteArgs;
use mote_node::genesis::extract_mote_config;

fn main() {
    reth_ethereum::cli::sigsegv_handler::install();

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: single-threaded at this point, before tokio runtime starts
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) =
        Cli::<EthereumChainSpecParser, MoteArgs>::parse().run(async move |builder, ext| {
            let chain_spec = Arc::clone(&builder.config().chain);
            let genesis_json = serde_json::to_value(chain_spec.genesis())?;
            let config = extract_mote_config(&genesis_json)?;
            info!(?config, "loaded mote chain config");

            let enable_exex = !ext.disable_exex();
            let socket_path = ext.exex_socket_path.clone();

            let handle = builder
                .with_types::<EthereumNode>()
                .with_components(EthereumNode::components().executor(
                    move |ctx: &reth_ethereum::node::builder::BuilderContext<_>| {
                        let tip_block = ctx.head().number;
                        let expiration_index = mote_engine::recovery::rebuild_expiration_index(
                            ctx.provider(),
                            &config,
                            tip_block,
                        );
                        let chain_spec = ctx.chain_spec();

                        async move {
                            let shared_index = Arc::new(Mutex::new(expiration_index?));

                            Ok(MoteEvmConfig::new(
                                EthEvmConfig::new(chain_spec),
                                config,
                                shared_index,
                            ))
                        }
                    },
                ))
                .with_add_ons(EthereumAddOns::default())
                .install_exex_if(enable_exex, "mote", move |ctx| {
                    let exex = mote_exex::install(socket_path);
                    async move { Ok(exex(ctx)) }
                })
                .launch_with_debug_capabilities()
                .await?;

            handle.wait_for_node_exit().await
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
