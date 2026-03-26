use std::sync::{Arc, Mutex};

use clap::{Args, Parser};
use mote_engine::executor::MoteEvmConfig;
use reth_node_builder::BuilderContext;
use reth_optimism_cli::Cli;
use reth_optimism_cli::chainspec::OpChainSpecParser;
use reth_optimism_evm::OpEvmConfig;
use reth_optimism_node::OpNode;
use reth_optimism_node::args::RollupArgs;
use tracing::info;

use mote_node::cli::MoteArgs;
use mote_node::genesis::extract_mote_config;

/// Combined CLI arguments: Optimism rollup args + Mote-specific args.
#[derive(Debug, Args)]
struct MoteOpArgs {
    #[command(flatten)]
    rollup: RollupArgs,

    #[command(flatten)]
    mote: MoteArgs,
}

fn main() {
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: single-threaded at this point, before tokio runtime starts
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) =
        Cli::<OpChainSpecParser, MoteOpArgs>::parse().run(async move |builder, ext| {
            let chain_spec = Arc::clone(&builder.config().chain);
            let genesis_json = serde_json::to_value(chain_spec.genesis())?;
            let config = extract_mote_config(&genesis_json)?;
            info!(?config, "loaded mote chain config");

            let enable_exex = !ext.mote.disable_exex();
            let socket_path = ext.mote.exex_socket_path.clone();
            let rollup_args = ext.rollup;

            let op_node = OpNode::new(rollup_args);

            let handle =
                builder
                    .with_types::<OpNode>()
                    .with_components(op_node.components().executor(
                        move |ctx: &BuilderContext<_>| {
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
                                    OpEvmConfig::optimism(chain_spec),
                                    config,
                                    shared_index,
                                ))
                            }
                        },
                    ))
                    .with_add_ons(reth_optimism_node::OpAddOns::default())
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
