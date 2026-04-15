use std::sync::Arc;

use parking_lot::Mutex;

use alloy_primitives::B256;
use clap::Parser;
use glint_engine::executor::GlintEvmConfig;
use glint_engine::expiration::ExpirationIndex;
use glint_engine::recovery::{
    load_checkpoint, rebuild_expiration_index, rebuild_expiration_index_partial, save_checkpoint,
};
use reth_ethereum::cli::Cli;
use reth_ethereum::cli::chainspec::EthereumChainSpecParser;
use reth_ethereum::evm::EthEvmConfig;
use reth_ethereum::node::{EthereumAddOns, EthereumNode};
use reth_provider::BlockHashReader;
use tracing::{info, warn};

use glint_node::cli::GlintArgs;
use glint_node::rpc::{GlintApiServer as _, GlintRpc};
use glint_primitives::config::GlintChainConfig;

fn main() {
    reth_ethereum::cli::sigsegv_handler::install();

    if std::env::var_os("RUST_BACKTRACE").is_none() {
        // SAFETY: single-threaded at this point, before tokio runtime starts
        unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
    }

    if let Err(err) =
        Cli::<EthereumChainSpecParser, GlintArgs>::parse().run(async move |builder, ext| {
            let chain_spec = Arc::clone(&builder.config().chain);
            let genesis_json = serde_json::to_value(chain_spec.genesis())?;
            let config = GlintChainConfig::from_genesis(&genesis_json)?;
            info!(?config, "loaded glint chain config");

            let enable_exex = !ext.disable_exex();
            let socket_path = ext.exex_socket_path.clone();
            let checkpoint_path = ext.checkpoint_path.clone();

            let shutdown_index: Arc<Mutex<Option<Arc<Mutex<ExpirationIndex>>>>> =
                Arc::new(Mutex::new(None));
            let shutdown_index_writer = Arc::clone(&shutdown_index);
            let shutdown_tip: Arc<Mutex<(u64, B256)>> = Arc::new(Mutex::new((0, B256::ZERO)));
            let shutdown_tip_writer = Arc::clone(&shutdown_tip);

            let handle = builder
                .with_types::<EthereumNode>()
                .with_components(EthereumNode::components().executor(
                    move |ctx: &reth_ethereum::node::builder::BuilderContext<_>| {
                        let tip_block = ctx.head().number;
                        let tip_hash = ctx.head().hash;

                        let expiration_index = try_load_or_rebuild(
                            ctx.provider(),
                            &config,
                            tip_block,
                            checkpoint_path.as_deref(),
                        );

                        let chain_spec = ctx.chain_spec();

                        async move {
                            let idx = expiration_index?;
                            let shared_index = Arc::new(Mutex::new(idx));
                            *shutdown_index_writer.lock() = Some(Arc::clone(&shared_index));
                            *shutdown_tip_writer.lock() = (tip_block, tip_hash);

                            Ok(GlintEvmConfig::new(
                                EthEvmConfig::new(chain_spec),
                                config,
                                shared_index,
                            ))
                        }
                    },
                ))
                .with_add_ons(EthereumAddOns::default())
                .extend_rpc_modules(|ctx| {
                    let provider = ctx.provider().clone();
                    let glint_rpc = GlintRpc::new(provider);
                    ctx.modules.merge_configured(glint_rpc.into_rpc())?;
                    tracing::info!("glint RPC namespace registered");
                    Ok(())
                })
                .install_exex_if(enable_exex, "glint", move |ctx| {
                    let cancel = tokio_util::sync::CancellationToken::new();
                    let ipc_server = glint_transport::ipc::IpcServer::new(socket_path, cancel)
                        .expect("failed to bind IPC socket");
                    let transports: Vec<Box<dyn glint_transport::ExExTransportServer>> =
                        vec![Box::new(ipc_server)];
                    let exex = glint_exex::install(transports);
                    async move { Ok(exex(ctx)) }
                })
                .launch_with_debug_capabilities()
                .await?;

            let result = handle.wait_for_node_exit().await;

            if let Some(ref path) = ext.checkpoint_path {
                let maybe_index = shutdown_index.lock().take();
                if let Some(index_arc) = maybe_index {
                    let (tip_block, tip_hash) = *shutdown_tip.lock();
                    let index = index_arc.lock();
                    if let Err(e) = save_checkpoint(&index, tip_block, &tip_hash, path) {
                        warn!(?e, "failed to save expiration checkpoint on shutdown");
                    }
                }
            }

            result
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

fn try_load_or_rebuild<P>(
    provider: &P,
    config: &glint_primitives::config::GlintChainConfig,
    tip_block: u64,
    checkpoint_path: Option<&std::path::Path>,
) -> eyre::Result<ExpirationIndex>
where
    P: reth_provider::ReceiptProvider + BlockHashReader,
{
    if let Some(index) = checkpoint_path.and_then(|p| try_load_checkpoint(provider, p, tip_block)) {
        return rebuild_expiration_index_partial(provider, config, tip_block, index.1, index.0);
    }

    rebuild_expiration_index(provider, config, tip_block)
}

fn try_load_checkpoint<P>(
    provider: &P,
    path: &std::path::Path,
    tip_block: u64,
) -> Option<(ExpirationIndex, u64)>
where
    P: BlockHashReader,
{
    if !path.exists() {
        return None;
    }

    let (index, ckpt_tip, ckpt_hash) = match load_checkpoint(path) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                ?e,
                "failed to load checkpoint, falling back to full rebuild"
            );
            return None;
        }
    };

    match provider.block_hash(ckpt_tip) {
        Ok(Some(canonical_hash)) if canonical_hash == ckpt_hash => {
            info!(
                ckpt_tip,
                tip_block, "checkpoint valid, using partial rebuild"
            );
            Some((index, ckpt_tip))
        }
        Ok(_) => {
            warn!(
                ckpt_tip,
                "checkpoint hash mismatch, falling back to full rebuild"
            );
            None
        }
        Err(e) => {
            warn!(
                ?e,
                "failed to verify checkpoint hash, falling back to full rebuild"
            );
            None
        }
    }
}
