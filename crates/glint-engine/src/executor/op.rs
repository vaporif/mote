use alloy_evm::{
    Database, EvmFactory,
    block::{BlockExecutorFactory, BlockExecutorFor},
    eth::EthTxResult,
    precompiles::PrecompilesMap,
};
use alloy_primitives::Log;
use reth_evm::{FromRecoveredTx, FromTxWithEncoded};
use revm::{Inspector, context::result::ResultAndState, database::State};
use std::marker::PhantomData;

use super::{GlintBlockExecutor, GlintBlockExecutorFactory, GlintResultBuilder, GlintTransaction};

pub struct OpGlintResultBuilder<H, T>(PhantomData<(H, T)>);

impl<H: Send + Sync + 'static, T: Default + Clone + Send + Sync + 'static> GlintResultBuilder
    for OpGlintResultBuilder<H, T>
{
    type HaltReason = H;
    type TxType = T;
    type Result = alloy_op_evm::block::OpTxResult<H, T>;

    fn build_crud_result(
        result: ResultAndState<H>,
        tx_type: T,
        sender: alloy_primitives::Address,
    ) -> alloy_op_evm::block::OpTxResult<H, T> {
        alloy_op_evm::block::OpTxResult {
            inner: EthTxResult {
                result,
                blob_gas_used: 0,
                tx_type,
            },
            is_deposit: false,
            sender,
        }
    }
}

impl<R, Spec, EvmF> BlockExecutorFactory
    for GlintBlockExecutorFactory<reth_optimism_evm::OpBlockExecutorFactory<R, Spec, EvmF>>
where
    R: alloy_op_evm::block::receipt_builder::OpReceiptBuilder<
            Transaction: GlintTransaction,
            Receipt: alloy_consensus::TxReceipt<Log = Log>,
        > + 'static,
    Spec: alloy_op_hardforks::OpHardforks + 'static,
    EvmF: EvmFactory<
            Tx: FromRecoveredTx<R::Transaction>
                    + FromTxWithEncoded<R::Transaction>
                    + alloy_op_evm::block::OpTxEnv,
            Precompiles = PrecompilesMap,
        > + 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = reth_optimism_evm::OpBlockExecutionCtx;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: reth_optimism_evm::OpBlockExecutionCtx,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        let inner = alloy_op_evm::block::OpBlockExecutor::new(
            evm,
            ctx,
            self.inner.spec(),
            self.inner.receipt_builder(),
        );
        GlintBlockExecutor::<_, OpGlintResultBuilder<_, _>> {
            inner,
            expiration_index: self.expiration_index.clone(),
            config: self.config.clone(),
            pending_logs: Vec::new(),
            pending_state: revm::state::EvmState::default(),
            _marker: PhantomData,
        }
    }
}
