use alloy_evm::{
    Database, EvmFactory,
    block::{BlockExecutorFactory, BlockExecutorFor},
    eth::{
        EthBlockExecutionCtx, EthBlockExecutor, EthBlockExecutorFactory, EthTxResult,
        receipt_builder::ReceiptBuilder, spec::EthExecutorSpec,
    },
    precompiles::PrecompilesMap,
};
use alloy_primitives::Log;
use reth_evm::{FromRecoveredTx, FromTxWithEncoded};
use revm::{Inspector, context::result::ResultAndState, database::State};
use std::marker::PhantomData;

use super::{GlintBlockExecutor, GlintBlockExecutorFactory, GlintResultBuilder, GlintTransaction};

pub struct EthGlintResultBuilder<H, T>(PhantomData<(H, T)>);

impl<H: Send + Sync + 'static, T: Default + Clone + Send + Sync + 'static> GlintResultBuilder
    for EthGlintResultBuilder<H, T>
{
    type HaltReason = H;
    type TxType = T;
    type Result = EthTxResult<H, T>;

    fn build_crud_result(result: ResultAndState<H>, tx_type: T) -> EthTxResult<H, T> {
        EthTxResult {
            result,
            blob_gas_used: 0,
            tx_type,
        }
    }
}

impl<R, Spec, EvmF> BlockExecutorFactory
    for GlintBlockExecutorFactory<EthBlockExecutorFactory<R, Spec, EvmF>>
where
    R: ReceiptBuilder<
            Transaction: GlintTransaction,
            Receipt: alloy_consensus::TxReceipt<Log = Log>,
        > + 'static,
    Spec: EthExecutorSpec + 'static,
    EvmF: EvmFactory<
            Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
            Precompiles = PrecompilesMap,
        > + 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = EthBlockExecutionCtx<'a>;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        self.inner.evm_factory()
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: EthBlockExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        let inner =
            EthBlockExecutor::new(evm, ctx, self.inner.spec(), self.inner.receipt_builder());
        GlintBlockExecutor::<_, EthGlintResultBuilder<_, _>> {
            inner,
            expiration_index: self.expiration_index.clone(),
            config: self.config.clone(),
            pending_logs: Vec::new(),
            _marker: PhantomData,
        }
    }
}
