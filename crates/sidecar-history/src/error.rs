pub trait IntoDataFusionError<T> {
    fn df_err(self) -> datafusion::error::Result<T>;
}

impl<T, E: std::error::Error + Send + Sync + 'static> IntoDataFusionError<T> for Result<T, E> {
    fn df_err(self) -> datafusion::error::Result<T> {
        self.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
    }
}

pub fn arrow_err(e: arrow::error::ArrowError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
}
