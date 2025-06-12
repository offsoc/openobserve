use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    datasource::{
        physical_plan::{FileScanConfig, ParquetSource},
        source::{DataSource, DataSourceExec},
    },
    physical_plan::ExecutionPlan,
};
use liquid_cache_common::coerce_to_liquid_cache_types;
use liquid_cache_parquet::{
    LiquidCacheRef, LiquidParquetSource, inprocess::InProcessLiquidCacheExec,
};

#[derive(Debug)]
#[allow(dead_code)]
pub struct InProcessRewriter {
    cache: LiquidCacheRef,
}

impl InProcessRewriter {
    #[allow(dead_code)]
    /// Create an optimizer with an existing cache instance
    fn with_cache(cache: LiquidCacheRef) -> Self {
        Self { cache }
    }

    /// Rewrite a data source plan to use liquid cache
    #[allow(dead_code)]
    fn rewrite_data_source_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let cache_mode = self.cache.cache_mode();

        let rewritten = plan
            .transform_up(|node| {
                let any_plan = node.as_any();
                if let Some(plan) = any_plan.downcast_ref::<DataSourceExec>() {
                    let data_source = plan.data_source();
                    let any_source = data_source.as_any();
                    if let Some(file_scan_config) = any_source.downcast_ref::<FileScanConfig>() {
                        let file_source = file_scan_config.file_source();
                        let any_file_source = file_source.as_any();

                        // Check if this is a ParquetSource (same logic as server code)
                        if let Some(parquet_source) =
                            any_file_source.downcast_ref::<ParquetSource>()
                        {
                            // Save the original schema before coercion
                            let original_schema = plan.schema();

                            // Create a new LiquidParquetSource from the existing ParquetSource
                            let liquid_source = LiquidParquetSource::from_parquet_source(
                                parquet_source.clone(),
                                file_scan_config.file_schema.clone(),
                                self.cache.clone(),
                                *cache_mode,
                            );

                            let mut new_config = file_scan_config.clone();
                            new_config.file_source = Arc::new(liquid_source);

                            // Coerce schema types for liquid cache compatibility
                            let coerced_schema = coerce_to_liquid_cache_types(
                                new_config.file_schema.as_ref(),
                                cache_mode,
                            );

                            new_config.file_schema = Arc::new(coerced_schema);

                            let new_data_source: Arc<dyn DataSource> = Arc::new(new_config);
                            let wrapped_plan = Arc::new(DataSourceExec::new(new_data_source));

                            // Wrap with InProcessLiquidCacheExec to handle schema adaptation
                            let adapter_plan = Arc::new(InProcessLiquidCacheExec::new(
                                wrapped_plan,
                                original_schema,
                            ));

                            return Ok(Transformed::new(
                                adapter_plan,
                                true,
                                TreeNodeRecursion::Continue,
                            ));
                        }
                    }
                }
                Ok(Transformed::no(node))
            })
            .unwrap();

        rewritten.data
    }
}
