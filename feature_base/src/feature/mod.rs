use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::custom_error::{BoxResult, column_not_found_in_ds_err};
use crate::ds::column::{check_value_and_type_match, ColumnType};
use crate::feature::count_feature::CountFeatureTemplate;
use crate::store::Store;
use crate::store::wal::Wal;
use crate::feature::FeatureTemplate::COUNT;

pub mod count_feature;
pub mod value;

#[derive(Serialize, Deserialize, Debug)]
pub enum FeatureTemplate {
    COUNT(CountFeatureTemplate),
}

/// 指标实例
#[derive(Serialize, Deserialize, Debug)]
pub struct Feature {
    pub id: u64,
    pub name: String,
    pub template: FeatureTemplate,
}

impl Feature {
    pub async fn calc(&self, data: &Value, columns: &HashMap<String, ColumnType>,
                wal: &Wal, store: &Store) -> BoxResult<()> {
        match &self.template {
            COUNT(cf) => cf.calc(data, self.id, columns, wal, store).await
        }
    }
}