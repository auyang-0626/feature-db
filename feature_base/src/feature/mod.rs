use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::custom_error::{BoxResult, column_not_found_in_ds_err};
use crate::ds::column::{check_value_and_type_match, ColumnType};
use crate::feature::count_feature::CountFeatureTemplate;
use crate::store::Store;
use crate::store::wal::{Wal, WalFeatureUpdateValue};
use crate::feature::FeatureTemplate::COUNT;
use crate::feature::value::FeatureValue;
use crate::store::page::Page;
use tokio::sync::RwLockWriteGuard;

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

    pub fn build_key(&self, event: &Value, column_type_map: &HashMap<String, ColumnType>) -> BoxResult<String>{
        match &self.template {
            COUNT(cf) => cf.build_key(event, self.id, column_type_map)
        }
    }

    pub async fn calc_and_update(&self, event: &Value,
                                 column_type_map: &HashMap<String, ColumnType>,
                                 key:&String,
                                 page:&mut RwLockWriteGuard<'_,Page>,
                                 wal: &Wal) -> BoxResult<WalFeatureUpdateValue> {
        match &self.template {
            COUNT(cf) => cf.calc_and_update(event, column_type_map,key, page, wal).await
        }
    }
}