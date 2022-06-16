use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::custom_error::{CustomResult};
use crate::ds::column::{ ColumnType};
use crate::feature::count_feature::CountFeatureTemplate;

use crate::store::wal::{Wal, WalFeatureUpdateValue};
use crate::feature::FeatureTemplate::COUNT;

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

    pub fn build_key(&self, event: &Value, column_type_map: &HashMap<String, ColumnType>) -> CustomResult<String>{
        match &self.template {
            COUNT(cf) => cf.build_key(event, self.id, column_type_map)
        }
    }

    pub async fn calc_and_update(&self, event: &Value,
                                 column_type_map: &HashMap<String, ColumnType>,
                                 key:&String,
                                 page:&mut RwLockWriteGuard<'_,Page>,
                                 wal: &Wal) -> CustomResult<WalFeatureUpdateValue> {
        match &self.template {
            COUNT(cf) => cf.calc_and_update(event, column_type_map,key, page, wal).await
        }
    }
}