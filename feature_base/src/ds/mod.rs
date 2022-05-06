use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use column::ColumnType;

use crate::custom_error::BoxResult;
use crate::feature::Feature;

pub mod column;

/// 命名空间,
#[derive(Serialize, Deserialize, Debug)]
pub struct DataSet {
    pub id: i64,
    // 名称
    pub name: String,
    // 描述
    pub desc: String,
    // 属性
    pub columns: HashMap<String, ColumnType>,
    // 指标
    pub features: Vec<Feature>,
}

/// 每个指标更新的结果
#[derive(Debug)]
pub struct FeatureUpdateResult {
    pub success: bool,
    pub msg: String,
}

impl FeatureUpdateResult {
    pub fn failed(msg: String) -> FeatureUpdateResult {
        FeatureUpdateResult {
            success: false,
            msg,
        }
    }
}

/// 数据集更新结果
#[derive(Debug)]
pub struct DsUpdateResult {
    pub id: i64,
    pub feature_result_map: HashMap<i64, FeatureUpdateResult>,
}


impl DataSet {
    pub fn update(&self, data: &Value) -> BoxResult<DsUpdateResult> {
        let mut result_map = HashMap::new();

        for feature in &self.features {
            if let Err(e) = feature.check_update_condition(data, &self.columns) {
                result_map.insert(feature.id, FeatureUpdateResult::failed(format!("{}", e)));
                continue;
            }
            //feature.update(data, &self.columns);
        }
        Ok(DsUpdateResult { id: self.id, feature_result_map: result_map })
    }
}