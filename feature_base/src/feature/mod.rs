pub mod count_feature;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use crate::feature::count_feature::CountFeatureTemplate;
use crate::custom_error::{BoxResult, column_not_found_in_ds_err};
use crate::ds::column::{ColumnType, check_value_and_type_match};

#[derive(Serialize, Deserialize, Debug)]
pub enum FeatureTemplate {
    COUNT(CountFeatureTemplate),
}

pub trait FeatureTemplateRequireColumns {
    fn require_columns(&self) -> Vec<&String>;
}

impl FeatureTemplate {
    pub fn require_columns_check(&self, data: &Value, column_map: &HashMap<String, ColumnType>) -> BoxResult<()> {
        let  require_columns:Vec<&String> = match self {
            FeatureTemplate::COUNT(t) => t.require_columns()
        };

        for column_key in require_columns {
            let column_type = column_map.get(column_key)
                .ok_or(column_not_found_in_ds_err(column_key))?;
            check_value_and_type_match(data, column_key, column_type)?;
        }

        Ok({})
    }
}

/// 指标实例
#[derive(Serialize, Deserialize, Debug)]
pub struct Feature {
    pub id: i64,
    pub name: String,
    pub template: FeatureTemplate,
}

impl Feature {

    /// 检查是否满足更新的条件
    pub fn check_update_condition(&self,data:&Value,columns: &HashMap<String, ColumnType>) -> BoxResult<()>{
        self.template.require_columns_check(data,columns)
    }

    pub fn update(&self,data:&Value,columns: &HashMap<String, ColumnType>) ->BoxResult<()>{
        todo!()
    }
}