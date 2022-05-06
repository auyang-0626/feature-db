use serde::{Deserialize, Serialize};
use crate::feature::FeatureTemplateRequireColumns;
use crate::WindowUnit;


/// 累加类型的指标模板
#[derive(Serialize, Deserialize, Debug)]
pub struct CountFeatureTemplate {
    // 分组字段
    pub group_keys: Vec<String>,
    // 时间字段
    pub time_key: String,
    // 时间单位
    pub window_unit: WindowUnit,
    // 窗口大小
    pub window_size: u32,
}

impl FeatureTemplateRequireColumns for CountFeatureTemplate {
    fn require_columns(&self) -> Vec<&String> {
        let mut require_columns = Vec::new();
        require_columns.push(&self.time_key);
        for k in &self.group_keys {
            require_columns.push(k);
        }
        require_columns
    }
}