use super::WindowUnit;
use crate::feature_template::FeatureTemplate;
use serde::{Deserialize, Serialize};

/// 累加类型的指标模板
#[derive(Serialize, Deserialize,Debug)]
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
