use super::WindowUnit;
use crate::feature_template::FeatureTemplate;

/// 累加类型的指标模板
pub struct CountFeatureTemplate {
    // 分组字段
    pub group_keys: Vec<String>,
    // 计算字段
    pub calc_key: String,
    // 时间字段
    pub time_key: String,
    // 时间单位
    pub window_unit: WindowUnit,
    // 窗口大小
    pub window_size: u32,
}

impl FeatureTemplate for CountFeatureTemplate {
    fn group_keys(&self) -> Vec<String> {
        self.group_keys.clone()
    }
}