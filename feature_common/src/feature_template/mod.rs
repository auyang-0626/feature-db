pub mod count_feature;

/// 时间单位
pub enum WindowUnit {
    SECOND,
    MINUTE,
    HOUR,
    DAY,
}

pub trait FeatureTemplate {
    fn group_keys(&self) -> Vec<String>;
}