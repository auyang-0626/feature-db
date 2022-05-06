pub mod count_feature;
use serde::{Deserialize, Serialize, Deserializer};
use std::prelude::rust_2015::Result::Ok;
use crate::feature_template::count_feature::CountFeatureTemplate;

/// 时间单位
#[derive(Serialize, Deserialize,Debug)]
pub enum WindowUnit {
    SECOND,
    MINUTE,
    HOUR,
    DAY,
}

#[derive(Serialize, Deserialize,Debug)]
pub enum FeatureTemplate {
    COUNT(CountFeatureTemplate),
}



/// 指标实例
#[derive(Serialize, Deserialize,Debug)]
pub struct Feature {
    pub id: i64,
    pub name: String,
    pub template: FeatureTemplate,
}