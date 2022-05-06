use std::collections::HashMap;

use crate::data_set::column::{Column, ColumnType};
use crate::feature_template::{Feature, FeatureTemplate};
use serde::{Deserialize, Serialize};
pub mod column;

/// 命名空间,
#[derive(Serialize, Deserialize,Debug)]
pub struct DataSet {
    pub id: i64,
    // 名称
    pub name: String,
    // 描述
    pub desc: String,
    // 属性
    pub attrs: HashMap<String, ColumnType>,
    // 指标
    pub features: Vec<Feature>,
}

impl DataSet {}

