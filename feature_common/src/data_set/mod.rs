use crate::data_set::column::Column;
use std::collections::HashMap;
use crate::feature_template::FeatureTemplate;

pub mod column;

/// 命名空间,
pub struct DataSet {
    // 名称
    pub name: String,
    // 属性
    pub attrs: HashMap<String, Column>,
    // 指标
    pub features: HashMap<Vec<String>, Vec<Box<dyn FeatureTemplate>>>,
}

impl DataSet {
    pub fn new(name: String, attrs: Vec<Column>) -> DataSet {
        let mut attr_map = HashMap::new();
        for x in attrs {
            attr_map.insert(x.name.clone(), x);
        }

        DataSet {
            name,
            attrs: attr_map,
            features: HashMap::new(),
        }
    }

    pub fn add_feature(&mut self, feature: Box<dyn FeatureTemplate>) {
        let key = feature.group_keys().clone();
        let mut features = self.features.entry(key).or_insert(Vec::new());
        features.push(feature);
    }
}

