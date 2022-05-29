use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::feature::value::FeatureValue;

/// 页
pub struct Page {
    pub id: usize,
    pub data: BTreeMap<String, Arc<RwLock<FeatureValue>>>,
}

impl Page {
    pub fn new(id: usize) -> Page {
        Page {
            id,
            data: BTreeMap::new(),
        }
    }

    pub fn put(&self,key:String, value:FeatureValue) {
        self.data.insert(key,Arcvalue)
    }
}