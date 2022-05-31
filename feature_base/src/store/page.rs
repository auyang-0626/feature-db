use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::custom_error::BoxResult;
use crate::feature::value::FeatureValue;

/// 页
#[derive(Debug)]
pub struct Page {
    pub id: usize,
    pub data: BTreeMap<String, FeatureValue>,
}

impl Page {
    pub fn new(id: usize) -> Page {
        Page {
            id,
            data: BTreeMap::new(),
        }
    }

    pub async fn get(&self, key: &String) -> Option<&FeatureValue> {
        self.data.get(key)
    }
    pub async fn put(&mut self, key: String, value: FeatureValue) -> BoxResult<()> {
        self.data.insert(key, value);
        Ok(())
    }
}