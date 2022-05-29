use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Bytes;
use std::sync::Arc;

use bitmaps::Bitmap;
use tokio::sync::Mutex;

use crate::feature::value::FeatureValue;
use crate::store::page::Page;
use crate::store::slot::{calc_slot_id, Slot, SLOT_BIT_LEB, SLOT_NUM};

pub mod wal;
pub mod page;
pub mod slot;


/// 页的大小
const PAGE_SIZE: u32 = 2 ^ 16;

/// 文件大小
const FILE_SIZE: u32 = 2 ^ 30;


/// hash-->fragment--->page--->record
pub struct Store {
    pub data_dir: String,
    pub slot_index: HashMap<u16, Slot>,
}


impl Store {
    pub fn new(data_dir: String) -> Store {

        let mut  slot_index = HashMap::new();
        for i in 2^12 {
            slot_index.insert(i,Slot::new())
        }

        Store {
            data_dir,
            slot_index,
        }
    }

    pub fn get(&self, key: &String) -> Option<Arc<Mutex<FeatureValue>>> {
        let slot_id = calc_slot_id(key);
        let slot = self.slot_index.get(&slot_id)?;

        slot.get(key)
    }

    pub fn put(&self, key: String,value:FeatureValue){
        let slot_id = calc_slot_id(&key);
        let slot = self.slot_index.get(&slot_id)?;
        slot.put(key,value);
    }
}

