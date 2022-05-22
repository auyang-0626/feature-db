use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Bytes;
use std::sync::Arc;

use bitmaps::Bitmap;
use tokio::sync::Mutex;

use crate::feature::value::StoreValue;
use crate::store::page::Page;

pub mod wal;
pub mod page;

/// 配置slice数量
const FRAGMENT_NUM: u16 = 10240;
/// 页的大小
const PAGE_SIZE: u32 = 2 ^ 16;
/// 每个Fragment最多拥有的page数量，乘以 PAGE_SIZE = Fragment最多存储的数据量
const PAGE_NUM: usize = 2 ^ 18;
/// 文件大小
const FILE_SIZE: u32 = 2 ^ 30;


/// hash-->fragment--->page--->record
pub struct Store {
    pub data_dir: String,
    pub fragment: HashMap<u16, Fragment>,
}

/// 分片
pub struct Fragment {
    pub id: u16,
    /// page空闲列表
    pub page_bit_map: Bitmap<PAGE_NUM>,
    pub page_tree: BTreeMap<String, Page>,
}




impl Store {
    pub fn new(data_dir: String) -> Store {
        Store {
            data_dir,
            fragment: HashMap::new(),
        }
    }

    pub fn get<T>(&self, key: &String) -> Option<Arc<Mutex<dyn StoreValue<T> + Send>>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_code = hasher.finish();

        let fragment_id = (hash_code % FRAGMENT_NUM as u64) as u16;
        let fragment = self.fragment.get(&fragment_id)?;

        fragment.get(key)
    }
}

impl Fragment {
    pub fn get<T>(&self, key: &String) -> Option<Arc<Mutex<dyn StoreValue<T> + Send>>> {
        let range = self.page_tree.range(key.clone()..).last()?;

        None
    }
}