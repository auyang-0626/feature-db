use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use bitmaps::Bitmap;
use tokio::sync::{Mutex, RwLock};

use crate::custom_error::BoxResult;
use crate::feature::value::FeatureValue;
use crate::store::page::Page;

/// 计算slot的值
pub fn calc_slot_id(key: &String) -> u16 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash_code = hasher.finish();

    (hash_code >> 20) as u16
}

/// 每个slot最多拥有的page数量，乘以 PAGE_SIZE = slot最多存储的数据量
const PAGE_NUM: usize = 2 ^ 18;

/// 分片
pub struct Slot {
    pub id: u16,
    /// page空闲列表
    pub page_bit_map: Bitmap<PAGE_NUM>,
    /// 只有在 分裂/合并page时，才需要获取page的写锁，其它一律读锁
    pub page_tree: BTreeMap<String, RwLock<Page>>,
    pub page_create_lock: Mutex<bool>,
}

impl Slot {
    pub async fn new(id: u16) -> Slot {
        let mut slot = Slot {
            id,
            page_bit_map: Bitmap::new(),
            page_tree: BTreeMap::new(),
            page_create_lock: Mutex::new(false),
        };
        slot.new_page("".to_string()).await;
        slot
    }

    pub async fn new_page(&mut self, min_key: String) -> BoxResult<()> {
        self.page_create_lock.lock().await;
        let page_id = self.page_bit_map.first_false_index()?;
        let next_page = Page::new(page_id);
        self.page_bit_map.set(page_id, true);
        self.page_tree.insert(min_key, RwLock::new(next_page));
        Ok(())
    }

    pub fn get(&self, key: &String) -> Option<Arc<Mutex<FeatureValue>>> {
        let (key, page) = self.page_tree.range(..key).last()?;

        None
    }

    pub async fn put(&self, key: String, value: FeatureValue) {
        let (min_key, page_rw) = self.page_tree.range(..key).last()?;
        let page = page_rw.read().await;
        page.
    }
}