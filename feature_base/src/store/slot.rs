use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use bitmaps::Bitmap;
use tokio::sync::{Mutex, RwLock};

use crate::custom_error::{BoxResult, common_err};
use crate::feature::value::FeatureValue;
use crate::store::page::Page;

/// 每个slot最多拥有的page数量，乘以 PAGE_SIZE = slot最多存储的数据量
const PAGE_NUM: usize = 2 ^ 18;
/// slot数量的bit表示法，即 2^12
pub const SLOT_NUM_BY_BIT: u16 = 12;

/// 分片
#[derive(Debug)]
pub struct Slot {
    pub id: u16,
    /// page空闲列表
    pub page_bit_map: Bitmap<PAGE_NUM>,
    /// 只有在 分裂/合并page时，才需要获取page的写锁，其它一律读锁
    pub page_tree: BTreeMap<u64, Arc<RwLock<Page>>>,
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
        slot.new_page(0).await;
        slot
    }

    pub async fn new_page(&mut self, min_key: u64) -> BoxResult<()> {
        self.page_create_lock.lock().await;
        let page_id = self.page_bit_map.first_false_index().ok_or(common_err(format!("分配page失败！")))?;
        let next_page = Page::new(page_id);
        self.page_bit_map.set(page_id, true);
        self.page_tree.insert(min_key, Arc::new(RwLock::new(next_page)));
        Ok(())
    }

    pub fn get_page(&self, key_hash: u64) -> BoxResult<(u64, Arc<RwLock<Page>>)> {
        let (mk, page) = self.page_tree.range(..key_hash).last()
            .ok_or(common_err(format!("找不到对应的page:{}", key_hash)))?;
        Ok((mk.clone(), page.clone()))
    }

    // pub fn get(&self, key_hash: u64, key: &String) -> Option<&FeatureValue> {
    //     let (key, page) = self.page_tree.range(..key_hash).last()?;
    //
    //
    //     None
    // }
    //
    // pub async fn put(&self, key_hash: u64, key: String, value: FeatureValue) -> BoxResult<()> {
    //     let (min_key, page_rw) = self.page_tree.range(..key_hash).last()
    //         .ok_or(common_err(format!("找不到对应的page:{}", &key)))?;
    //     let mut page = page_rw.write().await;
    //     page.put(key, value).await
    // }
}