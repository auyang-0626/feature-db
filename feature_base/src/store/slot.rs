use std::collections::BTreeMap;


use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bitmaps::Bitmap;
use bytes::{BufMut, BytesMut};
use tokio::sync::{Mutex, RwLock};

use crate::custom_error::{common_err, CustomResult};

use crate::store::page::Page;
use crate::store::wal::{generate_tid, Wal};



/// 每个slot最多拥有的page数量，乘以 PAGE_SIZE = slot最多存储的数据量
const PAGE_NUM: usize = 2 ^ 18;
/// slot数量的bit表示法，即 2^12
pub const SLOT_NUM_BY_BIT: u16 = 12;

/// 分片
#[derive(Debug)]
pub struct Slot {
    pub id: u16,
    pub data_dir: String,
    /// page空闲列表
    pub page_bit_map: Mutex<Bitmap<PAGE_NUM>>,
    /// 只有在 分裂/合并page时，才需要获取page的写锁，其它一律读锁
    pub page_tree: RwLock<BTreeMap<u64, Arc<RwLock<Page>>>>,
    pub dirty: AtomicBool,
}

impl Slot {
    pub async fn new(id: u16, data_dir: String) -> Slot {
        let slot = Slot {
            id,
            data_dir,
            page_bit_map: Mutex::new(Bitmap::new()),
            page_tree: RwLock::new(BTreeMap::new()),
            dirty: AtomicBool::new(false),
        };
        slot
    }

    /// 创建新的page,不插入索引树中
    pub async fn new_page(&self) -> CustomResult<Page> {
        // 申请新的page id
        let mut bitmap = self.page_bit_map.lock().await;
        let page_id = bitmap.first_false_index().ok_or(common_err(format!("分配page失败！")))?;
        bitmap.set(page_id, true);

        let next_page = Page::new(self.id, page_id as u64);
        Ok(next_page)
    }

    pub async fn get_page(&self, key_hash: u64) -> CustomResult<(u64, Arc<RwLock<Page>>)> {
        let page_tree = self.page_tree.read().await;
        let (mk, page) = page_tree.range(..key_hash).last()
            .ok_or(common_err(format!("找不到对应的page:{}", key_hash)))?;
        Ok((mk.clone(), page.clone()))
    }

    pub async fn store_page_index(&self, wal: &Wal)->CustomResult<()> {
        let tid = generate_tid();
        wal.send_begin_log(tid).await?;

        let mut buf = BytesMut::new();
        let page_tree = self.page_tree.read().await;
        for (k, v) in page_tree.iter() {
            buf.put_u64(k.clone());
            buf.put_u64(v.read().await.id);
        }

        // let mut f = OpenOptions::new()
        //     .read(true)
        //     .write(true)
        //     .create(true)
        //     .truncate(true)
        //     .open(wal_log_path)
        //     .await?;
        // f.write_buf(&mut buf).await?;
        // f.sync_data().await?;


        Ok(())
    }

    fn get_slot_index_path(&self) -> String {
        format!("{}/slot_{}_index", self.data_dir, self.id)
    }

    fn get_slot_index_path_bk(&self) -> String {
        self.get_slot_index_path() + "_bk"
    }
}