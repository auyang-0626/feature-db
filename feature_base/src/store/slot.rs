use std::cmp::min;
use std::collections::BTreeMap;
use std::io::SeekFrom;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use log::info;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

use crate::custom_error::{common_err, CustomResult};
use crate::store::{Dirty, Storable};
use crate::store::page::Page;
use crate::store::wal::{generate_tid, Wal, WalPageBkStoreValue, WalPageIndexStoreValue};
use crate::tools::bitmap::BitMap;

/// 每个slot最多拥有的page数量，乘以 PAGE_SIZE = slot最多存储的数据量
const PAGE_NUM: u32 = 1 << 18;
/// slot数量的bit表示法，即 2^12
pub const SLOT_NUM_BY_BIT: u16 = 12;

/// 页的大小
pub const PAGE_SIZE: u32 = 1 << 16;

/// 文件大小
pub const FILE_SIZE: u32 = 1 << 30;

/// 分片
#[derive(Debug)]
pub struct Slot {
    pub id: u16,
    pub data_dir: String,
    // page空闲列表
    pub page_bit_map: Mutex<BitMap>,
    // 只有在 分裂/合并page时，才需要获取page的写锁，其它一律读锁
    pub page_tree: RwLock<BTreeMap<u64, Arc<RwLock<Page>>>>,
    // 记录page的所有是否发生变更
    pub index_dirty: Dirty,
    // 已修改的page id
    pub dirty_pages: Mutex<Vec<u64>>,
}

impl Slot {
    pub async fn new(id: u16, data_dir: String) -> Slot {
        let slot = Slot {
            id,
            data_dir,
            page_bit_map: Mutex::new(BitMap::new(PAGE_NUM as u64)),
            page_tree: RwLock::new(BTreeMap::new()),
            index_dirty: Dirty::new(),
            dirty_pages: Mutex::new(Vec::new()),
        };
        slot
    }

    /// 创建新的page,不插入索引树中
    pub async fn new_page(&self, min_pk: u64, max_pk: u64) -> CustomResult<Page> {
        // 申请新的page id
        let mut bitmap = self.page_bit_map.lock().await;

        let page_id = bitmap.first_false_value().ok_or(common_err(format!("分配page失败！")))?;
        bitmap.set(page_id, true);

        let next_page = Page::new(self.id, page_id as u64, min_pk, max_pk);
        Ok(next_page)
    }

    ///释放指定page，方便复用
    pub async fn freed_page(&self, page_id: u64) {
        let mut bitmap = self.page_bit_map.lock().await;
        bitmap.set(page_id, false);
    }

    pub async fn get_page(&self, key_hash: u64) -> CustomResult<(u64, Arc<RwLock<Page>>)> {
        let page_tree = self.page_tree.read().await;
        let (mk, page) = page_tree.range(..key_hash).last()
            .ok_or(common_err(format!("找不到对应的page:{}", key_hash)))?;
        Ok((mk.clone(), page.clone()))
    }

    pub async fn store_page_index(&self, wal: &Wal) -> CustomResult<()> {
        if !self.index_dirty.is_dirty().await {
            return Ok(());
        }
        info!("store_page_index start ....");

        let tid = generate_tid();
        wal.send_begin_log(tid).await?;

        let mut buf = BytesMut::new();
        let page_tree = self.page_tree.read().await;
        for (k, v) in page_tree.iter() {
            buf.put_u64(k.clone());
            buf.put_u64(v.read().await.id);
        }

        let mut bk_f = self.get_slot_index_path_bk().await?;
        bk_f.write_buf(&mut buf).await?;
        bk_f.sync_data().await?;
        // 刷盘完成(写副本)
        wal.send_page_index_store_log(tid, WalPageIndexStoreValue::new(self.id)).await?;

        let mut cp_buf = buf.clone();
        let mut f = self.get_slot_index_path().await?;
        f.write_buf(&mut cp_buf).await?;
        f.sync_data().await?;

        self.index_dirty.reset().await;
        // 写完成，提交事务
        wal.commit_log(tid).await?;
        Ok(())
    }

    pub async fn get_wait_store_page(&self) -> Vec<Arc<RwLock<Page>>> {
        let mut dp = self.dirty_pages.lock().await;

        // 拿出来需要持久化的page
        let mut pages = vec![];
        for i in 0..min(10, dp.len()) {
            let key = dp.remove(0);

            if let Some(page) = self.page_tree.read().await.get(&key) {
                pages.push(page.clone());
            }
        }
        pages
    }

    pub async fn store_page(&self, wal: &Wal) -> CustomResult<()> {
        let pages = self.get_wait_store_page().await;

        for p in pages {
            let page = p.read().await;

            let tid = generate_tid();
            wal.send_begin_log(tid).await?;

            let mut buf = BytesMut::new();
            page.encode(&mut buf)?;
            let mut buf_bk = buf.clone();
            info!("待写入page:{},size:{}", page.id, buf.len());

            // 写入备份
            let mut shard_f = self.get_shard_page_store_file().await?;
            shard_f.write_buf(&mut buf).await;
            shard_f.sync_data().await?;
            let bk_action_id = wal.send_page_bk_store_log(tid, WalPageBkStoreValue {
                slot_id: page.slot_id,
                page_id: page.id,
                min_pk: page.min_pk,
                max_pk: page.max_pk,
            }).await?;

            if (page.need_space() as u32) < PAGE_SIZE {
                let mut page_file = self.get_page_store_file(page.id).await?;
                page_file.write_buf(&mut buf_bk).await?;
                page_file.sync_data().await?;
                page.dirty.reset().await;
                info!("保存page:{}成功！", page.id);
            } else {
                let slit_page = page.split(&self).await?;

                let slit_page_ids: Vec<u64> = slit_page.iter().map(|p| p.id).collect();
                info!("page分裂:old={},new:{:?}", page.id, slit_page_ids);

                let mut page_tree = self.page_tree.write().await;
                for p in slit_page {
                    let mut buf = BytesMut::new();
                    p.encode(&mut buf)?;
                    let mut pf = self.get_page_store_file(p.id).await?;
                    pf.write_buf(&mut buf).await?;
                    pf.sync_data().await?;

                    info!("插入page:{}:{}", p.min_pk, p.id);
                    page_tree.insert(p.min_pk, Arc::new(RwLock::new(p)));
                }

                // 释放之前的page
                self.freed_page(page.id).await;
                self.index_dirty.update(bk_action_id).await;
            }
            wal.commit_log(tid).await?;
        }

        Ok(())
    }

    async fn get_slot_index_path(&self) -> CustomResult<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!("{}/slot_{}_index", self.data_dir, self.id))
            .await?)
    }

    async fn get_slot_index_path_bk(&self) -> CustomResult<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!("{}/slot_{}_index_bk", self.data_dir, self.id))
            .await?)
    }

    /// 共享的page写文件，dubbo write的第一次写入文件
    async fn get_shard_page_store_file(&self) -> CustomResult<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(format!("{}/slot_{}_shard", self.data_dir, self.id))
            .await?)
    }

    /// 共享的page写文件，dubbo write的第一次写入文件
    async fn get_page_store_file(&self, page_id: u64) -> CustomResult<File> {
        let page_id = page_id as u32;
        let file_index = PAGE_SIZE * page_id / FILE_SIZE;
        let seek_pos = PAGE_SIZE * page_id % FILE_SIZE;

        let mut page_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/slot_{}_page_{}", self.data_dir, self.id, file_index))
            .await?;
        page_file.seek(SeekFrom::Start(seek_pos as u64)).await?;
        Ok(page_file)
    }
}
