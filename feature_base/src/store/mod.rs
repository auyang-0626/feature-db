use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Bytes, Cursor};
use std::sync::Arc;

use bitmaps::Bitmap;
use log::info;
use tokio::sync::{Mutex, RwLock};
use tokio::time;

use crate::calc_hash;
use crate::custom_error::{common_err, CustomResult};
use crate::feature::value::FeatureValue;
use crate::store::page::Page;
use crate::store::slot::{Slot, SLOT_NUM_BY_BIT};
use bytes::BytesMut;

pub mod wal;
pub mod page;
pub mod slot;
mod recover;


/// 页的大小
const PAGE_SIZE: u32 = 2 ^ 16;

/// 文件大小
const FILE_SIZE: u32 = 2 ^ 30;


/// store-->slot--->page--->record
pub struct Store {
    pub data_dir: String,
    pub slot_index: HashMap<u16, Slot>,
}


impl Store {
    pub async fn new(data_dir: String) -> Store {
        let mut slot_index = HashMap::new();
        for i in 0..1 << SLOT_NUM_BY_BIT {
            slot_index.insert(i, Slot::new(i, data_dir.clone()).await);
        }
        let mut store = Store {
            data_dir,
            slot_index,
        };
        recover::recover(&mut store).await.expect("恢复失败！");
        store
    }

    /// 计算slot的值
    pub fn get_slot(&self, key_hash: u64) -> CustomResult<&Slot> {
        let slot_id = (key_hash >> (64 - SLOT_NUM_BY_BIT)) as u16;
        self.slot_index.get(&slot_id).ok_or(common_err(format!("获取slot失败！")))
    }

    pub async fn get_page(&self, key_hash: u64) -> CustomResult<(u64, Arc<RwLock<Page>>)> {
        let slot = self.get_slot(key_hash)?;
        slot.get_page(key_hash).await
    }
}

/// 可存储的接口定义
pub trait Storable {
    /// 转为字节
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()>;

    /// 从字节中实例化
    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized;

    /// 需要的字节大小
    fn need_space(&self) -> usize;
}