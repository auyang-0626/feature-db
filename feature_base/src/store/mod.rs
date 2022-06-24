use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::{ RwLock};
use serde::{Deserialize, Serialize};
use crate::custom_error::{common_err, CustomResult};
use crate::store::page::Page;
use crate::store::slot::{Slot, SLOT_NUM_BY_BIT};
use crate::store::wal::Wal;

pub mod wal;
pub mod page;
pub mod slot;
mod recover;

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
        // let slot_id = (key_hash >> (64 - SLOT_NUM_BY_BIT)) as u16;
        let slot_id = 0;
        self.slot_index.get(&slot_id).ok_or(common_err(format!("获取slot失败！")))
    }

    pub async fn get_page(&self, key_hash: u64) -> CustomResult<(u64, Arc<RwLock<Page>>)> {
        let slot = self.get_slot(key_hash)?;
        slot.get_page(key_hash).await
    }

    pub async fn check_point(&self, wal: &Wal) -> CustomResult<()> {
        for (_, slot) in &self.slot_index {
            slot.store_page(wal).await?;
            slot.store_page_index(wal).await?;
        }

        Ok(())
    }
}

/// 可存储的接口定义
pub trait Storable: Any + Debug + Send + Sync + Downcast {
    /// 转为字节
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()>;

    /// 从字节中实例化
    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized;

    /// 需要的字节大小
    fn need_space(&self) -> usize;
}


pub trait Downcast: Any {
    fn as_any(&mut self) -> &mut dyn Any;
}

impl<T: Any> Downcast for T {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

/// 数据被改变的记录
#[derive(Debug, Serialize, Deserialize)]
pub struct DirtyRecord {
    pub first_action_id: u64,
    pub last_action_id: u64,
}

#[derive(Debug)]
pub struct Dirty( RwLock<Option<DirtyRecord>>);

impl Dirty {
    pub fn new() -> Dirty {
        Dirty(RwLock::new(None))
    }

    pub async fn is_dirty(&self) -> bool {
        self.0.read().await.is_some()
    }

    pub async fn update(&self, action_id: u64) -> bool{
        // 是否是第一次改动
        let mut first_update = false;

        let mut op = self.0.write().await;
        *op = match *op {
            None => {
                first_update = true;
                Some(DirtyRecord {
                    first_action_id: action_id,
                    last_action_id: action_id,
                })
            }
            Some(ref record) => {
                Some(
                    DirtyRecord {
                        first_action_id: record.first_action_id,
                        last_action_id: action_id,
                    }
                )
            }
        };
        first_update
    }

    pub async fn reset(&self){
        let mut op = self.0.write().await;
        *op = None
    }
}