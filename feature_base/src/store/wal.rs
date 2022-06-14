use std::convert::{Infallible, TryFrom, TryInto};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{BufMut, BytesMut, Buf};
use log::{info, warn};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex, OnceCell, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time;

use crate::custom_error::{decode_failed_by_insufficient_data_err, CustomResult, CustomError};
use crate::feature::value::ValueKind;
use crate::store::Storable;
use std::io::Cursor;

static T_ID: AtomicU64 = AtomicU64::new(0);
pub static Wal_File_Name: &str = "";

pub fn get_wal_file_path(data_dir: String) -> String {
    format!("{}/redo.log", data_dir)
}

/// 生成事务ID
pub fn generate_tid() -> u64 {
    T_ID.fetch_add(1, Ordering::AcqRel)
}

/// 动作ID
static ACTION_ID: AtomicU64 = AtomicU64::new(0);

/// 生成ID
pub fn generate_action_id() -> u64 {
    ACTION_ID.fetch_add(1, Ordering::AcqRel)
}

/// 预写日志
pub struct Wal {
    pub send: Mutex<Sender<WalLogItem>>,
    pub state: Arc<RwLock<WalState>>,
}

impl Wal {
    pub async fn send_log(&self, tid: u64, kind: WalLogKind, value: Option<WalFeatureUpdateValue>) -> CustomResult<u64> {
        let send = self.send.lock().await;
        let action_id = generate_action_id();
        send.send(WalLogItem {
            tid,
            kind,
            action_id,
            value,
        }).await?;

        Ok(action_id)
    }

    pub async fn send_begin_log(&self, tid: u64) -> CustomResult<u64> {
        self.send_log(tid, WalLogKind::Begin, None).await
    }

    pub async fn send_feature_update_log(&self, tid: u64, value: WalFeatureUpdateValue) -> CustomResult<u64> {
        self.send_log(tid, WalLogKind::FeatureUpdate, Some(value)).await
    }

    pub async fn commit_log(&self, tid: u64) -> CustomResult<()> {
        let action_id = self.send_log(tid, WalLogKind::Commit, None).await?;
        let mut interval = time::interval(time::Duration::from_millis(10));
        loop {
            interval.tick().await;
            let state = self.state.read().await;
            info!("stored_num:{},action_id:{}", state.stored_num, action_id);
            if state.stored_num >= action_id {
                info!("commit_log:{}",tid);
                return Ok(());
            }
        }
    }

    pub fn start_write(&self, mut f: File, mut rx: Receiver<WalLogItem>) {
        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let mut buf = BytesMut::new();
                match message.encode(&mut buf) {
                    Ok(_) => {
                        f.write_buf(&mut buf).await;
                    }
                    Err(e) => {
                        warn!("序列化失败:{:?}", message);
                    }
                }
               // if message.action_id % 100 == 0 {
                    f.sync_data().await;
                    let mut lock = state.write().await;
                    (*lock).stored_num = message.action_id;
               // }
            }
        });
    }
}

pub struct WalState {
    pub stored_num: u64,
}

impl WalState {
    pub fn new() -> WalState {
        WalState { stored_num: 0 }
    }
}

pub async fn crate_wal(data_dir: String) -> CustomResult<Wal> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(get_wal_file_path(data_dir))
        .await?;

    let (tx, rx): (Sender<WalLogItem>, Receiver<WalLogItem>) = mpsc::channel(10000);
    let state = WalState::new();

    let wal = Wal { send: Mutex::new(tx), state: Arc::new(RwLock::new(state)) };
    wal.start_write(f, rx);
    Ok(wal)
}


#[repr(u8)]
#[derive(Debug, TryFromPrimitive, IntoPrimitive, Clone)]
pub enum WalLogKind {
    Begin = 1,
    Commit = 2,
    End = 3,

    // 指标更新
    FeatureUpdate = 4,

    // 申请page
    PageNew = 5,
    // page刷盘到缓存文件
    PageBufferSync = 6,
    // page刷盘到数据文件
    PageSync = 7,
}

#[derive(Debug)]
pub struct WalLogItem {
    pub tid: u64,
    pub kind: WalLogKind,
    pub action_id: u64,
    pub value: Option<WalFeatureUpdateValue>,
}

impl Storable for WalLogItem {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        buf.put_u32(self.need_space() as u32);
        buf.put_u64(self.tid);
        let kind: u8 = self.kind.clone().into();
        buf.put_u8(kind);
        buf.put_u64(self.action_id);

        match &self.value {
            None => {}
            Some(v) => {
                v.encode(buf)?;
            }
        }
        Ok(())
    }

    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized {
        if buf.remaining() < 4 {
            return Err(decode_failed_by_insufficient_data_err());
        }
        let item_len = buf.get_u32();
        if buf.remaining() < item_len as usize {
            return Err(decode_failed_by_insufficient_data_err());
        }
        let tid = buf.get_u64();
        let kind = WalLogKind::try_from(buf.get_u8())?;
        let action_id = buf.get_u64();
        let value = match kind {
            WalLogKind::FeatureUpdate => {
                Some(WalFeatureUpdateValue::decode(buf)?)
            }
            _ => None
        };
        Ok(WalLogItem{
            tid,
            kind,
            action_id,
            value
        })
    }

    fn need_space(&self) -> usize {
        8 + 1 + 8 + match &self.value {
            None => 0,
            Some(v) => v.need_space()
        }
    }
}


#[derive(Debug)]
pub struct WalFeatureUpdateValue {
    pub key: u64,
    pub undo_v: Option<ValueKind>,
    pub redo_v: ValueKind,
}

impl Storable for WalFeatureUpdateValue {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        buf.put_u64(self.key);
        match &self.undo_v {
            None => {
                buf.put_u8(0);
            }
            Some(v) => {
                buf.put_u8(1);
                v.encode(buf)?;
            }
        }
        self.redo_v.encode(buf)
    }

    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized {
        let key = buf.get_u64();
        let undo_v_flag = buf.get_u8();
        let undo_v = if (undo_v_flag == 0){
            None
        } else {
            Some(ValueKind::decode(buf)?)
        };
        let redo_v = ValueKind::decode(buf)?;
        Ok(WalFeatureUpdateValue{
            key,
            undo_v,
            redo_v
        })
    }

    fn need_space(&self) -> usize {
        8 + match &self.undo_v {
            None => 1,
            Some(v) => {
                1 + v.need_space()
            }
        } + self.redo_v.need_space()
    }
}

