
use std::convert::{ TryFrom};

use std::io::Cursor;
use std::option::Option::Some;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Buf, BufMut, BytesMut};
use log::{info, warn};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex, oneshot, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};


use crate::custom_error::{common_err, CustomResult, decode_failed_by_insufficient_data_err};
use crate::feature::value::ValueKind;
use crate::store::Storable;
use std::fmt::Debug;

pub type Callback = oneshot::Sender<u64>;

impl Storable for Callback {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        Ok(())
    }

    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized {
        Err(common_err(format!("Callback 不支持decode")))
    }

    fn need_space(&self) -> usize {
        0
    }
}

pub fn get_wal_file_path(data_dir: String) -> String {
    format!("{}/redo.log", data_dir)
}

static T_ID: AtomicU64 = AtomicU64::new(0);

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
    pub async fn send_log(&self, tid: u64, kind: WalLogKind, value: Option<Box<dyn Storable>>,callback:Option<Callback>) -> CustomResult<u64> {
        let send = self.send.lock().await;
        let action_id = generate_action_id();
        send.send(WalLogItem {
            tid,
            kind,
            action_id,
            value,
            callback
        }).await?;

        Ok(action_id)
    }

    pub async fn send_begin_log(&self, tid: u64) -> CustomResult<u64> {
        self.send_log(tid, WalLogKind::Begin, None,None).await
    }

    pub async fn send_feature_update_log(&self, tid: u64, value: WalFeatureUpdateValue) -> CustomResult<u64> {
        self.send_log(tid, WalLogKind::FeatureUpdate, Some(Box::new(value)),None).await
    }

    pub async fn commit_log(&self, tid: u64) -> CustomResult<()> {
        let (tx, rx) = oneshot::channel();
        let action_id = self.send_log(tid, WalLogKind::Commit,  None,Some(tx)).await?;

        let tid: u64 = rx.await?;
        info!("commit_log:{}", tid);
        Ok(())
    }


    pub fn start_write(&self, mut f: File, mut rx: Receiver<WalLogItem>) {
        let state = self.state.clone();

        tokio::spawn(async move {
            loop {
                if let Some(message) = rx.recv().await {
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

                    if let Some(callback) = message.callback {
                        callback.send(message.tid);
                        info!("send callback :{:?}", message.tid);
                    }
                } else {
                    info!("recv none");
                }
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

    let (tx, rx): (Sender<WalLogItem>, Receiver<WalLogItem>) = mpsc::channel(100);
    let state = WalState::new();

    let wal = Wal { send: Mutex::new(tx), state: Arc::new(RwLock::new(state)) };
    wal.start_write(f, rx);
    Ok(wal)
}


#[repr(u8)]
#[derive(Debug, TryFromPrimitive, IntoPrimitive, Clone,PartialEq)]
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
    pub value: Option<Box<dyn Storable>>,
    pub callback:Option<Callback>
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
        info!("item_len:{},buf.len:{}", item_len, buf.remaining());
        if buf.remaining() < item_len as usize {
            return Err(decode_failed_by_insufficient_data_err());
        }
        let tid = buf.get_u64();
        let kind = WalLogKind::try_from(buf.get_u8())?;
        let action_id = buf.get_u64();
        let value: Option<Box<dyn Storable>> = match kind {
            WalLogKind::FeatureUpdate => {
                Some(Box::new(WalFeatureUpdateValue::decode(buf)?))
            }
            _ => None
        };
        Ok(WalLogItem {
            tid,
            kind,
            action_id,
            value,
            callback:None,
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
    // feature key
    pub fk: String,
    // 时间分片key
    pub tk: u64,
    pub undo_v: Option<ValueKind>,
    pub redo_v: ValueKind,
}

impl Storable for WalFeatureUpdateValue {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        buf.put_u32(self.fk.len() as u32);
        buf.put(self.fk.as_bytes());
        buf.put_u64(self.tk);
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
        let fk_len = buf.get_u32();
        let fk = String::from_utf8(buf.copy_to_bytes(fk_len as usize).to_vec())?;
        let tk = buf.get_u64();
        let undo_v_flag = buf.get_u8();
        let undo_v = if undo_v_flag == 0 {
            None
        } else {
            Some(ValueKind::decode(buf)?)
        };
        let redo_v = ValueKind::decode(buf)?;
        Ok(WalFeatureUpdateValue {
            fk,
            tk,
            undo_v,
            redo_v,
        })
    }

    fn need_space(&self) -> usize {
        4 + self.fk.len() + 8 + match &self.undo_v {
            None => 1,
            Some(v) => {
                1 + v.need_space()
            }
        } + self.redo_v.need_space()
    }
}


