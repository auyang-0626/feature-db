use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs::{File, OpenOptions};
use tokio::sync::{mpsc, Mutex, OnceCell, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::custom_error::{BoxErr, BoxResult};


static T_ID: AtomicU64 = AtomicU64::new(0);

/// 生成事务ID
pub fn generate_tid() -> u64{
    T_ID.fetch_add(1,Ordering::AcqRel)
}

/// 动作ID
static ACTION_ID: AtomicU64 = AtomicU64::new(0);

/// 生成ID
pub fn generate_action_id() -> u64 {
    ACTION_ID.fetch_add(1, Ordering::AcqRel)
}

/// 预写日志
pub struct Wal {
    pub send: Mutex<Sender<RedoLogItem>>,
    pub state:Arc<RwLock<WalState>>
}

impl Wal {
    pub async fn send_begin_log(&self, tid: u64) -> BoxResult<u64> {
        let send = self.send.lock().await;
        let action_id = generate_action_id();
        send.send(RedoLogItem {
            tid,
            kind: RedoLogKind::Begin,
            action_id,
            value: None,
        }).await.map_err(|e| -> BoxErr{ e.into() })?;

        Ok(action_id)
    }

    pub fn start_write(&self, f: File, mut rx: Receiver<RedoLogItem>) {
        let state = self.state.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                println!("GOT = {:?}", message);
                let mut lock = state.write().await;
                (*lock).action_log_stored_num = message.action_id;
            }
        });
    }
}

pub struct WalState {
    pub action_log_stored_num: u64,
}

impl WalState {
    pub fn new() -> WalState {
        WalState{ action_log_stored_num: 0 }
    }
}

pub async fn crate_wal(data_dir: String) -> BoxResult<Wal> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(format!("{}/redo.log", data_dir))
        .await
        .map_err(|e| -> BoxErr { e.into() })?;

    let (tx, rx): (Sender<RedoLogItem>, Receiver<RedoLogItem>) = mpsc::channel(10000);
    let state = WalState::new();

    let wal = Wal{ send: Mutex::new(tx), state:Arc::new(RwLock::new(state)) };
    wal.start_write(f,rx);
    Ok(wal)
}


#[derive(Debug)]
pub enum RedoLogKind {
    Begin,
    FeatureUpdate,
    Commit,
    End,
}

#[derive(Debug)]
pub struct RedoLogItem {
    pub tid: u64,
    pub kind: RedoLogKind,
    pub action_id: u64,
    pub value: Option<String>,
}

