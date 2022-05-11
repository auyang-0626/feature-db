

use tokio::fs::{File, OpenOptions};
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::custom_error::{BoxErr, BoxResult};

pub struct RedoLog {
    pub log_file: File,
    pub send: Sender<RedoLogItem>,
}

impl RedoLog {
    pub async fn new(data_dir: String) -> BoxResult<RedoLog> {
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(format!("{}/redo.log", data_dir))
            .await
            .map_err(|e| -> BoxErr { e.into() })?;

        let (tx, rx): (Sender<RedoLogItem>, Receiver<RedoLogItem>) = mpsc::channel(10000);

        tokio::spawn(async move {
            consumer_and_store_log(rx).await;
        });


        Ok(RedoLog {
            log_file: f,
            send: tx,
        })
    }
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
    pub lid: Option<u64>,
    pub value: Option<String>,
}

impl RedoLogItem {
    pub fn new_begin_log_item(tid: u64) -> RedoLogItem {
        RedoLogItem {
            tid,
            kind: RedoLogKind::Begin,
            lid: None,
            value: None,
        }
    }
}

lazy_static! {
    static ref REDO_LOCK: Mutex<u64> = Mutex::new(0);
}


pub async fn send_log(send: &Sender<RedoLogItem>, mut item: RedoLogItem) -> BoxResult<u64> {
    let mut log_num = REDO_LOCK.lock().await;
    *log_num += 1;
    item.lid = Some(*log_num);
    send.send(item)
        .await.map_err(|e| -> BoxErr { e.into() });
    Ok(*log_num)
}

pub async fn consumer_and_store_log(mut rx: Receiver<RedoLogItem>) {
    while let Some(message) = rx.recv().await {
        println!("GOT = {:?}", message);
    }
}