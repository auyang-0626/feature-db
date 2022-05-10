use std::error::Error;
use std::path::Path;

use tokio::fs::{File, OpenOptions};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::config::Config;
use crate::custom_error::{BoxErr, BoxResult};

pub struct RedoLog {
    pub log_file: File,
    pub send: Sender<String>,
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

        let (tx, mut rx): (Sender<String>, Receiver<String>) = mpsc::channel(10000);

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                println!("GOT = {}", message);
            }
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

pub async fn send_tx_begin_log(send: &Sender<String>, tid: u64) ->BoxResult<()> {
    send.send(format!("{:?},{}\n", RedoLogKind::Begin, tid))
        .await.map_err(|e|e.into())
}