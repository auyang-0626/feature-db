use std::sync::Arc;

use log::{info,warn};
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;


use crate::store::{Store, Storable};
use crate::store::wal::{get_wal_file_path, WalLogItem};
use bytes::{Bytes, BytesMut, Buf};
use std::error::Error;
use crate::custom_error::{CustomResult, CustomError, DECODE_FAILED_BY_INSUFFICIENT_DATA_CODE};
use std::io::Cursor;

pub async fn recover(store: &mut Store) -> CustomResult<()> {
    //初始化
    for (slot_id, slot) in &store.slot_index {
        let page = slot.new_page().await?;
        slot.page_tree.write().await
            .insert(0, Arc::new(RwLock::new(page)));
    }
    //todo 从磁盘恢复
    let wal_log_path = get_wal_file_path(store.data_dir.clone());

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(wal_log_path)
        .await?;

    let mut buf = BytesMut::with_capacity(1024);

    loop {

        let res = WalLogItem::decode(&mut buf);
        match res {
            Ok(item) => {
                info!("item:{:?}",item);
            }
            Err(e) => {
                if e.code == DECODE_FAILED_BY_INSUFFICIENT_DATA_CODE {
                    if f.read_buf(&mut buf).await?  == 0 {
                        break;
                    }
                } else {
                    warn!("解析出现错误:buf={},{}", buf.len(), e);
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::store::Store;
    use crate::config::Config;

    #[test]
    pub fn test_recover(){
        crate::init_log();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let config = Config {
                data_dir: "/Users/yang/feature_db".to_string()
            };

            Store::new(config.data_dir.clone()).await;
        });


    }
}