use std::sync::Arc;

use log::info;
use tokio::fs::OpenOptions;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

use crate::custom_error::{BoxErr, BoxResult};
use crate::store::{Store, Storable};
use crate::store::wal::{get_wal_file_path, WalLogItem};
use bytes::{Bytes, BytesMut};
use std::error::Error;

pub async fn recover(store: &mut Store) -> BoxResult<()> {
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
        .await
        .map_err(|e| -> BoxErr { e.into() })?;

    let mut bytebuffer = BytesMut::with_capacity(1024);

    while f.read_buf(&mut bytebuffer).await? > 0 {
        info!("bytebuffer:{}", bytebuffer.len());
        let res = WalLogItem::decode(&mut bytebuffer);
        match res {
            Ok(item) => {
                info!("item:{:?}",item);
            }
            Err(e) => {
                info!("ee:{}",e)
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