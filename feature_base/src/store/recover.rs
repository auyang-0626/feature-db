
use std::io::{Cursor, SeekFrom};
use std::sync::Arc;

use bytes::{ BytesMut};
use log::{info, warn};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;

use crate::custom_error::{ CustomResult, DECODE_FAILED_BY_INSUFFICIENT_DATA_CODE};
use crate::store::{Storable, Store};
use crate::store::wal::{get_wal_file_path, WalLogItem};

pub async fn recover(store: &mut Store) -> CustomResult<()> {
    // 初始化
    for (slot_id, slot) in &store.slot_index {
        let page = slot.new_page(0,1<<64-1).await?;
        slot.page_tree.write().await
            .insert(0, Arc::new(RwLock::new(page)));
    }
    // 从磁盘恢复
    let wal_log_path = get_wal_file_path(store.data_dir.clone());

    let mut f = OpenOptions::new()
        .read(true)
        .open(wal_log_path)
        .await?;

    let mut buf = BytesMut::with_capacity(1024);
    let mut before_pos = 0;
    let mut pos = 0;

    loop {
        let res = {
            let mut cursor: Cursor<&[u8]> = Cursor::new(&*buf);
            cursor.seek(SeekFrom::Start(before_pos)).await?;
            let res = WalLogItem::decode(&mut cursor);
            pos = cursor.position();
            res
        };
        match res {
            Ok(item) => {
                before_pos = pos;
                info!("item:{:?}", item);
            }
            Err(e) => {
                if e.code == DECODE_FAILED_BY_INSUFFICIENT_DATA_CODE {
                    let mut buf2 = BytesMut::with_capacity(1024);
                    if f.read_buf(&mut buf2).await? == 0 {
                        break;
                    } else {
                        buf.extend(buf2);
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
    use crate::config::Config;
    use crate::store::{Store};


    #[test]
    pub fn test_recover() {
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