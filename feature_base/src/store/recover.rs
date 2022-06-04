use std::sync::Arc;

use tokio::sync::RwLock;

use crate::custom_error::BoxResult;
use crate::store::Store;

pub async fn recover(store: &mut Store) -> BoxResult<()> {
    //todo 从磁盘恢复
    //初始化
    for (slot_id, slot) in &store.slot_index {
        let page = slot.new_page().await?;
        slot.page_tree.write().await
            .insert(0, Arc::new(RwLock::new(page)));
    }
    Ok(())
}