use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;

use bytes::{Buf, BufMut, BytesMut};
use log::info;
use serde::Serialize;

use crate::calc_hash;
use crate::custom_error::{common_err, CustomResult};
use crate::feature::value::FeatureValue;
use crate::store::{Dirty, Storable, Store};
use crate::store::slot::{PAGE_SIZE, Slot};

/// 页
#[derive(Debug, Serialize)]
pub struct Page {
    pub slot_id: u16,
    pub id: u64,
    pub data: BTreeMap<String, FeatureValue>,
    pub min_pk: u64,
    pub max_pk: u64,
    /// 是否是脏页
    #[serde(skip_serializing)]
    pub dirty: Dirty,
}

impl Page {
    pub fn new(slot_id: u16, id: u64, min_pk: u64, max_pk: u64) -> Page {
        Page {
            slot_id,
            id,
            data: BTreeMap::new(),
            min_pk,
            max_pk,
            dirty: Dirty::new(),
        }
    }

    pub async fn get(&self, key: &String) -> Option<&FeatureValue> {
        self.data.get(key)
    }
    pub async fn put(&mut self, key: String, value: FeatureValue) -> CustomResult<()> {
       // info!("page[{},{}] key len:{},insert key:{}", self.slot_id,self.id,self.data.keys().len(),&key);
        self.data.insert(key, value);
        Ok(())
    }

    /// 更新page后调用，参数为数据变更的大小，可为负值
    pub async fn after_update(&mut self, action_id: u64, store: &Store) {
        if self.dirty.update(action_id).await {
            // 首次发生改动，加入slot的 dirty_pasge，等待刷到磁盘
            if let Some(slot) = store.slot_index.get(&self.slot_id) {
                let mut dp = slot.dirty_pages.lock().await;
                dp.push(self.min_pk);
                info!("加入 dirty 列表:{}", self.id);
            }
        }
    }

    pub async fn split(&self, slot: &Slot) -> CustomResult<Vec<Page>> {
        let mut pages = vec![];

        let mut data = BTreeMap::new();
        for (k, v) in &self.data {
            let hash = calc_hash(k);
            let entry = data.entry(hash).or_insert(vec![]);
            entry.push((k.clone(), v.clone()));
        }

        let mut new_page = slot.new_page(self.min_pk, self.max_pk).await?;
        for (hash, values) in data {
            if new_page.need_space() > (PAGE_SIZE / 2) as usize {
                new_page.max_pk = hash;
                pages.push(new_page);

                new_page = slot.new_page(hash, self.max_pk).await?;
            }
            for (k, v) in values {
                new_page.data.insert(k, v);
            }
        }

        pages.push(new_page);
        Ok(pages)
    }
}

impl Storable for Page {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        let size = self.need_space() as u64;
        buf.put_u64(size);
        buf.put_u16(self.slot_id);
        buf.put_u64(self.id as u64);
        buf.put_u64(self.min_pk);
        buf.put_u64(self.max_pk);

        for (k, v) in &self.data {
            buf.put_u16(k.len() as u16);
            buf.put(k.as_bytes());
            v.encode(buf)?;
        }
        Ok(())
    }

    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized {
        if buf.remaining() < 8 {
            return Err(common_err(format!("page数据格式非法，解析失败！")));
        }
        let size = buf.get_u64();
        let slot_id = buf.get_u16();
        let page_id = buf.get_u64();
        let min_key = buf.get_u64();
        let max_key = buf.get_u64();
        let mut page = Page::new(slot_id, page_id, min_key, max_key);

        while buf.remaining() > 0 {
            let key_len = buf.get_u16();

            // buf.read
            let bytes = buf.copy_to_bytes(key_len as usize);
            let key = String::from_utf8(bytes.to_vec())?;

            let value = FeatureValue::decode(buf)?;
            page.data.insert(key, value);
        }

        Ok(page)
    }

    fn need_space(&self) -> usize {
        let mut space = 8 + 2 + 8 + 8 + 8;
        for (k, v) in &self.data {
            space = space + 2 + k.len() + v.need_space();
        }
        space
    }
}