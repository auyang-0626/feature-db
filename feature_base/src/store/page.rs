use std::collections::BTreeMap;
use std::sync::Arc;

use bytebuffer::ByteBuffer;
use tokio::sync::{Mutex, RwLock};

use crate::custom_error::{BoxResult, common_err, BoxErr};
use crate::feature::value::FeatureValue;
use crate::store::Storable;
use serde::{Serialize,Deserialize};


/// 页
#[derive(Debug,Serialize,Deserialize)]
pub struct Page {
    pub slot_id: u16,
    pub id: usize,
    pub data: BTreeMap<String, FeatureValue>,
    pub need_space:u32,
}

impl Page {
    pub fn new(slot_id: u16, id: usize) -> Page {
        Page {
            slot_id,
            id,
            data: BTreeMap::new(),
            need_space:0
        }
    }

    pub async fn get(&self, key: &String) -> Option<&FeatureValue> {
        self.data.get(key)
    }
    pub async fn put(&mut self, key: String, value: FeatureValue) -> BoxResult<()> {
        self.data.insert(key, value);
        Ok(())
    }
}

impl Storable for Page {
    fn encode(&self, buf: &mut ByteBuffer) {
        buf.write_u16(self.slot_id);
        buf.write_u64(self.id as u64);
        for (k, v) in &self.data {
            buf.write_u16(k.len()  as u16);
            buf.write_bytes(k.as_bytes());
            v.encode(buf);
        }
    }

    fn decode(buf: &mut ByteBuffer) -> BoxResult<Self> where Self: Sized {
        if buf.get_wpos() - buf.get_rpos() < 10 {
            return Err(common_err(format!("page数据格式非法，解析失败！")))
        }
        let slot_id = buf.read_u16();
        let page_id = buf.read_u64();
        let mut  page = Page::new(slot_id, page_id as usize);

        while buf.get_rpos() < buf.get_wpos() {
            let key_len = buf.read_u16();
            let key = String::from_utf8(buf.read_bytes(key_len as usize))
                .map_err(|e|->BoxErr {e.into()})?;
            let value = FeatureValue::decode(buf)?;
            page.data.insert(key,value);
        }

        Ok(page)
    }

    fn need_space(&self) -> usize {
        self.need_space as usize
    }
}