use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::custom_error::{common_err, CustomResult, CustomError};
use crate::feature::value::FeatureValue;
use crate::store::Storable;
use bytes::{BytesMut, BufMut, Buf};

/// 页
#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub slot_id: u16,
    pub id: usize,
    pub data: BTreeMap<String, FeatureValue>,
    /// 是否是脏页
    pub dirty: bool,
}

impl Page {
    pub fn new(slot_id: u16, id: usize) -> Page {
        Page {
            slot_id,
            id,
            data: BTreeMap::new(),
            dirty:false
        }
    }

    pub async fn get(&self, key: &String) -> Option<&FeatureValue> {
        self.data.get(key)
    }
    pub async fn put(&mut self, key: String, value: FeatureValue) -> CustomResult<()> {
        self.data.insert(key, value);
        Ok(())
    }

    /// 更新page后调用，参数为数据变更的大小，可为负值
    pub fn after_update(&mut self) {
        self.dirty = true;
    }
}

impl Storable for Page {
    fn encode(&self, buf: &mut BytesMut) ->CustomResult<()>{
        buf.put_u16(self.slot_id);
        buf.put_u64(self.id as u64);
        for (k, v) in &self.data {
            buf.put_u16(k.len() as u16);
            buf.put(k.as_bytes());
            v.encode(buf);
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> CustomResult<Self> where Self: Sized {
        if buf.len() < 10 {
            return Err(common_err(format!("page数据格式非法，解析失败！")));
        }
        let slot_id = buf.get_u16();
        let page_id = buf.get_u64();
        let mut page = Page::new(slot_id, page_id as usize);

        while buf.len() > 0 {
            let key_len = buf.get_u16();

            let bytes = buf.split_to(key_len as usize);
            let key = String::from_utf8(bytes.to_vec())?;

            let value = FeatureValue::decode(buf)?;
            page.data.insert(key, value);
        }

        Ok(page)
    }

    fn need_space(&self) -> usize {
        0
    }
}