use std::collections::BTreeMap;
use std::io::Cursor;


use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};


use crate::custom_error::{common_err, CustomResult};
use crate::feature::value::FeatureValue;
use crate::store::Storable;

/// 页
#[derive(Debug, Serialize, Deserialize)]
pub struct Page {
    pub slot_id: u16,
    pub id: u64,
    pub data: BTreeMap<String, FeatureValue>,
    /// 是否是脏页
    pub dirty: bool,
}

impl Page {
    pub fn new(slot_id: u16, id: u64) -> Page {
        Page {
            slot_id,
            id,
            data: BTreeMap::new(),
            dirty: false,
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
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        buf.put_u16(self.slot_id);
        buf.put_u64(self.id as u64);
        for (k, v) in &self.data {
            buf.put_u16(k.len() as u16);
            buf.put(k.as_bytes());
            v.encode(buf);
        }
        Ok(())
    }

    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized {
        if buf.remaining() < 10 {
            return Err(common_err(format!("page数据格式非法，解析失败！")));
        }
        let slot_id = buf.get_u16();
        let page_id = buf.get_u64();
        let mut page = Page::new(slot_id, page_id);

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
        let mut space = 2 + 8;
        for (k, v) in &self.data {
            space = space + 2 + k.len() + v.need_space();
        }
        space
    }
}