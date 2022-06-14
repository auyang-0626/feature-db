use std::cell::RefCell;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, Serializer};
use tokio::io::AsyncWriteExt;

use crate::custom_error::{common_err, CustomResult};
use crate::store::Storable;
use crate::store::wal::{WalLogItem, Wal, WalFeatureUpdateValue};
use bytes::{BytesMut, BufMut, Buf};
use std::io::Cursor;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ValueKind {
    Int(u64),
    Float(f64),
}

/// ValueKind序列号的代码
const VALUE_KIND_INT: u8 = 1;
const VALUE_KIND_FLOAT: u8 = 2;

impl Storable for ValueKind {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        match self {
            ValueKind::Int(v) => {
                buf.put_u8(VALUE_KIND_INT);
                buf.put_u64(*v);
            }
            ValueKind::Float(v) => {
                buf.put_u8(VALUE_KIND_FLOAT);
                buf.put_f64(*v);
            }
        };
        Ok(())
    }
    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<ValueKind> {
        let kind_num = buf.get_u8();
        match kind_num {
            VALUE_KIND_INT => Ok(ValueKind::Int(buf.get_u64())),
            VALUE_KIND_FLOAT => Ok(ValueKind::Float(buf.get_f64())),
            _ => Err(common_err(format!("反序列化失败，不识别的kind_num：{}", kind_num)))
        }
    }

    fn need_space(&self) -> usize {
        9 as usize
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FeatureValue(BTreeMap<u64, ValueKind>);

impl FeatureValue {
    pub fn new() -> FeatureValue {
        FeatureValue(BTreeMap::new())
    }

    pub fn add_int(&self, time: u64, window_size: u64, value: u64) ->CustomResult<WalFeatureUpdateValue> {
        let t = time - time % window_size;

        let res = match self.0.get(&t) {
            None => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                let new_value = ValueKind::Int(value);
                mutable_t.0.insert(t, new_value.clone());

                Ok(WalFeatureUpdateValue{
                    key: t,
                    undo_v: None,
                    redo_v: new_value
                })
            }
            Some(value_kind) => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                if let ValueKind::Int(v) = value_kind {
                    let new_value = ValueKind::Int(v + value);
                    mutable_t.0.insert(t, new_value.clone());
                    Ok(WalFeatureUpdateValue{
                        key: t,
                        undo_v: Some(ValueKind::Int(*v)),
                        redo_v: new_value
                    })
                } else {
                    Err(common_err(format!("value_kind 类型不匹配！")))
                }
            }
        };
        res
    }

    pub fn add_float(&self, time: u64, window_size: u64, value: f64) {
        let t = time - time % window_size;

        match self.0.get(&t) {
            None => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                mutable_t.0.insert(t, ValueKind::Float(value));
            }
            Some(value_kind) => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                if let ValueKind::Float(v) = value_kind {
                    mutable_t.0.insert(t, ValueKind::Float(v + value));
                }
            }
        };
    }
}

impl Storable for FeatureValue {
    fn encode(&self, buf: &mut BytesMut) -> CustomResult<()> {
        buf.put_u32(self.0.len() as u32);
        for (k, v) in &self.0 {
            buf.put_u64(*k);
            v.encode(buf);
        }
        Ok(())
    }

    fn decode(buf: &mut Cursor<&[u8]>) -> CustomResult<Self> where Self: Sized {
        let len = buf.get_u32();
        let mut tree = BTreeMap::new();
        for i in 0..len {
            let key = buf.get_u64();
            let v = ValueKind::decode(buf)?;
            tree.insert(key, v);
        }
        Ok(FeatureValue(tree))
    }

    fn need_space(&self) -> usize {
        // key = 8, value = 9
        4 + self.0.len() * 17
    }
}



#[cfg(test)]
mod tests {
    use std::fmt::Formatter;

    use log::info;
    use serde::Serialize;

    use crate::feature::value::{ValueKind, FeatureValue};
    use crate::init_log;
    use crate::store::Storable;
    use std::collections::BTreeMap;
    use crate::store::page::Page;
    use bytes::BytesMut;
    use std::io::Cursor;

    #[test]
    pub fn test_value_serialize() {
        init_log();

        let mut rt = tokio::runtime::Runtime::new().unwrap();

        let v = ValueKind::Int(212);
        let v2 = ValueKind::Float(212222.0);

        let mut feature_value = FeatureValue(BTreeMap::new());
        feature_value.0.insert(1, v);

        let mut feature_value2 = FeatureValue(BTreeMap::new());
        feature_value2.0.insert(1, v2);

        let mut page = Page::new(5, 1);
        page.data.insert("xxx杨".to_string(), feature_value);
        page.data.insert("xxx杨2".to_string(), feature_value2);

        let mut buf = BytesMut::new();
        page.encode(&mut buf);
        info!("buf:{:?}", buf.len());
        info!("buf:{:?}", buf);

        let mut cursor: Cursor<&[u8]> = Cursor::new(&*buf);
        let v = Page::decode(&mut cursor).expect("aaa");
        info!("v:{:?}", v);
        info!("v.need_space:{:?}", v.need_space());
        info!("buf:{:?}", buf.len());
        info!("buf:{:?}", buf);

        let json_byte = serde_json::to_vec(&v).expect("xxxxawee");
        info!("json_byte:{:?}", json_byte);
        info!("json_byte:{:?}", json_byte.len());
    }
}