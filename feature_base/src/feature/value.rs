use std::collections::BTreeMap;
use std::cell::RefCell;
use serde::{Serialize, Serializer};

#[derive(Debug)]
pub enum ValueKind {
    INT(u64),
    FLOAT(f64),
}

impl Serialize for ValueKind{
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        serializer.serialize_i32()
        todo!()
    }
}



#[derive(Debug)]
pub struct FeatureValue(BTreeMap<u64, ValueKind>);

impl FeatureValue {
    pub fn new() -> FeatureValue {
        FeatureValue(BTreeMap::new())
    }

    pub fn add_int(&self, time: u64, window_size: u64, value: u64) {
        let t = time - time % window_size;

        match self.0.get(&t) {
            None => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                mutable_t.0.insert(t, ValueKind::INT(value));
            }
            Some(value_kind) => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                if let ValueKind::INT(v) = value_kind {
                    mutable_t.0.insert(t, ValueKind::INT(v + value));
                }
            }
        };
    }

    pub fn add_float(&self, time: u64, window_size: u64, value: f64) {
        let t = time - time % window_size;

        match self.0.get(&t) {
            None => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                mutable_t.0.insert(t, ValueKind::FLOAT(value));
            }
            Some(value_kind) => unsafe {
                let mutable_t: &mut FeatureValue = &mut *(self as *const Self as *mut Self);
                if let ValueKind::FLOAT(v) = value_kind {
                    mutable_t.0.insert(t, ValueKind::FLOAT(v + value));
                }
            }
        };
    }
}