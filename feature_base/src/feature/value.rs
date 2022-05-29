use std::collections::BTreeMap;

pub enum ValueKind {
    INT(u64),
    FLOAT(f64),
}


pub struct FeatureValue(BTreeMap<u64, ValueKind>);

impl FeatureValue {
    pub fn new() -> FeatureValue {
        FeatureValue(BTreeMap::new())
    }

    pub fn add_int(&mut self, time: u64, window_size: u64, value: u64) {
        let t = time - time % window_size;

        match self.0.get(&t) {
            None => {
                self.0.insert(t, ValueKind::INT(value));
            }
            Some(value_kind) => {
                if let ValueKind::INT(v) = value_kind {
                    self.0.insert(t, ValueKind::INT(v + value));
                }
            }
        };
    }

    pub fn add_float(&mut self, time: u64, window_size: u64, value: f64) {
        let t = time - time % window_size;

        match self.0.get(&t) {
            None => {
                self.0.insert(t, ValueKind::FLOAT(value));
            }
            Some(value_kind) => {
                if let ValueKind::FLOAT(v) = value_kind {
                    self.0.insert(t, ValueKind::FLOAT(v + value));
                }
            }
        };
    }
}