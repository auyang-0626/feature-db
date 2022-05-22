use std::collections::BTreeMap;

pub trait StoreValue<T> {
    fn add(&mut self, time: u64, window_size: u64, value: T);
}

pub struct SumStoreValue {
    pub time_slice: BTreeMap<u64, u64>,
}

impl SumStoreValue {
    pub fn new() -> SumStoreValue {
        SumStoreValue { time_slice: BTreeMap::new() }
    }
}

impl StoreValue<u64> for SumStoreValue {
    fn add(&mut self, time: u64, window_size: u64, value: u64) {
        let t = time - time % window_size;
        *self.time_slice.entry(t).or_insert(0) + value;
    }
}