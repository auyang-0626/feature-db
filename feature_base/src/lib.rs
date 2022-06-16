
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

pub mod custom_error;
pub mod feature;
pub mod ds;
pub mod store;
pub mod config;

/// 时间单位
#[derive(Serialize, Deserialize, Debug)]
pub enum WindowUnit {
    SECOND,
    MINUTE,
    HOUR,
    DAY,
}

impl WindowUnit {
    pub fn to_millis(&self, v: u64) -> u64 {
        match self {
            WindowUnit::SECOND => v * 1000,
            WindowUnit::MINUTE => v * 60 * 1000,
            WindowUnit::HOUR => v * 60 * 60 * 1000,
            WindowUnit::DAY => v * 24 * 60 * 60 * 1000,
        }
    }
}

pub fn init_log() {
    env_logger::init();
}

/// 计算hash
pub fn calc_hash(key: &String) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
