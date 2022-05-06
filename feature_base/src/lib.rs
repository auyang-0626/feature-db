use log::LevelFilter;
use serde::{Deserialize, Serialize};

pub mod custom_error;
pub mod feature;
pub mod ds;
pub mod store;

/// 时间单位
#[derive(Serialize, Deserialize,Debug)]
pub enum WindowUnit {
    SECOND,
    MINUTE,
    HOUR,
    DAY,
}

pub fn init_log(){
    env_logger::init();
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
