use std::error;
use std::fmt;

use serde_json::Value;

// 为 `Box<error::Error>` 取别名。
pub type BoxErr = Box<dyn error::Error + Send + Sync>;
pub type BoxResult<T> = std::result::Result<T, BoxErr>;

#[derive(Debug)]
pub struct CustomError {
    pub code: usize,
    pub message: String,
}

// 根据错误码显示不同的错误信息
impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "错误码：{}，详细信息:{}", self.code, self.message)
    }
}

impl error::Error for CustomError {}


pub fn common_err(msg: String) -> BoxErr {
    CustomError {
        code: 10000,
        message: msg,
    }.into()
}

pub fn value_not_found_err(data: &Value, key: &str) -> BoxErr {
    CustomError {
        code: 10001,
        message: format!("数据:{} 未找到key:{} 对应的值", data, key),
    }.into()
}

pub fn value_type_not_match_err(data: &Value, key: &str) -> BoxErr {
    CustomError {
        code: 10002,
        message: format!("数据:{} key:{} 类型不匹配", data, key),
    }.into()
}

pub fn column_not_found_in_ds_err(key: &str) -> BoxErr {
    CustomError {
        code: 10003,
        message: format!("数据集中没有对应的key:{}", key),
    }.into()
}

pub fn decode_failed_by_insufficient_data_err() -> BoxErr {
    CustomError {
        code: 20001,
        message: format!("解析失败，数据长度不足"),
    }.into()
}