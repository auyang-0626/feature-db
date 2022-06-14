use std::error;
use std::error::Error;
use std::fmt;
use std::string::FromUtf8Error;

use num_enum::TryFromPrimitiveError;
use serde_json::Value;

// 为 `Box<error::Error>` 取别名。
//pub type BoxErr = Box<dyn error::Error + Send + Sync>;
pub type CustomResult<T> = std::result::Result<T, CustomError>;

#[derive(Debug)]
pub struct CustomError {
    pub code: usize,
    pub message: String,
}

impl CustomError {
    pub fn new(e: Box<dyn Error>) -> CustomError {
        common_err(e.to_string())
    }
}

impl From<std::io::Error> for CustomError {
    fn from(e: std::io::Error) -> Self {
        common_err(e.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for CustomError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        common_err(e.to_string())
    }
}

impl From<FromUtf8Error> for CustomError {
    fn from(e: FromUtf8Error) -> Self {
        common_err(e.to_string())
    }
}

impl<T: num_enum::TryFromPrimitive> From<TryFromPrimitiveError<T>> for CustomError {
    fn from(e: TryFromPrimitiveError<T>) -> Self {
        common_err(e.to_string())
    }
}

impl From<serde_json::Error> for CustomError {
    fn from(e: serde_json::Error) -> Self {
        common_err(e.to_string())
    }
}

// 根据错误码显示不同的错误信息
impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "错误码：{}，详细信息:{}", self.code, self.message)
    }
}

impl error::Error for CustomError {}


pub fn common_err(msg: String) -> CustomError {
    CustomError {
        code: 10000,
        message: msg,
    }
}

pub fn value_not_found_err(data: &Value, key: &str) -> CustomError {
    CustomError {
        code: 10001,
        message: format!("数据:{} 未找到key:{} 对应的值", data, key),
    }
}

pub fn value_type_not_match_err(data: &Value, key: &str) -> CustomError {
    CustomError {
        code: 10002,
        message: format!("数据:{} key:{} 类型不匹配", data, key),
    }
}

pub fn column_not_found_in_ds_err(key: &str) -> CustomError {
    CustomError {
        code: 10003,
        message: format!("数据集中没有对应的key:{}", key),
    }
}

/// 因为数据不足导致的失败，错误码
pub static DECODE_FAILED_BY_INSUFFICIENT_DATA_CODE: usize = 20001;
pub fn decode_failed_by_insufficient_data_err() -> CustomError {
    CustomError {
        code: DECODE_FAILED_BY_INSUFFICIENT_DATA_CODE,
        message: format!("解析失败，数据长度不足"),
    }
}