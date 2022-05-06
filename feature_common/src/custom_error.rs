use std::fmt;
use std::error;
use crate::BoxResult;

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
impl error::Error for CustomError{

}


pub fn common_err(msg: String) ->  Box<dyn error::Error> {
    CustomError {
        code: 10000,
        message: msg,
    }.into()
}

pub fn value_not_found_err(key: &str) -> Box<dyn error::Error> {
    CustomError {
        code: 10001,
        message: format!("未找到key:{}对应的值", key),
    }.into()
}