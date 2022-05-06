use std::error;

pub mod feature_template;
pub mod data_set;
pub mod custom_error;

// 为 `Box<error::Error>` 取别名。
pub type BoxResult<T> = std::result::Result<T, Box<dyn error::Error>>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
