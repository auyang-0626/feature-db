use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::custom_error::{BoxResult, value_not_found_err, value_type_not_match_err};

/// 字段类型
#[derive(Serialize, Deserialize, Debug)]
pub enum ColumnType {
    // 文本
    TEXT,
    // 数字-整形
    INT,
    // 数字-浮点
    FLOAT,
    // 时间
    DATETIME,
}


pub fn get_value_as_int(data: &Value, column: &str) -> BoxResult<i64> {
    data.get(column)
        .ok_or(value_not_found_err(&data, column))?
        .as_i64().ok_or(value_type_not_match_err(&data, column))
}

pub fn get_value_as_u64(data: &Value, column: &str) -> BoxResult<u64> {
    data.get(column)
        .ok_or(value_not_found_err(&data, column))?
        .as_u64().ok_or(value_type_not_match_err(&data, column))
}

pub fn get_value_to_str(event: &Value, column: &str, column_type: &ColumnType) -> BoxResult<String> {
    let value = event.get(column)
        .ok_or(value_not_found_err(&event, column))?;
    let value = match column_type {
        ColumnType::TEXT => {
            value.as_str().ok_or(value_type_not_match_err(&event, column))?.to_string()
        }
        ColumnType::INT => {
            value.as_i64().ok_or(value_type_not_match_err(&event, column))?.to_string()
        }
        ColumnType::FLOAT => {
            value.as_f64().ok_or(value_type_not_match_err(&event, column))?.to_string()
        }
        ColumnType::DATETIME => {
            value.as_u64().ok_or(value_type_not_match_err(&event, column))?.to_string()
        }
    };

    Ok(value)
}

pub fn check_value_and_type_match(event: &Value, column: &str, column_type: &ColumnType) -> BoxResult<()> {
    let value = event.get(column)
        .ok_or(value_not_found_err(&event, column))?;
    match column_type {
        ColumnType::TEXT => {
            value.as_str().ok_or(value_type_not_match_err(&event, column))?;
        }
        ColumnType::INT => {
            value.as_i64().ok_or(value_type_not_match_err(&event, column))?;
        }
        ColumnType::FLOAT => {
            value.as_f64().ok_or(value_type_not_match_err(&event, column))?;
        }
        ColumnType::DATETIME => {
            value.as_i64().ok_or(value_type_not_match_err(&event, column))?;
        }
    };

    Ok({})
}