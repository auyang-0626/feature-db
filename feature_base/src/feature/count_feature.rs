use std::collections::HashMap;

use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use string_builder::Builder;
use tokio::sync::RwLockWriteGuard;

use crate::custom_error::{column_not_found_in_ds_err, CustomError, CustomResult};
use crate::ds::column::{ColumnType, get_value_as_u64, get_value_to_str};
use crate::feature::value::FeatureValue;

use crate::store::page::Page;
use crate::store::wal::{Wal, WalFeatureUpdateValue};
use crate::WindowUnit;

/// 累加类型的指标模板
#[derive(Serialize, Deserialize, Debug)]
pub struct CountFeatureTemplate {
    // 分组字段
    pub group_keys: Vec<String>,
    // 时间字段
    pub time_key: String,
    // 时间单位
    pub window_unit: WindowUnit,
    // 窗口大小
    pub window_size: u64,
}

impl CountFeatureTemplate {
    pub fn build_key(&self, event: &Value,
                     feature_id: u64,
                     column_type_map: &HashMap<String, ColumnType>) -> CustomResult<String> {
        // 拼接主键
        let mut builder = Builder::default();
        for k in &self.group_keys {
            let column_type = column_type_map.get(k)
                .ok_or(column_not_found_in_ds_err(k))?;

            builder.append(get_value_to_str(event, k, column_type)?);
        }
        builder.append(feature_id.to_string());
        builder.string().map_err(|e| -> CustomError { e.into() })
    }


    pub async fn calc_and_update<'a>(&self, event: &Value,
                                     column_type_map: &HashMap<String, ColumnType>,
                                     key: &String,
                                     page: &mut RwLockWriteGuard<'_, Page>,
                                     wal: &Wal) -> CustomResult<WalFeatureUpdateValue> {

        // 事件时间
        let time = get_value_as_u64(event, &self.time_key)?;

        let old_value = page.get(key).await;
        info!("old_value:{:?}", old_value);
        //
        let update_res = match old_value {
            None => {
                let  sv = FeatureValue::new();
                let update_res = sv.add_int(key, time, self.window_unit.to_millis(self.window_size), 1)?;
                page.put(key.clone(), sv).await;
                update_res
            }
            Some(sv) => {
                sv.add_int(key, time, self.window_unit.to_millis(self.window_size), 1)?
            }
        };
        Ok(update_res)
    }
}