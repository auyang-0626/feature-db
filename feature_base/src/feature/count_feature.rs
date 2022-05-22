use std::collections::HashMap;

use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use string_builder::Builder;

use crate::custom_error::{BoxErr, BoxResult, column_not_found_in_ds_err};
use crate::ds::column::{ColumnType, get_value_as_u64, get_value_to_str};
use crate::feature::value::{SumStoreValue, StoreValue};
use crate::store::Store;
use crate::store::wal::Wal;
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
    pub async fn calc(&self, data: &Value,
                feature_id: u64,
                column_type_map: &HashMap<String, ColumnType>,
                wal: &Wal, store: &Store) -> BoxResult<()> {

        // 拼接主键
        let mut builder = Builder::default();
        for k in &self.group_keys {
            let column_type = column_type_map.get(k)
                .ok_or(column_not_found_in_ds_err(k))?;

            builder.append(get_value_to_str(data, k, column_type)?);
        }
        builder.append(feature_id.to_string());
        let key = builder.string().map_err(|e| -> BoxErr { e.into() })?;

        // 事件时间
        let time = get_value_as_u64(data, &self.time_key)?;

        //
        match store.get(&key) {
            None => {
                let mut sv = SumStoreValue::new();
                sv.add(time, self.window_unit.to_millis(self.window_size), 1);

            }
            Some( sv) => {

                let mut locked_sv = sv.lock().await;
                locked_sv.add(time, self.window_unit.to_millis(self.window_size), 1);

            }
        };

        Ok(())
    }
}