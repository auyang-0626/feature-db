use std::collections::HashMap;

use log::info;
use serde_json::Value;

use feature_common::BoxResult;
use feature_common::custom_error;
use feature_common::data_set::DataSet;

use crate::meta_client;
use crate::operate::Operate;
use feature_common::custom_error::{common_err,value_not_found_err};


const KEY_DS: &str = "ds";

pub struct Node {
    pub datasets: HashMap<i64, DataSet>,
}

impl Operate for Node {
    fn write(&self, data: Value) -> BoxResult<()> {
        let ds_value = data.get(KEY_DS)
            .ok_or(value_not_found_err(KEY_DS))?
            .as_i64().ok_or(common_err(format!("key:{} 的值必须是数字整形",KEY_DS)))?;
        info!("ds_value:{}", ds_value);

        Ok(())
    }
}

pub fn create_and_init() -> Node {
    let ds_vec = meta_client::fetch_all_dataset().expect("获取元数据失败");
    let mut datasets = HashMap::new();
    for ds in ds_vec {
        datasets.insert(ds.id, ds);
    }
    Node {
        datasets
    }
}


#[cfg(test)]
mod tests {
    use log::info;
    use serde_json::Value;

    use feature_common::data_set::column::{Column, ColumnType};
    use feature_common::data_set::DataSet;
    use feature_common::feature_template::count_feature::CountFeatureTemplate;
    use feature_common::feature_template::WindowUnit;

    use crate::init;
    use crate::meta_client;
    use crate::node::create_and_init;
    use crate::operate::Operate;

    #[test]
    pub fn count_test() {
        init::init();
        let node = create_and_init();

        let data = r#"
        {
            "ds":101,
            "user_id": 123422,
            "amount": 43,
            "ts": 1651134356123
        }"#;

        let v: Value = serde_json::from_str(data).expect("xxx");
        info!("write result:{:?}", node.write(v));
    }
}