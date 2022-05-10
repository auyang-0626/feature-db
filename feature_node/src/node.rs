use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use feature_base::config::Config;
use feature_base::custom_error::{BoxErr, BoxResult, common_err};
use feature_base::ds::{DataSet, DsUpdateResult};
use feature_base::ds::column::get_value_as_int;
use feature_base::store::redo_log::RedoLog;

use crate::meta_client;

pub struct Node {
    pub config: Config,
    pub datasets: HashMap<i64, Arc<DataSet>>,
    pub redo_log: RedoLog,
}

const KEY_DS: &str = "ds";


impl Node {
    async fn update(&self, data: Value) -> BoxResult<DsUpdateResult> {
        let ds_value = get_value_as_int(&data, KEY_DS)?;
        info!("ds_value:{}", ds_value);

        let ds = self.datasets.get(&ds_value)
            .ok_or(common_err(format!("找不到对应的ds:{}", ds_value)))?.clone();
        let send = self.redo_log.send.clone();
        tokio::spawn(async move {
            ds.update(&data, send).await
        }).await.map_err(|e| -> BoxErr{ e.into() })?
    }
}

pub async fn create_and_init() -> BoxResult<Node> {
    let ds_vec = meta_client::fetch_all_dataset().expect("创建node失败");
    let mut datasets = HashMap::new();
    for ds in ds_vec {
        datasets.insert(ds.id, Arc::new(ds));
    }

    let config = Config {
        data_dir: "/Users/yang/feature_db".to_string()
    };
    let data_dir = config.data_dir.clone();
    Ok(Node {
        config,
        datasets,
        redo_log: RedoLog::new(data_dir).await?,
    })
}


#[cfg(test)]
mod tests {
    use log::info;
    use serde_json::Value;

    use crate::meta_client;
    use crate::node::create_and_init;

    #[test]
    pub fn count_test() {
        feature_base::init_log();

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let node = create_and_init().await.expect("创建node失败！");


            let data = r#"
            {
                "ds":101,
                "user_id": 123422,
                "amount": 43,
                "ts": 1651134356123
            }"#;
            let v: Value = serde_json::from_str(data).expect("xxx");
            info!("write result:{:?}", node.update(v).await);
        });
    }
}