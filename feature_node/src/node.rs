use std::collections::HashMap;
use std::sync::Arc;

use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use feature_base::config::Config;
use feature_base::custom_error::{BoxErr, BoxResult, common_err};
use feature_base::ds::{DataSet, DsUpdateResult};
use feature_base::ds::column::get_value_as_int;
use feature_base::store::Store;
use feature_base::store::wal::{crate_wal, Wal, WalState, generate_tid};

use crate::meta_client;

pub struct Node {
    pub config: Config,
    pub datasets: HashMap<i64, DataSet>,
    pub wal: Wal,
    pub store: Store,
}

const KEY_DS: &str = "ds";


impl Node {
    async fn update(&self, data: Value) -> BoxResult<DsUpdateResult> {
        let ds_value = get_value_as_int(&data, KEY_DS)?;
        info!("ds_value:{}", ds_value);

        let ds = self.datasets.get(&ds_value)
            .ok_or(common_err(format!("找不到对应的ds:{}", ds_value)))?.clone();

        let mut result_map = HashMap::new();

        let tid = generate_tid();
        let action_id = self.wal.send_begin_log(tid).await?;

        for feature in &ds.features {
            feature.calc(&data, &ds.columns,&self.wal,&self.store).await;
        }
        Ok(DsUpdateResult { id: ds.id, feature_result_map: result_map })
    }
}

pub async fn create_and_init() -> BoxResult<Node> {
    let ds_vec = meta_client::fetch_all_dataset().expect("创建node失败");
    let mut datasets = HashMap::new();
    for ds in ds_vec {
        datasets.insert(ds.id, ds);
    }

    let config = Config {
        data_dir: "/Users/yang/feature_db".to_string()
    };

    // 初始化redo log
    let wal = crate_wal(config.data_dir.clone()).await?;
    let store = Store::new(config.data_dir.clone());
    Ok(Node {
        config,
        datasets,
        wal,
        store,
    })
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, Duration};

    use chrono::Local;
    use log::info;
    use rand::{Rng, SeedableRng};
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use crate::meta_client;
    use crate::node::create_and_init;

    #[derive(Serialize, Deserialize, Debug)]
    struct Event {
        pub ds: i64,
        pub user_id: i64,
        pub amount: f64,
        pub ts: u64,
    }

    #[test]
    pub fn count_test() {
        feature_base::init_log();

        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let node_bs = Arc::new(create_and_init().await.expect("创建node失败！"));
            let dt = Local::now();

            for i in 0..100 {
                let node = node_bs.clone();
                rt.spawn(async move {
                    let mut rng = rand::rngs::StdRng::from_entropy();
                    let user_id: i64 = rng.gen_range(1000..1010);
                    let amount: f64 = rng.gen_range(100.0..200.0);
                    let e = Event {
                        ds: 101,
                        user_id,
                        amount,
                        ts: (dt.timestamp_millis() + i) as u64,
                    };
                    let data = serde_json::to_string(&e).expect("序列号异常！");

                    let v: Value = serde_json::from_str(&data).expect("xxx");
                    info!("write result:{:?}", node.update(v).await);
                });
            }
        });


        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}