use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::time;

use feature_base::calc_hash;
use feature_base::config::Config;
use feature_base::custom_error::{BoxErr, BoxResult, common_err};
use feature_base::ds::{DataSet, DsUpdateResult, FeatureUpdateResult};
use feature_base::ds::column::get_value_as_int;
use feature_base::feature::Feature;
use feature_base::store::Store;
use feature_base::store::wal::{crate_wal, generate_tid, Wal, WalFeatureUpdateValue, WalState};

use crate::meta_client;

pub struct Node {
    pub config: Config,
    pub datasets: HashMap<i64, DataSet>,
    pub wal: Wal,
    pub store: Store,
}

const KEY_DS: &str = "ds";


impl Node {
    /// 根据数据，更新关联的所有指标
    async fn update(&self, event: Value) -> BoxResult<DsUpdateResult> {
        let ds_value = get_value_as_int(&event, KEY_DS)?;
        let ds = self.datasets.get(&ds_value)
            .ok_or(common_err(format!("找不到对应的ds:{}", ds_value)))?.clone();

        let mut result_map = HashMap::new();

        let tid = generate_tid();
        let action_id = self.wal.send_begin_log(tid).await?;

        // 先根据feature构建所有的key
        let (key_feature_map, key_error_map) = build_feature_keys(&event, &ds);

        // 这里如果把result_map的可变引用传入build_feature_keys，会导致下面有问题，所以现在只能复制一份
        for (k, v) in key_error_map {
            result_map.insert(k, v);
        }

        // 根据这些key，找到page，并锁定
        let mut locks = vec![];
        let mut page_map = HashMap::new();
        let mut feature_mk_map = HashMap::new();
        for (key, _) in &key_feature_map {
            let hash = calc_hash(&key);
            let (mk, page) = self.store.get_page(hash).await?;
            locks.push((mk, page));
            feature_mk_map.insert(key, mk);
        }
        for (mk, page) in &locks {
            if !page_map.contains_key(mk) {
                let l = page.write().await;
                page_map.insert(mk, l);
            }
        }

        // 锁定后，计算feature新值，并更新
        for (key, feature) in &key_feature_map {
            if let Some(mk) = feature_mk_map.get(key) {
                if let Some(mut locked_page) = page_map.get_mut(mk) {
                    info!("key = {}", key);
                    match feature.calc_and_update(&event, &ds.column_type_map, key, locked_page, &self.wal).await {
                        Ok(res) => {
                            locked_page.after_update();
                            self.wal.send_feature_update_log(tid, res).await;
                        }
                        Err(e) => {
                            result_map.insert(feature.id, FeatureUpdateResult::failed(e.to_string()));
                        }
                    }
                }
            }
        }
        self.wal.commit_log(tid).await;

        Ok(DsUpdateResult { id: ds.id, feature_result_map: result_map })
    }

    pub async fn check_point(&self) {
        let mut interval = time::interval(time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            info!("check_point run!");
        }
    }
}

/// 根据feature构建所有的key
fn build_feature_keys<'a>(data: &'a Value, ds: &'a DataSet) -> (HashMap<String, &'a Feature>, HashMap<u64, FeatureUpdateResult>) {
    let mut key_feature_map = HashMap::new();
    let mut key_error_map = HashMap::new();
    for feature in &ds.features {
        match feature.build_key(&data, &ds.column_type_map) {
            Ok(key) => {
                key_feature_map.insert(key, feature);
            }
            Err(e) => {
                key_error_map.insert(feature.id, FeatureUpdateResult::failed(e.to_string()));
            }
        }
    }
    (key_feature_map, key_error_map)
}

/// 创建和初始化node
pub async fn create_and_init() -> BoxResult<Arc<Node>> {
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
    let store = Store::new(config.data_dir.clone()).await;

    let node = Arc::new(Node {
        config,
        datasets,
        wal,
        store,
    });

    let node2 = node.clone();
    tokio::spawn(async move {
        node2.check_point().await
    });

    Ok(node)
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

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

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let node_bs = create_and_init().await.expect("创建node失败！");
            let dt = Local::now();

            for i in 0..100 {
                let node = node_bs.clone();
                rt.spawn(async move {
                    let mut rng = rand::rngs::StdRng::from_entropy();
                    //let user_id: i64 = rng.gen_range(1000..1010);
                    let user_id: i64 = 1011;
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