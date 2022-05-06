use log::info;
use serde_json::Value;

use feature_common::BoxResult;
use feature_common::data_set::DataSet;

pub trait Operate {
    fn write(&self, data: Value) -> BoxResult<()>;
}

impl Operate for DataSet {
    fn write(&self, v: Value) -> BoxResult<()> {
        // for (group_keys,group_feature) in &self.features {
        //
        //     let group_values = group_keys.iter().map(|gk|{
        //         let c = self.attrs.get(gk).expect("指定的group key 不存在！");
        //         c.get_value_as_str(&v)
        //     }).fold("".to_string(),|x,v|format!("{}{}",x,v));
        //
        //     info!("group_values {:?}",group_values);
        // }
        Ok(())
    }
}


