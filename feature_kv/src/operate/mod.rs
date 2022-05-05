use feature_common::data_set::DataSet;
use serde_json::Value;
use log::{info};

pub trait Operate{
    fn write(&self, v:Value);
}

impl Operate for DataSet{
    fn write(&self, v:Value) {
        for (group_keys,group_feature) in &self.features {

            let group_values = group_keys.iter().map(|gk|{
                let c = self.attrs.get(gk).expect("指定的group key 不存在！");
                c.get_value_as_str(&v)
            }).fold("".to_string(),|x,v|format!("{}{}",x,v));

            info!("group_values {:?}",group_values);
        }
    }
}




#[cfg(test)]
mod tests {

    use serde_json::Value;
    use feature_common::data_set::column::{Column, ColumnType};
    use feature_common::data_set::DataSet;
    use feature_common::feature_template::count_feature::CountFeatureTemplate;
    use feature_common::feature_template::WindowUnit;
    use crate::operate::Operate;
    use crate::init;

    #[test]
    pub fn count_test() {
        init::init();

        let attrs = vec![
            Column {
                name: "user_id".to_string(),
                column_type: ColumnType::NUMBER,
                nullable: true,
            }
            , Column {
                name: "balance".to_string(),
                column_type: ColumnType::NUMBER,
                nullable: true,
            }, Column {
                name: "ts".to_string(),
                column_type: ColumnType::DATETIME,
                nullable: true,
            },
        ];

        let mut ns = DataSet::new("user_balance".to_string(), attrs);

        let count_feature = CountFeatureTemplate {
            group_keys: vec!["user_id".to_string()],
            calc_key: "balance".to_string(),
            time_key: "ts".to_string(),
            window_unit: WindowUnit::SECOND,
            window_size: 10,
        };
        ns.add_feature(Box::new(count_feature));


        let data = r#"
        {
            "user_id": 123422,
            "balance": 43,
            "ts": 1651134356123
        }"#;

        let v: Value = serde_json::from_str(data).expect("xxx");

        ns.write(v);
    }
}