

use feature_base::custom_error::CustomResult;
use feature_base::ds::DataSet;

pub fn fetch_all_dataset() -> CustomResult<Vec<DataSet>> {
    let data = r#"
    [
     {
        "id":101,
        "name":"ds_user_order",
        "desc":"用户订单数据集",
        "column_type_map":{
          "user_id":"INT",
          "amount":"FLOAT",
          "ts":"DATETIME"
        },
        "features":[
          {
            "id":10001,
            "name":"用户最近30天订单数量",
            "template":{
              "COUNT":{
                  "group_keys":["user_id"],
                  "time_key":"ts",
                  "window_unit":"DAY",
                  "window_size":30
              }
            }
          }
        ]
     }
    ]
        "#;
    serde_json::from_str(data).map_err(|e|e.into())
}