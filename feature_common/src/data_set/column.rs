use serde_json::Value;

/// 字段类型
pub enum ColumnType {
    // 文本
    TEXT,
    // 数字
    NUMBER,
    // 时间
    DATETIME,
}

/// 属性
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

impl Column {

    pub fn  get_value_as_str(&self, v:&Value) -> String{
        match v.get(&self.name){
            None => "".to_string(),
            Some(column_value) => column_value.to_string()
        }
    }
}