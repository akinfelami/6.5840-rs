#[derive(Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub trait MapReduce {
    fn map(&self, key: String, value: String) -> Vec<KeyValue>;
    fn reduce(&self, key: &str, values: &[String]) -> String;
}
