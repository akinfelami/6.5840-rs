#[derive(Debug)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub fn worker<M, R>(socketname: &str, map: M, reduce: R)
where
    M: Fn(&str, &str) -> Vec<KeyValue>,
    R: Fn(&str, &[String]) -> String,
{
    // Your worker implementation here.
}
