use std::collections::HashMap;

use mr::worker::KeyValue;

fn map(key: &str, value: &str) -> Vec<mr::worker::KeyValue> {
    let mut m = HashMap::new();
    let mut res = Vec::new();
    for word in value
        .split(|c: char| !c.is_alphabetic())
        .filter(|s| !s.is_empty())
    {
        m.insert(word, true);
    }

    for (k, _) in &m {
        res.push(KeyValue {
            key: k.to_string(),
            value: key.to_string(),
        });
    }
    res
}

fn reduce(_key: &str, values: &[String]) -> String {
    let mut values = values.to_vec();
    values.sort();
    format!("{} {}", values.len(), values.join(","))
}
