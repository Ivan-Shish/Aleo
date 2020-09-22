use crate::Round;

use serde_json;
use std::io::{stdout, Write};
// use curl::easy::Easy;
//
// pub fn fetch(buffer: &mut Vec<u8>) {
//     let mut handle = Easy::new();
//     handle.url("http://167.71.156.62:8080/ceremony").unwrap();
//
//     handle.write_function(|data| {
//         buffer.extend_from_slice(data);
//         Ok(data.len())
//     }).unwrap();
//
//     handle.perform().unwrap()
// }

pub fn ceremony() -> Round {
    let mut result = vec![];
    fetch(&mut result);

    let output: Round = serde_json::from_str(&String::from_utf8(result).unwrap()).unwrap();
    output
}
