use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::Context;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    for input in inputs {
        let input = input.context("failed to deserialize input")?;
    }

    Ok(())
}
