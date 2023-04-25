use std::io::Write;
use anyhow::Context;
use serde::{Deserialize, Serialize};

use distributed_systems_challenges::{Body, Init, Message, Node, main_loop};


#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    node_id: String,
    msg_id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn step<T: Write>(&mut self, input: Message<EchoPayload>, mut output_writer: T) -> anyhow::Result<()> {
        let output = match input.body.payload {
            EchoPayload::Echo { echo } => {
                Some(Message {
                    src: self.node_id.clone(),
                    dst: input.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input.body.id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                })
            }
            _ => { None }
        };
        if output.is_some() {
            self.msg_id += 1;
            let output_bytes = serde_json::to_string(&output).context("failed to serialize output")?.into_bytes();
            output_writer.write_all(&output_bytes)?;
            output_writer.write_all(b"\n").context("failed to write newline")?;
        }

        Ok(())
    }
    fn from_init(init: Init) -> Self {
        EchoNode {
            node_id: init.node_id,
            msg_id: 0,
        }
    }
}

fn main() {
    main_loop::<EchoNode, EchoPayload>().expect("failed to run main loop");
}