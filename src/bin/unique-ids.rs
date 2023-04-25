use std::io::Write;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use distributed_systems_challenges::{Body, Init, Message, Node, main_loop};


#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum GeneratePayload {
    Generate,
    GenerateOk { id: String },
}

struct UniqueIDsNode {
    node_id: String,
    msg_id: usize,
}

impl UniqueIDsNode {
    fn generate_id(&mut self) -> String {
        // format!("{}-{}", self.node_id, self.msg_id)
        Ulid::new().to_string()
    }
}

impl Node<GeneratePayload> for UniqueIDsNode {
    fn step<T: Write>(&mut self, input: Message<GeneratePayload>, mut output_writer: T) -> anyhow::Result<()> {
        let output = match input.body.payload {
            GeneratePayload::Generate => {
                Some(Message {
                    src: self.node_id.clone(),
                    dst: input.src,
                    body: Body {
                        id: Some(self.msg_id),
                        in_reply_to: input.body.id,
                        payload: GeneratePayload::GenerateOk { id: self.generate_id() },
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
        UniqueIDsNode {
            node_id: init.node_id,
            msg_id: 0,
        }
    }
}

fn main() {
    main_loop::<UniqueIDsNode, GeneratePayload>().expect("failed to run main loop");
}