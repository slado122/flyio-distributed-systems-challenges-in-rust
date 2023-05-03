use serde::{Deserialize, Serialize};
use std::sync::mpsc;
// use ulid::Ulid;

use distributed_systems_challenges::{main_loop, Event, Init, Node, Writer};

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
        format!("{}-{}", self.node_id, self.msg_id)
        // Ulid::new().to_string()
    }
}

impl Node<GeneratePayload> for UniqueIDsNode {
    fn step(
        &mut self,
        input: Event<GeneratePayload>,
        output_writer: &impl Writer,
    ) -> anyhow::Result<()> {
        let mut reply = if let Event::Message(input) = input {
            input.into_reply(Some(self.msg_id))
        } else {
            panic!("received injected message when not expecting one");
        };
        let output = match reply.body.payload {
            GeneratePayload::Generate => {
                reply.body.payload = GeneratePayload::GenerateOk {
                    id: self.generate_id(),
                };
                Some(reply)
            }
            GeneratePayload::GenerateOk { .. } => None,
        };

        if let Some(output) = output {
            self.msg_id += 1;
            output_writer.write_message(output)?;
        }

        Ok(())
    }
    fn from_init(init: Init, _sender: mpsc::Sender<Event<GeneratePayload>>) -> Self {
        UniqueIDsNode {
            node_id: init.node_id,
            msg_id: 0,
        }
    }
}

fn main() {
    main_loop::<UniqueIDsNode, GeneratePayload, _>().expect("failed to run main loop");
}
