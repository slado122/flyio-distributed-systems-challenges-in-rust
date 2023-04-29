use serde::{Deserialize, Serialize};
// use ulid::Ulid;

use distributed_systems_challenges::{main_loop, Init, Message, Node, Writer};

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
    fn step<T: Writer>(
        &mut self,
        input: Message<GeneratePayload>,
        output_writer: &T,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(self.msg_id));
        let output = match reply.body.payload {
            GeneratePayload::Generate => {
                reply.body.payload = GeneratePayload::GenerateOk {
                    id: self.generate_id(),
                };
                Some(reply)
            }
            _ => None,
        };

        if let Some(output) = output {
            self.msg_id += 1;
            output_writer.write_message(output)?;
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
