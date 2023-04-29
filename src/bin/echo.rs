use serde::{Deserialize, Serialize};

use distributed_systems_challenges::{main_loop, Init, Message, Node, Writer};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    msg_id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn step<T: Writer>(
        &mut self,
        input: Message<EchoPayload>,
        output_writer: &T,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(self.msg_id));
        let output = match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
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
    fn from_init(_init: Init) -> Self {
        EchoNode { msg_id: 0 }
    }
}

fn main() {
    main_loop::<EchoNode, EchoPayload>().expect("failed to run main loop");
}
