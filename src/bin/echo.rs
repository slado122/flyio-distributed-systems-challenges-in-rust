use serde::{Deserialize, Serialize};
use std::sync::mpsc;

use distributed_systems_challenges::{main_loop, Event, Init, Node, Writer};

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
    fn step(
        &mut self,
        input: Event<EchoPayload>,
        output_writer: &impl Writer,
    ) -> anyhow::Result<()> {
        let mut reply = if let Event::Message(input) = input {
            input.into_reply(Some(self.msg_id))
        } else {
            panic!("received injected message when not expecting one");
        };
        let output = match reply.body.payload {
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                Some(reply)
            }
            EchoPayload::EchoOk { .. } => None,
        };
        if let Some(output) = output {
            self.msg_id += 1;
            output_writer.write_message(output)?;
        }

        Ok(())
    }
    fn from_init(_init: Init, _sender: mpsc::Sender<Event<EchoPayload>>) -> Self {
        EchoNode { msg_id: 0 }
    }
}

fn main() {
    main_loop::<EchoNode, EchoPayload, _>().expect("failed to run main loop");
}
