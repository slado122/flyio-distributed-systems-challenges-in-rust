use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::thread;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
}

impl<Payload> Message<Payload> {
    pub fn into_reply(self, id: Option<usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id,
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub trait Node<Payload, InjectedPayload = ()> {
    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output_writer: &impl Writer,
    ) -> anyhow::Result<()>;
    fn from_init(init: Init, sender: mpsc::Sender<Event<Payload, InjectedPayload>>) -> Self;
}

pub trait Writer {
    fn write_message<Payload: Serialize>(&self, message: Message<Payload>) -> anyhow::Result<()>;
}

pub struct StdoutWriter;

impl Writer for StdoutWriter {
    fn write_message<Payload: Serialize>(&self, message: Message<Payload>) -> anyhow::Result<()> {
        let stdout = std::io::stdout();
        serde_json::to_writer(stdout.lock(), &message).context("failed to serialize message")?;
        stdout
            .lock()
            .write_all(b"\n")
            .context("failed to write newline")?;
        Ok(())
    }
}

fn extract_init_message() -> anyhow::Result<(Message<InitPayload>, Init)> {
    let stdin = std::io::stdin();

    let init_message = stdin
        .lock()
        .lines()
        .next()
        .expect("no init message received")
        .context("failed to read init message")?;
    let init_message: Message<InitPayload> =
        serde_json::from_str(&init_message).context("failed to deserialize init message")?;
    let InitPayload::Init(init) = init_message.body.payload.clone() else {
        panic!("first message should be init");
    };
    Ok((init_message, init))
}

fn reply_to_init_message(
    init_message: Message<InitPayload>,
    writer: &impl Writer,
) -> anyhow::Result<()> {
    let mut reply = init_message.into_reply(None);
    reply.body.payload = InitPayload::InitOk;
    writer
        .write_message(reply)
        .context("failed to write init reply")?;
    Ok(())
}

fn spawn_stdin_thread<Payload, InjectedPayload>(
    sender: mpsc::Sender<Event<Payload, InjectedPayload>>,
) where
    Payload: DeserializeOwned + Send + 'static,
    InjectedPayload: Send + 'static,
{
    let stdin = std::io::stdin();
    thread::spawn(move || {
        let inputs =
            serde_json::Deserializer::from_reader(stdin.lock()).into_iter::<Message<Payload>>();
        for input in inputs {
            if let Ok(input) = input {
                sender
                    .send(Event::Message(input))
                    .expect("failed to send message to main thread");
            } else {
                panic!("failed to deserialize input");
            }
        }
    });
}

pub fn main_loop<N, Payload, InjectedPayload>() -> anyhow::Result<()>
where
    Payload: DeserializeOwned + Send + 'static,
    InjectedPayload: Send + 'static,
    N: Node<Payload, InjectedPayload>,
{
    let writer = StdoutWriter;

    let (init_message, init) = extract_init_message()?;
    reply_to_init_message(init_message, &writer)?;

    let (sender, receiver) = mpsc::channel();

    spawn_stdin_thread(sender.clone());

    let mut node: N = Node::from_init(init, sender.clone());
    for event in receiver {
        node.step(event, &writer)?;
    }

    Ok(())
}
