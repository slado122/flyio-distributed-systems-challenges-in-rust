use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc;
use std::thread;

use distributed_systems_challenges::{main_loop, Body, Event, Init, Message, Node, Writer};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        known: HashSet<usize>,
    },
    GossipOk,
}

enum BroadcastInjectedPayload {
    Gossip,
}

struct BroadcastNode {
    node_id: String,
    msg_id: RefCell<usize>,
    messages: HashSet<usize>,
    neighbors: Vec<String>,
    known: HashMap<String, HashSet<usize>>,
    sent_gossip_messages: HashMap<usize, HashSet<usize>>,
}

impl BroadcastNode {
    fn generate_msg_id(&self) -> usize {
        let mut msg_id = self.msg_id.borrow_mut();
        let ret = msg_id.clone();
        *msg_id += 1;
        ret
    }
}

impl BroadcastNode {
    fn spawn_gossip_thread(
        sender: mpsc::Sender<Event<BroadcastPayload, BroadcastInjectedPayload>>,
    ) {
        thread::spawn(move || loop {
            thread::sleep(std::time::Duration::from_millis(200));
            sender
                .send(Event::Injected(BroadcastInjectedPayload::Gossip))
                .unwrap();
        });
    }
}

impl BroadcastNode {
    fn handle_broadcast(
        &mut self,
        message: usize,
        sender: String,
        msg_id: Option<usize>,
    ) -> Message<BroadcastPayload> {
        self.messages.insert(message);
        Message {
            src: self.node_id.clone(),
            dst: sender,
            body: Body {
                id: Some(self.generate_msg_id()),
                in_reply_to: msg_id,
                payload: BroadcastPayload::BroadcastOk,
            },
        }
    }

    fn handle_read(&mut self, sender: String, msg_id: Option<usize>) -> Message<BroadcastPayload> {
        Message {
            src: self.node_id.clone(),
            dst: sender,
            body: Body {
                id: Some(self.generate_msg_id()),
                in_reply_to: msg_id,
                payload: BroadcastPayload::ReadOk {
                    messages: self.messages.clone(),
                },
            },
        }
    }

    fn handle_topology(
        &mut self,
        topology: &mut HashMap<String, Vec<String>>,
        sender: String,
        msg_id: Option<usize>,
    ) -> Message<BroadcastPayload> {
        self.neighbors = topology
            .remove(&self.node_id)
            .unwrap_or_else(|| panic!("node {} not in topology", self.node_id));
        Message {
            src: self.node_id.clone(),
            dst: sender,
            body: Body {
                id: Some(self.generate_msg_id()),
                in_reply_to: msg_id,
                payload: BroadcastPayload::TopologyOk,
            },
        }
    }

    fn handle_gossip(&mut self, known: HashSet<usize>, sender: String) {
        self.known
            .entry(sender)
            .and_modify(|known_to_n| known_to_n.extend(known.clone()))
            .or_insert_with(|| known.clone());
        self.messages.extend(known);
    }

    fn handle_gossip_ok(&mut self, in_reply_to: usize, sender: String) {
        let sent_gossip_messages = self
            .sent_gossip_messages
            .remove(&in_reply_to)
            .unwrap_or_else(|| panic!("no sent gossip messages with id {}", in_reply_to));
        self.known
            .entry(sender)
            .and_modify(|known_to_n| known_to_n.extend(sent_gossip_messages.clone()))
            .or_insert_with(|| sent_gossip_messages.clone());
    }
}

impl BroadcastNode {
    fn gossip(&mut self, output_writer: &impl Writer) -> anyhow::Result<()> {
        for neighbor in &self.neighbors {
            let known_to_neighbor = self.known.get(neighbor);
            let unknown_to_neighbor = if let Some(known_to_neighbor) = known_to_neighbor {
                self.messages
                    .iter()
                    .cloned()
                    .filter(|msg| !known_to_neighbor.contains(msg))
                    .collect()
            } else {
                self.messages.clone()
            };
            eprintln!("{}/{}", unknown_to_neighbor.len(), self.messages.len());
            let msg_id = self.generate_msg_id();
            let message = Message {
                src: self.node_id.clone(),
                dst: neighbor.clone(),
                body: Body {
                    id: Some(msg_id),
                    in_reply_to: None,
                    payload: BroadcastPayload::Gossip {
                        known: unknown_to_neighbor.clone(),
                    },
                },
            };
            self.sent_gossip_messages
                .insert(msg_id, unknown_to_neighbor.clone());
            output_writer.write_message(message)?;
        }
        Ok(())
    }
}

impl Node<BroadcastPayload, BroadcastInjectedPayload> for BroadcastNode {
    fn step(
        &mut self,
        input: Event<BroadcastPayload, BroadcastInjectedPayload>,
        output_writer: &impl Writer,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let output = match input.body.payload {
                    BroadcastPayload::Broadcast { message } => {
                        Some(self.handle_broadcast(message, input.src, input.body.id))
                    }
                    BroadcastPayload::Read => Some(self.handle_read(input.src, input.body.id)),
                    BroadcastPayload::Topology { mut topology } => {
                        Some(self.handle_topology(&mut topology, input.src, input.body.id))
                    }
                    BroadcastPayload::Gossip { known } => {
                        self.handle_gossip(known, input.src);
                        None
                    }
                    BroadcastPayload::GossipOk => {
                        let in_reply_to = input
                            .body
                            .in_reply_to
                            .unwrap_or_else(|| panic!("no in_reply_to for gossip ok message"));
                        self.handle_gossip_ok(in_reply_to, input.src);
                        None
                    }
                    BroadcastPayload::BroadcastOk
                    | BroadcastPayload::ReadOk { .. }
                    | BroadcastPayload::TopologyOk => None,
                };
                if let Some(output_inner) = output {
                    output_writer.write_message(output_inner)?;
                }
            }
            Event::Injected(injected) => match injected {
                BroadcastInjectedPayload::Gossip => self.gossip(output_writer)?,
            },
        }

        Ok(())
    }
    fn from_init(
        init: Init,
        sender: mpsc::Sender<Event<BroadcastPayload, BroadcastInjectedPayload>>,
    ) -> Self {
        BroadcastNode::spawn_gossip_thread(sender);
        BroadcastNode {
            node_id: init.node_id,
            msg_id: RefCell::new(0),
            messages: HashSet::new(),
            neighbors: Vec::new(),
            known: HashMap::new(),
            sent_gossip_messages: HashMap::new(),
        }
    }
}

fn main() {
    main_loop::<BroadcastNode, _, _>().expect("failed to run main loop");
}
