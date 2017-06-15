#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]
#![feature(box_syntax)]

extern crate csv;
#[macro_use]
extern crate serde_derive;
extern crate sha1;
extern crate rand;
#[macro_use]
extern crate tarpc;
extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;

use std::io;
use std::collections::HashMap;

mod rpc;
mod node;
mod query;
mod query_engine;
pub mod utils;

pub use rpc::*;
pub use node::*;
pub use query::*;
pub use query_engine::*;

/// List of node IDs, representing the hops from the request node to the target node.
//pub type Route = Vec<Key>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definition {
    #[serde(rename = "defid")]
    pub urban_id: u64,
    #[serde(rename = "word")]
    pub term: String,
    #[serde(rename = "entry")]
    pub canonical_term: String,
    pub definition: String,
    pub author: String,
    pub example: String,
    pub thumbs_up: u64,
    pub thumbs_down: u64,
}

impl Definition {
    pub fn canonical_hash(&self) -> [u32; 5] {
        canonical_hash(self.canonical_term.clone())
    }
}

pub fn canonical_hash(s: String) -> [u32; 5] {
    let mut m = sha1::Sha1::new();
    m.update(s.as_bytes());
    let u8_20 = m.digest().bytes();
    let mut u32s = [0; 5];
    for i in 0..5 {
        let mut u32_: u32 = u8_20[i * 4] as u32;
        u32_ = (u32_ << 8) | (u8_20[i * 4 + 1] as u32);
        u32_ = (u32_ << 8) | (u8_20[i * 4 + 2] as u32);
        u32_ = (u32_ << 8) | (u8_20[i * 4 + 3] as u32);
        u32s[i] = u32_;
    }
    u32s
}

impl From<Definition> for (Key, Definition) {
    fn from(definition: Definition) -> (Key, Definition) {
        (definition.canonical_hash(), definition)
    }
}

pub fn definitions_from_stdin() -> HashMap<Key, Definition> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let des: csv::DeserializeRecordsIter<_, Definition> = rdr.deserialize();

    let mut definitions: HashMap<Key, Definition> = HashMap::new();
    for result in des {
        if let Ok(definition) = result {
            let (key, definition) = definition.into();
            if definitions.contains_key(&key) {
                let existing_score = (definitions[&key].thumbs_up as isize) -
                                     (definitions[&key].thumbs_down as isize);
                let new_score = (definition.thumbs_up as isize) - (definition.thumbs_down as isize);
                if new_score <= existing_score {
                    continue;
                }
            }
            definitions.insert(key, definition);
        }
    }
    definitions
}

// fn node_client(pred_id: Option<Key>) -> SyncClient {
//     let (tx, rx) = mpsc::channel();
//     thread::spawn(move || {
//         let mut rng = StdRng::new().unwrap();
//         let mut node = Node::rand(&mut rng);
//         if let Some(pred_id) = pred_id {
//             node.predecessor_id = Some(pred_id);
//         }
//         let chord_server: ChordServer = ChordServer { node: Arc::new(RwLock::new(node)) };
//         let handle = chord_server
//             .listen("localhost:0", server::Options::default())
//             .unwrap();
//         tx.send(handle.addr()).unwrap();
//         handle.run();
//     });
//     SyncClient::connect(rx.recv().unwrap(), client::Options::default()).unwrap()
// }
