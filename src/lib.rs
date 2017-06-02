#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]

extern crate csv;
#[macro_use]
extern crate serde_derive;
extern crate sha1;
extern crate rand;
#[macro_use]
extern crate tarpc;

use std::io;
use std::collections::HashMap;
use rand::{Rng, Rand, StdRng};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use tarpc::sync::{client, server};
use tarpc::sync::client::ClientExt;
use tarpc::util::Never;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::str::FromStr;

mod rpc;
mod node;
mod resolver;

pub use rpc::*;
pub use node::*;
pub use resolver::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Definition {
    #[serde(rename = "defid")]
    urban_id: u64,
    #[serde(rename = "word")]
    term: String,
    #[serde(rename = "entry")]
    canonical_term: String,
    definition: String,
    author: String,
    example: String,
    thumbs_up: u64,
    thumbs_down: u64,
}

impl Definition {
    pub fn canonical_hash(&self) -> [u32; 5] {
        let mut m = sha1::Sha1::new();
        m.update(self.canonical_term.as_bytes());
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
}

impl From<Definition> for (Key, Definition) {
    fn from(definition: Definition) -> (Key, Definition) {
        (definition.canonical_hash(), definition)
    }
}

fn definitions_from_stdin() -> Vec<(Key, Definition)> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let des: csv::DeserializeRecordsIter<_, Definition> = rdr.deserialize();

    let mut definitions = Vec::new();
    for result in des {
        if let Ok(definition) = result {
            definitions.push(definition.into());
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
