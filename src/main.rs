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

impl From<Definition> for Item<Definition> {
    fn from(definition: Definition) -> Item<Definition> {
        Item {
            key: definition.canonical_hash(),
            value: definition,
        }
    }
}

fn definitions_from_stdin() -> Vec<Definition> {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    let des: csv::DeserializeRecordsIter<_, Definition> = rdr.deserialize();

    let mut definitions = Vec::new();
    for result in des {
        if let Ok(definition) = result {
            definitions.push(definition);
        }
    }
    definitions
}

pub type Key = [u32; 5];

#[derive(Clone, Debug)]
pub struct Item<V> {
    pub key: Key,
    pub value: V,
}

#[derive(Clone, Debug)]
pub struct Node<V> where Item<V>: From<V> {
    pub id: Key,
    pub items: HashMap<Key, Item<V>>,
    pub successor_ids: Vec<Key>,
    pub predecessor_ids: Vec<Key>,
}

impl<V> Node<V> where Item<V>: From<V> {
    pub fn new(id: Key) -> Node<V> {
        Node {
            id: id,
            items: HashMap::new(),
            successor_ids: Vec::new(),
            predecessor_ids: Vec::new(),
        }
    }

    /// Lookup a key if it should be present on this node.
    ///
    /// If this node's ID is a successor of the key, we must check whether the
    /// predecessor also was before assuming the key should be present on this node.
    pub fn lookup(&self, key: Key) -> Lookup<&V> {
        if self.predecessor_ids[0] > self.id {
            // Is key > self.predecessor_ids[0] or key < self.id.
            if (key > self.predecessor_ids[0]) || (key <= self.id) {
                let item = self.items.get(&key);
                return Lookup::Found(key, item.map(|i| &i.value));
            }
        } else {
            // Is key < self.id and key > self.predecessor_ids[0].
            if (key <= self.id) && (key > self.predecessor_ids[0]) {
                let item = self.items.get(&key);
                return Lookup::Found(key, item.map(|i| &i.value));
            }
        }
        Lookup::NextHop(self.successor_ids[0])
    }

    /// Lookup a key if it should be present on this node.
    ///
    /// If this node's ID is a successor of the key, we must check whether the
    /// predecessor also was before assuming the key should be present on this node.
    pub fn set(&mut self, key: Key, value: V) -> Lookup<&V> {
        if self.predecessor_ids[0] > self.id {
            // Is key > self.predecessor_ids[0] or key < self.id.
            if (key > self.predecessor_ids[0]) || (key <= self.id) {
                self.items.insert(key, Item::from(value));
                let item = self.items.get(&key);
                return Lookup::Found(key, item.map(|i| &i.value));
            }
        } else {
            // Is key < self.id and key > self.predecessor_ids[0].
            if (key <= self.id) && (key > self.predecessor_ids[0]) {
                self.items.insert(key, Item::from(value));
                let item = self.items.get(&key);
                return Lookup::Found(key, item.map(|i| &i.value));
            }
        }
        Lookup::NextHop(self.successor_ids[0])
    }
}

impl<V> Rand for Node<V> where Item<V>: From<V> {
    fn rand<R: Rng>(rng: &mut R) -> Node<V> {
        Node::new(rng.gen())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Route {
    Reached,
    NextHop(Key),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lookup<V> {
    Found(Key, Option<V>),
    NextHop(Key),
}

service! {
    rpc id() -> Key;
    rpc about() -> (Key, Option<Key>, Option<Key>);
    rpc ping() -> String;
    rpc set_next(key: Key) -> ();
    rpc set_prev(key: Key) -> ();
    rpc exists(key: Key) -> Route;
    rpc get(key: Key) -> Lookup<Definition>;
    rpc set(key: Key, value: Definition) -> Lookup<Definition>;
}

#[derive(Clone)]
struct ChordServer {
    node: Arc<RwLock<Node<Definition>>>,
}

impl SyncService for ChordServer {
    fn id(&self) -> Result<Key, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        Ok(node.id)
    }

    fn about(&self) -> Result<(Key, Option<Key>, Option<Key>), Never> {
        let node = self.node.read().expect("Could not acquire node.");
        Ok((node.id, node.predecessor_ids.first().cloned(), node.successor_ids.first().cloned()))
    }

    fn ping(&self) -> Result<String, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        Ok(format!("PONG from {:?}", node.id))
    }

    fn set_next(&self, next_id: Key) -> Result<(), Never> {
        let mut node = self.node.write().expect("Could not acquire node.");
        node.successor_ids.push(next_id);
        Ok(())
    }

    fn set_prev(&self, prev_id: Key) -> Result<(), Never> {
        let mut node = self.node.write().expect("Could not acquire node.");
        node.predecessor_ids.push(prev_id);
        Ok(())
    }

    fn exists(&self, key: Key) -> Result<Route, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        match node.lookup(key) {
            Lookup::Found(_, _) => Ok(Route::Reached),
            Lookup::NextHop(id) => Ok(Route::NextHop(id)),
        }
    }

    fn get(&self, key: Key) -> Result<Lookup<Definition>, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        Ok(match node.lookup(key) {
               Lookup::Found(key, Some(s)) => Lookup::Found(key, Some(s.clone())),
               Lookup::Found(key, None) => Lookup::Found(key, None),
               Lookup::NextHop(id) => Lookup::NextHop(id),
           })
    }

    fn set(&self, key: Key, value: Definition) -> Result<Lookup<Definition>, Never> {
        let mut node = self.node.write().expect("Could not acquire node.");
        Ok(match node.set(key, value) {
               Lookup::Found(key, Some(s)) => Lookup::Found(key, Some(s.clone())),
               Lookup::Found(key, None) => Lookup::Found(key, None),
               Lookup::NextHop(id) => Lookup::NextHop(id),
           })
    }

    // fn delete(&self, key: Key) -> Result<Lookup<String>, Never> { }
}

fn node_client(pred_id: Option<Key>) -> SyncClient {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut rng = StdRng::new().unwrap();
        let mut node = Node::rand(&mut rng);
        if let Some(pred_id) = pred_id {
            node.predecessor_ids.push(pred_id);
        }
        let chord_server: ChordServer =
            ChordServer { node: Arc::new(RwLock::new(node)) };
        let handle = chord_server
            .listen("localhost:0", server::Options::default())
            .unwrap();
        tx.send(handle.addr()).unwrap();
        handle.run();
    });
    SyncClient::connect(rx.recv().unwrap(), client::Options::default()).unwrap()
}

fn main() {
    let node_clients: HashMap<_,_> = (0..16).into_iter().map(|_| node_client(None)).map(|nc| (nc.id().unwrap(), nc)).collect();
    let mut node_client_ids: Vec<_> = node_clients.keys().cloned().collect();
    node_client_ids.sort();

    for w in node_client_ids.windows(2) {
        if let &[a, b] = w {
            node_clients[&a].set_next(b).unwrap();
            node_clients[&b].set_prev(a).unwrap();
        } else {
            unreachable!();
        }
    }
    let first_id = node_client_ids[0];
    let last_id = node_client_ids.last().unwrap();
    node_clients[&first_id].set_prev(*last_id).unwrap();
    node_clients[last_id].set_next(first_id).unwrap();

    for node_client in node_clients.values() {
        match node_client.about() {
            Ok(s) => println!("{:?}", s),
            Err(e) => println!("{:?}", e)
        }
    }

    let definitions = definitions_from_stdin();
    let definition_items: Vec<Item<Definition>> = definitions.into_iter().map(Item::from).collect();
    println!("3");

    let mut hopcount = 0;
    for definition_item in definition_items.clone() {
        let mut current_node = &node_clients[&first_id];
        let mut item_hopcount = 0;
        loop {
            hopcount += 1;
            item_hopcount += 1;
            match current_node.set(definition_item.key, definition_item.value.clone()).unwrap() {
                Lookup::Found(key, Some(s)) => {
                    //assert_eq!(s, definition_item.value);
                    break;
                },
                Lookup::Found(key, None) => {
                    unreachable!();
                },
                Lookup::NextHop(id) => {
                    current_node = &node_clients[&id];
                },
            }
        }
        //println!("{:?} {:?} {:?}", item_hopcount, definition_item.key, current_node.id().unwrap());
    }
    println!("4 {:?} {:?}", hopcount, (hopcount as f64) / (definition_items.len() as f64));
}
