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
use std::cmp::Ordering;
use super::*;

pub type Key = [u32; 5];

/// List of node IDs, representing the hops from the request node to the target node.
pub type Route = Vec<Key>;

/// Result of a lookup of a `Key` giving the value set if there is one, and the ID of
/// the final node queried.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lookup<T>
    where T: Clone + Debug
{
    pub node_id: Key,
    pub key: Key,
    pub item: Option<T>,
}

// pub struct QueryOutput<T> where T: Clone + Debug {
//     pub node_id: Key,
//     pub output: Option<T>,
// }

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    pub key: Key,
    pub addr: SocketAddr,
}

impl NodeId {
    pub fn client(&self) -> SyncClient {
        SyncClient::connect(self.addr, client::Options::default()).unwrap()
    }
}

impl Ord for NodeId {
    fn cmp(&self, other: &NodeId) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for NodeId {
    fn partial_cmp(&self, other: &NodeId) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

#[derive(Clone, Debug)]
pub struct Node<T>
    where T: Clone + Debug
{
    pub id: NodeId,
    pub successor_id: Option<NodeId>,
    pub predecessor_id: Option<NodeId>,
    pub items: HashMap<Key, T>,
}

impl<T> Node<T>
    where T: Clone + Debug
{
    pub fn new(node_id: NodeId) -> Node<T> {
        Node {
            id: node_id,
            successor_id: None,
            predecessor_id: None,
            items: HashMap::new(),
        }
    }

    pub fn owns(&self, key: Key) -> bool {
        // The lowest-keyed node is responsible for keys greater than the highest-keyed
        // node and for those less than itself.
        // Otherwise a node is merely responsible for keys greater than its predecessor.
        //key > self.predecessor_id.unwrap().key &&
        //(self.predecessor_id.unwrap().key > self.id.key || key <= self.id.key)
        //(self.predecessor_ids[0] > self.id && (key > self.predecessor_ids[0] || key <= self.id)) || ((key <= self.id) && (key > self.predecessor_ids[0]))

        if self.predecessor_id.unwrap().key > self.id.key {
            if (key > self.predecessor_id.unwrap().key) || (key <= self.id.key) {
                return true;
            }
        } else {
            // Is key < self.id and key > self.predecessor_ids[0].
            if (key <= self.id.key) && (key > self.predecessor_id.unwrap().key) {
                return true;
            }
        }
        false
    }


    pub fn exists(&self, key: Key) -> bool {
        self.owns(key) && self.items.contains_key(&key)
    }

    pub fn get(&self, key: Key) -> Option<&T> {
        if self.owns(key) {
            self.items.get(&key)
        } else {
            None
        }
    }

    pub fn set(&mut self, key: Key, value: T) -> bool {
        if self.owns(key) {
            self.items.insert(key, value);
            true
        } else {
            false
        }
    }

    pub fn delete(&mut self, key: Key) -> bool {
        if self.owns(key) {
            self.items.remove(&key).is_some()
        } else {
            false
        }
    }
}

impl<T> Rand for Node<T>
    where T: Clone + Debug
{
    fn rand<R: Rng>(rng: &mut R) -> Node<T> {
        Node::new(NodeId {
                      key: rng.gen(),
                      addr: "localhost:0".parse().unwrap(),
                  })
    }
}

impl<T> From<SocketAddr> for Node<T>
    where T: Clone + Debug
{
    fn from(addr: SocketAddr) -> Node<T> {
        let mut m = sha1::Sha1::new();
        m.update(addr.to_string().as_bytes());
        let u8_20 = m.digest().bytes();
        let mut u32s = [0; 5];
        for i in 0..5 {
            let mut u32_: u32 = u8_20[i * 4] as u32;
            u32_ = (u32_ << 8) | (u8_20[i * 4 + 1] as u32);
            u32_ = (u32_ << 8) | (u8_20[i * 4 + 2] as u32);
            u32_ = (u32_ << 8) | (u8_20[i * 4 + 3] as u32);
            u32s[i] = u32_;
        }
        Node::new(NodeId {
                      key: u32s,
                      addr: addr,
                  })
    }
}
