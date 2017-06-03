use std::collections::HashMap;
use rand::{Rng, Rand};
use std::fmt::Debug;
use std::net::SocketAddr;
use super::*;

pub type NodeResult<T> = Result<T, Key>;

#[derive(Clone, Debug)]
pub struct Node<T>
    where T: Clone + Debug
{
    pub id: Key,
    pub successor_id: Option<Key>,
    pub predecessor_id: Option<Key>,
    pub items: HashMap<Key, T>,
}

impl<T> Node<T>
    where T: Clone + Debug
{
    pub fn new(node_id: Key) -> Node<T> {
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
        if self.predecessor_id.unwrap() > self.id {
            key > self.predecessor_id.unwrap() || key <= self.id
        } else {
            key <= self.id && key > self.predecessor_id.unwrap()
        }
    }

    pub fn exists(&self, key: Key) -> NodeResult<bool> {
        if self.owns(key) {
            Ok(self.items.contains_key(&key))
        } else {
            Err(self.successor_id.unwrap())
        }
    }

    pub fn get(&self, key: Key) -> NodeResult<Option<&T>> {
        if self.owns(key) {
            Ok(self.items.get(&key))
        } else {
            Err(self.successor_id.unwrap())
        }
    }

    pub fn set(&mut self, key: Key, value: T) -> NodeResult<()> {
        if self.owns(key) {
            self.items.insert(key, value);
            Ok(())
        } else {
            Err(self.successor_id.unwrap())
        }
    }

    pub fn delete(&mut self, key: Key) -> NodeResult<bool> {
        if self.owns(key) {
            Ok(self.items.remove(&key).is_some())
        } else {
            Err(self.successor_id.unwrap())
        }
    }
}

impl<T> Rand for Node<T>
    where T: Clone + Debug
{
    fn rand<R: Rng>(rng: &mut R) -> Node<T> {
        Node::new(rng.gen())
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
        Node::new(u32s)
    }
}
