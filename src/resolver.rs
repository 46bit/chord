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
use super::*;

#[derive(Clone, Debug)]
pub struct Resolver<T>
    where T: Clone + Debug
{
    pub node: Node<T>,
}

impl Resolver<Definition> {
    pub fn new(node: Node<Definition>) -> Resolver<Definition> {
        Resolver { node }
    }

    pub fn exists(&self, key: Key) -> bool {
        if self.node.owns(key) {
            self.node.exists(key)
        } else {
            self.node
                .successor_id
                .unwrap()
                .client()
                .exists(key)
                .unwrap()
        }
    }

    pub fn get(&self, key: Key) -> Option<Definition> {
        if self.node.owns(key) {
            self.node.get(key).cloned()
        } else {
            self.node
                .successor_id
                .unwrap()
                .client()
                .get(key)
                .unwrap()
        }
    }

    pub fn set(&mut self, key: Key, value: Definition) -> bool {
        if self.node.owns(key) {
            self.node.set(key, value)
        } else {
            self.node
                .successor_id
                .unwrap()
                .client()
                .set(key, value)
                .unwrap()
        }
    }

    pub fn delete(&mut self, key: Key) -> bool {
        if self.node.owns(key) {
            self.node.delete(key)
        } else {
            self.node
                .successor_id
                .unwrap()
                .client()
                .delete(key)
                .unwrap()
        }
    }
}
