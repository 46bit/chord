use std::io;
use std::collections::HashMap;
use rand::{Rng, Rand, StdRng};
use std::sync::{mpsc, Arc, Mutex, RwLock};
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
    pub successor_client: Option<Arc<Mutex<SyncClient>>>,
}

impl Resolver<Definition> {
    pub fn new(node: Node<Definition>) -> Resolver<Definition> {
        Resolver {
            node: node,
            successor_client: None,
        }
    }

    pub fn exists(&mut self, key: Key) -> bool {
        if self.node.owns(key) {
            self.node.exists(key)
        } else {
            self.successor_client();
            self.successor_client
                .as_mut()
                .unwrap()
                .lock()
                .unwrap()
                .exists(key)
                .unwrap()
        }
    }

    pub fn get(&mut self, key: Key) -> Option<Definition> {
        if self.node.owns(key) {
            self.node.get(key).cloned()
        } else {
            self.successor_client();
            self.successor_client
                .as_mut()
                .unwrap()
                .lock()
                .unwrap()
                .get(key)
                .unwrap()
        }
    }

    pub fn set(&mut self, key: Key, value: Definition) -> bool {
        if self.node.owns(key) {
            self.node.set(key, value)
        } else {
            self.successor_client();
            self.successor_client
                .as_mut()
                .unwrap()
                .lock()
                .unwrap()
                .set(key, value)
                .unwrap()
        }
    }

    pub fn delete(&mut self, key: Key) -> bool {
        if self.node.owns(key) {
            self.node.delete(key)
        } else {
            self.successor_client();
            self.successor_client
                .as_mut()
                .unwrap()
                .lock()
                .unwrap()
                .delete(key)
                .unwrap()
        }
    }

    fn successor_client(&mut self) {
        if self.successor_client.is_none() {
            self.successor_client =
                Some(Arc::new(Mutex::new(self.node.successor_id.unwrap().client())));
        }
    }
}
