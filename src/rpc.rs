use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use std::cmp::Ordering;
use tarpc::sync::client;
use tarpc::sync::client::ClientExt;
use tarpc::util::Never;
use super::*;

pub type Key = [u32; 5];

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id {
    pub addr: SocketAddr,
    pub key: Key,
}

impl NodeId for Id {
    type Key = Key;

    fn key(&self) -> Key {
        self.key
    }
}

impl Ord for Id {
    fn cmp(&self, other: &Id) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for Id {
    fn partial_cmp(&self, other: &Id) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl From<SocketAddr> for Id {
    fn from(addr: SocketAddr) -> Id {
        let mut m = sha1::Sha1::new();
        m.update(addr.to_string().as_bytes());
        let u8_20 = m.digest().bytes();
        let mut key = [0; 5];
        for i in 0..5 {
            let mut u32_: u32 = u8_20[i * 4] as u32;
            u32_ = (u32_ << 8) | (u8_20[i * 4 + 1] as u32);
            u32_ = (u32_ << 8) | (u8_20[i * 4 + 2] as u32);
            u32_ = (u32_ << 8) | (u8_20[i * 4 + 3] as u32);
            key[i] = u32_;
        }
        Id { addr, key }
    }
}

service! {
    rpc meta() -> NodeMeta<Id>;
    rpc ping() -> String;
    rpc set_next(key: Id) -> ();
    rpc set_prev(key: Id) -> ();
    rpc exists(key: Key) -> bool;
    rpc get(key: Key) -> Option<Definition>;
    rpc set(key: Key, value: Definition) -> bool;
    rpc delete(key: Key) -> bool;
}

#[derive(Clone)]
pub struct ChordServer {
    node: Arc<RwLock<Node<Id, Definition>>>,
}

impl ChordServer {
    pub fn new(node: Node<Id, Definition>) -> ChordServer {
        ChordServer { node: Arc::new(RwLock::new(node)) }
    }

    fn client(&self, next_id: Id) -> SyncClient {
        SyncClient::connect(next_id.addr, client::Options::default()).unwrap()
    }
}

impl SyncService for ChordServer {
    fn meta(&self) -> Result<NodeMeta<Id>, Never> {
        let node = self.node.read().expect("Could not acquire resolver.");
        Ok(node.meta)
    }

    fn ping(&self) -> Result<String, Never> {
        let node = self.node.read().expect("Could not acquire resolver.");
        Ok(format!("PONG from {:?}", node.meta.id))
    }

    fn set_next(&self, next_id: Id) -> Result<(), Never> {
        let mut node = self.node.write().expect("Could not acquire resolver.");
        node.meta.successor_id = Some(next_id);
        Ok(())
    }

    fn set_prev(&self, prev_id: Id) -> Result<(), Never> {
        let mut node = self.node.write().expect("Could not acquire resolver.");
        node.meta.predecessor_id = Some(prev_id);
        Ok(())
    }

    fn exists(&self, key: Key) -> Result<bool, Never> {
        let node = self.node.read().expect("Could not acquire resolver.");
        match node.exists(key) {
            Ok(answer) => Ok(answer),
            Err(next_id) => Ok(self.client(next_id).exists(key).unwrap()),
        }
    }

    fn get(&self, key: Key) -> Result<Option<Definition>, Never> {
        let node = self.node.read().expect("Could not acquire resolver.");
        match node.get(key) {
            Ok(answer) => Ok(answer.cloned()),
            Err(next_id) => Ok(self.client(next_id).get(key).unwrap()),
        }
    }

    fn set(&self, key: Key, value: Definition) -> Result<bool, Never> {
        let mut node = self.node.write().expect("Could not acquire resolver.");
        match node.set(key, value.clone()) {
            Ok(()) => Ok(true),
            Err(next_id) => Ok(self.client(next_id).set(key, value).unwrap()),
        }
    }

    fn delete(&self, key: Key) -> Result<bool, Never> {
        let mut node = self.node.write().expect("Could not acquire resolver.");
        match node.delete(key) {
            Ok(answer) => Ok(answer),
            Err(next_id) => Ok(self.client(next_id).delete(key).unwrap()),
        }
    }
}
