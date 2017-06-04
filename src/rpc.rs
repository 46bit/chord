use std::sync::{Arc, RwLock, Mutex};
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
    rpc assign_relations(predecessor_id: Id, successor_id: Id) -> String;
    rpc exists(key: Key) -> bool;
    rpc get(key: Key) -> Option<Definition>;
    rpc set(key: Key, value: Definition) -> bool;
    rpc delete(key: Key) -> bool;
}

#[derive(Clone)]
pub struct ChordServer {
    node: Arc<RwLock<Node<Id, Definition>>>,
    next: Arc<Mutex<Option<SyncClient>>>,
}

impl ChordServer {
    pub fn new(node: Node<Id, Definition>) -> ChordServer {
        let node = Arc::new(RwLock::new(node));
        let next = Arc::new(Mutex::new(None));
        ChordServer { node, next }
    }
}

impl SyncService for ChordServer {
    fn meta(&self) -> Result<NodeMeta<Id>, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        Ok(node.meta)
    }

    fn assign_relations(&self, predecessor_id: Id, successor_id: Id) -> Result<String, Never> {
        let mut node = self.node.write().expect("Could not acquire node.");
        node.assign_relations(predecessor_id, successor_id);

        let mut next = self.next.lock().expect("Could not acquire next.");
        *next = Some(SyncClient::connect(successor_id.addr, client::Options::default()).unwrap());
        Ok("is okay".to_string())
    }

    fn exists(&self, key: Key) -> Result<bool, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        match node.exists(key) {
            Ok(answer) => Ok(answer),
            Err(next_id) => {
                let n = self.next.lock().expect("Could not acquire next.");
                Ok(n.as_ref().expect("No next set.").exists(key).unwrap())
            }
        }
    }

    fn get(&self, key: Key) -> Result<Option<Definition>, Never> {
        let node = self.node.read().expect("Could not acquire node.");
        match node.get(key) {
            Ok(answer) => Ok(answer.cloned()),
            Err(next_id) => {
                let n = self.next.lock().expect("Could not acquire next.");
                Ok(n.as_ref().expect("No next set.").get(key).unwrap())
            }
        }
    }

    fn set(&self, key: Key, value: Definition) -> Result<bool, Never> {
        let mut node = self.node.write().expect("Could not acquire node.");
        match node.set(key, value.clone()) {
            Ok(()) => Ok(true),
            Err(next_id) => {
                let n = self.next.lock().expect("Could not acquire next.");
                Ok(n.as_ref()
                       .expect("No next set.")
                       .set(key, value)
                       .unwrap())
            }
        }
    }

    fn delete(&self, key: Key) -> Result<bool, Never> {
        let mut node = self.node.write().expect("Could not acquire node.");
        match node.delete(key) {
            Ok(answer) => Ok(answer),
            Err(next_id) => {
                let n = self.next.lock().expect("Could not acquire next.");
                Ok(n.as_ref().expect("No next set.").delete(key).unwrap())
            }
        }
    }
}
