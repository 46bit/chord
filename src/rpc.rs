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

service! {
    rpc meta() -> (NodeId, Option<NodeId>, Option<NodeId>);
    rpc ping() -> String;
    rpc set_next(key: NodeId) -> ();
    rpc set_prev(key: NodeId) -> ();
    rpc exists(key: Key) -> bool;
    rpc get(key: Key) -> Option<Definition>;
    rpc set(key: Key, value: Definition) -> bool;
    rpc delete(key: Key) -> bool;
}

#[derive(Clone)]
pub struct ChordServer {
    resolver: Arc<RwLock<Resolver<Definition>>>,
}

impl ChordServer {
    pub fn new(resolver: Resolver<Definition>) -> ChordServer {
        ChordServer { resolver: Arc::new(RwLock::new(resolver)) }
    }
}

impl SyncService for ChordServer {
    fn meta(&self) -> Result<(NodeId, Option<NodeId>, Option<NodeId>), Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        Ok((resolver.node.id, resolver.node.predecessor_id, resolver.node.successor_id))
    }

    fn ping(&self) -> Result<String, Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        Ok(format!("PONG from {:?}", resolver.node.id))
    }

    fn set_next(&self, next_id: NodeId) -> Result<(), Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        resolver.node.successor_id = Some(next_id);
        Ok(())
    }

    fn set_prev(&self, prev_id: NodeId) -> Result<(), Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        resolver.node.predecessor_id = Some(prev_id);
        Ok(())
    }

    fn exists(&self, key: Key) -> Result<bool, Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        Ok(resolver.exists(key))
    }

    fn get(&self, key: Key) -> Result<Option<Definition>, Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        Ok(resolver.get(key))
    }

    fn set(&self, key: Key, value: Definition) -> Result<bool, Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        Ok(resolver.set(key, value))
    }

    fn delete(&self, key: Key) -> Result<bool, Never> {
        let mut resolver = self.resolver
            .write()
            .expect("Could not acquire resolver.");
        Ok(resolver.delete(key))
    }
}
