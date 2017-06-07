use std::net::SocketAddr;
use std::cmp::Ordering;
use futures::{Future, Sink};
use futures::sync::{mpsc, oneshot};
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
    rpc meta() -> NodeMeta<Id> | bool;
    rpc owner(key: Key) -> Id | bool;
    rpc precede(predecessor_id: Id) -> PrecedeReply<Id, Definition> | bool;
    rpc exists(key: Key) -> bool | bool;
    rpc get(key: Key) -> Option<Definition> | bool;
    rpc set(key: Key, value: Definition) -> () | bool;
    rpc delete(key: Key) -> bool | bool;
}

#[derive(Clone)]
pub struct ChordServer {
    query_tx: mpsc::Sender<QueryForReply<Id, Definition>>,
}

impl ChordServer {
    pub fn new(query_tx: mpsc::Sender<QueryForReply<Id, Definition>>) -> ChordServer {
        ChordServer { query_tx }
    }

    fn query<R>(&self,
                query: QueryForReply<Id, Definition>,
                reply_rx: oneshot::Receiver<R>)
                -> Box<Future<Item = R, Error = bool>>
        where R: Send + 'static
    {
        self.query_tx
            .clone()
            .send(query)
            .map_err(|_| false)
            .and_then(|_| reply_rx.map_err(|_| false))
            .boxed()
    }
}

impl FutureService for ChordServer {
    type MetaFut = Box<Future<Item = NodeMeta<Id>, Error = bool>>;
    type OwnerFut = Box<Future<Item = Id, Error = bool>>;
    type PrecedeFut = Box<Future<Item = PrecedeReply<Id, Definition>, Error = bool>>;
    type ExistsFut = Box<Future<Item = bool, Error = bool>>;
    type GetFut = Box<Future<Item = Option<Definition>, Error = bool>>;
    type SetFut = Box<Future<Item = (), Error = bool>>;
    type DeleteFut = Box<Future<Item = bool, Error = bool>>;

    fn meta(&self) -> Self::MetaFut {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Meta(MetaQuery, reply_tx), reply_rx)
    }

    fn owner(&self, key: Key) -> Self::OwnerFut {
        let query = OwnerQuery { key };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Owner(query, reply_tx), reply_rx)
    }

    fn precede(&self, id: Id) -> Self::PrecedeFut {
        let query = PrecedeQuery { id };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Precede(query, reply_tx), reply_rx)
    }

    fn exists(&self, key: Key) -> Self::ExistsFut {
        let query = ExistsQuery { key };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Exists(query, reply_tx), reply_rx)
    }

    fn get(&self, key: Key) -> Self::GetFut {
        let query = GetQuery { key };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Get(query, reply_tx), reply_rx)
    }

    fn set(&self, key: Key, value: Definition) -> Self::SetFut {
        let query = SetQuery { key, value };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Set(query, reply_tx), reply_rx)
    }

    fn delete(&self, key: Key) -> Self::DeleteFut {
        let query = DeleteQuery { key };
        let (reply_tx, reply_rx) = oneshot::channel();
        self.query(QueryForReply::Delete(query, reply_tx), reply_rx)
    }
}
