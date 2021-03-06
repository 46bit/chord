use std::net::SocketAddr;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Duration;
use futures::{future, Future};
use tarpc::future::client;
use tarpc::future::client::ClientExt;
use futures::future::Either;
use tokio_timer::*;
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

#[derive(Debug, Serialize, Deserialize)]
pub enum TimeoutErr<E> {
    FutureErr(E),
    TimedOut,
}

impl<E, F> From<TimeoutError<F>> for TimeoutErr<E> {
    fn from(_: TimeoutError<F>) -> TimeoutErr<E> {
        TimeoutErr::TimedOut
    }
}

service! {
    rpc meta() -> NodeMeta<Id> | TimeoutErr<bool>;
    rpc owner(key: Key) -> Id | TimeoutErr<bool>;
    rpc rename(new_node_id: Id) -> bool | bool;
    rpc join(existing_node_id: Id) -> bool | bool;
    rpc precede(predecessor_id: Id) -> PrecedeReply<Id, Definition> | TimeoutErr<bool>;
    rpc succeed(successor_id: Id) -> bool | bool;
    rpc exists(key: Key) -> bool | TimeoutErr<bool>;
    rpc get(key: Key) -> Option<Definition> | TimeoutErr<bool>;
    rpc set(key: Key, value: Definition) -> () | TimeoutErr<bool>;
    rpc delete(key: Key) -> bool | TimeoutErr<bool>;
}

#[derive(Clone)]
pub struct ChordServer {
    query_engine: QueryEngine<Id, Definition>,
    client_pool: Arc<Mutex<HashMap<Id, FutureClient>>>,
    timer: Timer,
}

impl ChordServer {
    pub fn new(query_engine: QueryEngine<Id, Definition>) -> ChordServer {
        ChordServer {
            query_engine: query_engine,
            client_pool: Arc::new(Mutex::new(HashMap::new())),
            timer: Timer::default(),
        }
    }

    fn client(&self, id: Id) -> FutureClient {
        let mut client_pool = self.client_pool.lock().unwrap();
        client_pool
            .entry(id)
            .or_insert_with(|| {
                                //println!("^^^ {:?}", id.addr);
                                let c = FutureClient::connect(id.addr, client::Options::default())
                                    .wait()
                                    .unwrap();
                                //println!("___ {:?}", id.addr);
                                c
                            })
            .clone()
    }
}

impl FutureService for ChordServer {
    type MetaFut = Box<Future<Item = NodeMeta<Id>, Error = TimeoutErr<bool>>>;
    type OwnerFut = Box<Future<Item = Id, Error = TimeoutErr<bool>>>;
    type RenameFut = Box<Future<Item = bool, Error = bool>>;
    type JoinFut = Box<Future<Item = bool, Error = bool>>;
    type PrecedeFut = Box<Future<Item = PrecedeReply<Id, Definition>, Error = TimeoutErr<bool>>>;
    type SucceedFut = Box<Future<Item = bool, Error = bool>>;
    type ExistsFut = Box<Future<Item = bool, Error = TimeoutErr<bool>>>;
    type GetFut = Box<Future<Item = Option<Definition>, Error = TimeoutErr<bool>>>;
    type SetFut = Box<Future<Item = (), Error = TimeoutErr<bool>>>;
    type DeleteFut = Box<Future<Item = bool, Error = TimeoutErr<bool>>>;

    fn meta(&self) -> Self::MetaFut {
        box match self.query_engine.meta() {
                QueryResult::Answer(answer) => Either::A(future::ok(answer)),
                QueryResult::Node(node_id) => {
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .meta()
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }

    fn owner(&self, key: Key) -> Self::OwnerFut {
        let query = OwnerQuery { key };
        box match self.query_engine.owner(query) {
                QueryResult::Answer(answer) => Either::A(future::ok(answer)),
                QueryResult::Node(node_id) => {
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .owner(key)
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }

    fn rename(&self, new_node_id: Id) -> Self::RenameFut {
        let mut node = self.query_engine.local_node.write().unwrap();
        node.meta.id = new_node_id;
        box future::ok(true)
    }

    fn join(&self, existing_node_id: Id) -> Self::JoinFut {
        let mut node = self.query_engine.local_node.write().unwrap();
        let precede_reply = self.client(existing_node_id)
            .precede(node.meta.id)
            .wait()
            .unwrap();
        node.apply_precede_reply(precede_reply);
        box future::ok(true)
    }

    fn precede(&self, predecessor_id: Id) -> Self::PrecedeFut {
        let query = PrecedeQuery { id: predecessor_id };
        box match self.query_engine.precede(query) {
                QueryResult::Answer(answer) => {
            if answer.predecessor_id != answer.successor_id {
                self.client(answer.predecessor_id)
                    .succeed(predecessor_id)
                    .wait()
                    .unwrap();
            }
            Either::A(future::ok(answer))
        }
                QueryResult::Node(node_id) => {
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .precede(predecessor_id)
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }

    fn succeed(&self, successor_id: Id) -> Self::JoinFut {
        let mut node = self.query_engine.local_node.write().unwrap();
        box match node.meta.relations.as_mut() {
                Some(ref mut relations) => {
            relations.successor_id = successor_id;
            future::ok(true)
        }
                None => future::err(false),
            }
    }

    fn exists(&self, key: Key) -> Self::ExistsFut {
        let query = ExistsQuery { key };
        box match self.query_engine.exists(query) {
                QueryResult::Answer(answer) => Either::A(future::ok(answer)),
                QueryResult::Node(node_id) => {
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .exists(key)
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }

    fn get(&self, key: Key) -> Self::GetFut {
        let query = GetQuery { key };
        box match self.query_engine.get(query) {
                QueryResult::Answer(answer) => Either::A(future::ok(answer)),
                QueryResult::Node(node_id) => {
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .get(key)
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }

    fn set(&self, key: Key, value: Definition) -> Self::SetFut {
        //println!("set {:?} on {:?}", key, self.meta().wait().unwrap().id.addr);
        let query = SetQuery {
            key: key,
            value: value.clone(),
        };
        box match self.query_engine.set(query) {
                QueryResult::Answer(answer) => Either::A(future::ok(answer)),
                QueryResult::Node(node_id) => {
            //println!("set {:?} forwarding to {:?}", key, node_id.addr);
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .set(key, value)
                                       .map(move |v| {
                                                //println!("set {:?} forwarded to {:?}",
                                                //         key,
                                                //         node_id.addr);
                                                v
                                            })
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }

    fn delete(&self, key: Key) -> Self::DeleteFut {
        let query = DeleteQuery { key };
        box match self.query_engine.delete(query) {
                QueryResult::Answer(answer) => Either::A(future::ok(answer)),
                QueryResult::Node(node_id) => {
            Either::B(self.timer
                          .timeout(self.client(node_id)
                                       .delete(key)
                                       .map_err(|e| {
                                                    //println!("{:?}", e);
                                                    TimeoutErr::FutureErr(false)
                                                }),
                                   Duration::from_secs(8)))
        }
            }
    }
}
