use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use futures::{Future, IntoFuture, BoxFuture, Sink};
use futures::sync::{mpsc, oneshot};
use super::*;

#[derive(Clone)]
pub struct QueryEngine<I, T>
    where I: NodeId + 'static,
          T: Clone + Debug + Send + 'static
{
    local_node: Arc<RwLock<Node<I, T>>>,
    remote_node_tx: mpsc::Sender<QueryNodeForReply<I, T>>,
}

impl<I, T> QueryEngine<I, T>
    where I: NodeId,
          T: Clone + Debug + Send + 'static
{
    pub fn new(local_node: Node<I, T>,
               remote_node_tx: mpsc::Sender<QueryNodeForReply<I, T>>)
               -> QueryEngine<I, T> {
        let local_node = Arc::new(RwLock::new(local_node));
        QueryEngine {
            local_node,
            remote_node_tx,
        }
    }

    pub fn query(&self, query: QueryForReply<I, T>) -> BoxFuture<(), ()> {
        match query {
            QueryForReply::Meta(_, reply_tx) => self.meta(reply_tx),
            QueryForReply::Owner(query, reply_tx) => self.owner(query, reply_tx),
            QueryForReply::Precede(query, reply_tx) => self.precede(query, reply_tx),
            QueryForReply::Exists(query, reply_tx) => self.exists(query, reply_tx),
            QueryForReply::Get(query, reply_tx) => self.get(query, reply_tx),
            QueryForReply::Set(query, reply_tx) => self.set(query, reply_tx),
            QueryForReply::Delete(query, reply_tx) => self.delete(query, reply_tx),
        }
    }

    fn meta(&self, reply_tx: oneshot::Sender<NodeMeta<I>>) -> BoxFuture<(), ()> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        reply_tx
            .send(local_node.meta)
            .map_err(|_| ())
            .into_future()
            .boxed()
    }

    fn owner(&self, query: OwnerQuery<I>, reply_tx: oneshot::Sender<I>) -> BoxFuture<(), ()> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        if local_node.meta.owns(query.key) {
            reply_tx
                .send(local_node.meta.id)
                .map_err(|_| ())
                .into_future()
                .boxed()
        } else {
            self.remote_node_tx
                .clone()
                .send(QueryNodeForReply {
                          node_id: local_node.meta.next(local_node.meta.id.key()),
                          query_for_reply: QueryForReply::Owner(query, reply_tx),
                      })
                .map_err(|_| ())
                .into_future()
                .map(|_| ())
                .boxed()
        }
    }

    fn precede(&self,
               query: PrecedeQuery<I>,
               reply_tx: oneshot::Sender<PrecedeReply<I, T>>)
               -> BoxFuture<(), ()> {
        let mut local_node = self.local_node
            .write()
            .expect("Could not acquire node.");
        if local_node.meta.owns(query.id.key()) {
            let mut predecessor_id = local_node.meta.id;
            local_node.meta.relations =
                Some(local_node
                         .meta
                         .relations
                         .map_or_else(|| {
                                          NodeRelations {
                                              predecessor_id: query.id,
                                              successor_id: query.id,
                                          }
                                      },
                                      |mut relations| {
                                          predecessor_id = relations.predecessor_id;
                                          relations.predecessor_id = query.id;
                                          relations
                                      }));
            reply_tx
                .send(PrecedeReply {
                          predecessor_id: predecessor_id,
                          successor_id: local_node.meta.id,
                          transfer_items: local_node.items.clone(),
                      })
                .map_err(|_| ())
                .into_future()
                .boxed()
        } else {
            self.remote_node_tx
                .clone()
                .send(QueryNodeForReply {
                          node_id: local_node.meta.next(local_node.meta.id.key()),
                          query_for_reply: QueryForReply::Precede(query, reply_tx),
                      })
                .map_err(|_| ())
                .into_future()
                .map(|_| ())
                .boxed()
        }
    }

    // fn assign_relations(&self, query: AssignRelationsQuery<I>, reply_tx: oneshot::Sender<()>) {
    //     let mut node = self.node.write().expect("Could not acquire node.");
    //     node.assign_relations(predecessor_id, successor_id);

    //     let mut next = self.next.lock().expect("Could not acquire next.");
    //     *next = Some(FutureClient::connect(successor_id.addr, client::Options::default()).unwrap());
    //     Ok("is okay".to_string())
    // }

    fn exists(&self, query: ExistsQuery<I>, reply_tx: oneshot::Sender<bool>) -> BoxFuture<(), ()> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        match local_node.exists(query.key) {
            Ok(answer) => {
                reply_tx
                    .send(answer)
                    .map_err(|_| ())
                    .into_future()
                    .boxed()
            }
            Err(next_id) => {
                self.remote_node_tx
                    .clone()
                    .send(QueryNodeForReply {
                              node_id: next_id,
                              query_for_reply: QueryForReply::Exists(query, reply_tx),
                          })
                    .map_err(|_| ())
                    .into_future()
                    .map(|_| ())
                    .boxed()
            }
        }
    }

    fn get(&self, query: GetQuery<I>, reply_tx: oneshot::Sender<Option<T>>) -> BoxFuture<(), ()> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        match local_node.get(query.key) {
            Ok(answer) => {
                reply_tx
                    .send(answer.cloned())
                    .map_err(|_| ())
                    .into_future()
                    .boxed()
            }
            Err(next_id) => {
                self.remote_node_tx
                    .clone()
                    .send(QueryNodeForReply {
                              node_id: next_id,
                              query_for_reply: QueryForReply::Get(query, reply_tx),
                          })
                    .map_err(|_| ())
                    .into_future()
                    .map(|_| ())
                    .boxed()
            }
        }
    }

    fn set(&self, query: SetQuery<I, T>, reply_tx: oneshot::Sender<()>) -> BoxFuture<(), ()> {
        let mut local_node = self.local_node
            .write()
            .expect("Could not acquire node.");
        match local_node.set(query.key, query.value.clone()) {
            Ok(()) => reply_tx.send(()).map_err(|_| ()).into_future().boxed(),
            Err(next_id) => {
                self.remote_node_tx
                    .clone()
                    .send(QueryNodeForReply {
                              node_id: next_id,
                              query_for_reply: QueryForReply::Set(query, reply_tx),
                          })
                    .map_err(|_| ())
                    .into_future()
                    .map(|_| ())
                    .boxed()
            }
        }
    }

    fn delete(&self, query: DeleteQuery<I>, reply_tx: oneshot::Sender<bool>) -> BoxFuture<(), ()> {
        let mut node = self.local_node
            .write()
            .expect("Could not acquire node.");
        match node.delete(query.key) {
            Ok(answer) => {
                reply_tx
                    .send(answer)
                    .map_err(|_| ())
                    .into_future()
                    .boxed()
            }
            Err(next_id) => {
                self.remote_node_tx
                    .clone()
                    .send(QueryNodeForReply {
                              node_id: next_id,
                              query_for_reply: QueryForReply::Delete(query, reply_tx),
                          })
                    .map_err(|_| ())
                    .into_future()
                    .map(|_| ())
                    .boxed()
            }
        }
    }
}
