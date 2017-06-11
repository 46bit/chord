use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use super::*;

#[derive(Clone)]
pub struct QueryEngine<I, T>
    where I: NodeId + 'static,
          T: Clone + Debug + Send + 'static
{
    pub local_node: Arc<RwLock<Node<I, T>>>,
}

impl<I, T> QueryEngine<I, T>
    where I: NodeId,
          T: Clone + Debug + Send + 'static
{
    pub fn new(local_node: Node<I, T>) -> QueryEngine<I, T> {
        let local_node = Arc::new(RwLock::new(local_node));
        QueryEngine { local_node }
    }

    // pub fn query(&self, query: Query<I, T>) -> BoxFuture<(), ()> {
    //     use Query::*;
    //     match query {
    //         Meta => self.meta(),
    //         Owner(query) => self.owner(query),
    //         Precede(query) => self.precede(query),
    //         Exists(query) => self.exists(query),
    //         Get(query) => self.get(query),
    //         Set(query) => self.set(query),
    //         Delete(query) => self.delete(query),
    //     }
    // }

    pub fn meta(&self) -> QueryResult<I, NodeMeta<I>> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        QueryResult::Answer(local_node.meta)
    }

    pub fn owner(&self, query: OwnerQuery<I>) -> QueryResult<I, I> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        if local_node.meta.owns(query.key) {
            QueryResult::Answer(local_node.meta.id)
        } else {
            QueryResult::Node(local_node.meta.next(local_node.meta.id.key()))
        }
    }

    pub fn precede(&self, query: PrecedeQuery<I>) -> QueryResult<I, PrecedeReply<I, T>> {
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
            QueryResult::Answer(PrecedeReply {
                                    predecessor_id: predecessor_id,
                                    successor_id: local_node.meta.id,
                                    transfer_items: local_node.items.clone(),
                                })
        } else {
            QueryResult::Node(local_node.meta.next(local_node.meta.id.key()))
        }
    }

    pub fn exists(&self, query: ExistsQuery<I>) -> QueryResult<I, bool> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        match local_node.exists(query.key) {
            Ok(answer) => QueryResult::Answer(answer),
            Err(next_id) => QueryResult::Node(next_id),
        }
    }

    pub fn get(&self, query: GetQuery<I>) -> QueryResult<I, Option<T>> {
        let local_node = self.local_node.read().expect("Could not acquire node.");
        match local_node.get(query.key) {
            Ok(answer) => QueryResult::Answer(answer.cloned()),
            Err(next_id) => QueryResult::Node(next_id),
        }
    }

    pub fn set(&self, query: SetQuery<I, T>) -> QueryResult<I, ()> {
        let mut local_node = self.local_node
            .write()
            .expect("Could not acquire node.");
        match local_node.set(query.key, query.value.clone()) {
            Ok(()) => QueryResult::Answer(()),
            Err(next_id) => QueryResult::Node(next_id),
        }
    }

    pub fn delete(&self, query: DeleteQuery<I>) -> QueryResult<I, bool> {
        let mut node = self.local_node
            .write()
            .expect("Could not acquire node.");
        match node.delete(query.key) {
            Ok(answer) => QueryResult::Answer(answer),
            Err(next_id) => QueryResult::Node(next_id),
        }
    }
}
