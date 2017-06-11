use std::fmt::Debug;
use std::collections::HashMap;
use super::*;

// query engine, resolver, database,

pub enum Query<I, T>
    where I: NodeId,
          T: Clone + Debug
{
    Meta,
    Precede(PrecedeQuery<I>),
    Owner(OwnerQuery<I>),
    Exists(ExistsQuery<I>),
    Get(GetQuery<I>),
    Set(SetQuery<I, T>),
    Delete(DeleteQuery<I>),
}

pub enum QueryResult<I, T>
    where I: NodeId,
          T: Clone + Debug
{
    Answer(T),
    Node(I),
}

// pub enum QueryForReply<I, T>
//     where I: NodeId,
//           T: Clone + Debug
// {
//     Meta(MetaQuery, oneshot::Sender<NodeMeta<I>>),
//     Owner(OwnerQuery<I>, oneshot::Sender<I>),
//     Precede(PrecedeQuery<I>, oneshot::Sender<PrecedeReply<I, T>>),
//     Exists(ExistsQuery<I>, oneshot::Sender<bool>),
//     Get(GetQuery<I>, oneshot::Sender<Option<T>>),
//     Set(SetQuery<I, T>, oneshot::Sender<()>),
//     Delete(DeleteQuery<I>, oneshot::Sender<bool>),
// }

// pub struct QueryNodeForReply<I, T>
//     where I: NodeId,
//           T: Clone + Debug
// {
//     pub node_id: I,
//     pub query_for_reply: QueryForReply<I, T>,
// }

pub struct OwnerQuery<I>
    where I: NodeId
{
    pub key: I::Key,
}

pub struct PrecedeQuery<I>
    where I: NodeId
{
    pub id: I,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrecedeReply<I, T>
    where I: NodeId,
          T: Clone + Debug
{
    pub predecessor_id: I,
    pub successor_id: I,
    pub transfer_items: HashMap<I::Key, T>,
}

pub struct ExistsQuery<I>
    where I: NodeId
{
    pub key: I::Key,
}

pub struct GetQuery<I>
    where I: NodeId
{
    pub key: I::Key,
}

pub struct SetQuery<I, T>
    where I: NodeId,
          T: Clone + Debug
{
    pub key: I::Key,
    pub value: T,
}

pub struct DeleteQuery<I>
    where I: NodeId
{
    pub key: I::Key,
}
