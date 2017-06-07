use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use futures::{Future, IntoFuture, BoxFuture, Sink};
use futures::sync::{mpsc, oneshot};
use super::*;

#[derive(Clone)]
pub struct QueryRemotes<I, T>
    where I: NodeId + 'static,
          T: Clone + Debug + Send + 'static
{
    remote_node_rx: mpsc::Receiver<QueryNodeForReply<I, T>>,
}

impl Future for QueryRemoteNodes {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            match self.remote_node_rx.poll() {
                Ok(Async::)
            }
        }
    }

    fn start_send(&mut self, cmd: NodeCmd) -> StartSend<NodeCmd, ()> {
        let node_client = self.node_clients.entry(&cmd.id).or_insert_with(|| {
            FutureClient::connect(cmd.id.addr, client::Options::default())
        });
        let fut = match cmd.cmd {
            Cmd::Meta => node_client.meta(),
            Cmd::AssignRelations { predecessor_id, successor_id } => node_client.assign_relations(predecessor_id, successor_id),
            Cmd::Exists(key) => node_client.exists(key),
            Cmd::Get(key) => node_client.get(key),
            Cmd::Set(key, value) => node_client.set(key, value),
            Cmd::Delete(key) => node_client.delete(key),
        }
        node_client
    }

    fn poll_complete(&mut self) -> Poll<(), ()> {

    }
}
