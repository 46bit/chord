extern crate tarpc;
extern crate chord;

use std::net::SocketAddr;
use tarpc::future::{client, server};
use tarpc::future::client::ClientExt;
use tarpc::futures::Future;
use tarpc::tokio_core::reactor;
use chord::*;

fn main() {
    let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let node_id = Id::from(addr);

    let base_node_addr: SocketAddr = "0.0.0.0:4646".parse().unwrap();
    let base_node_id = Id::from(base_node_addr);

    let mut reactor = reactor::Core::new().unwrap();

    let node = Node::new(node_id);
    let query_server = QueryEngine::new(node);
    let chord_server = ChordServer::new(query_server);

    let (server_handle, server) = chord_server
        .listen(addr, &reactor.handle(), server::Options::default())
        .unwrap();
    println!("Node listening on {:?}", server_handle.addr());

    let node_client = FutureClient::connect(server_handle.addr(), client::Options::default());

    let joiner = node_client
        .map_err(|_| ())
        .and_then(move |c| {
            let rename = c.rename(Id::from(server_handle.addr()))
                .map_err(|e| {
                             println!("not renamed {:?}", e);
                         });
            let join = c.join(base_node_id)
                .map_err(|e| {
                             println!("not joined {:?}", e);
                         });
            rename.and_then(join)
        })
        .map(|_| {
                 println!("joined!");
             });
    reactor.handle().spawn(joiner);

    println!("Stopped listening: {:?}", reactor.run(server));
}
