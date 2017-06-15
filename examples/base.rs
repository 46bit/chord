extern crate tarpc;
extern crate chord;

use std::net::SocketAddr;
use tarpc::future::server;
use tarpc::tokio_core::reactor;
use chord::*;

fn main() {
    let addr: SocketAddr = "0.0.0.0:4646".parse().unwrap();
    let node_id = Id::from(addr);

    let mut reactor = reactor::Core::new().unwrap();

    let node = Node::new(node_id);
    let query_server = QueryEngine::new(node);
    let chord_server = ChordServer::new(query_server);

    let (_, server) = chord_server
        .listen(addr, &reactor.handle(), server::Options::default())
        .unwrap();
    println!("Base node listening on {:?}", addr);
    println!("Stopped listening: {:?}", reactor.run(server));
}
