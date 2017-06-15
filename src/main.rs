#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]

extern crate csv;
extern crate sha1;
extern crate rand;
extern crate tarpc;
extern crate chord;

use std::time::Duration;
use std::collections::HashMap;
use std::sync::mpsc as stdmpsc;
use std::thread;
use std::net::SocketAddr;
use tarpc::future::{client, server};
use tarpc::future::client::ClientExt;
use tarpc::futures::Future;
use tarpc::tokio_core::reactor;
use rand::{Rng, StdRng};
use chord::*;

fn node(i: usize) -> (Id, FutureClient) {
    let (tx, rx) = stdmpsc::channel();
    thread::spawn(move || {
        let mut reactor = reactor::Core::new().unwrap();
        let addr: SocketAddr = format!("0.0.0.0:{:?}", 4646 + i).parse().unwrap();
        let node_id = Id::from(addr);
        let node = Node::new(node_id);
        let query_server = QueryEngine::new(node);
        let chord_server = ChordServer::new(query_server);
        let (_, server) = chord_server
            .listen(addr, &reactor.handle(), server::Options::default())
            .unwrap();
        reactor
            .handle()
            .spawn(server
                       .map(|v| {
                                println!("server ended ok: {:?}", v);
                            })
                       .map_err(|e| {
                                    println!("server ended err: {:?}", e);
                                }));
        tx.send(node_id).unwrap();
        loop {
            reactor.turn(None)
        }
    });
    let node_id = rx.recv().unwrap();
    (node_id,
     FutureClient::connect(node_id.addr, client::Options::default())
         .wait()
         .unwrap())
}

fn main() {
    let mut rng = StdRng::new().unwrap();

    let mut node_clients = HashMap::new();

    let base_node = node(0);
    println!("base node {:?}", base_node.1.meta().wait().unwrap());
    node_clients.insert(base_node.0, base_node.1.clone());
    println!();

    let number_of_nodes = 8;
    for i in 1..number_of_nodes {
        let (new_node_id, new_node) = node(i);
        println!("new node {:?}\n    {:?}",
                 new_node_id.addr,
                 new_node.meta().wait().unwrap());

        let existing_node_ids = node_clients.keys().cloned().collect::<Vec<_>>();
        let existing_node_id = rng.choose(&existing_node_ids).cloned().unwrap();
        println!("trying to join to {:?}", existing_node_id.addr);
        new_node.join(existing_node_id).wait().unwrap();
        println!("new node {:?} after join\n    {:?}",
                 new_node_id.addr,
                 new_node.meta().wait().unwrap());

        node_clients.insert(new_node_id, new_node);
        println!();
    }

    println!("done");

    let sleep_intervals = Duration::from_secs(3);
    loop {
        thread::sleep(sleep_intervals);
    }
}
