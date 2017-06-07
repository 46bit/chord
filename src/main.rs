#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]

extern crate csv;
extern crate sha1;
extern crate rand;
extern crate tarpc;
extern crate chord;

use std::io;
use std::io::Write;
use std::collections::HashMap;
use std::sync::mpsc as stdmpsc;
use std::thread;
use std::net::SocketAddr;
use tarpc::future::{client, server};
use tarpc::future::client::ClientExt;
use tarpc::futures::sync::mpsc;
use tarpc::tokio_core::reactor;
use chord::*;

fn main() {
    let mut node_clients = HashMap::new();
    for i in 0..8 {
        let (tx, rx) = stdmpsc::channel();
        thread::spawn(move || {
            let mut reactor = reactor::Core::new().unwrap();
            let addr: SocketAddr = format!("0.0.0.0:{:?}", 4646 + i).parse().unwrap();
            let node_id = Id::from(addr);
            let (query_tx, query_rx) = mpsc::channel(5);
            let (remote_tx, remote_rx) = mpsc::channel(5);
            let node = Node::new(node_id);
            let query_server = QueryEngine::new(node, remote_tx);
            let chord_server = ChordServer::new(query_tx);
            let (handle, server) = chord_server
                .listen(addr, &reactor.handle(), server::Options::default())
                .unwrap();
            tx.send(node_id).unwrap();
            reactor.handle().spawn(server);
        });
        let node_id = rx.recv().unwrap();
        let node_client = FutureClient::connect(node_id.addr, client::Options::default()).unwrap();
        node_clients.insert(node_id, node_client);
    }

    let mut node_client_ids: Vec<_> = node_clients.keys().cloned().collect();
    node_client_ids.sort();
    println!("{:?}", node_client_ids);

    let mut pred_ids: HashMap<Id, Id> = HashMap::new();
    let mut next_ids: HashMap<Id, Id> = HashMap::new();
    for w in node_client_ids.windows(2) {
        if let &[a, b] = w {
            next_ids.insert(a, b);
            pred_ids.insert(b, a);
        } else {
            unreachable!();
        }
    }
    let first_id = node_client_ids[0];
    let last_id = *node_client_ids.last().unwrap();
    next_ids.insert(last_id, first_id);
    pred_ids.insert(first_id, last_id);

    for node_client_id in node_client_ids {
        let next_id: Id = next_ids[&node_client_id];
        let pred_id: Id = pred_ids[&node_client_id];
        println!("2.6");
        println!("{:?}",
                 node_clients[&node_client_id].assign_relations(pred_id, next_id));
        println!("2.7");
    }

    println!("2.8");
    for node_client in node_clients.values() {
        match node_client.meta() {
            Ok(s) => println!("{:?}", s),
            Err(e) => println!("{:?}", e),
        }
    }

    println!("2.9");
    let definitions = definitions_from_stdin();
    println!("3");

    let mut hopcount = 0;
    for (key, definition) in definitions.clone() {
        hopcount += 1;
        if hopcount % 1000 == 0 {
            print!("a");
            io::stdout()
                .flush()
                .ok()
                .expect("Could not flush stdout");
            for node_client in node_clients.values() {
                match node_client.meta() {
                    Ok(s) => println!("{:?}", s),
                    Err(e) => println!("{:?}", e),
                }
            }
        }
        let current_node = &node_clients[&first_id];
        assert!(current_node.set(key, definition.clone()).unwrap());
        // match current_node.get(key).unwrap() {
        //     Some(s) => {
        //         assert_eq!(s, definition);
        //     }
        //     None => {
        //         unreachable!();
        //     }
        // }
        //println!("{:?} {:?} {:?}", item_hopcount, definition_item.key, current_node.id().unwrap());
    }
    println!("4 {:?} {:?}",
             hopcount,
             (hopcount as f64) / (definitions.len() as f64));
}
