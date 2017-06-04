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
use std::sync::mpsc;
use std::thread;
use tarpc::sync::{client, server};
use tarpc::sync::client::ClientExt;
use std::net::SocketAddr;
use chord::*;

fn main() {
    let mut node_clients = HashMap::new();
    for i in 0..256 {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let addr: SocketAddr = format!("0.0.0.0:{:?}", 4646 + i).parse().unwrap();
            let node_id = Id::from(addr);
            let node = Node::new(node_id);
            let chord_server = ChordServer::new(node);
            let handle = chord_server
                .listen(addr, server::Options::default())
                .unwrap();
            tx.send(node_id).unwrap();
            handle.run();
        });
        let node_id = rx.recv().unwrap();
        let node_client = SyncClient::connect(node_id.addr, client::Options::default()).unwrap();
        node_clients.insert(node_id, node_client);
    }

    let mut node_client_ids: Vec<_> = node_clients.keys().cloned().collect();
    node_client_ids.sort();
    println!("{:?}", node_client_ids);

    for w in node_client_ids.windows(2) {
        if let &[a, b] = w {
            node_clients[&a].set_next(b).unwrap();
            node_clients[&b].set_prev(a).unwrap();
        } else {
            unreachable!();
        }
    }
    let first_id = node_client_ids[0];
    let last_id = node_client_ids.last().unwrap();
    node_clients[&first_id].set_prev(*last_id).unwrap();
    node_clients[last_id].set_next(first_id).unwrap();

    for node_client in node_clients.values() {
        match node_client.meta() {
            Ok(s) => println!("{:?}", s),
            Err(e) => println!("{:?}", e),
        }
    }

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
