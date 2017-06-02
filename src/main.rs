#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]

extern crate csv;
#[macro_use]
extern crate serde_derive;
extern crate sha1;
extern crate rand;
#[macro_use]
extern crate tarpc;
extern crate chord;

use std::io;
use std::collections::HashMap;
use rand::{Rng, Rand, StdRng};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use tarpc::sync::{client, server};
use tarpc::sync::client::ClientExt;
use tarpc::util::Never;
use std::net::SocketAddr;
use chord::*;

// fn node_client(pred_id: Option<Key>) -> SyncClient {
//     let (tx, rx) = mpsc::channel();
//     thread::spawn(move || {
//         let mut rng = StdRng::new().unwrap();
//         let mut node = Node::rand(&mut rng);
//         if let Some(pred_id) = pred_id {
//             node.predecessor_id = Some(pred_id);
//         }
//         let chord_server: ChordServer = ChordServer { node: Arc::new(RwLock::new(node)) };
//         let handle = chord_server
//             .listen("localhost:0", server::Options::default())
//             .unwrap();
//         tx.send(handle.addr()).unwrap();
//         handle.run();
//     });
//     SyncClient::connect(rx.recv().unwrap(), client::Options::default()).unwrap()
// }

fn main() {
    let mut node_clients = vec![];
    for i in 0..16 {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let addr: SocketAddr = format!("0.0.0.0:{:?}", 14646 + i).parse().unwrap();
            let node = Node::from(addr);
            let node_id = node.id;
            let resolver = Resolver::new(node);
            let chord_server = ChordServer::new(resolver);
            let handle = chord_server
                .listen(addr, server::Options::default())
                .unwrap();
            tx.send(node_id).unwrap();
            handle.run();
        });
        let node_id = rx.recv().unwrap();
        let node_client = SyncClient::connect(node_id.addr, client::Options::default()).unwrap();
        node_clients.push(node_client);
    }

    // let node_clients: HashMap<_, _> = (0..16)
    //     .into_iter()
    //     .map(|_| node_client(None))
    //     .map(|nc| (nc.id().unwrap(), nc))
    //     .collect();
    // let mut node_client_ids: Vec<_> = node_clients.keys().cloned().collect();
    // node_client_ids.sort();

    // for w in node_client_ids.windows(2) {
    //     if let &[a, b] = w {
    //         node_clients[&a].set_next(b).unwrap();
    //         node_clients[&b].set_prev(a).unwrap();
    //     } else {
    //         unreachable!();
    //     }
    // }
    // let first_id = node_client_ids[0];
    // let last_id = node_client_ids.last().unwrap();
    // node_clients[&first_id].set_prev(*last_id).unwrap();
    // node_clients[last_id].set_next(first_id).unwrap();

    // for node_client in node_clients.values() {
    //     match node_client.about() {
    //         Ok(s) => println!("{:?}", s),
    //         Err(e) => println!("{:?}", e),
    //     }
    // }

    // let definitions = definitions_from_stdin();
    // let definition_items: Vec<Item<Definition>> = definitions.into_iter().map(Item::from).collect();
    // println!("3");

    // let mut hopcount = 0;
    // for definition_item in definition_items.clone() {
    //     let mut current_node = &node_clients[&first_id];
    //     let mut item_hopcount = 0;
    //     loop {
    //         hopcount += 1;
    //         item_hopcount += 1;
    //         match current_node
    //                   .set(definition_item.key, definition_item.value.clone())
    //                   .unwrap() {
    //             Lookup::Found(key, Some(s)) => {
    //                 //assert_eq!(s, definition_item.value);
    //                 break;
    //             }
    //             Lookup::Found(key, None) => {
    //                 unreachable!();
    //             }
    //             Lookup::NextHop(id) => {
    //                 current_node = &node_clients[&id];
    //             }
    //         }
    //     }
    //     //println!("{:?} {:?} {:?}", item_hopcount, definition_item.key, current_node.id().unwrap());
    // }
    // println!("4 {:?} {:?}",
    //          hopcount,
    //          (hopcount as f64) / (definition_items.len() as f64));
}
