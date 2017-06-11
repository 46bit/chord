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
        reactor.handle().spawn(server);
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
    println!("base node {:?}", base_node.1.meta().wait());
    node_clients.insert(base_node.0, base_node.1.clone());
    println!();

    // let node = node(2);
    // println!("new node {:?}", node.1.meta().wait());
    // println!();

    // node.1.join(base_node.0).wait().unwrap();
    // println!("base node after join {:?}", base_node.1.meta().wait());
    // println!("base node after join {:?}", node.1.meta().wait());
    // node_clients.insert(node.0, node.1);
    // println!();

    // for _ in 0..2 {
    //     for node_client in node_clients.values_mut() {
    //         println!("{:?}", node_client.meta().wait());
    //     }
    // }

    let number_of_nodes = 8;
    for i in 1..number_of_nodes {
        let (new_node_id, new_node) = node(i);
        println!("new node {:?}\n    {:?}",
                 new_node_id.addr,
                 new_node.meta().wait());

        let existing_node_ids = node_clients.keys().cloned().collect::<Vec<_>>();
        let existing_node_id = rng.choose(&existing_node_ids).cloned().unwrap();
        println!("trying to join to {:?}", existing_node_id.addr);
        new_node.join(existing_node_id).wait().unwrap();
        println!("new node {:?} after join\n    {:?}",
                 new_node_id.addr,
                 new_node.meta().wait());

        node_clients.insert(new_node_id, new_node);
        println!();
    }

    // let definitions = definitions_from_stdin();
    // println!("3");

    // let mut n = 0;
    // for (key, definition) in definitions.clone() {
    //     let node_clients_vec = node_clients.values().collect::<Vec<_>>();
    //     let node_client = rng.choose(&node_clients_vec).cloned().unwrap();
    //     node_client.set(key, definition.clone()).wait().unwrap();
    //     // match node_client.get(key).wait().unwrap() {
    //     //     Some(gotten_definition) => {
    //     //         assert_eq!(gotten_definition, definition);
    //     //     }
    //     //     None => {
    //     //         unreachable!();
    //     //     }
    //     // }
    //     n += 1;

    //     if n % 1000 == 0 {
    //         println!("n = {:?}", n);
    //     }
    // }

    println!("done");

    let sleep_intervals = Duration::from_secs(3);
    loop {
        thread::sleep(sleep_intervals);
    }

    //let mut hopcount = 0;
    // for (key, definition) in definitions.clone() {
    //     if hopcount % 1000 == 0 {
    //         print!("a");
    //         io::stdout()
    //             .flush()
    //             .ok()
    //             .expect("Could not flush stdout");
    //         for node_client in node_clients.values_mut() {
    //             println!("{:?}",
    //                      node_client
    //                          .and_then(|c| {
    //                                        c.meta()
    //                                            .map_err(|e| {
    //                                                         println!("{:?}", e);
    //                                                         false
    //                                                     })
    //                                    })
    //                          .wait());
    //         }
    //     }
    //     // let current_node = &node_clients[&first_id];
    //     // assert!(current_node.set(key, definition.clone()).unwrap());
    //     // match current_node.get(key).unwrap() {
    //     //     Some(s) => {
    //     //         assert_eq!(s, definition);
    //     //     }
    //     //     None => {
    //     //         unreachable!();
    //     //     }
    //     // }
    //     //println!("{:?} {:?} {:?}", item_hopcount, definition_item.key, current_node.id().unwrap());
    // }
    // println!("4 {:?} {:?}",
    //          hopcount,
    //          (hopcount as f64) / (definitions.len() as f64));

    // let mut node_client_ids: Vec<_> = node_clients.keys().cloned().collect();
    // node_client_ids.sort();
    // println!("{:?}", node_client_ids);

    // let mut pred_ids: HashMap<Id, Id> = HashMap::new();
    // let mut next_ids: HashMap<Id, Id> = HashMap::new();
    // for w in node_client_ids.windows(2) {
    //     if let &[a, b] = w {
    //         next_ids.insert(a, b);
    //         pred_ids.insert(b, a);
    //     } else {
    //         unreachable!();
    //     }
    // }
    // let first_id = node_client_ids[0];
    // let last_id = *node_client_ids.last().unwrap();
    // next_ids.insert(last_id, first_id);
    // pred_ids.insert(first_id, last_id);

    // for node_client_id in node_client_ids {
    //     let next_id: Id = next_ids[&node_client_id];
    //     let pred_id: Id = pred_ids[&node_client_id];
    //     println!("2.6");
    //     println!("{:?}",
    //              node_clients[&node_client_id].assign_relations(pred_id, next_id));
    //     println!("2.7");
    // }

    // println!("2.8");
    // for node_client in node_clients.values() {
    //     match node_client.meta() {
    //         Ok(s) => println!("{:?}", s),
    //         Err(e) => println!("{:?}", e),
    //     }
    // }

    // println!("2.9");
    // let definitions = definitions_from_stdin();
    // println!("3");

    // let mut hopcount = 0;
    // for (key, definition) in definitions.clone() {
    //     hopcount += 1;
    //     if hopcount % 1000 == 0 {
    //         print!("a");
    //         io::stdout()
    //             .flush()
    //             .ok()
    //             .expect("Could not flush stdout");
    //         for node_client in node_clients.values() {
    //             match node_client.meta() {
    //                 Ok(s) => println!("{:?}", s),
    //                 Err(e) => println!("{:?}", e),
    //             }
    //         }
    //     }
    //     let current_node = &node_clients[&first_id];
    //     assert!(current_node.set(key, definition.clone()).unwrap());
    //     // match current_node.get(key).unwrap() {
    //     //     Some(s) => {
    //     //         assert_eq!(s, definition);
    //     //     }
    //     //     None => {
    //     //         unreachable!();
    //     //     }
    //     // }
    //     //println!("{:?} {:?} {:?}", item_hopcount, definition_item.key, current_node.id().unwrap());
    // }
    // println!("4 {:?} {:?}",
    //          hopcount,
    //          (hopcount as f64) / (definitions.len() as f64));
}
