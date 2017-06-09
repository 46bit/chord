#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]

extern crate csv;
extern crate sha1;
extern crate rand;
extern crate tarpc;
extern crate chord;
extern crate futures;
extern crate tokio_core;

use std::time::Duration;
use std::collections::HashMap;
use std::sync::mpsc as stdmpsc;
use std::thread;
use std::net::SocketAddr;
use tarpc::future::{client, server};
use tarpc::future::client::ClientExt;
use futures::{stream, Future, Stream, Sink};
use futures::sync::mpsc;
use tokio_core::reactor::Core;
use rand::{Rng, StdRng};
use chord::*;

fn main() {
    let number_of_nodes = 8;

    //let (tx, rx) = mpsc::channel(10);
    // thread::spawn(move || {
    //     let number_of_nodes = number_of_nodes;
    //     let mut core = Core::new().unwrap();
    //     let handle = core.handle();
    //     let server = rx.map(|v| {
    //                             println!("v");
    //                             v
    //                         })
    //         .buffer_unordered(number_of_nodes)
    //         .map(|r| {
    //                  println!("r {:?}", r);
    //              })
    //         .into_future();
    //     handle.spawn(server.map(|_| ()).map_err(|_| {
    //         println!("err");
    //         ()
    //     }));
    //     //core.run(server).unwrap();
    //     loop {
    //         core.turn(None)
    //     }
    // });

    let mut rng = StdRng::new().unwrap();

    let mut node_clients = HashMap::new();
    for i in 1..number_of_nodes {
        let addr: SocketAddr = format!("213.138.101.13:{:?}", 4646 + i).parse().unwrap();
        let node_id = Id::from(addr);
        node_clients.insert(node_id,
                            FutureClient::connect(node_id.addr, client::Options::default())
                                .wait()
                                .unwrap());
    }

    let definitions = definitions_from_stdin();
    println!("3");

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    // let server = rx;
    //     handle.spawn(server.map(|_| ()).map_err(|_| {
    //         println!("err");
    //         ()
    //     }));

    let definitions = definitions
        .into_iter()
        .map(|(k, d)| Ok((k, d)))
        .collect::<Vec<Result<_, ()>>>();
    let server = stream::iter(definitions)
        .map(move |(key, definition)| {
                 let node_clients_vec = node_clients.values().collect::<Vec<_>>();
                 let node_client = rng.choose(&node_clients_vec).cloned().unwrap();
                 //println!("a");
                 node_client.set(key, definition.clone()).map_err(|_| ())
             })
        .map(|v| {
                 //println!("v");
                 v
             })
        .map_err(|_| {
                     println!("err3");
                     ()
                 })
        .buffer_unordered(number_of_nodes * 5)
        .map(|r| {
                 //println!("r {:?}", r);
             })
        .map_err(|e| {
                     println!("err1");
                 })
        .collect();
    handle.spawn(server
                     .map(|_| {
                              println!("```");
                              ()
                          })
                     .map_err(|_| {
                                  println!("err2");
                                  ()
                              }));

    // let server = rx.map(|v| {
    //                             println!("v");
    //                             v
    //                         })
    //         .buffer_unordered(number_of_nodes)
    //         .map(|r| {
    //                  println!("r {:?}", r);
    //              })
    //         .into_future();
    //     handle.spawn(server.map(|_| ()).map_err(|_| {
    //         println!("err");
    //         ()
    //     }));

    // let definitions = definitions
    //     .into_iter()
    //     .map(|(k, d)| Ok((k, d)))
    //     .collect::<Vec<Result<_, ()>>>();
    // let server = stream::iter(definitions)
    //     .map(move |(key, definition)| {
    //              let node_clients_vec = node_clients.values().collect::<Vec<_>>();
    //              let node_client = rng.choose(&node_clients_vec).cloned().unwrap();
    //              println!("a");
    //              node_client.set(key, definition.clone()).map_err(|_| ())
    //          })
    //     .forward(tx.sink_map_err(|e| {
    //         println!("err {:?}", e);
    //         ()
    //     }));
    // handle.spawn(server.map(|_| ()).map_err(|_| ()));
    //core.run(server).unwrap();
    loop {
        core.turn(None)
    }
}
