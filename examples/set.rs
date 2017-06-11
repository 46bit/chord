#![feature(inclusive_range_syntax)]
#![feature(plugin)]
#![plugin(tarpc_plugins)]
#![feature(slice_patterns)]
#![feature(box_syntax)]

extern crate csv;
extern crate sha1;
extern crate rand;
extern crate tarpc;
extern crate chord;
extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;

use std::time::Duration;
use std::collections::HashMap;
use std::net::SocketAddr;
use tarpc::future::client;
use tarpc::future::client::ClientExt;
use futures::{stream, Future, Stream};
use tokio_core::reactor::{Core, Handle};
use tokio_timer::*;
use rand::{Rng, StdRng};
use chord::*;

fn new_client(id: Id, handle: Handle) -> Connect<FutureClient> {
    let client_options = client::Options::default(); //.handle(handle);
    FutureClient::connect(id.addr, client_options)
}

//pub type TimeoutResult<I, E> = Result<I, TimeoutErr<E>>;

#[derive(Debug)]
pub enum TimeoutErr<E> {
    FutureErr(E),
    TimedOut,
}

impl<E, F> From<TimeoutError<F>> for TimeoutErr<E> {
    fn from(_: TimeoutError<F>) -> TimeoutErr<E> {
        TimeoutErr::TimedOut
    }
}

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

    let timer = Timer::default();

    let mut rng = StdRng::new().unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let mut node_clients = HashMap::new();
    for i in 1..number_of_nodes {
        //213.138.101.13
        let addr: SocketAddr = format!("0.0.0.0:{:?}", 4646 + i).parse().unwrap();
        let node_id = Id::from(addr);
        println!("{:?}", node_id);

        let client_future = new_client(node_id, handle.clone());
        //let client = utils::wait_timeout(client_future, Duration::from_secs(10)).unwrap();
        let client = client_future.wait().unwrap();
        //let meta = utils::wait_timeout(client.meta(), Duration::from_secs(10)).unwrap();
        //println!("{:?}", meta);
        node_clients.insert(node_id, client);
        // let client_options = client::Options::default().handle(handle.clone());
        // node_clients.insert(node_id,
        //                     FutureClient::connect(node_id.addr, client_options)
        //                         .wait()
        //                         .unwrap());
    }

    let definitions = definitions_from_stdin();
    println!("3");

    // let server = rx;
    //     handle.spawn(server.map(|_| ()).map_err(|_| {
    //         println!("err");
    //         ()
    //     }));

    let definitions = definitions
        .into_iter()
        .map(|(k, d)| Ok((k, d)))
        .collect::<Vec<Result<_, ()>>>();
    let mut i = 0;
    let server = stream::iter(definitions)
        .map(move |(key, definition)| {
            let node_clients_vec = node_clients.iter().collect::<Vec<_>>();
            let (node_id, node_client) = rng.choose(&node_clients_vec).unwrap().clone();
            let node_id = *node_id;
            let node_client = node_client.clone();
            //println!("a {:?}", node_id.addr);
            timer
                .timeout(node_client
                             .set(key, definition.clone())
                             .map_err(|e| TimeoutErr::FutureErr(e)),
                         Duration::from_secs(20))
                .map(move |r| (node_id, r))
                .map_err(move |e| {
                             println!("err3 {:?} {:?}", node_id.addr, e);
                         })
        })
        .buffer_unordered(number_of_nodes * 4)
        .map(move |(id,r)| {
                 //println!("r {:?} {:?}", id.addr, r);
                 i += 1;
                 if i % 1000 == 0 {
                     println!("i = {:?}", i);
                 }
             })
        .map_err(|e| {
                     println!("err1 {:?}", e);
                     ()
                 })
        .collect();
    // handle.spawn(server
    //                  .map(|_| {
    //                           println!("```");
    //                           ()
    //                       })
    //                  .map_err(|_| {
    //                               println!("err2");
    //                               ()
    //                           }));
    core.run(server
                 .map(|_| {
                          println!("```");
                      })
                 .map_err(|e| {
                              println!("err2 {:?}", e);
                          }))
        .unwrap();

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
