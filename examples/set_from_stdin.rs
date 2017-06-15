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
use std::io::prelude::*;
use std::io::stdout;
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

    let timer = Timer::default();

    let mut rng = StdRng::new().unwrap();

    let mut reactor = Core::new().unwrap();
    let handle = reactor.handle();

    let mut node_clients = HashMap::new();
    for i in 1..number_of_nodes {
        //213.138.101.13
        let addr: SocketAddr = format!("0.0.0.0:{:?}", 4646).parse().unwrap();
        let node_id = Id::from(addr);
        print!("Connecting to {:?}...", node_id);
        stdout().flush().unwrap();

        let client_future = new_client(node_id, handle.clone());
        let client = client_future.wait().unwrap();
        node_clients.insert(node_id, client);
        println!("connected.");
    }

    let definitions = definitions_from_stdin();
    println!("Loaded defintions.");

    let definitions = definitions
        .into_iter()
        .map(|(k, d)| Ok((k, d)))
        .collect::<Vec<Result<_, ()>>>();
    let mut i = 0;
    let server = stream::iter(definitions)
        .map(move |(key, definition)| {
            // Choose a random client connection.
            let node_clients_vec = node_clients.iter().collect::<Vec<_>>();
            let (node_id, node_client) = rng.choose(&node_clients_vec).unwrap().clone();
            let node_id = *node_id;
            let node_client = node_client.clone();
            // Perform SET operation with a timeout.
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
        .map(move |(id, r)| {
                 i += 1;
                 if i % 1 == 0 {
                     println!("Set {:?}th definition.", i);
                 }
             })
        .map_err(|e| {
                     println!("err1 {:?}", e);
                 })
        .collect()
        .map(|_| {
                 println!("Done.");
             })
        .map_err(|e| {
                     println!("Failed {:?}.", e);
                 });

    reactor.run(server).unwrap();
}
