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

    for i in 1..number_of_nodes {
        //213.138.101.13
        let addr: SocketAddr = format!("0.0.0.0:{:?}", 4646 + i).parse().unwrap();
        let node_id = Id::from(addr);
        println!("{:?}", node_id);

        let client_future = new_client(node_id, handle.clone());
        //let client = utils::wait_timeout(client_future, Duration::from_secs(10)).unwrap();
        let client = client_future.wait().unwrap();

        if let Ok(meta) = utils::wait_timeout(client.meta(), Duration::from_secs(1)) {
            //println!("{:?}", meta);
        }
        if let Ok(delete) = utils::wait_timeout(client.exists([3387451404, 2239804246, 3566425740, 2864059721, 3427156997]), Duration::from_secs(1)) {
            println!("{:?}", delete);
        }

        //node_clients.insert(node_id, client);
        // let client_options = client::Options::default().handle(handle.clone());
        // node_clients.insert(node_id,
        //                     FutureClient::connect(node_id.addr, client_options)
        //                         .wait()
        //                         .unwrap());
    }
}
