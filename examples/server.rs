#![deny(warnings)]
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
#[macro_use] extern crate lazy_static;
extern crate libc;
extern crate memmap;
extern crate owning_ref;
extern crate pretty_env_logger;

use futures::Future;
use futures::future;
use futures_cpupool::CpuPool;
use hyper::{Get, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Http, Service, Request, Response};
use memmap::{Mmap, Protection};
use std::io::Read;
use std::fs::File;

#[derive(Clone, Copy)]
struct Echo;

fn read(mut f: File, len: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(len as usize);
    unsafe { v.set_len(len as usize); }
    f.read_exact(&mut v[..]).unwrap();
    v
}

fn copy(data: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(data.len());
    unsafe { v.set_len(data.len()); }
    v.copy_from_slice(data);
    v
}

enum Type {
    DirectRead,
    CopyRead,
    DirectMemmap{mlock: bool},
    CopyMemmap,
}

fn get(t: Type) -> Response {
    let f = File::open("f").unwrap();
    let len = f.metadata().unwrap().len();
    let chunk: hyper::Chunk = match t {
        Type::DirectRead => read(f, len).into(),
        Type::CopyRead => copy(&read(f, len)[..]).into(),
        Type::DirectMemmap{mlock} => {
            let m =
                Box::new(Mmap::open_with_offset(&f, Protection::Read, 0, len as usize).unwrap());
            if mlock && unsafe { libc::mlock(m.ptr() as *const libc::c_void, m.len()) } < 0 {
                panic!("mlock: {}", ::std::io::Error::last_os_error());
            }
            let data = owning_ref::BoxRef::new(m);
            let data = data.map(|m| unsafe { m.as_slice() });
            data.into()
        },
        Type::CopyMemmap => {
            let m = Mmap::open_with_offset(&f, Protection::Read, 0, len as usize).unwrap();
            copy(unsafe { m.as_slice() }).into()
        },
    };
    Response::new()
        .with_header(ContentLength(len))
        .with_body(chunk)
}

fn inline(t: Type) -> ::futures::BoxFuture<Response, hyper::Error> { future::ok(get(t)).boxed() }

fn threaded(t: Type) -> ::futures::BoxFuture<Response, hyper::Error> {
    lazy_static! {
        static ref POOL: CpuPool = CpuPool::new(1);
    }
    POOL.spawn_fn(move || Ok(get(t))).boxed()
}

impl Service for Echo {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = ::futures::BoxFuture<Response, hyper::Error>;

    fn call(&self, req: Request) -> Self::Future {
        match (req.method(), req.path()) {
            (&Get, "/inline-direct-read") => inline(Type::DirectRead),
            (&Get, "/threaded-direct-read") => threaded(Type::DirectRead),
            (&Get, "/inline-copy-read") => inline(Type::CopyRead),
            (&Get, "/threaded-copy-read") => threaded(Type::CopyRead),
            (&Get, "/inline-direct-memmap") => inline(Type::DirectMemmap{mlock: false}),
            (&Get, "/threaded-direct-memmap") => threaded(Type::DirectMemmap{mlock: true}),
            (&Get, "/inline-copy-memmap") => inline(Type::CopyMemmap),
            (&Get, "/threaded-copy-memmap") => threaded(Type::CopyMemmap),
            _ => future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        }
    }
}

fn main() {
    pretty_env_logger::init().unwrap();
    let addr = "127.0.0.1:1337".parse().unwrap();

    let server = Http::new().bind(&addr, || Ok(Echo)).unwrap();
    println!("Listening on http://{} with 1 thread.", server.local_addr().unwrap());
    server.run().unwrap();
}
