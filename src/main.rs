extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;
extern crate futures_cpupool;
extern crate hyper;

extern crate r2d2;
extern crate r2d2_postgres;

extern crate dotenv;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate chrono;

#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use chrono::prelude::*;

use std::io;
use dotenv::dotenv;
use std::env;

use tokio_service::Service;
use futures::{Future, BoxFuture};
use futures_cpupool::CpuPool;
use hyper::server::{Http, Request, Response};
use hyper::status::StatusCode;
use r2d2_postgres::{TlsMode, PostgresConnectionManager};

// user
#[derive(Serialize, Deserialize, Debug)]
struct User {
  id: i32,
  email: String,
  password: String,
}

// Log middleware
#[derive(Clone)]
pub struct Log<S> {
  upstream: S,
}

impl<S> Log<S> {
  pub fn new(upstream: S) -> Log<S> {
    Log { upstream: upstream }
  }
}

impl<S> Service for Log<S>
  where S: Service<Request = Request, Response = Response>
{
  type Request = S::Request;
  type Response = S::Response;
  type Error = S::Error;
  type Future = S::Future;

  fn call(&self, request: Self::Request) -> Self::Future {
    let method = request.method().to_string();
    let uri = request.uri().to_string();

    let before = Local::now();
    let response = self.upstream.call(request);
    let after = Local::now();

    info!("[{}] - {} {} {} μs",
          after,
          method,
          uri,
          after.signed_duration_since(before).num_microseconds().unwrap());

    response
  }
}

// Main Service
struct UserService {
  thread_pool: CpuPool,
  db_pool: r2d2::Pool<PostgresConnectionManager>,
}

impl UserService {
  fn new(thread_pool: CpuPool, db_pool: r2d2::Pool<PostgresConnectionManager>) -> UserService {
    UserService {
      thread_pool: thread_pool,
      db_pool: db_pool,
    }
  }
}

impl Service for UserService {
  type Request = Request;
  type Response = Response;
  type Error = hyper::Error;
  type Future = BoxFuture<Response, hyper::Error>;

  fn call(&self, _req: Request) -> Self::Future {
    let db = self.db_pool.clone();

    self.thread_pool
      .spawn_fn(move || {
        let conn = db.get()
          .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("timeout: {}", e)))
          .unwrap();

        let stmt = conn.prepare_cached("select id, email, password, created_at from users")
          .unwrap();

        let rows = stmt.query(&[])
          .unwrap()
          .iter()
          .map(|row| {
            User {
              id: row.get("id"),
              email: row.get("email"),
              password: row.get("password"),
            }
          })
          .collect::<Vec<_>>();

        Ok(rows)
      })
      .map(|msg| {
        Response::new()
          .with_status(StatusCode::Ok)
          .with_body(serde_json::to_string(&msg).unwrap())
      })
      .boxed()
  }
}

fn main() {
  dotenv().ok();
  env_logger::init().unwrap();

  let bind = env::var("BIND").unwrap_or(String::from("127.0.0.1"));
  let port = env::var("PORT").unwrap_or(String::from("9000"));
  let addr = format!("{}:{}", bind, port).parse().unwrap();

  let db_url = env::var("DATABASE_URL").unwrap();
  let db_config = r2d2::Config::default();
  let db_manager = PostgresConnectionManager::new(db_url, TlsMode::None).unwrap();
  let db_pool = r2d2::Pool::new(db_config, db_manager).unwrap();

  let thread_pool = CpuPool::new(10);
  Http::new()
    .bind(&addr,
          move || Ok(Log::new(UserService::new(thread_pool.clone(), db_pool.clone()))))
    .unwrap()
    .run()
    .unwrap();
}
