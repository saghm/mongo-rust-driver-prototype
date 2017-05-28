//! # MongoDB Rust Driver
//!
//! A driver written in pure Rust, providing a native interface to MongoDB.
//!
//! ## Connecting to MongoDB
//!
//! The Client is an entry-point to interacting with a MongoDB instance.
//!
//! ```no_run
//! # use mongodb::{Connector, ThreadedClient};
//! #
//!
//! // Create a new Connector to create Clients from.
//! let connector = Connector::new();
//!
//! // Connects to
//! let client = connector.connect("localhost", 27017)
//!     .expect("Failed to initialize client.");
//!
//! // Connect to a complex server topology, such as a replica set
//! // or sharded cluster, using a connection string uri.
//! let client = connector.connect_with_uri("mongodb://localhost:27017,localhost:27018/")
//!     .expect("Failed to initialize client.");
//! ```
//!
//! ## Interacting with MongoDB Collections
//!
//! ```no_run
//! # #[macro_use] extern crate bson;
//! # extern crate mongodb;
//! # use mongodb::{Connector, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # use bson::Bson;
//! #
//! # fn main() {
//! # let connector = Connector::new();
//! # let client = connector.connect("localhost", 27017).unwrap();
//! #
//! let coll = client.db("media").collection("movies");
//! coll.insert_one(doc!{ "title" => "Back to the Future" }, None).unwrap();
//! coll.update_one(doc!{}, doc!{ "director" => "Robert Zemeckis" }, None).unwrap();
//! coll.delete_many(doc!{}, None).unwrap();
//!
//! let mut cursor = coll.find(None, None).unwrap();
//! for result in cursor {
//!     if let Ok(item) = result {
//!         if let Some(&Bson::String(ref title)) = item.get("title") {
//!             println!("title: {}", title);
//!         }
//!     }
//! }
//! # }
//! ```
//!
//! ## Command Monitoring
//!
//! The driver provides an intuitive interface for monitoring and responding to runtime information
//! about commands being executed on the server. Arbitrary functions can be used as start and
//! completion hooks, reacting to command results from the server.
//!
//! ```no_run
//! # use mongodb::{Client, CommandResult, Connector, ThreadedClient};
//! #
//! fn log_query_duration(client: Client, command_result: &CommandResult) {
//!     match *command_result {
//!         CommandResult::Success { duration, .. } => {
//!             println!("Command took {} nanoseconds.", duration);
//!         },
//!         _ => println!("Failed to execute command."),
//!     }
//! }
//!
//! let connector = Connector::new();
//! let mut client = connector.connect("localhost", 27017).unwrap();
//! client.add_completion_hook(log_query_duration).unwrap();
//! ```
//!
//! ## Topology Monitoring
//!
//! Each server within a MongoDB server set is monitored asynchronously for changes in status, and
//! the driver's view of the current topology is updated in response to this. This allows the
//! driver to be aware of the status of the server set it is communicating with, and to make server
//! selections appropriately with regards to the user-specified `ReadPreference` and `WriteConcern`.
//!
//! ## Connection Pooling
//!
//! XXX:Rewrite documentation here once r2d2 is integrated.

// Clippy lints
#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![cfg_attr(feature = "clippy", allow(
    doc_markdown,
// allow double_parens for bson/doc macro.
    double_parens,
// more explicit than catch-alls.
    match_wild_err_arm,
    too_many_arguments,
))]
#![cfg_attr(feature = "clippy", warn(
    cast_precision_loss,
    enum_glob_use,
    filter_map,
    if_not_else,
    invalid_upcast_comparisons,
    items_after_statements,
    mem_forget,
    mut_mut,
    mutex_integer,
    non_ascii_literal,
    nonminimal_bool,
    option_map_unwrap_or,
    option_map_unwrap_or_else,
    print_stdout,
    shadow_reuse,
    shadow_same,
    shadow_unrelated,
    similar_names,
    unicode_not_nfc,
    unseparated_literal_suffix,
    used_underscore_binding,
    wrong_pub_self_convention,
))]

#[doc(html_root_url = "https://docs.rs/mongodb")]
#[macro_use(bitflags)]
extern crate bitflags;
#[macro_use(bson, doc)]
extern crate bson;
extern crate bufstream;
extern crate byteorder;
extern crate chrono;
extern crate crypto;
extern crate data_encoding;
#[cfg(feature = "ssl")]
extern crate openssl;
extern crate rand;
#[macro_use]
extern crate scan_fmt;
extern crate semver;
extern crate separator;
extern crate textnonce;
extern crate time;

pub mod db;
pub mod coll;
pub mod common;
pub mod connstring;
pub mod cursor;
pub mod error;
pub mod gridfs;
pub mod pool;
pub mod stream;
pub mod topology;
pub mod wire_protocol;

mod apm;
mod auth;
mod command_type;

pub use apm::{CommandStarted, CommandResult};
pub use command_type::CommandType;
pub use error::{Error, ErrorCode, Result};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicIsize, Ordering, ATOMIC_ISIZE_INIT};

use apm::Listener;
use bson::Bson;
use common::{ReadPreference, WriteConcern};
use connstring::ConnectionString;
use db::{Database, ThreadedDatabase};
use error::Error::ResponseError;
use pool::PooledStream;
use stream::ConnectMethod;
use topology::{Topology, TopologyDescription, DEFAULT_HEARTBEAT_FREQUENCY_MS,
               DEFAULT_LOCAL_THRESHOLD_MS, DEFAULT_SERVER_SELECTION_TIMEOUT_MS};
use topology::server::Server;

/// Interfaces with a MongoDB server or replica set.
pub struct ClientInner {
    req_id: Arc<AtomicIsize>,
    topology: Topology,
    listener: Listener,
    log_file: Option<Mutex<File>>,
}

/// Configuration options for a client.
pub struct Connector {
    /// File path for command logging.
    pub log_file: Option<String>,
    /// Frequency of server monitor updates; default 10000 ms.
    pub heartbeat_frequency_ms: u32,
    /// Timeout for selecting an appropriate server for operations; default 30000 ms.
    pub server_selection_timeout_ms: i64,
    /// The size of the latency window for selecting suitable servers; default 15 ms.
    pub local_threshold_ms: i64,
}

impl Default for Connector {
    fn default() -> Self {
        Self {
            log_file: None,
            heartbeat_frequency_ms: DEFAULT_HEARTBEAT_FREQUENCY_MS,
            server_selection_timeout_ms: DEFAULT_SERVER_SELECTION_TIMEOUT_MS,
            local_threshold_ms: DEFAULT_LOCAL_THRESHOLD_MS,
        }
    }
}

impl Connector {
    /// Creates a new default options struct.
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates a new options struct with a specified log file.
    pub fn log_file(&mut self, file: &str) -> &mut Self {
        self.log_file = Some(file.to_string());
        self
    }

    pub fn connect(&self, host: &str, port: u16) -> Result<Client> {
        self.connect_with_connection_string(ConnectionString::new(host, port), Default::default())
    }

    pub fn connect_with_uri(&self, uri: &str) -> Result<Client> {
        self.connect_with_connection_string(ConnectionString::parse(uri)?, Default::default())
    }

    #[cfg(feature = "ssl")]
    pub fn connect_with_ssl(&self,
                            uri: &str,
                            ca_file: &str,
                            certificate_file: &str,
                            key_file: &str,
                            verify_peer: bool)
                            -> Result<Client> {
        let connect_method =
            ConnectMethod::with_ssl(ca_file, certificate_file, key_file, verify_peer);
        self.connect_with_connection_string(ConnectionString::parse(uri)?, connect_method)
    }


    fn connect_with_connection_string(&self,
                                      connection_string: ConnectionString,
                                      connect_method: ConnectMethod)
                                      -> Result<Client> {
        let listener = Listener::new();

        let file = match self.log_file {
            Some(ref string) => {
                let _ = listener.add_start_hook(log_command_started);
                let _ = listener.add_completion_hook(log_command_completed);

                Some(Mutex::new(OpenOptions::new()
                                    .write(true)
                                    .append(true)
                                    .create(true)
                                    .open(string)?))
            }
            None => None,
        };


        let description = TopologyDescription::new(connect_method.clone());

        let client = Arc::new(ClientInner {
                                  req_id: Arc::new(ATOMIC_ISIZE_INIT),
                                  topology: Topology::new(connection_string.clone(),
                                                          Some(description),
                                                          connect_method.clone())?,
                                  listener: listener,
                                  log_file: file,
                              });

        // Fill servers array and set options
        {
            let top_description = &client.topology.description;
            let mut top = top_description.write()?;
            top.heartbeat_frequency_ms = self.heartbeat_frequency_ms;
            top.server_selection_timeout_ms = self.server_selection_timeout_ms;
            top.local_threshold_ms = self.local_threshold_ms;

            for host in &connection_string.hosts {
                let server = Server::new(client.clone(),
                                         host.clone(),
                                         top_description.clone(),
                                         true,
                                         connect_method.clone());

                top.servers.insert(host.clone(), server);
            }
        }

        Ok(client)

    }
}

pub trait ThreadedClient: Sync + Sized {
    /// Creates a database representation.
    fn db(&self, db_name: &str) -> Database;
    /// Creates a database representation with custom read and write controls.
    fn db_with_prefs(&self,
                     db_name: &str,
                     read_preference: Option<ReadPreference>,
                     write_concern: Option<WriteConcern>)
                     -> Database;
    /// Acquires a connection stream from the pool, along with slave_ok and should_send_read_pref.
    fn acquire_stream(&self, read_pref: ReadPreference) -> Result<(PooledStream, bool, bool)>;
    /// Acquires a connection stream from the pool for write operations.
    fn acquire_write_stream(&self) -> Result<PooledStream>;
    /// Returns a unique operational request id.
    fn get_req_id(&self) -> i32;
    /// Returns a list of all database names that exist on the server.
    fn database_names(&self) -> Result<Vec<String>>;
    /// Drops the database defined by `db_name`.
    fn drop_database(&self, db_name: &str) -> Result<()>;
    /// Reports whether this instance is a primary, master, mongos, or standalone mongod instance.
    fn is_master(&self) -> Result<bool>;
    /// Sets a function to be run every time a command starts.
    fn add_start_hook(&mut self, hook: fn(Client, &CommandStarted)) -> Result<()>;
    /// Sets a function to be run every time a command completes.
    fn add_completion_hook(&mut self, hook: fn(Client, &CommandResult)) -> Result<()>;
}

pub type Client = Arc<ClientInner>;

impl ThreadedClient for Client {
    fn db(&self, db_name: &str) -> Database {
        Database::open(self.clone(), db_name, None, None)
    }

    fn db_with_prefs(&self,
                     db_name: &str,
                     read_preference: Option<ReadPreference>,
                     write_concern: Option<WriteConcern>)
                     -> Database {
        Database::open(self.clone(), db_name, read_preference, write_concern)
    }

    fn acquire_stream(&self,
                      read_preference: ReadPreference)
                      -> Result<(PooledStream, bool, bool)> {
        self.topology.acquire_stream(read_preference)
    }

    fn acquire_write_stream(&self) -> Result<PooledStream> {
        self.topology.acquire_write_stream()
    }

    fn get_req_id(&self) -> i32 {
        self.req_id.fetch_add(1, Ordering::SeqCst) as i32
    }

    fn database_names(&self) -> Result<Vec<String>> {
        let mut doc = bson::Document::new();
        doc.insert("listDatabases", Bson::I32(1));

        let db = self.db("admin");
        let res = try!(db.command(doc, CommandType::ListDatabases, None));
        if let Some(&Bson::Array(ref batch)) = res.get("databases") {
            // Extract database names
            let map = batch
                .iter()
                .filter_map(|bdoc| {
                                if let Bson::Document(ref doc) = *bdoc {
                                    if let Some(&Bson::String(ref name)) = doc.get("name") {
                                        return Some(name.to_owned());
                                    }
                                }
                                None
                            })
                .collect();
            return Ok(map);
        }

        Err(ResponseError(String::from("Server reply does not contain 'databases'.")))
    }

    fn drop_database(&self, db_name: &str) -> Result<()> {
        let db = self.db(db_name);
        try!(db.drop_database());
        Ok(())
    }

    fn is_master(&self) -> Result<bool> {
        let mut doc = bson::Document::new();
        doc.insert("isMaster", Bson::I32(1));

        let db = self.db("local");
        let res = try!(db.command(doc, CommandType::IsMaster, None));

        match res.get("ismaster") {
            Some(&Bson::Boolean(is_master)) => Ok(is_master),
            _ => Err(ResponseError(String::from("Server reply does not contain 'ismaster'."))),
        }
    }

    fn add_start_hook(&mut self, hook: fn(Client, &CommandStarted)) -> Result<()> {
        self.listener.add_start_hook(hook)
    }

    fn add_completion_hook(&mut self, hook: fn(Client, &CommandResult)) -> Result<()> {
        self.listener.add_completion_hook(hook)
    }
}

fn log_command_started(client: Client, command_started: &CommandStarted) {
    let mutex = match client.log_file {
        Some(ref mutex) => mutex,
        None => return,
    };

    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    let _ = writeln!(guard.deref_mut(), "{}", command_started);
}

fn log_command_completed(client: Client, command_result: &CommandResult) {
    let mutex = match client.log_file {
        Some(ref mutex) => mutex,
        None => return,
    };

    let mut guard = match mutex.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };

    let _ = writeln!(guard.deref_mut(), "{}", command_result);
}
