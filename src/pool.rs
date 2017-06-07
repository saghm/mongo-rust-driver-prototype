//! Connection pooling for a single MongoDB server.
use error::Error::{self, ArgumentError, OperationError};
use error::Result;

use connstring::Host;
use stream::{Stream, StreamConnector};

use bufstream::BufStream;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};

pub static DEFAULT_POOL_SIZE: usize = 5;

/// Holds an available socket, with logic to return the socket
/// to the connection pool when dropped.
pub struct PooledStream {
    // This socket option will always be Some(stream) until it is
    // returned to the pool using take().
    socket: Option<BufStream<Stream>>,
    // A reference to the pool that the stream was taken from.
    pool: Arc<Mutex<Pool>>,
    // A reference to the waiting condvar associated with the pool.
    wait_lock: Arc<Condvar>,
    // The pool iteration at the moment of extraction.
    iteration: usize,
}

impl PooledStream {
    /// Returns a reference to the socket.
    pub fn get_socket(&mut self) -> &mut BufStream<Stream> {
        self.socket.as_mut().unwrap()
    }
}

impl Drop for PooledStream {
    fn drop(&mut self) {
        // Attempt to lock and return the socket to the pool,
        // or give up if the pool lock has been poisoned.
        if let Ok(mut locked) = self.pool.lock() {
            if self.iteration == locked.iteration {
                locked.sockets.push(self.socket.take().unwrap());
                // Notify waiting threads that the pool has been repopulated.
                self.wait_lock.notify_one();
            }
        }
    }
}
