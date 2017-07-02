use bufstream::BufStream;
use r2d2::{Config, ManageConnection, Pool, PooledConnection};

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::net::SocketAddr;
use std::ops::Deref;

use error::{Error, Result};

use Client;

#[cfg(feature = "ssl")]
use openssl::ssl::{Ssl, SslMethod, SslContext, SslStream, SSL_OP_NO_COMPRESSION, SSL_OP_NO_SSLV2,
                   SSL_OP_NO_SSLV3, SSL_VERIFY_NONE, SSL_VERIFY_PEER};
#[cfg(feature = "ssl")]
use openssl::x509::X509_FILETYPE_PEM;

pub type PooledStream = PooledConnection<StreamConnector>;

pub fn create_pool(hostname: &str,
                   port: u16,
                   connector_type: StreamConnectorType,
                   pool_size: Option<u32>) -> Result<Pool<StreamConnector>> {
    let mut builder = Config::builder().initialization_fail_fast(false);

    if let Some(size) = pool_size {
        builder = builder.pool_size(size);
    }

    Pool::new(builder.build(), StreamConnector {
        hostname: hostname.to_string(),
        port,
        connector_type
    }).map_err(Error::from)
}

#[derive(Clone)]
pub struct StreamConnector {
    hostname: String,
    port: u16,
    connector_type: StreamConnectorType,
}

/// Encapsulates the functionality for how to connect to the server.
#[derive(Clone)]
pub enum StreamConnectorType {
    /// Connect to the server through a regular TCP stream.
    Tcp,
    #[cfg(feature = "ssl")]
    /// Connect to the server through a TCP stream encrypted with SSL.
    Ssl {
        ca_file: String,
        certificate_file: String,
        key_file: String,
        verify_peer: bool,
    },
}

impl Default for StreamConnectorType {
    fn default() -> Self {
        Self::Tcp
    }
}

impl ManageConnection for StreamConnector {
    type Connection = Stream;
    type Error = Error;

    fn connect(&self) -> Result<Stream> {
        match self.connector_type {
            StreamConnectorType::Tcp => Ok(Stream(BufStream::new(StreamType::Tcp(TcpStream::connect((self.hostname.as_str(), self.port))?)))),
            #[cfg(feature = "ssl")]
            StreamConnectorType::Ssl {
                ref ca_file,
                ref certificate_file,
                ref key_file,
                verify_peer
            } => {
                let inner_stream = TcpStream::connect((&self.hostname, self.port))?;

                let mut ssl_context = SslContext::builder(SslMethod::tls())?;
                ssl_context.set_cipher_list("ALL:!EXPORT:!eNULL:!aNULL:HIGH:@STRENGTH")?;
                ssl_context.set_options(SSL_OP_NO_SSLV2);
                ssl_context.set_options(SSL_OP_NO_SSLV3);
                ssl_context.set_options(SSL_OP_NO_COMPRESSION);
                ssl_context.set_ca_file(ca_file)?;
                ssl_context.set_certificate_file(certificate_file, X509_FILETYPE_PEM)?;
                ssl_context.set_private_key_file(key_file, X509_FILETYPE_PEM)?;

                let verify = if verify_peer {
                    SSL_VERIFY_PEER
                } else {
                    SSL_VERIFY_NONE
                };
                ssl_context.set_verify(verify);

                let mut ssl = Ssl::new(&ssl_context.build())?;
                ssl.set_hostname(hostname)?;

                match ssl.connect(inner_stream) {
                    Ok(s) => Ok(Stream(BufStream::new(Stream::Ssl(s)))),
                    Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                }
            }
        }
    }

    fn is_valid(&self, conn: &mut Stream) -> Result<()> {
        unimplemented!()
    }

    fn has_broken(&self, conn: &mut Stream) -> bool {
        false
    }
}

pub struct Stream(BufStream<StreamType>);

impl Deref for Stream {
    type Target = BufStream<StreamType>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Stream {
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match *self.get_ref() {
            StreamType::Tcp(ref stream) => stream.peer_addr(),
            #[cfg(feature = "ssl")]
            StreamType::Ssl(ref stream) => stream.get_ref().peer_addr(),
        }
    }
}

pub enum StreamType {
    Tcp(TcpStream),
    #[cfg(feature = "ssl")]
    Ssl(SslStream<TcpStream>),
}

impl Read for StreamType {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            StreamType::Tcp(ref mut s) => s.read(buf),
            #[cfg(feature = "ssl")]
            StreamType::Ssl(ref mut s) => s.read(buf),
        }
    }
}

impl Write for StreamType {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            StreamType::Tcp(ref mut s) => s.write(buf),
            #[cfg(feature = "ssl")]
            StreamType::Ssl(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            StreamType::Tcp(ref mut s) => s.flush(),
            #[cfg(feature = "ssl")]
            StreamType::Ssl(ref mut s) => s.flush(),
        }
    }
}
