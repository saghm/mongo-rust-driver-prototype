use bufstream::BufStream;
use r2d2::{ManageConnection, PooledConnection};

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

impl Default for StreamConnector {
    fn default() -> Self {
        Self {
            hostname: "localhost".to_string(),
            port: 27017,
            connector_type: StreamConnectorType::Tcp,
        }
    }
}

impl StreamConnector {
    pub fn new(hostname: &str, port: u16) -> Self {
        Self {
            hostname: hostname.to_string(),
            port: port,
            connector_type: StreamConnectorType::Tcp,
        }
    }

    #[cfg(feature = "ssl")]
    /// Creates a StreamConnector that will connect with SSL encryption.
    ///
    /// The SSL connection will use the cipher with the longest key length available to both the
    /// server and client, with the following caveats:
    ///   * SSLv2 and SSlv3 are disabled
    ///   * Export-strength ciphers are disabled
    ///   * Ciphers not offering encryption are disabled
    ///   * Ciphers not offering authentication are disabled
    ///   * Ciphers with key lengths of 128 or fewer bits are disabled.
    ///
    /// Note that TLS compression is disabled for SSL connections.
    ///
    /// # Arguments
    ///
    /// `ca_file` - Path to the file containing trusted CA certificates.
    /// `certificate_file` - Path to the file containing the client certificate.
    /// `key_file` - Path to the file containing the client private key.
    /// `verify_peer` - Whether or not to verify that the server's certificate is trusted.
    pub fn with_ssl(hostname: &str,
                    port: u16,
                    ca_file: &str,
                    certificate_file: &str,
                    key_file: &str,
                    verify_peer: bool)
                    -> Self {
        Self {
            hostname: hostname.to_string(),
            port: port,
            connector_type: StreamConnectorType::Ssl {
                ca_file: String::from(ca_file),
                certificate_file: String::from(certificate_file),
                key_file: String::from(key_file),
                verify_peer: verify_peer,
            }
        }
    }
}

impl ManageConnection for StreamConnector {
    type Connection = Stream;
    type Error = Error;

    fn connect(&self) -> Result<Stream> {
        match self.connector_type {
            StreamConnectorType::Tcp => Ok(Stream(BufStream::new(StreamType::Tcp(TcpStream::connect((self.hostname.as_str(), self.port))?)))),
            #[cfg(feature = "ssl")]
            StreamConnectorType::Ssl { ref ca_file,
                                   ref certificate_file,
                                   ref key_file,
                                   verify_peer } => {
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
