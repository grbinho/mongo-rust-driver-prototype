use std::io::Read;
use std::io::Write;
use std::io::Error;
use std::io::Result;
use std::io::empty;

use bufstream::BufStream;
use std::net::TcpStream;
use native_tls::{TlsConnector,TlsStream};

pub struct BufConnection {
    pub tls: bool,
    // Since this is private, there is no reason for it to be option, but one will always be empty
    tls_stream: Option<BufStream<TlsStream<TcpStream>>>,
    tcp_stream: Option<BufStream<TcpStream>>
}

impl<'a> Read for BufConnection {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
    	let result = match self.tls {
    		true => match self.tls_stream {
            	Some(ref mut stream) => stream.read(buf),
            	None => panic!("Using SSL, expected TLS stream.")
            },           
            false => match self.tcp_stream {
            	Some(ref mut stream) => stream.read(buf),
            	None => panic!("Not using SSL, expected TCP stream.")
            }
        };
        
        result
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {  

		let result = match self.tls {
    		true => match self.tls_stream {
            	Some(ref mut stream) => stream.read_to_end(buf),
            	None => panic!("Using SSL, expected TLS stream.")
            },           
            false => match self.tcp_stream {
            	Some(ref mut stream) => stream.read_to_end(buf),
            	None => panic!("Not using SSL, expected TCP stream.")
            }
        };
        
        result
    }

}

impl<'a> Write for BufConnection {
	fn write(&mut self, buf: &[u8]) -> Result<usize> {

		let result = match self.tls {
    		true => match self.tls_stream {
            	Some(ref mut stream) => stream.write(buf),
            	None => panic!("Using SSL, expected TLS stream.")
            }, 
            false => match self.tcp_stream {
            	Some(ref mut stream) => stream.write(buf),
            	None => panic!("Not using SSL, expected TCP stream.")
            }
        };
        
        result		
	}

	fn flush(&mut self) -> Result<()> {
		let result = match self.tls {
    		true => match self.tls_stream {
            	Some(ref mut stream) => stream.flush(),
            	None => panic!("Using SSL, expected TLS stream.")
            },           
            false => match self.tcp_stream {
            	Some(ref mut stream) => stream.flush(),
            	None => panic!("Not using SSL, expected TCP stream.")
            }
        };
        
        result
	}
}

impl BufConnection {

    pub fn new_tcp(stream: BufStream<TcpStream>) -> BufConnection {
        BufConnection {
            tls: false,
            tcp_stream: Some(stream),
            tls_stream: None
        }
    }

    pub fn new_tls(stream: BufStream<TlsStream<TcpStream>>) -> BufConnection {
        BufConnection {
            tls: true,
            tcp_stream: None,
            tls_stream: Some(stream)
        }
    }

    pub fn get_ref(&self) -> &TcpStream {
        match self.tls {
            true =>  match self.tls_stream.as_ref() {
            	Some(stream) => stream.get_ref().get_ref(),
            	None => panic!("Using SSL, expected TLS stream.")
            },
            false => match self.tcp_stream.as_ref() {
            	Some(stream) => stream.get_ref(),
            	None => panic!("Not using SSL, expected TCP stream.")
            }
        }
    }

    pub fn get_mut(&mut self) -> &mut TcpStream {
    	match self.tls {
            true =>  match self.tls_stream {
            	Some(ref mut stream) => stream.get_mut().get_mut(),
            	None => panic!("Using SSL, expected TLS stream.")
            },
            false => match self.tcp_stream {
            	Some(ref mut stream) => stream.get_mut(),
            	None => panic!("Not using SSL, expected TCP stream.")
            }
        }    	
    }
}
