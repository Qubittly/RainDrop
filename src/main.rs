use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use std::error::Error;
use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use chrono::Utc;

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 10; // 10 MB
const MAX_MEMBERS: usize = 10;

/// Represents metadata for a file in the network.
struct FileMetadata {
    name: String,
    size: u64,
    path: String,
    owner: String,
    shared_with: HashSet<String>,
    timestamp: chrono::DateTime<Utc>,
}

/// Represents a peer in the network.
struct Peer {
    username: String,
    ip_address: String,
    port: u16,
    online: bool,
}

/// Represents a group in the network.
struct Group {
    name: String,
    owner: String,
    members: HashSet<String>,
    pending_requests: HashSet<String>,
    files: Vec<(String, FileMetadata)>,
}

/// Represents a user in the network.
struct User {
    username: String,
    groups: HashSet<String>,
    pending_requests: HashSet<String>,
    shared_files: HashSet<String>,
    ip_address: String,
    port: u16,
    online: bool,
}

struct State {
    groups: Arc<Mutex<HashMap<String, Group>>>,
    users: Arc<Mutex<HashMap<String, User>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8082".to_string());
    let listener = TcpListener::bind(&server_addr).await?;

    let state = Arc::new(Mutex::new(State { groups, users }));
    loop {
        let (socket, addr) = listener.accept().await?;

        tokio::spawn(async move {
            println!("Accepted connection from {}", addr);
            handle_connection(socket, addr).await;
        });
    }
}

async fn handle_connection<S>(socket: S, addr: SocketAddr) 
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    println!("Handling connection from {}", addr);
}
