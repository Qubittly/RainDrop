use tokio::net::TcpListener;
use std::error::Error;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use chrono::Utc;

struct FileMetadata {
    name: String,
    size: u64,
    path: String,
    owner: String,
    shared_with: HashSet<String>,
    timestamp: chrono::DateTime<Utc>,
}

struct Group {
    name: String,
    owner: String,
    members: HashSet<String>,
    pending_requests: HashSet<String>,
    files: Vec<(String, FileMetadata)>,
}

struct User {
    username: String,
    groups: HashSet<String>,
    pending_group_requests: HashSet<String>,
    shared_files: HashSet<String>,
    ip_address: String,
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8082".to_string());
    let listener = TcpListener::bind(&server_addr).await?;

    let groups = Arc::new(Mutex::new(HashMap::<String, Group>::new()));
    let users = Arc::new(Mutex::new(HashMap::<String, User>::new()));

    loop {
        let (socket, addr) = listener.accept().await?;

        tokio::spawn(async move {
            println!("Accepted connection from {}", addr);
        });
    }

    drop(listener);
    Ok(())
}
