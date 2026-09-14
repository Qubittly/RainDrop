use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::RwLock;
use tokio::net::{TcpListener, TcpStream};
use std::error::Error;
use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use chrono::Utc;

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 10; // 10 MB
const MAX_MEMBERS: usize = 10;

/// Represents metadata for a file in the network.
#[derive(Default, Clone)]
struct FileMetadata {
    name: String,
    size: u64,
    path: String,
    owner: String,
    shared_with: HashSet<String>,
    timestamp: chrono::DateTime<Utc>,
}

/// Represents a peer in the network.
#[derive(Default, Clone)]
struct Peer {
    username: String,
    ip_address: String,
    port: u16,
    online: bool,
}

impl Peer {
    fn new(username: String, ip_address: String, port: u16) -> Self {
        Peer {
            username,
            ip_address,
            port,
            online: true,
        }
    }

    fn is_online(&self) -> bool {
        self.online
    }

    fn set_online(&mut self, online: bool) {
        self.online = online;
    }
}

/// Represents a group in the network.
#[derive(Default, Clone)]
struct Group {
    name: String,
    owner: String,
    members: HashSet<String>,
    pending_requests: HashSet<String>,
    files: Vec<(String, FileMetadata)>,
}

impl Group {
    fn new(name: String, owner: String) -> Self {
        Group {
            name,
            owner,
            members: HashSet::new(),
            pending_requests: HashSet::new(),
            files: Vec::new(),
        }
    }

    fn add_member(&mut self, username: String) {
        self.members.insert(username);
    }

    fn remove_member(&mut self, username: &str) {
        self.members.remove(username);
    }

    fn add_pending_request(&mut self, username: String) {
        self.pending_requests.insert(username);
    }

    fn remove_pending_request(&mut self, username: &str) {
        self.pending_requests.remove(username);
    }

    fn add_file(&mut self, file_name: String, metadata: FileMetadata) {
        self.files.push((file_name, metadata));
    }

    fn remove_file(&mut self, file_name: &str) {
        self.files.retain(|(name, _)| name != file_name);
    }
}

/// Represents a user in the network.
#[derive(Default, Clone)]
struct User {
    username: String,
    groups: HashSet<String>,
    pending_requests: HashSet<String>,
    shared_files: HashSet<String>,
    ip_address: String,
    port: u16,
    online: bool,
}

impl User {
    fn new(username: String, ip_address: String, port: u16) -> Self {
        User {
            username,
            groups: HashSet::new(),
            pending_requests: HashSet::new(),
            shared_files: HashSet::new(),
            ip_address,
            port,
            online: true,
        }
    }

    fn is_online(&self) -> bool {
        self.online
    }

    fn set_online(&mut self, online: bool) {
        self.online = online;
    }

    fn add_group(&mut self, group_name: String) {
        self.groups.insert(group_name);
    }

    fn remove_group(&mut self, group_name: &str) {
        self.groups.remove(group_name);
    }

    fn add_pending_request(&mut self, request: String) {
        self.pending_requests.insert(request);
    }

    fn remove_pending_request(&mut self, request: &str) {
        self.pending_requests.remove(request);
    }

    fn add_shared_file(&mut self, file_name: String) {
        self.shared_files.insert(file_name);
    }

    fn remove_shared_file(&mut self, file_name: &str) {
        self.shared_files.remove(file_name);
    }

    fn get_ip_address(&self) -> &str {
        &self.ip_address
    }

    fn set_ip_address(&mut self, ip_address: String) {
        self.ip_address = ip_address;
    }

    fn get_port(&self) -> u16 {
        self.port
    }

    fn set_port(&mut self, port: u16) {
        self.port = port;
    }
}
/// Represents the application state.
struct AppState {
    groups: RwLock<HashMap<String, Group>>,
    users: RwLock<HashMap<String, User>>,
}

impl AppState {
    fn new() -> Arc<Self> {
        Arc::new(AppState {
            groups: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
        })
    }

    async fn get_groups(&self) -> HashMap<String, Group> {
        self.groups.read().await.clone()
    }

    async fn get_group(&self, group_name: &str) -> Option<Group> {
        let groups = self.groups.read().await;
        groups.get(group_name).cloned()
    }

    async fn add_group(&self, group: Group) {
        let mut groups = self.groups.write().await;
        groups.insert(group.name.clone(), group);
    }

    async fn get_users(&self) -> HashMap<String, User> {
        self.users.read().await.clone()
    }

    async fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(username).cloned()
    }

    async fn add_user(&self, user: User) {
        let mut users = self.users.write().await;
        users.insert(user.username.clone(), user);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8082".to_string());
    let listener = TcpListener::bind(&server_addr).await?;

    let mut state = AppState::new();
    loop {
        let (socket, addr) = listener.accept().await?;
        let mut state = state.clone();

        tokio::spawn(async move {
            println!("Accepted connection from {}", addr);
            handle_connection(socket, addr, state).await;
        });
    }
}

async fn handle_connection<S>(socket: S, addr: SocketAddr, state: Arc<AppState>) 
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(socket);

    
}
