use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{RwLock, mpsc};
use tokio::net::TcpListener;
use std::error::Error;
use std::net::SocketAddr;
use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use chrono::Utc;
use serde::{Deserialize, Serialize};

const MAX_MEMBERS: usize = 10;

/// Represents metadata for a file in the network.
#[derive(Default, Clone)]
struct FileMetadata {
    name: String,
    size: u64,
    path: String,
    mime_type: String,
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
    clients: RwLock<HashMap<String, mpsc::Sender<OutgoingData>>>,
}

impl AppState {
    fn new() -> Arc<Self> {
        Arc::new(AppState {
            groups: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
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

    async fn register_client(&self, username: String, sender: mpsc::Sender<OutgoingData>) {
        let mut clients = self.clients.write().await;
        clients.insert(username, sender);
    }

    async fn remove_client(&self, username: &str, sender: &mpsc::Sender<OutgoingData>) {
        let mut clients = self.clients.write().await;
        if clients
            .get(username)
            .is_some_and(|current| current.same_channel(sender))
        {
            clients.remove(username);
        }
    }

    async fn send_to(&self, username: &str, data: Vec<u8>) -> bool {
        let sender = {
            let clients = self.clients.read().await;
            clients.get(username).cloned()
        };

        match sender {
            Some(sender) => sender.send(OutgoingData { data }).await.is_ok(),
            None => false,
        }
    }
}

struct OutgoingData {
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
enum ClientMessage {
    Identify { username: String },
    Send { to: String, message: String },
}

#[derive(Serialize)]
enum ServerMessage {
    Identified { username: String },
    Message { from: String, message: String },
    Error { message: String },
}

fn encode_message(message: ServerMessage) -> Result<Vec<u8>, serde_json::Error> {
    let mut data = serde_json::to_vec(&message)?;
    data.push(b'\n');
    Ok(data)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = std::env::var("SERVER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8082".to_string());
    let listener = TcpListener::bind(&server_addr).await?;

    let state = AppState::new();
    loop {
        let (socket, addr) = listener.accept().await?;
        let state = state.clone();

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
    let (reader, mut writer) = tokio::io::split(socket);
    let mut lines = BufReader::new(reader).lines();

    let username = match lines.next_line().await {
        Ok(Some(line)) => match serde_json::from_str::<ClientMessage>(&line) {
            Ok(ClientMessage::Identify { username }) => {
                if !username.trim().is_empty() {
                    username
                } else {
                    let error_message = ServerMessage::Error {
                        message: "Username cannot be empty".to_string(),
                    };
                    let encoded = encode_message(error_message).unwrap();
                    writer.write_all(&encoded).await.unwrap();
                    return;
                }
            }
            _ => {
                let error_message = ServerMessage::Error {
                    message: "The first line must be an identification message".to_string(),
                };
                let encoded = encode_message(error_message).unwrap();
                writer.write_all(&encoded).await.unwrap();
                return;
            }
        }
        _ => {
            let error_message = ServerMessage::Error {
                message: "Invalid identification message".to_string(),
            };
            let encoded = encode_message(error_message).unwrap();
            writer.write_all(&encoded).await.unwrap();
            return;
        }
    };

    let (sender, mut reciever) = mpsc::channel::<OutgoingData>(100);
    state.register_client(username.clone(), sender.clone()).await;
    let mut user = match state.get_user(&username).await {
        Some(mut user) => {
            user.set_online(true);
            user.set_ip_address(addr.ip().to_string());
            user.set_port(addr.port());
            user
        }
        None => User::new(username.clone(), addr.ip().to_string(), addr.port()),
    };
    state.add_user(user.clone()).await;

    // Spawn a task to handle outgoing messages to the client
    // This task will listen for messages sent to this client 
    // (Draiing the receiver channel) and writing to the socket in a loop
    let writer_task = tokio::spawn (async move {
        while let Some(outgoing) = reciever.recv().await {
            if writer.write_all(&outgoing.data).await.is_err() {
                break;
            }
        }
    });

    if let Ok(data) = encode_message(ServerMessage::Identified { 
        username: username.clone() 
    }) {
        // Send the identification confirmation message to the client
        let _  = sender.send(OutgoingData { data }).await;
    }

    while let Ok(Some(line)) = lines.next_line().await {
        let message = match serde_json::from_str::<ClientMessage>(&line) {
            Ok(message) => message,
            Err(_) => {
                if let Ok(data) = encode_message(ServerMessage::Error {
                    message: "invalid JSON message".to_string(),
                }) {
                    let _ = sender.send(OutgoingData { data }).await;
                }
                continue;
            }
        };

        match message {
            ClientMessage::Identify { .. } => {
                if let Ok(data) = encode_message(ServerMessage::Error {
                    message: "the username can only be sent once".to_string(),
                }) {
                    let _ = sender.send(OutgoingData { data }).await;
                }
            }
            ClientMessage::Send { to, message } => {
                let data = encode_message(ServerMessage::Message {
                    from: username.clone(),
                    message,
                });
                if let Ok(data) = data {
                    if !state.send_to(&to, data).await {
                        if let Ok(error) = encode_message(ServerMessage::Error {
                            message: format!("user {to} is not connected"),
                        }) {
                            let _ = sender.send(OutgoingData { data: error }).await;
                        }
                    }
                }
            }
        }
    }
    // Stop accepting messages from the client and close the connection
    state.remove_client(&username, &sender).await;

    user.set_online(false);
    state.add_user(user).await;

    drop(sender);
    let _ = writer_task.await;
}