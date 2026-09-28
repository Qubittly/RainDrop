use serde::{ Deserialize, Serialize };
use std::collections::HashSet;
use chrono::Utc;

const MAX_MEMBERS: usize = 10;

/// Represents metadata for a file in the network.
#[derive(Default, Clone, Serialize)]
pub struct FileMetadata {
    pub name: String,
    pub size: u64,
    pub path: String,
    pub mime_type: String,
    pub owner: String,
    pub shared_with: HashSet<String>,
    pub timestamp: chrono::DateTime<Utc>,
}

impl FileMetadata {
    pub fn new(name: String, size: u64, path: String, mime_type: String) -> Self {
        Self { name, size, path, mime_type, timestamp: Utc::now(), ..Self::default() }
    }
}

/// Represents a peer in the network.
#[derive(Default, Clone)]
pub struct Peer {
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
pub struct Group {
    pub name: String,
    pub owner: String,
    pub members: HashSet<String>,
    pub pending_requests: HashSet<String>,
    pub files: Vec<(String, FileMetadata)>,
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
pub struct User {
    pub username: String,
    pub groups: HashSet<String>,
    pub pending_requests: HashSet<String>,
    pub shared_files: HashSet<String>,
    pub ip_address: String,
    pub port: u16,
    pub online: bool,
}

impl User {
    pub fn new(username: String, ip_address: String, port: u16) -> Self {
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

    pub fn set_online(&mut self, online: bool) {
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

    pub fn set_ip_address(&mut self, ip_address: String) {
        self.ip_address = ip_address;
    }

    fn get_port(&self) -> u16 {
        self.port
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
}

#[derive(Clone)]
pub struct Data {
    pub data: Vec<u8>,
}

impl Data {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

#[derive(Debug, Deserialize)]
pub enum ClientMessage {
    Identify {
        username: String,
    },
    Send {
        to: String,
        message: String,
    },
}

#[derive(Serialize)]
pub enum ServerMessage {
    Identified {
        username: String,
    },
    Message {
        from: String,
        message: String,
    },
    Error {
        message: String,
    },
}

pub fn encode_message(message: ServerMessage) -> Result<Vec<u8>, serde_json::Error> {
    let mut data = serde_json::to_vec(&message)?;
    data.push(b'\n');
    Ok(data)
}
