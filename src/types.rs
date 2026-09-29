use serde::{ Deserialize, Serialize };
use tokio::io::{AsyncRead, AsyncWrite};
use std::collections::HashSet;
use std::io;
use chrono::Utc;
use uuid::Uuid;

const MAX_MEMBERS: usize = 10;
const MAX_CHUNK_SIZE: usize = 64 * 1024; // 64KB

/// Represents metadata for a file in the network.
#[derive(Debug, Default, Clone, Serialize)]
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


#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    Identify {
        username: String,
    },
    Send {
        to: String,
        message: String,
    },
}

#[derive(Serialize, Deserialize)]
pub enum ServerMessage {
    Identified {
        username: String,
    },
    Message {
        from: String,
        message: String,
    },
    IncomingTransfer {
        id: Uuid,
        from: String,
        file_name: String,
        size: u64,
        addr: String,
    },
    Error {
        message: String,
    },
}

pub fn encode_message<'a, T>(message: T) -> Result<Vec<u8>, serde_json::Error> 
where
    T: Serialize + Deserialize<'a>,
{
    let mut data = serde_json::to_vec(&message)?;
    data.push(b'\n');
    Ok(data)
}

/// Messages over a direct peer-to-peer transfer connection
#[derive(Debug, Serialize, Deserialize)]
pub enum TransferMessage {
    Chunk { offset: u64, bytes: Vec<u8>},
    Progress { transferred: u64 },
    Complete,
}

/// Binary transfer frame: message type, payload length, then raw payload. This is big endian.
/// Returns a binary frame which is a Vec<u8>.
// Frame
//  byte 0        bytes 1-8              bytes 9..N)
// [type tag]  [payload length, u64]    [payload]

// Payload
//  offset (bytes 0 - 7)        file chunk data (bytes 8..N)
// [8 bytes, big endian]        [remaining bytes]
pub fn encode_transfer_message(message: &TransferMessage) -> Vec<u8> {
    let (message_type, payload) = match message {
        TransferMessage::Chunk { offset, bytes } => {
            // The actual data starts from position 8 in the vec
            let mut payload = Vec::with_capacity(8 + bytes.len());
            payload.extend_from_slice(&offset.to_be_bytes());
            payload.extend_from_slice(&bytes);
            (0u8, payload)
        }
        TransferMessage::Progress { transferred } => (1u8, transferred.to_be_bytes().to_vec()),
        TransferMessage::Complete => (2u8, Vec::new()),
    };
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.push(message_type);
    frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Decode complete binary transfer frame.
pub fn decode_transfer_message(frame: &[u8]) -> io::Result<TransferMessage> {
    if frame.len() < 9 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete transfer frame"));
    }

    let mut length_bytes = [0u8; 8];
    length_bytes.copy_from_slice(&frame[1..9]);
    let payload_length = u64::from_be_bytes(length_bytes) as usize;
    if frame.len() != 9 + payload_length {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid transfer frame length"));
    }

    let payload = &frame[9..];
    match frame[0] {
        0 if payload.len() >= 8 => {
            let mut offset_bytes = [0u8; 8];
            offset_bytes.copy_from_slice(&payload[..8]);
            Ok(TransferMessage::Chunk {
                offset: u64::from_be_bytes(offset_bytes),
                bytes: payload[8..].to_vec(),
            })
        }
        1 if payload.len() == 8 => {
            let mut transferred_bytes = [0u8; 8];
            transferred_bytes.copy_from_slice(payload);
            Ok(TransferMessage::Progress {
                transferred: u64::from_be_bytes(transferred_bytes),
            })
        }
        2 if payload.is_empty() => Ok(TransferMessage::Complete),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid transfer message")),
    }
}

pub async fn read_transfer_message<R>(reader: &mut R) -> io::Result<TransferMessage> 
where 
    R: AsyncRead + AsyncWrite + Unpin,
{
    // TODO: Create logic: this should assemble a complete frame for the decoder
    Ok(TransferMessage::Complete)
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferProgress {
    pub id: Uuid,
    pub transferred: u64,
    pub total: u64,
    pub status: TransferStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TransferStatus {
    Waiting,
    Transferring,
    Completed,
    Failed,
}
