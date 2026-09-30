use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{ RwLock, mpsc };
use crate::types::{ Group, User };
use crate::transfer::TransferState;
use uuid::Uuid;

/// Represents the application state.
pub struct AppState {
    groups: RwLock<HashMap<String, Group>>,
    users: RwLock<HashMap<String, User>>,
    clients: RwLock<HashMap<String, mpsc::Sender<Vec<u8>>>>,
    active_transfers: Arc<RwLock<HashMap<Uuid, Arc<RwLock<TransferState>>>>>,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        Arc::new(AppState {
            groups: RwLock::new(HashMap::new()),
            users: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
            active_transfers: Default::default(),
        })
    }

    pub async fn get_groups(&self) -> HashMap<String, Group> {
        self.groups.read().await.clone()
    }

    pub async fn get_group(&self, group_name: &str) -> Option<Group> {
        let groups = self.groups.read().await;
        groups.get(group_name).cloned()
    }

    pub async fn add_group(&self, group: Group) {
        let mut groups = self.groups.write().await;
        groups.insert(group.name.clone(), group);
    }

    pub async fn get_users(&self) -> HashMap<String, User> {
        self.users.read().await.clone()
    }

    pub async fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(username).cloned()
    }

    pub async fn add_user(&self, user: User) {
        let mut users = self.users.write().await;
        users.insert(user.username.clone(), user);
    }

    pub async fn register_client(&self, username: String, sender: mpsc::Sender<Vec<u8>>) {
        let mut clients = self.clients.write().await;
        clients.insert(username, sender);
    }

    pub async fn remove_client(&self, username: &str, sender: &mpsc::Sender<Vec<u8>>) {
        let mut clients = self.clients.write().await;
        if clients.get(username).is_some_and(|current| current.same_channel(sender)) {
            clients.remove(username);
        }
    }

    pub async fn send_to(&self, username: &str, encoded: Vec<u8>) -> bool {
        let sender = {
            let clients = self.clients.read().await;
            clients.get(username).cloned()
        };

        match sender {
            Some(sender) => sender.send(encoded).await.is_ok(),
            None => false,
        }
    }

    pub async fn add_transfer(&self, id: Uuid, transfer: TransferState) {
        self.active_transfers.write().await.insert(id, Arc::new(RwLock::new(transfer)));
    }

    pub async fn get_transfer(&self, id: &Uuid) -> Option<Arc<RwLock<TransferState>>> {
        self.active_transfers.read().await.get(id).cloned()
    }
}
