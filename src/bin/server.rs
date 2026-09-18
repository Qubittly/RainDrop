use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{ AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader };
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use raindrop::{ AppState, ClientMessage, Data, ServerMessage, User, encode_message };

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server_addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8082".to_string());
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
    where S: AsyncRead + AsyncWrite + Unpin + Send + 'static
{
    let (reader, mut writer) = tokio::io::split(socket);
    let mut lines = BufReader::new(reader).lines();

    let username = match lines.next_line().await {
        Ok(Some(line)) =>
            match serde_json::from_str::<ClientMessage>(&line) {
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

    let (sender, mut reciever) = mpsc::channel::<Data>(100);
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
    let writer_task = tokio::spawn(async move {
        while let Some(outgoing) = reciever.recv().await {
            if writer.write_all(&outgoing.data).await.is_err() {
                break;
            }
        }
    });

    if
        let Ok(data) = encode_message(ServerMessage::Identified {
            username: username.clone(),
        })
    {
        // Send the identification confirmation message to the client
        let _ = sender.send(Data { data }).await;
    }

    while let Ok(Some(line)) = lines.next_line().await {
        let message = match serde_json::from_str::<ClientMessage>(&line) {
            Ok(message) => message,
            Err(_) => {
                if
                    let Ok(data) = encode_message(ServerMessage::Error {
                        message: "invalid JSON message".to_string(),
                    })
                {
                    let _ = sender.send(Data { data }).await;
                }
                continue;
            }
        };

        match message {
            ClientMessage::Identify { .. } => {
                if
                    let Ok(data) = encode_message(ServerMessage::Error {
                        message: "the username can only be sent once".to_string(),
                    })
                {
                    let _ = sender.send(Data { data }).await;
                }
            }
            ClientMessage::Send { to, message } => {
                let data = encode_message(ServerMessage::Message {
                    from: username.clone(),
                    message,
                });
                if let Ok(data) = data {
                    if !state.send_to(&to, data).await {
                        if
                            let Ok(error) = encode_message(ServerMessage::Error {
                                message: format!("user {to} is not connected"),
                            })
                        {
                            let _ = sender.send(Data { data: error }).await;
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
