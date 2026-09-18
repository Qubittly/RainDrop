use std::path::PathBuf;

use axum::{
    Router,
    extract::{ DefaultBodyLimit, Multipart, Path, Query, State, ws::WebSocketUpgrade },
    http::{ HeaderMap, StatusCode, header },
    response::IntoResponse,
    routing::{ get, post },
    Json,
};
use ratatui::macros;
use serde::Serialize;
use tokio::fs;
use tokio::sync::broadcast;

use crate::Data;
use crate::AppState;

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 10; // 10 MB

pub struct TransferState {
    file_name: String,
    size: u64,
    bytes_transferred: u64,
    status: TransferStatus,
    /// Piping Data from uploader to downloader
    data_tx: broadcast::Sender<Data>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferStatus {
    Waiting,
    InTransfer,
    Completed,
    Failed,
}

fn mime_type(path: &PathBuf) -> String {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("jpg") | Some("jpeg") => "image/jpeg".to_string(),
        Some("png") => "image/png".to_string(),
        Some("gif") => "image/gif".to_string(),
        Some("mp4") => "video/mp4".to_string(),
        Some("mp3") => "audio/mpeg".to_string(),
        Some("txt") => "text/plain".to_string(),
        Some("html") => "text/html".to_string(),
        Some("pdf") => "application/pdf".to_string(),
        Some("json") => "application/json".to_string(),
        _ => "application/octet-stream".to_string(), // Default MIME type
    }
}

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(
        |socket| async move {
            //TODO: Handle the WebSocket connection
        }
    )
}

async fn upload_file(State(state): State<AppState>, mut multipart: Multipart) -> impl IntoResponse {
    while let Some(field) = multipart.next_field().await.unwrap() {
        let file_name = field.file_name().unwrap_or("file").to_owned();
        let _content_type = field.content_type().unwrap_or("application/octet-stream").to_owned();
        let data = field.bytes().await.unwrap();

        if (data.len() as u64) > MAX_FILE_SIZE {
            return (StatusCode::BAD_REQUEST, "File size exceeds the limit").into_response();
        }

        let file_path = PathBuf::from(format!("./uploads/{}", file_name));
        fs::write(&file_path, &data).await.unwrap();

        //TODO: Add logic to update the state with the new file metadata
    }

    (StatusCode::OK, "File uploaded successfully").into_response()
}

async fn download_file(
    State(state): State<AppState>,
    Path(file_name): Path<String>
) -> impl IntoResponse {
    let file_path = PathBuf::from(format!("./uploads/{}", file_name));
    if !file_path.exists() {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    }

    let data = fs::read(&file_path).await.unwrap();
    let mime = mime_type(&file_path);

    (StatusCode::OK, [(header::CONTENT_TYPE, mime)], data).into_response()
}
