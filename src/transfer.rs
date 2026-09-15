use std::path::PathBuf;

use axum::{
    Router,
    extract::{ 
        DefaultBodyLimit, Multipart, Path, Query, State,
        ws::{WebSocketUpgrade, WebSocket} 
    },
    http::{ HeaderMap, StatusCode, header },
    response::IntoResponse,
    routing::{ get, post },
    Json,
};
use serde::Serialize;
use tokio::fs;

const MAX_FILE_SIZE: u64 = 1024 * 1024 * 10; // 10 MB



fn mime_type(path: &PathBuf) -> String {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("jpg") | Some("jpeg") => "image/jpeg".to_string(),
        Some("png") => "image/png".to_string(),
        Some("gif") => "image/gif".to_string()
        Some("mp4") => "video/mp4".to_string(),
        Some("mp3") => "audio/mpeg".to_string(),
        Some("txt") => "text/plain".to_string(),
        Some("html") => "text/html".to_string(),,
        Some("pdf") => "application/pdf".to_string(),
        Some("json") => "application/json".to_string(),
        _ => "application/octet-stream".to_string(), // Default MIME type
    }
}

