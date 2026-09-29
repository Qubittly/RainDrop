use std::{ path::{ Path, PathBuf }, sync::{ Arc } };

use axum::{
    Json,
    Router,
    body::Body,
    extract::{
        DefaultBodyLimit,
        Multipart,
        Path as AxumPath,
        State,
        WebSocketUpgrade,
        ws::Message,
    },
    http::{ StatusCode, header },
    response::{ IntoResponse, Response },
    routing::{ get, post },
};
use serde::Serialize;
use tokio::{ fs, io::AsyncWriteExt, sync::watch };
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{ AppState, FileMetadata, TransferStatus, TransferProgress };

const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10MB
const UPLOAD_DIRECTORY: &str = "uploads";

#[derive(Debug, Clone)]
pub struct TransferState {
    pub file_meta: FileMetadata,
    pub progress_tx: watch::Sender<TransferProgress>,
}

impl TransferState {
    fn new(id: Uuid, file_meta: FileMetadata) -> Self {
        let (progress_tx, _) = watch::channel(TransferProgress {
            id,
            transferred: 0,
            total: file_meta.size,
            status: TransferStatus::Waiting,
        });
        Self { file_meta, progress_tx }
    }
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub id: Uuid,
    pub file_name: String,
    pub size: u64,
    pub download_path: String,
}

/// Build the HTTP routes used for large-file transfers.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/transfers", post(upload_file_server))
        .route("/transfers/{id}", get(download_file_server))
        .layer(DefaultBodyLimit::max((MAX_FILE_SIZE + 1024 * 1024) as usize))
        .with_state(state)
}

async fn upload_file_server(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart
) -> Result<Json<UploadResponse>, Response> {
    fs::create_dir_all(UPLOAD_DIRECTORY).await.map_err(internal_error)?;

    let mut upload = None;
    while let Some(mut field) = multipart.next_field().await.map_err(bad_request)? {
        let Some(raw_name) = field.file_name() else {
            continue;
        };
        let file_name = sanitize_file_name(raw_name);
        let content_type = field.content_type().unwrap_or("application/octet-stream").to_owned();
        let id = Uuid::new_v4();
        let file_path = PathBuf::from(UPLOAD_DIRECTORY).join(format!("{id}.upload"));
        let mut file = fs::File::create(&file_path).await.map_err(internal_error)?;
        let mut transfer_state = TransferState::new(
                id,
                FileMetadata::new(
                file_name.clone(),
                0,
                file_path.to_string_lossy().into_owned(),
                content_type.clone()
            )
        );
        let mut size = 0;

        while let Some(chunk) = field.chunk().await.map_err(bad_request)? {
            size += chunk.len() as u64;
            if size > MAX_FILE_SIZE {
                let _ = fs::remove_file(&file_path).await;
                return Err(
                    (StatusCode::PAYLOAD_TOO_LARGE, "file exceeds the 10 MiB limit").into_response()
                );
            }
            file.write_all(&chunk).await.map_err(internal_error)?;
            transfer_state.progress_tx.send_modify(|p| {
                p.transferred = size;
            });
        }
        file.flush().await.map_err(internal_error)?;

        transfer_state.file_meta.size = size;
        transfer_state.progress_tx.send_modify(|p| {
            p.total = size;
        });
        state.add_transfer(id, transfer_state).await;
        upload = Some(UploadResponse {
            id,
            file_name,
            size,
            download_path: format!("/transfers/{id}"),
        });
        break;
    }

    upload
        .map(Json)
        .ok_or_else(||
            (StatusCode::BAD_REQUEST, "multipart request did not contain a file").into_response()
        )
}

async fn download_file_server(
    State(state): State<Arc<AppState>>,
    AxumPath(id): AxumPath<Uuid>
) -> Result<Response, Response> {
    let transfer = state
        .get_transfer(&id).await
        .ok_or_else(|| StatusCode::NOT_FOUND.into_response())?;
    let transfer = transfer.read().await;
    let file = fs::File
        ::open(&transfer.file_meta.path).await
        .map_err(|_| StatusCode::NOT_FOUND.into_response())?;
    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);
    let disposition = format!("attachment; filename=\"{}\"", transfer.file_meta.name);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, &transfer.file_meta.mime_type)
        .header(header::CONTENT_LENGTH, transfer.file_meta.size)
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(body)
        .map_err(internal_error)
}

async fn transfer_progress(
    State(state): State<Arc<AppState>>,
    AxumPath(id): AxumPath<Uuid>,
    ws: WebSocketUpgrade
) -> impl IntoResponse {
    ws.on_upgrade(move |mut socket| async move {
        let Some(transfer) = state.get_transfer(&id).await else {
            return;
        };
        let mut rx = transfer.read().await.progress_tx.subscribe();
        loop {
            match rx.changed().await {
                Ok(()) => {
                    let progress = rx.borrow().clone();
                    let Ok(payload) = serde_json::to_string(&progress) else {
                        break;
                    };
                    if socket.send(Message::Text(payload.into())).await.is_err() {
                        break;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    })
}

fn sanitize_file_name(name: &str) -> String {
    let name = Path::new(name)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let sanitized: String = name
        .chars()
        .map(|character| if character.is_control() || character == '"' { '_' } else { character })
        .collect();
    if sanitized.is_empty() {
        "file".to_owned()
    } else {
        sanitized
    }
}

fn bad_request(error: impl std::fmt::Display) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}

fn internal_error(error: impl std::fmt::Display) -> Response {
    eprintln!("transfer error: {error}");
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}
