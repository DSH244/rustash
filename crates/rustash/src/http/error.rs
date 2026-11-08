//! The single API error type: every failure path funnels into one response shape.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::duration::ValidationError;

/// `{ "error": "<message>" }` with the relevant status (all API errors).
pub struct AppError {
    pub status: u16,
    pub message: String,
}

impl AppError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        AppError {
            status,
            message: message.into(),
        }
    }
    pub fn validation(message: impl Into<String>) -> Self {
        AppError {
            status: 400,
            message: message.into(),
        }
    }
    pub fn internal() -> Self {
        AppError::new(500, "internal server error")
    }
}

impl From<ValidationError> for AppError {
    fn from(e: ValidationError) -> Self {
        AppError::validation(e.0)
    }
}

impl From<rusqlite::Error> for AppError {
    fn from(_: rusqlite::Error) -> Self {
        AppError::internal()
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(json!({ "error": self.message }))).into_response()
    }
}

pub(super) type ApiResult<T> = Result<T, AppError>;
