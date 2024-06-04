use thiserror::Error;
use bincode::error::{DecodeError, EncodeError};
use miette::Diagnostic;

#[derive(Debug, Error, Diagnostic)]
pub enum StackGraphStorageError {
    #[error("cancelled at {0}")]
    Cancelled(&'static str),
    #[error("unsupported database version {0}")]
    IncorrectVersion(usize),
    #[error("database does not exist {0}")]
    MissingDatabase(String),
    #[error("invalid database tuple")]
    InvalidTuple,
    #[error(transparent)]
    Serde(#[from] stack_graphs::serde::Error),
    #[error(transparent)]
    SerializeFail(#[from] EncodeError),
    #[error(transparent)]
    DeserializeFail(#[from] DecodeError),
    #[error("missing data: {0}")]
    MissingData(String),
    #[error("misc: {0}")] // TODO: Rewrite to proper variants
    Misc(String)
}

pub type Result<T, E = StackGraphStorageError> = std::result::Result<T, E>;

impl From<stack_graphs::CancellationError> for StackGraphStorageError {
    fn from(err: stack_graphs::CancellationError) -> Self {
        Self::Cancelled(err.0)
    }
}
