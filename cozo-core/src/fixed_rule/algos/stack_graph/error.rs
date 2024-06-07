use bincode::error::{DecodeError, EncodeError};
use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
#[non_exhaustive]
pub enum Error {
    #[error("cancelled at {0}")]
    Cancelled(&'static str),
    #[error("unsupported database version {0}")]
    IncorrectVersion(usize),
    #[error("database does not exist {0}")]
    MissingDatabase(String),
    #[error("invalid database tuple")]
    Tuple(#[from] super::blobs::TupleError),
    #[error(transparent)]
    Serde(#[from] stack_graphs::serde::Error),
    #[error(transparent)]
    SerializeFail(#[from] EncodeError),
    #[error(transparent)]
    DeserializeFail(#[from] DecodeError),
    #[error("missing data: {0}")]
    MissingData(String),
    #[error("misc: {0}")] // TODO: Rewrite to proper variants
    Misc(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<stack_graphs::CancellationError> for Error {
    fn from(err: stack_graphs::CancellationError) -> Self {
        Self::Cancelled(err.0)
    }
}
