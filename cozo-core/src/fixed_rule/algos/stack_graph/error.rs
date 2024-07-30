use miette::Diagnostic;
use thiserror::Error;

#[derive(Debug, Error, Diagnostic)]
#[non_exhaustive]
pub enum Error {
    #[error("cancelled at {0}")]
    Cancelled(&'static str),
    #[error("invalid database tuple")]
    Tuple(#[from] TupleError),
    #[error(transparent)]
    SourcePos(SourcePosError),
    #[error("invalid output-missing-files option")]
    OutputMissingFiles,
    #[error("duplicate blobs for file with ID {0:?}")]
    DuplicateGraph(String),
    #[error("path blob refers to unknown file with ID {0:?}")]
    UnknownFile(String),
    #[error("missing {0}")]
    MissingData(String),
    #[error("failed to deserialize blob for {what}")]
    DeserializeBlob {
        what: String,
        source: DeserializeBlobError,
    },
    #[error("failed to find reference at source position {0}")]
    Query(super::SourcePos),
}

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum SourcePosError {
    #[error("invalid references type; expected {expected}")]
    InvalidType { expected: &'static str, },
    #[error("invalid source position {got:?}")]
    Parse { got: String, source: super::source_pos::ParseError },
    // TODO: Better handle `miette::Report`s?
    #[error("invalid source positions: {0:#}")]
    Other(miette::Report),
}

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum TupleError {
    #[error("invalid tuple length; expected {expected} but got {got}")]
    Len { expected: usize, got: usize },
    #[error("invalid tuple element type at index {idx}; expected {expected} but got {got}")]
    ElemType {
        idx: usize,
        expected: &'static str,
        got: &'static str,
    },
    // TODO: Better handle `miette::Report`s?
    #[error("invalid tuple: {0:#}")]
    Report(miette::Report),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct DecodeError(#[from] bincode::error::DecodeError);

#[derive(Debug, Error)]
#[error(transparent)]
pub struct LoadError(#[from] stack_graphs::serde::Error);

#[derive(Debug, Error)]
pub enum DeserializeBlobError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Load(#[from] LoadError),
}

impl Error {
    pub(super) fn decode(what: String, source: bincode::error::DecodeError) -> Self {
        Self::DeserializeBlob {
            what,
            source: DeserializeBlobError::Decode(DecodeError(source)),
        }
    }

    pub(super) fn load(what: String, source: stack_graphs::serde::Error) -> Self {
        Self::DeserializeBlob {
            what,
            source: DeserializeBlobError::Load(LoadError(source)),
        }
    }
}

impl Error {
    pub(super) fn tuple_report(report: miette::Report) -> Self {
        Self::Tuple(TupleError::Report(report))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<stack_graphs::CancellationError> for Error {
    fn from(err: stack_graphs::CancellationError) -> Self {
        Self::Cancelled(err.0)
    }
}
