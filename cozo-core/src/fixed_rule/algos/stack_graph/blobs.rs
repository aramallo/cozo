use crate::data::tuple::Tuple;

use super::error::Error;

pub struct GraphBlob {
    /// BLOB_OID, or maybe full file path if needed
    pub file_id: Box<str>,
    pub blob: Box<[u8]>,
}

pub struct NodePathBlob {
    /// BLOB_OID, or maybe full file path if needed
    pub file_id: Box<str>,
    pub start_node_local_id: u32,
    pub blob: Box<[u8]>,
}

pub struct RootPathBlob {
    /// BLOB_OID, or maybe full file path if needed
    pub file_id: Box<str>,
    pub precondition_symbol_stack: Box<str>,
    pub blob: Box<[u8]>,
}

impl TryFrom<Tuple> for GraphBlob {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 2 {
            return Err(Self::Error::InvalidTuple);
        }

        // TODO: More specific errors (e.g. “InvalidTupleElemType”)?
        let file_id = tuple[0].get_str().ok_or(Self::Error::InvalidTuple)?;
        let blob = tuple[1].get_bytes().ok_or(Self::Error::InvalidTuple)?;

        Ok(Self {
            file_id: file_id.into(),
            blob: blob.into(),
        })
    }
}

impl TryFrom<Tuple> for NodePathBlob {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 3 {
            return Err(Self::Error::InvalidTuple);
        }

        // TODO: More specific errors (e.g. “InvalidTupleElemType”)?
        let file_id = tuple[0].get_str().ok_or(Self::Error::InvalidTuple)?;
        let start_node_local_id = tuple[1]
            .get_non_neg_int()
            .ok_or(Self::Error::InvalidTuple)?;
        let start_node_local_id = start_node_local_id
            .try_into()
            .map_err(|_| Self::Error::InvalidTuple)?;
        let blob = tuple[2].get_bytes().ok_or(Self::Error::InvalidTuple)?;

        // TODO: replace unwrap and handle error
        Ok(Self {
            file_id: file_id.into(),
            start_node_local_id,
            blob: blob.into(),
        })
    }
}

impl TryFrom<Tuple> for RootPathBlob {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 3 {
            return Err(Self::Error::InvalidTuple);
        }

        // TODO: More specific errors (e.g. “InvalidTupleElemType”)?
        let file_id = tuple[0].get_str().ok_or(Self::Error::InvalidTuple)?;
        let precondition_symbol_stack = tuple[1].get_str().ok_or(Self::Error::InvalidTuple)?;
        let blob = tuple[2].get_bytes().ok_or(Self::Error::InvalidTuple)?;

        // TODO: replace unwrap and handle error
        Ok(Self {
            file_id: file_id.into(),
            precondition_symbol_stack: precondition_symbol_stack.into(),
            blob: blob.into(),
        })
    }
}
