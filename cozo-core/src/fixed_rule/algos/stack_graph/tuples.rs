use crate::data::{tuple::Tuple, value::DataValue};

use super::error::{Error, TupleError};


pub struct Blob {
    pub uncompressed_len: usize,
    pub data: Box<[u8]>,
}

pub struct Graph {
    pub file_id: Box<str>,
    pub blob: Blob,
}

pub struct NodePath {
    pub file_id: Box<str>,
    pub start_node_local_id: u32,
    pub blob: Blob,
}

pub struct RootPath {
    pub file_id: Box<str>,
    pub precondition_symbol_stack: Box<str>,
    pub blob: Blob,
}

pub struct RootPathSymbolStackFileId {
    pub root_path_symbol_stack: Box<str>,
    pub file_id: Box<str>,
}


impl TryFrom<Tuple> for Graph {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        tuple.check_len(3)?;

        let file_id = tuple.get_elem(0, DataValue::get_str, "string", None)?;
        let uncompressed_blob_len =
            tuple.get_elem(1, DataValue::get_non_neg_int, "unsigned integer", None)?;
        let blob = tuple.get_elem(2, DataValue::get_bytes, "bytes", None)?;

        Ok(Self {
            file_id: file_id.into(),
            blob: Blob {
                uncompressed_len: uncompressed_blob_len as _,
                data: blob.into(),
            },
        })
    }
}

impl TryFrom<Tuple> for NodePath {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        tuple.check_len(4)?;

        let file_id = tuple.get_elem(0, DataValue::get_str, "string", None)?;
        let start_node_local_id =
            tuple.get_elem(1, DataValue::get_non_neg_int, "non-negative integer", None)?;
        let start_node_local_id = start_node_local_id.try_into().map_err(|_| {
            TupleError::elem_type(1, "32-bit integer", Some("bigger integer"), &tuple)
        })?;
        let uncompressed_blob_len =
            tuple.get_elem(2, DataValue::get_non_neg_int, "unsigned integer", None)?;
        let blob = tuple.get_elem(3, DataValue::get_bytes, "bytes", None)?;

        // TODO: replace unwrap and handle error
        Ok(Self {
            file_id: file_id.into(),
            start_node_local_id,
            blob: Blob {
                uncompressed_len: uncompressed_blob_len as _,
                data: blob.into(),
            },
        })
    }
}

impl TryFrom<Tuple> for RootPath {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        tuple.check_len(4)?;

        let file_id = tuple.get_elem(0, DataValue::get_str, "string", None)?;
        let precondition_symbol_stack = tuple.get_elem(1, DataValue::get_str, "string", None)?;
        let uncompressed_blob_len =
            tuple.get_elem(2, DataValue::get_non_neg_int, "unsigned integer", None)?;
        let blob = tuple.get_elem(3, DataValue::get_bytes, "bytes", None)?;

        // TODO: replace unwrap and handle error
        Ok(Self {
            file_id: file_id.into(),
            precondition_symbol_stack: precondition_symbol_stack.into(),
            blob: Blob {
                uncompressed_len: uncompressed_blob_len as _,
                data: blob.into(),
            },
        })
    }
}
impl TryFrom<Tuple> for RootPathSymbolStackFileId {
    type Error = Error;
    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        tuple.check_len(2)?;

        let root_path_symbol_stack = tuple.get_elem(0, DataValue::get_str, "string", None)?;
        let file_id = tuple.get_elem(1, DataValue::get_str, "string", None)?;

        // TODO: replace unwrap and handle error
        Ok(Self {
            root_path_symbol_stack: root_path_symbol_stack.into(),
            file_id: file_id.into(),
        })
    }
}

pub(super) trait TupleExt {
    fn check_len(&self, expected: usize) -> Result<(), TupleError>;
    fn get_elem<'t, T, F>(
        &'t self,
        idx: usize,
        get: F,
        expected: &'static str,
        got: Option<&'static str>,
    ) -> Result<T, TupleError>
    where
        F: FnOnce(&'t DataValue) -> Option<T>;
}

impl TupleExt for Tuple {
    fn check_len(&self, expected: usize) -> Result<(), TupleError> {
        if self.len() != expected {
            return Err(TupleError::Len {
                expected,
                got: self.len(),
            });
        }
        Ok(())
    }

    fn get_elem<'t, T, F>(
        &'t self,
        idx: usize,
        get: F,
        expected: &'static str,
        got: Option<&'static str>,
    ) -> Result<T, TupleError>
    where
        F: FnOnce(&'t DataValue) -> Option<T>,
    {
        get(&self[idx]).ok_or_else(|| TupleError::elem_type(idx, expected, got, self))
    }
}

impl TupleError {
    fn elem_type(
        idx: usize,
        expected: &'static str,
        got: Option<&'static str>,
        tuple: &Tuple,
    ) -> Self {
        Self::ElemType {
            idx,
            expected,
            got: got.unwrap_or_else(|| match &tuple[idx] {
                DataValue::Null => "null",
                DataValue::Bool(_) => "boolean",
                DataValue::Num(_) => "number",
                DataValue::Str(_) => "string",
                DataValue::Bytes(_) => "bytes",
                DataValue::Uuid(_) => "uuid",
                DataValue::Regex(_) => "regex",
                DataValue::List(_) => "list",
                DataValue::Set(_) => "set",
                DataValue::Vec(_) => "vec",
                DataValue::Json(_) => "json",
                DataValue::Validity(_) => "validity",
                DataValue::Bot => "bot",
            }),
        }
    }
}
