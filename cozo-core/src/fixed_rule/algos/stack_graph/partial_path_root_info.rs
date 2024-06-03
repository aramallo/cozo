use crate::data::tuple::Tuple;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError::InvalidTuple;

pub struct PartialPathRootInfo {
    pub repository_id: String,
    pub blob_id: String,
    symbol_stack: String,
    value: Vec<u8>,
}

impl PartialPathRootInfo {
    pub fn new(
        repository_id: String,
        blob_id: String,
        symbol_stack: String,
        value: Vec<u8>,
    ) -> Self {
        Self {
            repository_id,
            blob_id,
            symbol_stack,
            value,
        }
    }
}

impl TryFrom<Tuple> for PartialPathRootInfo {
    type Error = StackGraphStorageError;

    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 4 {
            return Err(InvalidTuple);
        }

        let repository_id = tuple[0].get_str();
        let blob_id = tuple[1].get_str();
        let symbol_stack = tuple[2].get_str();
        let value = tuple[3].get_bytes();

        // TODO: replace unwrap and handle error
        Ok(Self {
            repository_id: String::from(repository_id.unwrap()),
            blob_id: String::from(blob_id.unwrap()),
            symbol_stack: String::from(symbol_stack.unwrap()),
            value: Vec::from(value.unwrap()),
        })
    }
}