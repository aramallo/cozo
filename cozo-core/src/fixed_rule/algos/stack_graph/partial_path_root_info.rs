use crate::data::tuple::Tuple;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError::InvalidTuple;

pub struct PartialPathRootInfo {
    pub file: String,
    pub symbol_stack: String,
    pub value: Vec<u8>,
}

impl PartialPathRootInfo {
    pub fn new(
        file: String,
        symbol_stack: String,
        value: Vec<u8>,
    ) -> Self {
        Self {
            file,
            symbol_stack,
            value,
        }
    }
}

impl TryFrom<Tuple> for PartialPathRootInfo {
    type Error = StackGraphStorageError;

    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 3 {
            return Err(InvalidTuple);
        }

        let file = tuple[0].get_str();
        let symbol_stack = tuple[1].get_str();
        let value = tuple[2].get_bytes();

        // TODO: replace unwrap and handle error
        Ok(Self {
            file: String::from(file.unwrap()),
            symbol_stack: String::from(symbol_stack.unwrap()),
            value: Vec::from(value.unwrap()),
        })
    }
}