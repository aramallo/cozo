use crate::data::tuple::Tuple;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError::InvalidTuple;

pub struct PartialPathFileInfo {
    pub file: String,
    pub local_id: u32,
    pub value: Vec<u8>,
}

impl PartialPathFileInfo {
    pub fn new(
        file: String,
        local_id: u32,
        value: Vec<u8>,
    ) -> Self {
        Self {
            file,
            local_id,
            value,
        }
    }
}

impl TryFrom<Tuple> for PartialPathFileInfo {
    type Error = StackGraphStorageError;

    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 3 {
            return Err(InvalidTuple);
        }

        let file = tuple[0].get_str();
        let local_id = tuple[1].get_int();
        let value = tuple[2].get_bytes();

        // TODO: replace unwrap and handle error
        Ok(Self {
            file: String::from(file.unwrap()),
            local_id: local_id.unwrap() as u32,
            value: Vec::from(value.unwrap()),
        })
    }
}
