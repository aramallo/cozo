use bincode::config;
use stack_graphs::graph::StackGraph;
use crate::data::tuple::Tuple;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError::InvalidTuple;

pub static BINCODE_CONFIG: config::Configuration = config::standard();

pub struct StackGraphInfo {
    pub file: String,
    pub tag: String,
    pub error: Option<String>,
    graph: Vec<u8>,
}

impl StackGraphInfo {
    pub fn new(
        file: String,
        tag: String,
        error: Option<String>,
        graph: Vec<u8>,
    ) -> Self {
        Self {
            file,
            tag,
            error,
            graph,
        }
    }

    pub fn read_stack_graph(&self) -> Result<StackGraph, StackGraphStorageError> {
        let (serde_graph, _bytes_read): (stack_graphs::serde::StackGraph, usize) =
            bincode::decode_from_slice(&self.graph, BINCODE_CONFIG)
                .map_err(StackGraphStorageError::from)?;

        let mut stack_graph = stack_graphs::graph::StackGraph::new();

        serde_graph
            .load_into(&mut stack_graph)
            .map_err(StackGraphStorageError::from)?;

        Ok(stack_graph)
    }
}

impl TryFrom<Tuple> for StackGraphInfo {
    type Error = StackGraphStorageError;

    fn try_from(tuple: Tuple) -> Result<Self, Self::Error> {
        if tuple.len() != 4 {
            return Err(InvalidTuple);
        }

        let file = tuple[0].get_str();
        let tag = tuple[1].get_str();
        let error = tuple[2].get_str();
        let graph = tuple[3].get_bytes();

        // TODO: replace unwrap and handle error
        Ok(Self {
            file: String::from(file.unwrap()),
            tag: String::from(tag.unwrap()),
            error: error.map(String::from),
            graph: Vec::from(graph.unwrap()),
        })
    }
}
