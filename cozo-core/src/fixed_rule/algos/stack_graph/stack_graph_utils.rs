use bincode::config;
use stack_graphs::graph::StackGraph;
use crate::fixed_rule::algos::stack_graph::stack_graph_storage_error::StackGraphStorageError;

pub static BINCODE_CONFIG: config::Configuration = config::standard();

pub fn deserialize_stack_graph(buffer: &[u8]) -> Result<StackGraph, StackGraphStorageError> {
    let (serde_graph, _bytes_read): (stack_graphs::serde::StackGraph, usize) =
        bincode::decode_from_slice(buffer, BINCODE_CONFIG)
            .map_err(StackGraphStorageError::from)?;

    let mut stack_graph = stack_graphs::graph::StackGraph::new();

    serde_graph
        .load_into(&mut stack_graph)
        .map_err(StackGraphStorageError::from)?;

    Ok(stack_graph)
}
