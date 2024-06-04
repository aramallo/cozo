use std::collections::HashMap;

use stack_graphs::{
    arena::Handle,
    graph::{Degree, File, Node, NodeID, StackGraph},
    partial::{PartialPath, PartialPaths, PartialSymbolStack},
    stitching::{Database, ForwardCandidates},
    CancellationFlag,
};

use super::stack_graph_storage_error::{Result, StackGraphStorageError};

/// State for a definition query. Fixed rules cannot themselves load data, so
/// all data they might need must be provid. The `*_blobs` fields hold binary
/// blobs representing partial graphs or paths that have not yet been “loaded”;
/// whenever one is needed it is removed from the corresponding collection,
/// parsed, and integrated into `graph`, `partials`, and/or `db`.
struct State {
    /// Indexed by Git `BLOB_OID`
    graph_blobs: HashMap<Handle<File>, Box<[u8]>>,
    /// Indexed by Git `BLOB_OID` & local ID
    node_path_blobs: HashMap<NodeID, Box<[Box<[u8]>]>>,
    /// Indexed by serialized symbol stacks
    root_path_blobs: HashMap<Box<str>, Box<[Box<[u8]>]>>,
    graph: StackGraph,
    partials: PartialPaths,
    db: Database,
    stats: Stats,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub file_loads: usize,
    pub file_cached: usize,
    pub root_path_loads: usize,
    pub root_path_cached: usize,
    pub node_path_loads: usize,
    pub node_path_cached: usize,
}

impl ForwardCandidates<Handle<PartialPath>, PartialPath, Database, StackGraphStorageError>
    for State
{
    fn get_forward_candidates<R>(&mut self, path: &PartialPath, result: &mut R)
    where
        R: std::iter::Extend<Handle<PartialPath>>,
    {
        todo!()
    }

    fn get_joining_candidate_degree(&self, path: &PartialPath) -> Degree {
        todo!()
    }

    fn get_graph_partials_and_db(&mut self) -> (&StackGraph, &mut PartialPaths, &Database) {
        todo!()
    }

    fn load_forward_candidates(
        &mut self,
        path: &PartialPath,
        cancellation_flag: &dyn stack_graphs::CancellationFlag,
    ) -> Result<(), StackGraphStorageError> {
        self.load_partial_path_extensions(path, cancellation_flag)
    }
}

impl State {
    fn load_paths_for_node(
        &mut self,
        node: Handle<Node>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        // See: https://github.com/github/stack-graphs/blob/2c97ba2/stack-graphs/src/storage.rs#L580
        todo!()
    }

    fn load_paths_for_root(
        &mut self,
        symbol_stack: PartialSymbolStack,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        // See: https://github.com/github/stack-graphs/blob/2c97ba2/stack-graphs/src/storage.rs#L631
        todo!()
    }

    pub fn load_partial_path_extensions(
        &mut self,
        path: &PartialPath,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        let end_node = self.graph[path.end_node].id();
        if self.graph[path.end_node].file().is_some() {
            self.load_paths_for_node(path.end_node, cancellation_flag)?;
        } else if end_node.is_root() {
            self.load_paths_for_root(path.symbol_stack_postcondition, cancellation_flag)?;
        }
        Ok(())
    }
}
