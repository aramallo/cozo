use std::collections::HashMap;

use itertools::Itertools as _;
use stack_graphs::{
    arena::Handle,
    graph::{Degree, File, Node, NodeID, StackGraph},
    partial::{PartialPath, PartialPaths, PartialSymbolStack},
    serde as sg_serde,
    stitching::{Database, ForwardCandidates},
    CancellationFlag,
};

use super::stack_graph_storage_error::{Result, StackGraphStorageError};

type Blob = Box<[u8]>;

/// State for a definition query. Fixed rules cannot themselves load data, so
/// all data they might need must be provid. The `*_blobs` fields hold binary
/// blobs representing partial graphs or paths that have not yet been “loaded”;
/// whenever one is needed it is taken out of the corresponding collection,
/// parsed, and integrated into `graph`, `partials`, and/or `db`. When a key
/// for one of the `*_blobs` collections exists but its value is empty, that
/// means the data has already been loaded.
struct State {
    /// Indexed by Git `BLOB_OID`
    graph_blobs: HashMap<Handle<File>, Option<Blob>>,
    /// Indexed by Git `BLOB_OID` & local ID
    node_path_blobs: HashMap<NodeID, BlobsLoadState>,
    /// Indexed by serialized symbol stacks
    root_path_blobs: HashMap<Box<str>, Vec<(Handle<File>, Blob)>>,
    graph: StackGraph,
    partials: PartialPaths,
    db: Database,
    stats: Stats,
}

enum BlobsLoadState {
    Unloaded(Vec<(Handle<File>, Blob)>),
    Loaded,
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
    fn load_forward_candidates(
        &mut self,
        path: &PartialPath,
        cancellation_flag: &dyn stack_graphs::CancellationFlag,
    ) -> Result<(), StackGraphStorageError> {
        self.load_partial_path_extensions(path, cancellation_flag)
    }

    fn get_forward_candidates<R>(&mut self, path: &PartialPath, result: &mut R)
    where
        R: std::iter::Extend<Handle<PartialPath>>,
    {
        self.db
            .find_candidate_partial_paths(&self.graph, &mut self.partials, path, result);
    }

    fn get_joining_candidate_degree(&self, path: &PartialPath) -> Degree {
        self.db.get_incoming_path_degree(path.end_node)
    }

    fn get_graph_partials_and_db(&mut self) -> (&StackGraph, &mut PartialPaths, &Database) {
        (&self.graph, &mut self.partials, &self.db)
    }
}

pub static BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

impl State {
    fn load_graph_for_file_inner(
        file: Handle<File>,
        graph: &mut StackGraph,
        graph_blobs: &mut HashMap<Handle<File>, Option<Blob>>,
        stats: &mut Stats,
    ) -> Result<()> {
        // copious_debugging!("--> Load graph for {}", file);

        let Some(blob) = graph_blobs.get_mut(&file) else {
            // copious_debugging!("   > Already loaded");
            eprintln!("No graph for key {file:?}");
            return Err(StackGraphStorageError::MissingData(format!(
                "graph for file key {:?}",
                graph[file].name(),
            )));
        };

        let Some(blob) = blob.take() else {
            // copious_debugging!(" * Already loaded");
            stats.file_cached += 1;
            return Ok(());
        };

        // copious_debugging!(" * Load from database");
        stats.file_loads += 1;
        let (file_graph, _): (sg_serde::StackGraph, _) =
            bincode::decode_from_slice(&blob, BINCODE_CONFIG)?;
        file_graph.load_into(graph)?;
        Ok(())
    }

    fn load_paths_for_node(
        &mut self,
        node: Handle<Node>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        // Adapted from:
        // https://github.com/github/stack-graphs/blob/2c97ba2/stack-graphs/src/storage.rs#L580

        // copious_debugging!(" * Load extensions from node {}", node.display(&self.graph));
        let id = self.graph[node].id();

        let Some(blobs_load_state) = self.node_path_blobs.get_mut(&id) else {
            eprintln!("No file paths for key {id:?}");
            return Err(StackGraphStorageError::MissingData(format!(
                "file paths for key {id:?}"
            )));
        };

        let blobs_load_state = std::mem::replace(blobs_load_state, BlobsLoadState::Loaded);
        let BlobsLoadState::Unloaded(paths) = blobs_load_state else {
            eprintln!("No file paths for key {:?}", id);
            self.stats.root_path_cached += 1;
            return Ok(());
        };

        self.stats.node_path_loads += 1;

        // #[cfg_attr(not(feature = "copious-debugging"), allow(unused))]
        // let mut count = 0usize;

        for path in paths {
            cancellation_flag.check("loading node paths")?;
            let (file, value) = path;
            Self::load_graph_for_file_inner(
                file,
                &mut self.graph,
                &mut self.graph_blobs,
                &mut self.stats,
            )?;
            let (path, _): (sg_serde::PartialPath, _) =
                bincode::decode_from_slice(&value, BINCODE_CONFIG)?;
            let path = path.to_partial_path(&mut self.graph, &mut self.partials)?;

            // copious_debugging!(
            //     "   > Loaded {}",
            //     path.display(&self.graph, &mut self.partials)
            // );
            // count += 1;

            self.db
                .add_partial_path(&self.graph, &mut self.partials, path);
        }
        // copious_debugging!("   > Loaded {}", count);
        Ok(())
    }

    fn load_paths_for_root(
        &mut self,
        symbol_stack: PartialSymbolStack,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        // Adapted from:
        // https://github.com/github/stack-graphs/blob/2c97ba2/stack-graphs/src/storage.rs#L631
        let (symbol_stack_patterns, _) = PartialSymbolStackExt(symbol_stack)
            .storage_key_patterns(&self.graph, &mut self.partials);
        for symbol_stack in symbol_stack_patterns {
            // copious_debugging!(
            //     " * Load extensions from root with prefix symbol stack {}",
            //     symbol_stack
            // );

            let Some(blobs) = self
                .root_path_blobs
                .get_mut(symbol_stack.as_ref() as &str)
                .map(|blobs| std::mem::take(blobs))
            else {
                // copious_debugging!("   > Already loaded");
                eprintln!("No root paths for key {:?}", symbol_stack);
                return Err(StackGraphStorageError::MissingData(format!(
                    "root paths for symbol stack key {:?}",
                    symbol_stack
                )));
            };
            if blobs.is_empty() {
                // copious_debugging!("   > Already loaded");
                self.stats.root_path_cached += 1;
                continue;
            }
            self.stats.root_path_loads += 1;

            // #[cfg_attr(not(feature = "copious-debugging"), allow(unused))]
            // let mut count = 0usize;

            for (file, blob) in blobs {
                cancellation_flag.check("loading root paths")?;
                Self::load_graph_for_file_inner(
                    file,
                    &mut self.graph,
                    &mut self.graph_blobs,
                    &mut self.stats,
                )?;
                let (path, _): (sg_serde::PartialPath, _) =
                    bincode::decode_from_slice(&blob, BINCODE_CONFIG)?;
                let path = path.to_partial_path(&mut self.graph, &mut self.partials)?;

                // copious_debugging!(
                //     "   > Loaded {}",
                //     path.display(&self.graph, &mut self.partials)
                // );
                // count += 1;

                self.db
                    .add_partial_path(&self.graph, &mut self.partials, path);
            }

            // copious_debugging!("   > Loaded {}", count);
        }
        Ok(())
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

/// Adapted from [Stack Graphs SQLite storage implementation][adapted_from].
///
/// [adapted_from]: https://github.com/github/stack-graphs/blob/3c4d1a6/stack-graphs/src/storage.rs#L724
struct PartialSymbolStackExt(PartialSymbolStack);

// Methods for computing keys and patterns for a symbol stack. The format of a storage key is:
//
//     has-var RS ( symbol (US symbol)* )?
//
// where has-var is "V" if the symbol stack has a variable, "X" otherwise.
impl PartialSymbolStackExt {
    /// Returns a string representation of this symbol stack for indexing in the database.
    fn storage_key(self, graph: &StackGraph, partials: &mut PartialPaths) -> String {
        let mut key = String::new();
        match self.0.has_variable() {
            true => key += "V\u{241E}",
            false => key += "X\u{241E}",
        }
        key += &self
            .0
            .iter(partials)
            .map(|s| &graph[s.symbol])
            .join("\u{241F}");
        key
    }

    /// Returns string representations for all prefixes of this symbol stack for querying the
    /// index in the database.
    fn storage_key_patterns(
        mut self,
        graph: &StackGraph,
        partials: &mut PartialPaths,
    ) -> (Vec<String>, String) {
        let mut key_patterns = Vec::new();
        let mut symbols = String::new();
        while let Some(symbol) = self.0.pop_front(partials) {
            if !symbols.is_empty() {
                symbols += "\u{241F}";
            }
            let symbol = graph[symbol.symbol]
                .replace("%", "\\%")
                .replace("_", "\\_")
                .to_string();
            symbols += &symbol;
            // patterns for paths matching a prefix of this stack
            key_patterns.push(format!("V\u{241E}{symbols}"));
        }
        // pattern for paths matching exactly this stack
        key_patterns.push(format!("X\u{241E}{symbols}"));
        if self.0.has_variable() {
            // patterns for paths for which this stack is a prefix
            key_patterns.push(format!("_\u{241E}{symbols}\u{241F}%"));
        }
        (key_patterns, "\\".to_string())
    }
}
