use std::{
    borrow::Cow,
    collections::{hash_map::Entry as HashEntry, HashMap},
    ops::Range as StdRange,
};

use itertools::Itertools as _;
use stack_graphs::{
    arena::Handle,
    graph::{Degree, File, Node, StackGraph},
    partial::{PartialPath, PartialPaths, PartialSymbolStack},
    serde as sg_serde,
    stitching::{Database, ForwardCandidates},
    CancellationFlag,
};

use super::{
    blobs::{GraphBlob, NodePathBlob, RootPathBlob},
    error::Result,
    Error, SourcePos,
};

/// Optionally Zstd-compressed (see [`decompress_if_needed`]).
type Blob = Box<[u8]>;

type FileID = Box<str>;
type NodeID = (FileID, u32);

/// State for a definition query. Fixed rules cannot themselves load data, so
/// all data they might need must be provided. The `*_blobs` fields initially
/// hold binary blobs representing partial graphs or paths that have not yet
/// been “loaded” (i.e. [`Unloaded`][`LoadState::Loaded`]). Whenever a blob
/// is needed it is “loaded”, i.e. it is taken out of the [`LoadState`],
/// parsed, and inserted into `graph`, `partials`, and/or `db`. When a key
/// for one of the `*_blobs` collections exists but its value is
/// [`Loaded`][`LoadState::Loaded`], that simply means the data has already
/// been loaded; if the key does not exist, that’s an error.
pub(super) struct State {
    /// Indexed by Git `BLOB_OID`
    graph_blobs: HashMap<FileID, LoadState<Blob>>,
    /// Indexed by Git `BLOB_OID` & local ID
    node_path_blobs: HashMap<NodeID, LoadState<Vec<Blob>>>,
    /// Indexed by serialized symbol stacks
    root_path_blobs: HashMap<Box<str>, LoadState<Vec<(FileID, Blob)>>>,
    pub(super) graph: StackGraph,
    partials: PartialPaths,
    db: Database,
    stats: Stats,
}

enum LoadState<T> {
    Unloaded(T),
    Loaded,
}

impl<T> LoadState<T> {
    fn load(&mut self) -> Option<T> {
        let Self::Unloaded(unloaded) = std::mem::replace(self, Self::Loaded) else {
            return None;
        };
        Some(unloaded)
    }
}

impl State {
    pub(super) fn new(
        graph_blobs: impl Iterator<Item = Result<GraphBlob>>,
        node_path_blobs: impl Iterator<Item = Result<NodePathBlob>>,
        root_path_blobs: impl Iterator<Item = Result<RootPathBlob>>,
    ) -> Result<Self> {
        let graph = StackGraph::new();

        let mut indexed_graph_blobs = HashMap::new();
        for graph_blob in graph_blobs {
            let graph_blob = graph_blob?;
            let HashEntry::Vacant(entry) = indexed_graph_blobs.entry(graph_blob.file_id.clone())
            else {
                return Err(Error::Misc(format!(
                    "file with ID {:?} already exists",
                    graph_blob.file_id
                )));
            };
            entry.insert(LoadState::Unloaded(graph_blob.blob));
        }

        let mut indexed_node_path_blobs = HashMap::new();
        for node_path_blob in node_path_blobs {
            let node_path_blob = node_path_blob?;
            if !indexed_graph_blobs.contains_key(node_path_blob.file_id.as_ref()) {
                return Err(Error::Misc(format!(
                    "no known file with ID {:?}",
                    node_path_blob.file_id
                )));
            }
            let node_id = (node_path_blob.file_id, node_path_blob.start_node_local_id);
            let LoadState::Unloaded(blobs) = indexed_node_path_blobs
                .entry(node_id)
                .or_insert_with(|| LoadState::Unloaded(Vec::new()))
            else {
                unreachable!()
            };
            blobs.push(node_path_blob.blob);
        }

        let mut indexed_root_path_blobs = HashMap::new();
        for root_path_blob in root_path_blobs {
            let root_path_blob = root_path_blob?;
            if !indexed_graph_blobs.contains_key(root_path_blob.file_id.as_ref()) {
                return Err(Error::Misc(format!(
                    "no known file with ID {:?}",
                    root_path_blob.file_id
                )));
            };
            let LoadState::Unloaded(files_blobs) = indexed_root_path_blobs
                .entry(root_path_blob.precondition_symbol_stack)
                .or_insert_with(|| LoadState::Unloaded(Vec::new()))
            else {
                unreachable!()
            };
            files_blobs.push((root_path_blob.file_id, root_path_blob.blob));
        }

        Ok(Self {
            graph_blobs: indexed_graph_blobs,
            node_path_blobs: indexed_node_path_blobs,
            root_path_blobs: indexed_root_path_blobs,
            graph,
            partials: PartialPaths::new(),
            db: Database::new(),
            stats: Stats::default(),
        })
    }

    pub(super) fn load_node(&mut self, source_pos: &SourcePos) -> Result<Option<Handle<Node>>> {
        let file = Self::load_graph_for_file_inner(
            &source_pos.file_id,
            &mut self.graph,
            &mut self.graph_blobs,
            &mut self.stats,
        )?;
        Ok(self
            .graph
            .nodes_for_file(file)
            .find(|&node| node_byte_range(&self.graph, node).is_some_and(|r| r == source_pos.byte_range)))
    }
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

impl ForwardCandidates<Handle<PartialPath>, PartialPath, Database, Error> for State {
    fn load_forward_candidates(
        &mut self,
        path: &PartialPath,
        cancellation_flag: &dyn stack_graphs::CancellationFlag,
    ) -> Result<()> {
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
    fn load_graph_for_file_inner<S: AsRef<str> + ?Sized>(
        file_id: &S,
        graph: &mut StackGraph,
        graph_blobs: &mut HashMap<FileID, LoadState<Blob>>,
        stats: &mut Stats,
    ) -> Result<Handle<File>> {
        let file_id: &str = file_id.as_ref();

        // copious_debugging!("--> Load graph for {}", file);

        let Some(blob) = graph_blobs.get_mut(file_id) else {
            // copious_debugging!("   > Already loaded");
            eprintln!("No graph for key {file_id:?}");
            return Err(Error::MissingData(format!(
                "graph for file key {:?}",
                file_id,
            )));
        };

        fn file_handle(graph: &StackGraph, file_id: &str) -> Result<Handle<File>> {
            graph.get_file(file_id).ok_or_else(|| {
                Error::Misc(format!("expected to have loaded file with ID {file_id:?}"))
            })
        }

        let Some(blob) = blob.load() else {
            // copious_debugging!(" * Already loaded");
            stats.file_cached += 1;
            return file_handle(graph, file_id);
        };

        let blob = decompress_if_needed(&blob);

        // copious_debugging!(" * Load from database");
        stats.file_loads += 1;
        let (file_graph, _): (sg_serde::StackGraph, _) =
            bincode::decode_from_slice(&blob, BINCODE_CONFIG)?;
        file_graph.load_into(graph)?;
        file_handle(graph, file_id)
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
        let Some(file_id) = id.file().map(|f| self.graph[f].name()) else {
            return Ok(());
        };

        let blob_key = (Box::from(file_id), id.local_id());
        let Some(blobs_load_state) = self.node_path_blobs.get_mut(&blob_key) else {
            // Not all nodes will have paths starting from them
            return Ok(());
        };

        let Some(blobs) = blobs_load_state.load() else {
            eprintln!("No file paths for key {blob_key:?}");
            self.stats.node_path_cached += 1;
            return Ok(());
        };

        self.stats.node_path_loads += 1;

        // #[cfg_attr(not(feature = "copious-debugging"), allow(unused))]
        // let mut count = 0usize;

        for blob in blobs {
            cancellation_flag.check("loading file paths")?;
            let blob = decompress_if_needed(&blob);
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

            let Some(blobs_load_state) =
                self.root_path_blobs.get_mut(symbol_stack.as_ref() as &str)
            else {
                // Not all symbol stack patterns will have results
                continue;
            };

            let Some(blobs) = blobs_load_state.load() else {
                // copious_debugging!("   > Already loaded");
                self.stats.root_path_cached += 1;
                continue;
            };
            self.stats.root_path_loads += 1;

            // #[cfg_attr(not(feature = "copious-debugging"), allow(unused))]
            // let mut count = 0usize;

            for (file, blob) in blobs {
                cancellation_flag.check("loading root paths")?;
                Self::load_graph_for_file_inner(
                    &file,
                    &mut self.graph,
                    &mut self.graph_blobs,
                    &mut self.stats,
                )?;
                let blob = decompress_if_needed(&blob);
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
    #[allow(dead_code)]
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
                .replace('%', "\\%")
                .replace('_', "\\_")
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

pub(super) fn node_byte_range(
    stack_graph: &StackGraph,
    stack_graph_node: Handle<Node>,
) -> Option<StdRange<u32>> {
    fn lsp_position_to_byte_offset(position: &lsp_positions::Position) -> u32 {
        let line_start = position.containing_line.start;
        let line_offset = position.column.utf8_offset;
        (line_start + line_offset) as u32
    }

    let source_info = stack_graph.source_info(stack_graph_node)?;
    let start = lsp_position_to_byte_offset(&source_info.span.start);
    let end = lsp_position_to_byte_offset(&source_info.span.end);

    if start == 0 && end == 0 {
        None
    } else {
        Some(start..end)
    }
}

fn decompress_if_needed(bytes: &[u8]) -> Cow<'_, [u8]> {
    // Check Zstd’s magic number
    if bytes.len() < 4 || bytes[..4] != [0x28, 0xb5, 0x2f, 0xfd] {
        return bytes.into();
    }

    // TODO: What is a reasonable `capacity`?
    // TODO: Maybe we should store the exact uncompressed size along with the blob in the DB?
    if let Ok(mut decompressed_bytes) = zstd::bulk::decompress(bytes, u16::MAX as _) {
        decompressed_bytes.shrink_to_fit();
        decompressed_bytes.into()
    } else {
        // Could not decompress, so just return the original bytes and let
        // decoding fail downstream
        bytes.into()
    }
}
