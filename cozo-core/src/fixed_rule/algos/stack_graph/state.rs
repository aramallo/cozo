use std::{
    borrow::Cow,
    collections::{hash_map::Entry as HashEntry, HashMap},
    ops::Range as StdRange,
};

use itertools::Itertools as _;
use log::debug;
use stack_graphs::{
    arena::Handle,
    graph::{Degree, File, Node, StackGraph},
    partial::{PartialPath, PartialPaths, PartialSymbolStack},
    serde as sg_serde,
    stitching::{Database, ForwardCandidates},
    CancellationFlag,
};

use super::{
    blobs::{Blob, GraphBlob, NodePathBlob, RootPathBlob},
    error::Result,
    pluralize, Error, SourcePos,
};

/// Optionally Zstd-compressed (see [`decompress_if_needed`]).

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
    /// Indexed by file ID.
    graph_blobs: HashMap<FileID, LoadState<Blob>>,
    /// Indexed by file ID & local ID.
    node_path_blobs: HashMap<NodeID, LoadState<Vec<Blob>>>,
    /// Indexed by symbol stacks patterns; multiple can refer to the same root path.
    root_paths_index: HashMap<Box<str>, Vec<usize>>,
    /// Storage indexed by [`root_paths_index`][`Storage::root_paths_index`] values.
    root_path_blobs: Vec<LoadState<(FileID, Blob)>>,
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

        debug!("Indexing state...");

        let mut indexed_graph_blobs = HashMap::new();
        for graph_blob in graph_blobs {
            let graph_blob = graph_blob?;
            let HashEntry::Vacant(entry) = indexed_graph_blobs.entry(graph_blob.file_id.clone())
            else {
                return Err(Error::DuplicateGraph(graph_blob.file_id.into()));
            };
            entry.insert(LoadState::Unloaded(graph_blob.blob));
        }

        debug!(
            " ↳ Indexed {}...",
            pluralize(indexed_graph_blobs.len(), "file graph"),
        );

        let mut count = 0;
        let mut indexed_node_path_blobs = HashMap::new();
        for node_path_blob in node_path_blobs {
            let node_path_blob = node_path_blob?;
            if !indexed_graph_blobs.contains_key(node_path_blob.file_id.as_ref()) {
                return Err(Error::UnknownFile(node_path_blob.file_id.into()));
            }
            let node_id = (node_path_blob.file_id, node_path_blob.start_node_local_id);
            let LoadState::Unloaded(blobs) = indexed_node_path_blobs
                .entry(node_id)
                .or_insert_with(|| LoadState::Unloaded(Vec::new()))
            else {
                unreachable!()
            };
            blobs.push(node_path_blob.blob);
            count += 1;
        }

        debug!(
            " ↳ Indexed {} from {}...",
            pluralize(count, "node path"),
            pluralize(indexed_node_path_blobs.len(), "node"),
        );

        let mut root_paths_index = HashMap::new();
        let mut all_root_path_blobs = Vec::with_capacity(root_path_blobs.size_hint().0);
        for root_path_blob in root_path_blobs {
            let root_path_blob = root_path_blob?;
            let idx = all_root_path_blobs.len();
            all_root_path_blobs.push(LoadState::Unloaded((
                root_path_blob.file_id.clone(),
                root_path_blob.blob,
            )));

            for symbol_stack_pattern in PartialSymbolStackExt::key_patterns_from_storage_key(
                &root_path_blob.precondition_symbol_stack,
            ) {
                let idxs = root_paths_index
                    .entry(symbol_stack_pattern)
                    .or_insert_with(Vec::new);
                idxs.push(idx);
            }
        }

        debug!(
            " ↳ Indexed {} from {}...",
            pluralize(all_root_path_blobs.len(), "root path"),
            pluralize(root_paths_index.len(), "symbol stack patterns"),
        );

        Ok(Self {
            graph_blobs: indexed_graph_blobs,
            node_path_blobs: indexed_node_path_blobs,
            root_paths_index,
            root_path_blobs: all_root_path_blobs,
            graph,
            partials: PartialPaths::new(),
            db: Database::new(),
            stats: Stats::default(),
        })
    }

    pub(super) fn load_nodes<'s>(
        &'s mut self,
        source_pos: &'s SourcePos,
    ) -> Result<impl Iterator<Item = Handle<Node>> + 's> {
        let file = Self::load_graph_for_file_inner(
            &source_pos.file_id,
            &mut self.graph,
            &mut self.graph_blobs,
            &mut self.stats,
        )?;
        Ok(self.graph.nodes_for_file(file).filter(|&node| {
            node_byte_range(&self.graph, node).is_some_and(|r| r == source_pos.byte_range)
        }))
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

        debug!("Load graph for {}", file_id);

        macro_rules! err_what {
            ($prefix:literal, $file_id:ident) => {
                format!("{}file with ID {:?}", $prefix, $file_id)
            };
        }

        let Some(blob) = graph_blobs.get_mut(file_id) else {
            debug!(" ↳ Failed to load graph");
            return Err(Error::MissingData(err_what!("data for ", file_id)));
        };

        fn file_handle(graph: &StackGraph, file_id: &str) -> Result<Handle<File>> {
            graph
                .get_file(file_id)
                .ok_or_else(|| Error::MissingData(err_what!("file handle in graph for ", file_id)))
        }

        let Some(blob) = blob.load() else {
            debug!(" ↳ Already loaded graph");
            stats.file_cached += 1;
            return file_handle(graph, file_id);
        };

        stats.file_loads += 1;
        debug!(" ↳ Found graph; decompressing, deserializing, & inserting");

        let blob = decompress_if_needed(&blob);
        let (file_graph, _): (sg_serde::StackGraph, _) =
            bincode::decode_from_slice(&blob, BINCODE_CONFIG)
                .map_err(|e| Error::decode(err_what!("graph in ", file_id), e))?;
        file_graph
            .load_into(graph)
            .map_err(|e| Error::load(err_what!("graph in ", file_id), e))?;

        debug!(" ↳ Loaded graph");

        file_handle(graph, file_id)
    }

    fn load_paths_for_node(
        &mut self,
        node: Handle<Node>,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        // Adapted from:
        // https://github.com/github/stack-graphs/blob/2c97ba2/stack-graphs/src/storage.rs#L580

        debug!(
            "Load node path extensions from node {}",
            node.display(&self.graph),
        );
        let id = self.graph[node].id();
        let Some(file_id) = id.file().map(|f| self.graph[f].name()) else {
            return Ok(());
        };

        let blob_key = (Box::from(file_id), id.local_id());
        let Some(blobs_load_state) = self.node_path_blobs.get_mut(&blob_key) else {
            debug!(" ↳ No node path extensions found");
            return Ok(());
        };

        let Some(blobs) = blobs_load_state.load() else {
            debug!(" ↳ Already loaded node path extensions");
            self.stats.node_path_cached += 1;
            return Ok(());
        };

        self.stats.node_path_loads += 1;
        debug!(
            " ↳ Found {}; decompressing, deserializing, & inserting...",
            pluralize(blobs.len(), "node path extension"),
        );

        let mut count = 0usize;

        let err_what = || {
            format!(
                "node path with start node {} in file with ID {:?}",
                blob_key.1, blob_key.0,
            )
        };

        for blob in blobs {
            cancellation_flag.check("loading node paths")?;

            let blob = decompress_if_needed(&blob);
            let (path, _): (sg_serde::PartialPath, _) =
                bincode::decode_from_slice(&blob, BINCODE_CONFIG)
                    .map_err(|e| Error::decode(err_what(), e))?;
            let path = path
                .to_partial_path(&mut self.graph, &mut self.partials)
                .map_err(|e| Error::load(err_what(), e))?;

            count += 1;
            debug!(
                " ↳ → Loaded node path extension {}",
                path.display(&self.graph, &mut self.partials),
            );

            self.db
                .add_partial_path(&self.graph, &mut self.partials, path);
        }

        debug!(" ↳ Loaded {}", pluralize(count, "node path extension"));

        Ok(())
    }

    fn load_paths_for_root(
        &mut self,
        symbol_stack: PartialSymbolStack,
        cancellation_flag: &dyn CancellationFlag,
    ) -> Result<()> {
        // Adapted from:
        // https://github.com/github/stack-graphs/blob/2c97ba2/stack-graphs/src/storage.rs#L631
        debug!(
            "Load root path extensions for symbol stack {}",
            symbol_stack.display(&self.graph, &mut self.partials)
        );
        let (symbol_stack_patterns, _) = PartialSymbolStackExt(symbol_stack)
            .storage_key_patterns_from_path(&self.graph, &mut self.partials);
        for symbol_stack_pattern in symbol_stack_patterns {
            debug!(
                " ↳ Load root path extensions for symbol stack pattern {:?}",
                symbol_stack_pattern,
            );

            let Some(idxs) = self.root_paths_index.get(symbol_stack_pattern.as_str()) else {
                debug!("    ↳ No root path extensions found");
                // Not all symbol stack patterns will have results
                continue;
            };

            self.stats.root_path_loads += 1;
            debug!(
                "    ↳ Found {}; decompressing, deserializing, & inserting...",
                pluralize(idxs.len(), "root path extension"),
            );

            let mut count = 0usize;

            for &idx in idxs {
                cancellation_flag.check("loading root paths")?;

                let Some((file, blob)) = self.root_path_blobs[idx].load() else {
                    debug!("    ↳ Already loaded root path extensions");
                    self.stats.root_path_cached += 1;
                    continue;
                };

                let err_what = || {
                    format!(
                        "root path with symbol stack pattern {:?}",
                        symbol_stack_pattern,
                    )
                };

                Self::load_graph_for_file_inner(
                    &file,
                    &mut self.graph,
                    &mut self.graph_blobs,
                    &mut self.stats,
                )?;
                let blob = decompress_if_needed(&blob);
                let (path, _): (sg_serde::PartialPath, _) =
                    bincode::decode_from_slice(&blob, BINCODE_CONFIG)
                        .map_err(|e| Error::decode(err_what(), e))?;
                let path = path
                    .to_partial_path(&mut self.graph, &mut self.partials)
                    .map_err(|e| Error::load(err_what(), e))?;

                count += 1;
                debug!(
                    "    ↳ → Loaded root path extension {}",
                    path.display(&self.graph, &mut self.partials),
                );

                self.db
                    .add_partial_path(&self.graph, &mut self.partials, path);
            }

            debug!("    ↳ Loaded {}", pluralize(count, "root path extension"));
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
    fn storage_key_patterns_from_path(
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
            symbols += &graph[symbol.symbol];
            // Patterns for paths matching a prefix of this stack
            key_patterns.push(format!("V\u{241E}{symbols}"));
        }
        // Pattern for paths matching exactly this stack
        key_patterns.push(format!("X\u{241E}{symbols}"));
        if self.0.has_variable() {
            let escaped_symbols = symbols.replace('%', "\\%").replace('_', "\\_");
            // Patterns for paths for which this stack is a prefix
            key_patterns.push(format!("_\u{241E}{escaped_symbols}\u{241F}%"));
        }
        (key_patterns, "\\".to_string())
    }

    /// Essentially implements the `LIKE` operator of the SQLite implementation.
    fn key_patterns_from_storage_key(key: &str) -> Vec<Box<str>> {
        let mut key_patterns = Vec::new();
        key_patterns.push(key.into());

        let mut offset = 0;
        let escaped_key = key.replace('%', "\\%").replace('_', "\\_");
        let mut prev = None;
        while offset < escaped_key.len() {
            // This makes sure we don’t prefix-match against the “full” key,
            // because we should only match against the full key exactly.
            if let Some(prev) = prev.take() {
                key_patterns.push(prev);
            }
            let Some(pos) = escaped_key[offset..]
                .char_indices()
                .find_map(|(pos, chr)| (chr == '\u{241F}').then_some(pos))
            else {
                break;
            };
            prev = Some(format!("_{}%", &escaped_key[1..offset + pos]).into());
            offset += pos + '\u{241F}'.len_utf8();
        }
        key_patterns.push(format!("_{}%", &escaped_key[1..]).into());

        key_patterns
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

fn decompress_if_needed(blob: &Blob) -> Cow<'_, [u8]> {
    // Check Zstd’s magic number
    if blob.data.len() < 4 || blob.data[..4] != [0x28, 0xb5, 0x2f, 0xfd] {
        return blob.data.as_ref().into();
    }

    // TODO: What is a reasonable `capacity`?
    // TODO: Maybe we should store the exact uncompressed size along with the blob in the DB?
    if let Ok(mut decompressed_bytes) = zstd::bulk::decompress(&blob.data, blob.uncompressed_len) {
        decompressed_bytes.shrink_to_fit();
        decompressed_bytes.into()
    } else {
        // Could not decompress, so just return the original bytes and let
        // decoding fail downstream
        blob.data.as_ref().into()
    }
}
