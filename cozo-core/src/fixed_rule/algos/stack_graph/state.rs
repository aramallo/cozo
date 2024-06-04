use std::collections::HashMap;

use stack_graphs::{
    arena::Handle,
    graph::{File, NodeID, StackGraph},
    partial::PartialPaths,
    stitching::{Database, Stats},
};

/// State for a definition query. Fixed rules cannot themselves load data, so
/// all data they might need must be provid. The `*_blobs` fields hold binary
/// blobs representing partial graphs or paths that have not yet been “loaded”;
/// whenever one is needed it is removed from the corresponding collection,
/// parsed, and integrated into `graph`, `partials`, and/or `db`.
struct State {
    /// Indexed by Git `BLOB_OID`
    graph_blobs: HashMap<Handle<File>, Box<[u8]>>,
    /// Indexed by Git `BLOB_OID` & local ID
    node_path_blobs: HashMap<NodeID, Box<[u8]>>,
    /// Indexed by serialized symbol stacks
    root_path_blobs: HashMap<Box<str>, Box<[u8]>>,
    graph: StackGraph,
    partials: PartialPaths,
    db: Database,
    stats: Stats,
}
