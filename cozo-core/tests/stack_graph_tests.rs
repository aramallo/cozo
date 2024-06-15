#![cfg(feature = "graph-algo")]

use std::collections::BTreeMap;

use env_logger::{Builder as LoggerBuilder, Env as LogEnv};
use pretty_assertions::assert_eq;

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};

fn apply_db_schema(db: &mut DbInstance) {
    let schema = include_str!("stack_graphs/schema.pl");
    db.run_script(schema, Default::default(), ScriptMutability::Mutable)
        .expect("Could not create relations");
}

fn import_graph_data(db: &mut DbInstance, file: &str, row: serialization::Blob) {
    db.import_relations(BTreeMap::from([(
        "sg_graphs".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "uncompressed_value_len".to_string(),
                "value".to_string(),
            ],
            rows: vec![vec![
                file.into(),
                (row.uncompressed_len as i64).into(),
                row.data.into_vec().into(),
            ]],
            next: None,
        },
    )]))
    .unwrap()
}

fn import_node_paths_data(
    db: &mut DbInstance,
    file: &str,
    rows: impl IntoIterator<Item = serialization::NodePathBlob>,
) {
    db.import_relations(BTreeMap::from([(
        "sg_node_paths".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "start_local_id".to_string(),
                "discriminator".to_string(),
                "uncompressed_value_len".to_string(),
                "value".to_string(),
            ],
            rows: rows
                .into_iter()
                .enumerate()
                .map(|(i, row)| {
                    vec![
                        file.into(),
                        (row.start_node_local_id as i64).into(),
                        (i as i64).into(),
                        (row.value.uncompressed_len as i64).into(),
                        row.value.data.into_vec().into(),
                    ]
                })
                .collect(),
            next: None,
        },
    )]))
    .unwrap();
}

fn import_root_paths_data(
    db: &mut DbInstance,
    file: &str,
    rows: impl IntoIterator<Item = serialization::RootPathBlob>,
) {
    db.import_relations(BTreeMap::from([(
        "sg_root_paths".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "symbol_stack".to_string(),
                "discriminator".to_string(),
                "uncompressed_value_len".to_string(),
                "value".to_string(),
            ],
            rows: rows
                .into_iter()
                .enumerate()
                .map(|(i, row)| {
                    vec![
                        file.into(),
                        row.symbol_stack.as_ref().into(),
                        (i as i64).into(),
                        (row.value.uncompressed_len as i64).into(),
                        row.value.data.into_vec().into(),
                    ]
                })
                .collect(),
            next: None,
        },
    )]))
    .unwrap();
}

fn import_stack_graph_blobs(db: &mut DbInstance, json: &[u8]) {
    let blobs: serialization::Blobs =
        serde_json::from_reader(json).expect("cannot deserialize blobs from JSON");
    let file = blobs.file.as_ref();
    import_graph_data(db, file, blobs.graph);
    import_node_paths_data(db, file, blobs.node_paths);
    import_root_paths_data(db, file, blobs.root_paths);
}

macro_rules! include_json_bytes {
    ( $path:literal ) => {
        include_bytes!(concat!("stack_graphs/", $path, ".json"))
    };
}

fn init_logging() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Overridde with env. var. `RUST_LOG` with target `cozo::fixed_rule::algos::stack_graph`
        // For example: `RUST_LOG=cozo::fixed_rule::algos::stack_graph=debug cargo test ...`
        LoggerBuilder::from_env(LogEnv::default().default_filter_or("info")).init();
    });
}

#[test]
fn it_finds_definition_in_single_file() {
    init_logging();

    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_stack_graph_blobs(&mut db, include_json_bytes!("single_file_python/simple.py"));

    // Perform a stack graph query
    let query = r#"
    graphs[file, uncompressed_value_len, value] :=
        *sg_graphs[file, uncompressed_value_len, value]
    node_paths[file, start_local_id, uncompressed_value_len, value] :=
        *sg_node_paths[file, start_local_id, _, uncompressed_value_len, value]
    root_paths[file, symbol_stack, uncompressed_value_len, value] :=
        *sg_root_paths[file, symbol_stack, _, uncompressed_value_len, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference: 'simple.py:13:14')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from("simple.py:0:1")]];
    assert_eq!(expected, query_result.rows);
}

#[test]
fn it_finds_definition_across_multiple_files() {
    init_logging();

    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/main.py"));
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/a.py"));
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/b.py"));

    // Perform a stack graph query
    let query = r#"
    graphs[file, uncompressed_value_len, value] :=
        *sg_graphs[file, uncompressed_value_len, value]
    node_paths[file, start_local_id, uncompressed_value_len, value] :=
        *sg_node_paths[file, start_local_id, _, uncompressed_value_len, value]
    root_paths[file, symbol_stack, uncompressed_value_len, value] :=
        *sg_root_paths[file, symbol_stack, _, uncompressed_value_len, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference: 'main.py:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from("b.py:0:3")]];
    assert_eq!(expected, query_result.rows);
}

#[test]
fn it_returns_empty_without_errors_if_definition_is_not_available() {
    init_logging();

    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/main.py"));

    // Perform a stack graph query
    let query = r#"
    graphs[file, uncompressed_value_len, value] :=
        *sg_graphs[file, uncompressed_value_len, value]
    node_paths[file, start_local_id, uncompressed_value_len, value] :=
        *sg_node_paths[file, start_local_id, _, uncompressed_value_len, value]
    root_paths[file, symbol_stack, uncompressed_value_len, value] :=
        *sg_root_paths[file, symbol_stack, _, uncompressed_value_len, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference: 'main.py:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    assert!(query_result.rows.is_empty());
}

// TODO: DRY (this is basically the same code as in `beam_tree_sitter::stack_graph::serialization`)
mod serialization {
    use base64::engine::general_purpose::STANDARD;
    use base64_serde::base64_serde_type;

    base64_serde_type!(pub Base64Standard, STANDARD);

    #[derive(serde::Deserialize)]
    pub struct Blob {
        /// Length of [`data`][`Blob::data`] before any compression.
        pub uncompressed_len: usize,
        /// Possibly Zstd-compressed.
        #[serde(with = "Base64Standard")]
        pub data: Box<[u8]>,
    }

    #[derive(serde::Deserialize)]
    pub struct NodePathBlob {
        /// The local ID of the node path’s start node.
        pub start_node_local_id: u32,
        /// The serialized node path.
        #[serde(flatten)]
        pub value: Blob,
    }

    #[derive(serde::Deserialize)]
    pub struct RootPathBlob {
        /// An indexing key representing the symbol stack precondition. This
        /// follows the SQLite storage implementation; see
        /// [`PartialSymbolStackExt`].
        pub symbol_stack: Box<str>,
        /// The serialized root path.
        #[serde(flatten)]
        pub value: Blob,
    }

    #[derive(serde::Deserialize)]
    pub struct Blobs {
        /// The path to the file of the serialized graph & paths.
        pub file: Box<str>,
        /// The serialized graph.
        pub graph: Blob,
        /// The serialized node paths; more than one can have the same
        /// [`start_node_local_id`][`NodePathBlob::start_node_local_id`].
        pub node_paths: Box<[NodePathBlob]>,
        /// The serialized node paths; more than one can have the same
        /// [`symbol_stack`][`RootPathBlob::symbol_stack`].
        pub root_paths: Box<[RootPathBlob]>,
    }
}
