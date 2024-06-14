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

fn import_graphs_data(db: &mut DbInstance, rows: Vec<Vec<DataValue>>) {
    db.import_relations(BTreeMap::from([(
        "sg_graphs".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "uncompressed_value_len".to_string(),
                "value".to_string(),
            ],
            rows,
            next: None,
        },
    )]))
    .unwrap()
}

fn import_node_paths_data(db: &mut DbInstance, rows: Vec<Vec<DataValue>>) {
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
            rows,
            next: None,
        },
    )]))
    .unwrap();
}

fn import_root_paths_data(db: &mut DbInstance, rows: Vec<Vec<DataValue>>) {
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
            rows,
            next: None,
        },
    )]))
    .unwrap();
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

fn import_stack_graph_blobs(db: &mut DbInstance, file_path: &str) {
    let json_path = format!("tests/stack_graphs/{file_path}.json");
    let file = std::fs::File::open(json_path).expect("missing blobs JSON file");
    let reader = std::io::BufReader::new(file);
    let blobs: serialization::Blobs =
        serde_json::from_reader(reader).expect("cannot deserialize blobs from JSON");
    // Populate the DB
    import_graphs_data(db, vec![blobs.graph.into()]);
    import_node_paths_data(db, blobs.node_paths.into_iter().map(From::from).collect());
    import_root_paths_data(db, blobs.root_paths.into_iter().map(From::from).collect());
}

#[test]
fn it_finds_definition_in_single_file() {
    init_logging();

    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_stack_graph_blobs(&mut db, "single_file_python/simple.py");

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
    import_stack_graph_blobs(&mut db, "multi_file_python/main.py");
    import_stack_graph_blobs(&mut db, "multi_file_python/a.py");
    import_stack_graph_blobs(&mut db, "multi_file_python/b.py");

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
    import_stack_graph_blobs(&mut db, "multi_file_python/main.py");

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

mod serialization {
    use super::DataValue;
    use base64::engine::general_purpose::STANDARD;
    use base64_serde::base64_serde_type;
    use serde::Deserialize;

    base64_serde_type!(pub Base64Standard, STANDARD);

    #[derive(Deserialize)]
    pub struct Blobs {
        pub graph: GraphBlob,
        pub node_paths: Vec<NodePathBlob>,
        pub root_paths: Vec<RootPathBlob>,
    }

    #[derive(Deserialize)]
    pub struct GraphBlob {
        file: Box<str>,
        #[serde(with = "Base64Standard")]
        binary_data: Box<[u8]>,
        uncompressed_len: usize,
    }

    #[derive(Deserialize)]
    pub struct NodePathBlob {
        file: Box<str>,
        start_node_local_id: u32,
        discriminant: usize,
        uncompressed_len: usize,
        #[serde(with = "Base64Standard")]
        binary_data: Box<[u8]>,
    }

    #[derive(Deserialize)]
    pub struct RootPathBlob {
        file: Box<str>,
        symbol_stack: Box<str>,
        discriminant: usize,
        uncompressed_len: usize,
        #[serde(with = "Base64Standard")]
        binary_data: Box<[u8]>,
    }

    impl From<GraphBlob> for Vec<DataValue> {
        fn from(value: GraphBlob) -> Self {
            vec![
                value.file.as_ref().into(),
                (value.uncompressed_len as i64).into(),
                value.binary_data.into_vec().into(),
            ]
        }
    }

    impl From<NodePathBlob> for Vec<DataValue> {
        fn from(value: NodePathBlob) -> Self {
            vec![
                value.file.as_ref().into(),
                (value.start_node_local_id as i64).into(),
                (value.discriminant as i64).into(),
                (value.uncompressed_len as i64).into(),
                value.binary_data.into_vec().into(),
            ]
        }
    }

    impl From<RootPathBlob> for Vec<DataValue> {
        fn from(value: RootPathBlob) -> Self {
            vec![
                value.file.as_ref().into(),
                value.symbol_stack.as_ref().into(),
                (value.discriminant as i64).into(),
                (value.uncompressed_len as i64).into(),
                value.binary_data.into_vec().into(),
            ]
        }
    }
}
