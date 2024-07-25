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
        "sg".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "size".to_string(),
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
        "sg_file_path".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "local_id".to_string(),
                "discriminator".to_string(),
                "size".to_string(),
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
        "sg_root_path".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "symbol_stack".to_string(),
                "discriminator".to_string(),
                "size".to_string(),
                "value".to_string(),
            ],
            rows,
            next: None,
        },
    )]))
    .unwrap();
}

fn import_stack_graph_blobs(db: &mut DbInstance, json: &[u8]) {
    let blobs: serialization::Blobs =
        serde_json::from_reader(json).expect("cannot deserialize blobs from JSON");
    import_graphs_data(db, vec![blobs.graph.into()]);
    import_node_paths_data(db, blobs.node_paths.into_iter().map(From::from).collect());
    import_root_paths_data(db, blobs.root_paths.into_iter().map(From::from).collect());
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
    file_path[file, local_id, size, value] :=
        *sg_file_path{file, local_id, size, value}
    root_path[file, symbol_stack, size, value] :=
        *sg_root_path{file, symbol_stack, size, value}

    ?[] <~ StackGraph(*sg[], file_path[], root_path[], references: ['simple.py:13:14'])
    "#;
    let query_result = db.run_default(query).unwrap();

    assert_eq!(query_result.rows, &[
        vec!["simple.py:13:14".into(), "simple.py:0:1".into(), DataValue::Null],
    ]);
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
    file_path[file, local_id, size, value] :=
        *sg_file_path{file, local_id, size, value}
    root_path[file, symbol_stack, size, value] :=
        *sg_root_path{file, symbol_stack, size, value}

    ?[] <~ StackGraph(*sg[], file_path[], root_path[], references: ['main.py:22:25'])
    "#;
    let query_result = db.run_default(query).unwrap();

    assert_eq!(query_result.rows, &[
        vec!["main.py:22:25".into(), "b.py:0:3".into(), DataValue::Null],
    ]);
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
    file_path[file, local_id, size, value] :=
        *sg_file_path{file, local_id, size, value}
    root_path[file, symbol_stack, size, value] :=
        *sg_root_path{file, symbol_stack, size, value}

    ?[] <~ StackGraph(*sg[], file_path[], root_path[], references: ['main.py:22:25'])
    "#;
    let query_result = db.run_default(query).unwrap();

    assert!(query_result.rows.is_empty());
}

#[test]
fn it_returns_missing_files_if_definition_is_not_available() {
    init_logging();

    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/main.py"));
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/a.py"));
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/b.py"));

    let query_template = r#"
    graph[file, size, value] :=
        FILTER_BY_FILE,
        *sg{file, size, value}
    file_path[file, local_id, size, value] :=
        FILTER_BY_FILE,
        *sg_file_path{file, local_id, size, value}
    root_path[file, symbol_stack, size, value] :=
        FILTER_BY_FILE,
        *sg_root_path{file, symbol_stack, size, value}
    root_path_symbol_stacks_files[symbol_stack, file] :=
        *sg_root_path{file, symbol_stack}

    ?[] <~ StackGraph(graph[], file_path[], root_path[], root_path_symbol_stacks_files[],
        references: ['main.py:22:25'],
        output_missing_files: true, # Not necessary -- 4th pos. arg. implies `true`
    )
    "#;

    // Perform a first stack graph query
    let first_query = query_template.replace("FILTER_BY_FILE", "file = 'main.py'");
    let first_query_result = db.run_default(&first_query).unwrap();
    assert_eq!(first_query_result.rows, &[
        vec!["main.py:22:25".into(), DataValue::Null, "a.py".into()],
    ]);

    // Perform a second stack graph query
    let second_query = query_template
        .replace("FILTER_BY_FILE", "file = 'main.py' or file = 'a.py'");
    let second_query_result = db.run_default(&second_query).unwrap();
    assert_eq!(second_query_result.rows, &[
        vec!["main.py:22:25".into(), DataValue::Null, "b.py".into()],
    ]);
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
