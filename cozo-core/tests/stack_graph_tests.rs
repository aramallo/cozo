#![cfg(feature = "graph-algo")]

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

fn apply_db_schema(db: &mut DbInstance) {
    let schema = include_str!("stack_graphs/schema.pl");
    db.run_script(schema, Default::default(), ScriptMutability::Mutable)
        .expect("Could not create relations");
}

macro_rules! include_graph_row {
    ($path:expr) => {
        vec![
            DataValue::from(include_str!(concat!($path, ".blob_oid.txt"))),
            DataValue::from(include_bytes!(concat!($path, ".graph.bin")).to_vec()),
        ]
    };
}

macro_rules! include_node_path_row {
    ($path:expr, $start:literal) => {
        vec![
            DataValue::from(include_str!(concat!($path, ".blob_oid.txt"))),
            DataValue::from($start),
            DataValue::from(include_bytes!(concat!($path, ".node_path.", $start, ".bin")).to_vec()),
        ]
    };
}

macro_rules! include_root_path_row {
    ($path:expr, $symbol_stack:literal) => {
        vec![
            DataValue::from(include_str!(concat!($path, ".blob_oid.txt"))),
            DataValue::from($symbol_stack),
            DataValue::from(
                include_bytes!(concat!($path, ".root_path.", $symbol_stack, ".bin")).to_vec(),
            ),
        ]
    };
}

fn import_graphs_data(db: &mut DbInstance, rows: Vec<Vec<DataValue>>) {
    db.import_relations(BTreeMap::from([(
        "sg_graphs".to_string(),
        NamedRows {
            headers: vec!["file".to_string(), "value".to_string()],
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
                "value".to_string(),
            ],
            rows,
            next: None,
        },
    )]))
    .unwrap();
}

#[test]
fn it_finds_definition_in_single_file() {
    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_graphs_data(
        &mut db,
        vec![include_graph_row!(
            "stack_graphs/single_file_python/simple.py"
        )],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, value] :=
        *sg_graphs[file, tag, error, value]
    node_paths[file, start_local_id, value] := []
    root_paths[file, symbol_stack, value] := []

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_pathsp[], reference_urn: 'urn:augr:c329c84559b085714c39b872fe5e8df0a39f0a64:13:14')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from(
        "urn:augr:c329c84559b085714c39b872fe5e8df0a39f0a64:0:1",
    )]];
    assert_eq!(expected, query_result.rows);
}

#[test]
fn it_finds_definition_across_multiple_files() {
    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_graphs_data(
        &mut db,
        vec![
            include_graph_row!("stack_graphs/multi_file_python/main.py"),
            include_graph_row!("stack_graphs/multi_file_python/a.py"),
            include_graph_row!("stack_graphs/multi_file_python/b.py"),
        ],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/multi_file_python/main.py", 0),
            include_node_path_row!("stack_graphs/multi_file_python/main.py", 6),
            include_node_path_row!("stack_graphs/multi_file_python/main.py", 8),
            include_node_path_row!("stack_graphs/multi_file_python/a.py", 0),
            include_node_path_row!("stack_graphs/multi_file_python/a.py", 6),
            include_node_path_row!("stack_graphs/multi_file_python/b.py", 0),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![
            include_root_path_row!("stack_graphs/multi_file_python/main.py", "V␞__main__"),
            include_root_path_row!("stack_graphs/multi_file_python/a.py", "V␞a"),
            include_root_path_row!("stack_graphs/multi_file_python/b.py", "V␞b"),
        ],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, value] :=
        *sg_graphs[file, value]
    node_paths[file, start_local_id, value] :=
        *sg_node_paths[file, start_local_id, value]
    root_paths[file, symbol_stack, value] :=
        *sg_root_paths[file, symbol_stack, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference_urn: 'urn:augr:d51340e6364531f6c2ab3325fb31157932afc17d:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from(
        "urn:augr:81ec7e8b7425cdc58b42995c832b7abf727ef570:0:3",
    )]];
    assert_eq!(expected, query_result.rows);
}
