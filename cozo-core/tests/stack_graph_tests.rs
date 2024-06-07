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
    ($dir:expr, $file:expr) => {
        vec![
            DataValue::from($file),
            DataValue::from(include_bytes!(concat!($dir, $file, ".graph.bin")).to_vec()),
        ]
    };
}

macro_rules! include_node_path_row {
    ($dir:expr, $file:expr, $start:literal) => {
        vec![
            DataValue::from($file),
            DataValue::from($start),
            DataValue::from(
                include_bytes!(concat!($dir, $file, ".node_path.", $start, ".bin")).to_vec(),
            ),
        ]
    };
}

macro_rules! include_root_path_row {
    ($dir:expr, $file:expr, $symbol_stack:literal) => {
        vec![
            DataValue::from($file),
            DataValue::from($symbol_stack),
            DataValue::from(
                include_bytes!(concat!($dir, $file, ".root_path.", $symbol_stack, ".bin")).to_vec(),
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
            "stack_graphs/single_file_python/",
            "simple.py"
        )],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/single_file_python/", "simple.py", 0),
            include_node_path_row!("stack_graphs/single_file_python/", "simple.py", 7),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![include_root_path_row!(
            "stack_graphs/single_file_python/",
            "simple.py",
            "V␞__main__"
        )],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, value] :=
        *sg_graphs[file, value]
    node_paths[file, start_local_id, value] :=
        *sg_node_paths[file, start_local_id, value]
    root_paths[file, symbol_stack, value] :=
        *sg_root_paths[file, symbol_stack, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference_urn: 'simple.py:13:14')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from("simple.py:0:1")]];
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
            include_graph_row!("stack_graphs/multi_file_python/", "main.py"),
            include_graph_row!("stack_graphs/multi_file_python/", "a.py"),
            include_graph_row!("stack_graphs/multi_file_python/", "b.py"),
        ],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 0),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 6),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 8),
            include_node_path_row!("stack_graphs/multi_file_python/", "a.py", 0),
            include_node_path_row!("stack_graphs/multi_file_python/", "a.py", 6),
            include_node_path_row!("stack_graphs/multi_file_python/", "b.py", 0),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![
            include_root_path_row!("stack_graphs/multi_file_python/", "main.py", "V␞__main__"),
            include_root_path_row!("stack_graphs/multi_file_python/", "a.py", "V␞a"),
            include_root_path_row!("stack_graphs/multi_file_python/", "b.py", "V␞b"),
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

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference_urn: 'main.py:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from("b.py:0:3")]];
    assert_eq!(expected, query_result.rows);
}

#[test]
fn it_returns_empty_without_errors_if_definition_is_not_available() {
    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_graphs_data(
        &mut db,
        vec![include_graph_row!(
            "stack_graphs/multi_file_python/",
            "main.py"
        )],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 0),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 6),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 8),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![include_root_path_row!(
            "stack_graphs/multi_file_python/",
            "main.py",
            "V␞__main__"
        )],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, value] :=
        *sg_graphs[file, value]
    node_paths[file, start_local_id, value] :=
        *sg_node_paths[file, start_local_id, value]
    root_paths[file, symbol_stack, value] :=
        *sg_root_paths[file, symbol_stack, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference_urn: 'main.py:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    assert!(query_result.rows.is_empty());
}
