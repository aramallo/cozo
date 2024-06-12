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
    ($dir:expr, $file:expr, $len:literal $(,)?) => {
        vec![
            DataValue::from($file),
            DataValue::from($len),
            DataValue::from(include_bytes!(concat!($dir, $file, ".graph.bin")).to_vec()),
        ]
    };
}

macro_rules! include_node_path_row {
    ($dir:expr, $file:expr, $discr:literal, $start:literal, $len:literal $(,)?) => {
        vec![
            DataValue::from($file),
            DataValue::from($start),
            DataValue::from($discr),
            DataValue::from($len),
            DataValue::from(
                include_bytes!(concat!(
                    $dir,
                    $file,
                    ".node_path",
                    $discr,
                    ".",
                    $start,
                    ".bin"
                ))
                .to_vec(),
            ),
        ]
    };
}

macro_rules! include_root_path_row {
    ($dir:expr, $file:expr, $discr:literal, $symbol_stack:literal, $len:literal $(,)?) => {
        vec![
            DataValue::from($file),
            DataValue::from($symbol_stack),
            DataValue::from($discr),
            DataValue::from($len),
            DataValue::from(
                include_bytes!(concat!(
                    $dir,
                    $file,
                    ".root_path",
                    $discr,
                    ".",
                    $symbol_stack,
                    ".bin"
                ))
                .to_vec(),
            ),
        ]
    };
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
            "simple.py",
            534,
        )],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/single_file_python/", "simple.py", 0, 0, 118),
            include_node_path_row!("stack_graphs/single_file_python/", "simple.py", 1, 7, 76),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![include_root_path_row!(
            "stack_graphs/single_file_python/",
            "simple.py",
            0,
            "V␞__main__",
            40,
        )],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, uncompressed_value_len, value] :=
        *sg_graphs[file, uncompressed_value_len, value]
    node_paths[file, start_local_id, uncompressed_value_len, value] :=
        *sg_node_paths[file, start_local_id, _, uncompressed_value_len, value]
    root_paths[file, symbol_stack, uncompressed_value_len, value] :=
        *sg_root_paths[file, symbol_stack, _, uncompressed_value_len, value]

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
            include_graph_row!("stack_graphs/multi_file_python/", "main.py", 523),
            include_graph_row!("stack_graphs/multi_file_python/", "a.py", 221),
            include_graph_row!("stack_graphs/multi_file_python/", "b.py", 319),
        ],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 0, 0, 118),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 1, 6, 88),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 2, 8, 39),
            include_node_path_row!("stack_graphs/multi_file_python/", "a.py", 0, 0, 79),
            include_node_path_row!("stack_graphs/multi_file_python/", "a.py", 1, 6, 33),
            include_node_path_row!("stack_graphs/multi_file_python/", "b.py", 0, 0, 70),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![
            include_root_path_row!(
                "stack_graphs/multi_file_python/",
                "main.py",
                0,
                "V␞__main__",
                38,
            ),
            include_root_path_row!("stack_graphs/multi_file_python/", "a.py", 0, "V␞a", 28),
            include_root_path_row!("stack_graphs/multi_file_python/", "b.py", 0, "V␞b", 28),
        ],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, uncompressed_value_len, value] :=
        *sg_graphs[file, uncompressed_value_len, value]
    node_paths[file, start_local_id, uncompressed_value_len, value] :=
        *sg_node_paths[file, start_local_id, _, uncompressed_value_len, value]
    root_paths[file, symbol_stack, uncompressed_value_len, value] :=
        *sg_root_paths[file, symbol_stack, _, uncompressed_value_len, value]

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
            "main.py",
            523,
        )],
    );

    import_node_paths_data(
        &mut db,
        vec![
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 0, 0, 118),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 1, 6, 88),
            include_node_path_row!("stack_graphs/multi_file_python/", "main.py", 2, 8, 39),
        ],
    );

    import_root_paths_data(
        &mut db,
        vec![include_root_path_row!(
            "stack_graphs/multi_file_python/",
            "main.py",
            0,
            "V␞__main__",
            38,
        )],
    );

    // Perform a stack graph query
    let query = r#"
    graphs[file, uncompressed_value_len, value] :=
        *sg_graphs[file, uncompressed_value_len, value]
    node_paths[file, start_local_id, uncompressed_value_len, value] :=
        *sg_node_paths[file, start_local_id, _, uncompressed_value_len, value]
    root_paths[file, symbol_stack, uncompressed_value_len, value] :=
        *sg_root_paths[file, symbol_stack, _, uncompressed_value_len, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_paths[], reference_urn: 'main.py:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    assert!(query_result.rows.is_empty());
}
