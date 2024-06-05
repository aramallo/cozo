#![cfg(feature = "graph-algo")]

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use std::collections::BTreeMap;

fn apply_db_schema(db: &mut DbInstance) {
    // Creates stored relations
    let schema = include_str!("stack_graphs/schema.pl");
    db.run_script(schema, Default::default(), ScriptMutability::Mutable)
        .expect("Could not create relations");
}

fn populate_graphs_relation(db: &mut DbInstance) {
    // Imports the rows into the stored relation
    // NOTE: Doing this via a CozoScript query takes considerably more.
    db.import_relations(BTreeMap::from([(
        "sg_graphs".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "tag".to_string(),
                "error".to_string(),
                "value".to_string(),
            ],
            rows: vec![vec![
                DataValue::from("d51340e6364531f6c2ab3325fb31157932afc17d"),
                DataValue::from("tag_value"),
                DataValue::Null,
                DataValue::from(include_bytes!("stack_graphs/graph.bin").to_vec()),
            ]],
            next: None,
        },
    )]))
    .unwrap();
}

#[test]
fn test_stack_graphs() {
    // Initializes database
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);
    populate_graphs_relation(&mut db);

    // Perform a stack graph query
    let query = r#"
    graphs[file, value] :=
        file = 'd51340e6364531f6c2ab3325fb31157932afc17d',
        *sg_graphs[file, tag, error, value]
    node_paths[file, start_local_id, value] :=
        file = 'd51340e6364531f6c2ab3325fb31157932afc17d',
        *sg_node_paths[file, start_local_id, value]
    root_paths[file, symbol_stack, value] :=
        file = 'd51340e6364531f6c2ab3325fb31157932afc17d',
        *sg_root_paths[file, symbol_stack, value]

    ?[urn] <~ StackGraph(graphs[], node_paths[], root_pathsp[], reference_urn: 'urn:augr:d51340e6364531f6c2ab3325fb31157932afc17d:22:25')
    "#;
    let query_result = db.run_default(query).unwrap();

    let expected = vec![vec![DataValue::from(
        "urn:augr:d51340e6364531f6c2ab3325fb31157932afc17d:22:25",
    )]];
    assert_eq!(expected, query_result.rows);
}
