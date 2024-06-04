#![cfg(feature = "graph-algo")]

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use std::collections::BTreeMap;
use std::fs;

fn apply_db_schema(db: &mut DbInstance) {
    // Creates stored relations
    let schema = {
        let buffer = fs::read("./tests/stack_graphs/schema.pl")
            .expect("Could not read schema file");
        String::from_utf8(buffer).expect("Cannot read schema file as UTF-8")
    };
    db.run_script(&schema, Default::default(), ScriptMutability::Mutable)
        .expect("Could not create relations");
}

fn populate_graphs_relation(db: &mut DbInstance) {
    // Reads stack graph into a buffer
    let stack_graph_buffer =
        fs::read("./tests/stack_graphs/graph.bin").expect("Could not read stack graph data");

    // Imports the rows into the stored relation
    // NOTE: Doing this via a CozoScript query takes considerably more.
    db.import_relations(BTreeMap::from([(
        "sg_graph".to_string(),
        NamedRows {
            headers: vec![
                "file".to_string(),
                "tag".to_string(),
                "error".to_string(),
                "graph".to_string(),
            ],
            rows: vec![vec![
                DataValue::from("file_id"),
                DataValue::from("tag_value"),
                DataValue::Null,
                DataValue::from(stack_graph_buffer),
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
    sg_starting[file, tag, error, graph] :=
        file = 'file_id',
        *sg_graph[file, tag, error, graph]

    ?[urn] <~ StackGraph(*sg_graph[], sg_starting[], reference_urn: 'urn:augr:file_id:4031:4048')
    "#;
    let query_result = db.run_default(query);
    match query_result {
        Err(err) => panic!("Error {}", err),
        Ok(result) => {
            println!("{:?}", result.rows);
            assert_eq!(result.rows[0][0], DataValue::from("urn:augr:file_id:4031:4048"));
        }
    }
}
