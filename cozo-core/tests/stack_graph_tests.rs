#![cfg(feature = "graph-algo")]

use cozo::{DataValue, DbInstance, NamedRows, ScriptMutability};
use std::collections::BTreeMap;
use std::fs;

fn create_db_schema(db: &mut DbInstance) {
    // Creates stored relation
    db.run_script(
        r#"
            {:create sg_graph {blob_id: String => repository_id: String, tag: String, error: String?, graph: Bytes}}
            "#,
        Default::default(),
        ScriptMutability::Mutable,
    )
        .expect("Could not create relation");
}

fn populate_db(db: &mut DbInstance) {
    // Reads stack graph into a buffer
    let stack_graph_buffer =
        fs::read("./tests/stack_graph.bin").expect("Could not read stack graph data");

    // Imports the rows into the stored relation
    // NOTE: Doing this via a CozoScript query takes considerably more.
    db.import_relations(BTreeMap::from([(
        "sg_graph".to_string(),
        NamedRows {
            headers: vec![
                "blob_id".to_string(),
                "repository_id".to_string(),
                "tag".to_string(),
                "error".to_string(),
                "graph".to_string(),
            ],
            rows: vec![vec![
                DataValue::from("blob_id"),
                DataValue::from("repository_id"),
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
    create_db_schema(&mut db);
    populate_db(&mut db);

    // Perform a stack graph query
    let query = r#"
    sg_repo[repository_id, blob_id, tag, error, graph] :=
        repository_id = 'repository_id',
        *sg_graph{repository_id, blob_id, tag, error, graph}

    sg_starting[repository_id, blob_id, tag, error, graph] :=
        blob_id = 'blob_id',
        sg_repo[repository_id, blob_id, tag, error, graph]

    ?[urn] <~ StackGraph(sg_repo[], sg_starting[], reference_urn: 'urn:augr:blob_id:4031:4048')
    "#;
    let query_result = db.run_default(&query);
    match query_result {
        Err(err) => panic!("Error {}", err),
        Ok(result) => {
            println!("{:?}", result.rows);
            assert_eq!(result.rows[0][0], DataValue::from("urn:augr:blob_id:4031:4048"));
        }
    }
}
