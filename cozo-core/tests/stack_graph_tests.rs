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

    assert_eq!(query_result.rows, &[] as &[Vec<_>]);
}

#[test]
fn it_optionally_outputs_root_path_symbol_stack_patterns() {
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

    ?[] <~ StackGraph(*sg[], file_path[], root_path[],
        references: ['main.py:22:25'],
        output_root_path_symbol_stack_patterns: true,
    )
    "#;
    let query_result = db.run_default(query).unwrap();

    assert_eq!(query_result.rows, &[
        vec!["main.py:22:25".into(), DataValue::Null, "V␞<builtins>".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "V␞<builtins>␟.".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "V␞<builtins>␟.␟foo".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "V␞a".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "V␞a␟.".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "V␞a␟.␟foo".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "X␞<builtins>␟.␟foo".into()],
        vec!["main.py:22:25".into(), DataValue::Null, "X␞a␟.␟foo".into()],
    ]);
}

#[test]
fn it_is_recursive() {
    init_logging();

    // Initialize the DB
    let mut db = DbInstance::default();
    apply_db_schema(&mut db);

    // Populate the DB
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/main.py"));
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/a.py"));
    import_stack_graph_blobs(&mut db, include_json_bytes!("multi_file_python/b.py"));

    let mut query_builder = QueryBuilder {
        accumulated_query: String::new(),
    };
    // query_builder.inspect_step(&mut db, r#"
    //     test[count(file)] :=
    //         *sg{file}
    // "#);

    println!("\n\n# Step 1");
    query_builder.add_step_and_inspect(&mut db, r#"
        files_to_load_1[first] <-
            [['main.py']]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        stack_graph_1[file, size, value] :=
            *sg{file, size, value},
            files_to_load_1[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        file_path_1[file, local_id, size, value] :=
            *sg_file_path{file, local_id, size, value},
            files_to_load_1[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        root_path_1[file, symbol_stack, size, value] :=
            *sg_root_path{file, symbol_stack, size, value},
            files_to_load_1[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        definition_search_result_1[reference, definition, symbol_stack] <~ StackGraph(
            stack_graph_1[],
            file_path_1[],
            root_path_1[],
            references: ['main.py:22:25'],
            output_root_path_symbol_stack_patterns: true,
        )
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        found_definitions_1[definition] :=
            definition_search_result_1[_, definition, null]
    "#);

    println!("\n\n# Step 2");
    // query_builder.inspect_step(&mut db, r#"
    //     files_to_load_2[symbol_stack, file, reference] := 
    //         definition_search_result_1[reference, definition, precondition_symbol_stack],
    //         *sg_root_path{file, symbol_stack},

    //         # It seems the SQLIte implementation uses: "WHERE symbol_stack LIKE _pattern%", so IIUC:
    //         #   it omits the has-var (V or X)
    //         #   it searches all that have the same prefix
    //         # but it seems all those options have already been explicitly returned by the fixed rule,
    //         # so we may not need that?
    //         str_includes(precondition_symbol_stack, symbol_stack)
    //         # starts_with(precondition_symbol_stack, symbol_stack)
    // "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        files_to_load_2[file] := files_to_load_1[file]
        files_to_load_2[file] := 
            # IIUC definition will always be null if there's a symbol_stack
            #   saying it explicitly (null vs _) may produce a faster query
            definition_search_result_1[_, null, symbol_stack],
            *sg_root_path{file, symbol_stack}
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        stack_graph_2[file, size, value] :=
            *sg{file, size, value},
            files_to_load_2[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        file_path_2[file, local_id, size, value] :=
            *sg_file_path{file, local_id, size, value},
            files_to_load_2[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        root_path_2[file, symbol_stack, size, value] :=
            *sg_root_path{file, symbol_stack, size, value},
            files_to_load_2[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        definition_search_result_2[reference, definition, symbol_stack] <~ StackGraph(
            stack_graph_2[],
            file_path_2[],
            root_path_2[],
            references: ['main.py:22:25'],
            output_root_path_symbol_stack_patterns: true,
        )
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        found_definitions_2[definition] :=
            definition_search_result_2[_, definition, null]
    "#);

    println!("\n\n# Step 3");
    query_builder.add_step_and_inspect(&mut db, r#"
        files_to_load_3[file] := files_to_load_2[file]
        files_to_load_3[file] := 
            # IIUC definition will always be null if there's a symbol_stack
            #   saying it explicitly (null vs _) may produce a faster query
            definition_search_result_2[_, null, symbol_stack],
            *sg_root_path{file, symbol_stack}
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        stack_graph_3[file, size, value] :=
            *sg{file, size, value},
            files_to_load_3[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        file_path_3[file, local_id, size, value] :=
            *sg_file_path{file, local_id, size, value},
            files_to_load_3[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        root_path_3[file, symbol_stack, size, value] :=
            *sg_root_path{file, symbol_stack, size, value},
            files_to_load_3[file]
    "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        definition_search_result_3[reference, definition, symbol_stack] <~ StackGraph(
            stack_graph_3[],
            file_path_3[],
            root_path_3[],
            references: ['main.py:22:25'],
            output_root_path_symbol_stack_patterns: true,
        )
    "#);
    // query_builder.inspect_step(&mut db, r#"
    //     found_definitions_3[count(definition)] :=
    //         definition_search_result_3[_, definition, null]
    // "#);
    query_builder.add_step_and_inspect(&mut db, r#"
        found_definitions_3[definition] :=
            definition_search_result_3[_, definition, null]
    "#);
}

struct QueryBuilder {
    accumulated_query: String,
}

impl QueryBuilder {
    pub fn add_step_and_inspect(&mut self, db: &mut DbInstance, query_step: &'static str) -> Result<NamedRows, cozo::Error> {
        self.add_step(db, query_step);
        self.inspect_step_without_adding(db, query_step, &self.accumulated_query)
    }

    pub fn add_step(&mut self, _db: &mut DbInstance, query_step: &'static str) {
        self.accumulated_query += query_step;
        // println!("query: ```{}```", self.accumulated_query);
    }

    pub fn inspect_step(&self, db: &mut DbInstance, query_step: &'static str) {
        let accumulated_query = format!("{}{}", self.accumulated_query, query_step);
        self.inspect_step_without_adding(db, query_step, &accumulated_query);
    }
    
    fn inspect_step_without_adding(&self, db: &mut DbInstance, query_step: &'static str, accumulated_query: &str) -> Result<NamedRows, cozo::Error> {
        let (rule, parameters) = Self::extract_first_bracketed(query_step).unwrap();
        let query = format!("{}\n?{parameters} := {rule}{parameters}", accumulated_query);
        // println!("query: ```{}```", query);
        println!("{query_step}");
        // println!("        ?{parameters} := {}{parameters}", rule.trim());
        let rule = rule.trim();
        let named_rows = db.run_default(&query);
        match &named_rows {
            Ok(named_rows) => Self::inspect(rule, named_rows),
            Err(e) => println!("----- {}, failed: {:?}", rule, e),
        }
        named_rows
    }

    fn extract_first_bracketed(query_step: &'static str) -> Option<(&'static str, &'static str)> {
        let Some(bracket_start) = query_step.find('[') else {
            return None;
        };
        let Some(bracket_end) = query_step.find(']') else {
            return None;
        };
        let rule = &query_step[0..bracket_start];
        let parameters = &query_step[bracket_start..=bracket_end];
        return Some((rule, parameters));
    }

    fn inspect(label: &str, named_rows: &NamedRows) {
        // Table pattern:
        //   (| valueN )+|
        //    ^ = SEPARATOR
        //     ^      ^ = PADDING
        //      ^....^ = WIDTH
        // Example:
        //   | value0 | value1 | value2 |
        
        const SEPARATOR: usize = 1;
        const PADDING: usize = 2;
        const WIDTH: usize = 25;
    
        const TITLE_PADDING: usize = 7;
        let column_count = named_rows.headers.len();
        let table_width = (SEPARATOR + PADDING + WIDTH)*column_count + SEPARATOR;
        let bar = "-".repeat(table_width.saturating_sub(label.len() + TITLE_PADDING));
        println!("----- {} {}", label, bar);
        //        ^^^^^^  ^ = TITLE_PADDING
        
        let bar = "-".repeat(table_width);
        for column in named_rows.headers.iter() {
            print!("| {:WIDTH$} ", column);
        }
        println!("|\n{}", bar);
        for row in named_rows.rows.iter() {
            for column in row {
                let mut value = column.to_string();
                if value.len() > WIDTH {
                    value = value.chars().take(WIDTH-1).collect::<String>()+"…";
                }
                print!("| {:WIDTH$} ", value);
            }
            println!("|");
        }
        println!("{}", bar);
        println!();
    }
}

impl From<QueryBuilder> for String {
    fn from(value: QueryBuilder) -> Self {
        value.accumulated_query
    }
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
