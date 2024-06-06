{:create sg_graphs {
    file: String
    =>
    value: Bytes
}}

{:create sg_node_paths {
    file: String,
    start_local_id: Int
    =>
    value: Bytes
}}

{:create sg_root_paths {
    file: String,
    symbol_stack: String
    =>
    value: Bytes
}}
