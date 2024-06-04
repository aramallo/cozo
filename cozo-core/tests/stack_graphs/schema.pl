{:create sg_graph {
    file: String =>
    tag: String,
    error: String?,
    graph: Bytes
}}

{:create sg_file_paths {
    file: String =>
    local_id: Int,
    value: Bytes
}}

{:create sg_root_paths {
    file: String =>
    symbol_stack: String,
    value: Bytes
}}
