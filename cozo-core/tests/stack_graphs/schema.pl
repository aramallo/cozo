{:create sg_graphs {
    file: String
    =>
    uncompressed_value_len: Int,
    value: Bytes
}}

{:create sg_node_paths {
    file: String,
    start_local_id: Int,
    discriminator: Int # Used to make primary key unique (`file` & `start_local_id` need not be)
    =>
    uncompressed_value_len: Int,
    value: Bytes
}}

{:create sg_root_paths {
    file: String,
    symbol_stack: String,
    discriminator: Int # Used to make primary key unique (`file` & `symbol_stack` need not be)
    =>
    uncompressed_value_len: Int,
    value: Bytes
}}
