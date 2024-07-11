{:create sg {
    file: String
    =>
    size: Int,
    value: Bytes
}}

{:create sg_file_path {
    file: String,
    local_id: Int,
    discriminator: Int # Used to make primary key unique (`file` & `local_id` need not be)
    =>
    size: Int,
    value: Bytes
}}

{:create sg_root_path {
    file: String,
    symbol_stack: String,
    discriminator: Int # Used to make primary key unique (`file` & `symbol_stack` need not be)
    =>
    size: Int,
    value: Bytes
}}
