// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#include <iostream>
#include <memory>
#include <cstdlib>
#include "db.h"
#include "cozorocks/src/bridge/mod.rs.h"
#include "rocksdb/utilities/options_util.h"

// Default block cache size: 2GB
// Can be overridden via COZO_ROCKSDB_BLOCK_CACHE_MB environment variable
static const size_t DEFAULT_BLOCK_CACHE_MB = 2048;

// Default max open files
// Can be overridden via COZO_ROCKSDB_MAX_OPEN_FILES environment variable
static const int DEFAULT_MAX_OPEN_FILES = 5000;

// Write buffer (memtable) settings - critical for controlling memory under write-heavy workloads
// Default write buffer size: 32MB per memtable (RocksDB default is 64MB)
// Can be overridden via COZO_ROCKSDB_WRITE_BUFFER_SIZE_MB environment variable
static const size_t DEFAULT_WRITE_BUFFER_SIZE_MB = 32;

// Maximum number of memtables (active + immutable) before stalling writes
// Can be overridden via COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER environment variable
static const int DEFAULT_MAX_WRITE_BUFFER_NUMBER = 3;

// Total memory budget for all memtables across the database (0 = unlimited)
// Can be overridden via COZO_ROCKSDB_DB_WRITE_BUFFER_SIZE_MB environment variable
static const size_t DEFAULT_DB_WRITE_BUFFER_SIZE_MB = 256;

// Shared block cache - created once, used by all database instances
static std::shared_ptr<Cache> get_shared_block_cache() {
    static std::shared_ptr<Cache> shared_cache = nullptr;
    if (shared_cache == nullptr) {
        size_t cache_size_mb = DEFAULT_BLOCK_CACHE_MB;
        const char* env_cache = std::getenv("COZO_ROCKSDB_BLOCK_CACHE_MB");
        if (env_cache != nullptr) {
            cache_size_mb = std::strtoul(env_cache, nullptr, 10);
            if (cache_size_mb == 0) cache_size_mb = DEFAULT_BLOCK_CACHE_MB;
        }
        shared_cache = NewLRUCache(cache_size_mb * 1024 * 1024);
    }
    return shared_cache;
}

Options default_db_options() {
    Options options = Options();
    options.bottommost_compression = kZSTD;
    options.compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.max_background_jobs = 6;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = kMinOverlappingRatio;

    // Write buffer settings for memory control
    options.write_buffer_size = DEFAULT_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
    options.max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;
    options.db_write_buffer_size = DEFAULT_DB_WRITE_BUFFER_SIZE_MB * 1024 * 1024;

    BlockBasedTableOptions table_options;
    table_options.block_cache = get_shared_block_cache();
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 5;

    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    return options;
}

ColumnFamilyOptions default_cf_options() {
    ColumnFamilyOptions options = ColumnFamilyOptions();
    options.bottommost_compression = kZSTD;
    options.compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.compaction_pri = kMinOverlappingRatio;

    // Write buffer settings for memory control (per column family)
    options.write_buffer_size = DEFAULT_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
    options.max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;

    BlockBasedTableOptions table_options;
    table_options.block_cache = get_shared_block_cache();
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 5;

    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    return options;
}

shared_ptr <RocksDbBridge> open_db(const DbOpts &opts, RocksDbStatus &status) {
    auto options = default_db_options();

    if (!opts.options_path.empty()) {
        DBOptions loaded_db_opt;
        std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
        ConfigOptions config_options;
        string options_path = convert_vec_to_string(opts.options_path);
        Status s = LoadOptionsFromFile(config_options, options_path, &loaded_db_opt,
                                       &loaded_cf_descs);
        if (!s.ok()) {
            write_status(s, status);
            return nullptr;
        }

        // Ensure loaded options use the shared block cache
        for (size_t i = 0; i < loaded_cf_descs.size(); ++i) {
            auto* loaded_bbt_opt =
                    loaded_cf_descs[i].options.table_factory->GetOptions<BlockBasedTableOptions>();
            loaded_bbt_opt->block_cache = get_shared_block_cache();
        }

        options = Options(loaded_db_opt, loaded_cf_descs[0].options);
    }

    if (opts.prepare_for_bulk_load) {
        options.PrepareForBulkLoad();
    }
    if (opts.increase_parallelism > 0) {
        options.IncreaseParallelism(opts.increase_parallelism);
    }
    if (opts.optimize_level_style_compaction) {
        options.OptimizeLevelStyleCompaction();
    }
    options.create_if_missing = opts.create_if_missing;
    options.paranoid_checks = opts.paranoid_checks;

    // Limit max open files to control memory usage
    const char* env_max_files = std::getenv("COZO_ROCKSDB_MAX_OPEN_FILES");
    if (env_max_files != nullptr) {
        options.max_open_files = std::atoi(env_max_files);
    } else {
        options.max_open_files = DEFAULT_MAX_OPEN_FILES;
    }

    // Write buffer (memtable) settings - override via environment variables
    const char* env_write_buffer_size = std::getenv("COZO_ROCKSDB_WRITE_BUFFER_SIZE_MB");
    if (env_write_buffer_size != nullptr) {
        size_t size_mb = std::strtoul(env_write_buffer_size, nullptr, 10);
        if (size_mb > 0) {
            options.write_buffer_size = size_mb * 1024 * 1024;
        }
    }

    const char* env_max_write_buffer_number = std::getenv("COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER");
    if (env_max_write_buffer_number != nullptr) {
        int num = std::atoi(env_max_write_buffer_number);
        if (num > 0) {
            options.max_write_buffer_number = num;
        }
    }

    const char* env_db_write_buffer_size = std::getenv("COZO_ROCKSDB_DB_WRITE_BUFFER_SIZE_MB");
    if (env_db_write_buffer_size != nullptr) {
        size_t size_mb = std::strtoul(env_db_write_buffer_size, nullptr, 10);
        // 0 means unlimited, which is a valid value
        options.db_write_buffer_size = size_mb * 1024 * 1024;
    }

    if (opts.enable_blob_files) {
        options.enable_blob_files = true;

        options.min_blob_size = opts.min_blob_size;

        options.blob_file_size = opts.blob_file_size;

        options.enable_blob_garbage_collection = opts.enable_blob_garbage_collection;
    }
    if (opts.use_bloom_filter) {
        BlockBasedTableOptions table_options;
        table_options.block_cache = get_shared_block_cache();
        table_options.filter_policy.reset(NewBloomFilterPolicy(opts.bloom_filter_bits_per_key, false));
        table_options.whole_key_filtering = opts.bloom_filter_whole_key_filtering;
        table_options.cache_index_and_filter_blocks = true;
        table_options.pin_l0_filter_and_index_blocks_in_cache = true;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }
    if (opts.use_capped_prefix_extractor) {
        options.prefix_extractor.reset(NewCappedPrefixTransform(opts.capped_prefix_extractor_len));
    }
    if (opts.use_fixed_prefix_extractor) {
        options.prefix_extractor.reset(NewFixedPrefixTransform(opts.fixed_prefix_extractor_len));
    }
    options.create_missing_column_families = true;

    shared_ptr <RocksDbBridge> db = make_shared<RocksDbBridge>();

    db->db_path = convert_vec_to_string(opts.db_path);

    TransactionDB *txn_db = nullptr;
    write_status(
            TransactionDB::Open(options, TransactionDBOptions(), db->db_path, &txn_db),
            status);
    db->db.reset(txn_db);
    db->destroy_on_exit = opts.destroy_on_exit;


    return db;
}

RocksDbBridge::~RocksDbBridge() {
    if (destroy_on_exit && (db != nullptr)) {
        cerr << "destroying database on exit: " << db_path << endl;
        auto status = db->Close();
        if (!status.ok()) {
            cerr << status.ToString() << endl;
        }
        db.reset();
        Options options{};
        auto status2 = DestroyDB(db_path, options);
        if (!status2.ok()) {
            cerr << status2.ToString() << endl;
        }
    }
}
