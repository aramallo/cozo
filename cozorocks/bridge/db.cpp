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

// Default block cache size: 512 MB
// Can be overridden via COZO_ROCKSDB_BLOCK_CACHE_MB environment variable
static const size_t DEFAULT_BLOCK_CACHE_MB = 2048;

// Default max open files (256 is reasonable for most workloads)
// Can be overridden via COZO_ROCKSDB_MAX_OPEN_FILES environment variable
static const int DEFAULT_MAX_OPEN_FILES = 5000;

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
