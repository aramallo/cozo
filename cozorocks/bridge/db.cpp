// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#include <iostream>
#include <memory>
#include <cstdlib>
#include <mutex>
#include "db.h"
#include "cozorocks/src/bridge/mod.rs.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/rate_limiter.h"

// ============================================================================
// Default values used when neither an OPTIONS file nor an env var is provided.
// These are baseline defaults only — they are NEVER applied unconditionally
// over an OPTIONS file.
//
// Precedence (highest wins):
//   1. Environment variables (COZO_ROCKSDB_*)
//   2. OPTIONS file (placed at <db_path>/options)
//   3. These defaults
// ============================================================================

static const size_t DEFAULT_BLOCK_CACHE_MB = 256;
static const int DEFAULT_MAX_OPEN_FILES = 1000;
static const size_t DEFAULT_WRITE_BUFFER_SIZE_MB = 16;
static const int DEFAULT_MAX_WRITE_BUFFER_NUMBER = 3;
static const size_t DEFAULT_DB_WRITE_BUFFER_SIZE_MB = 128;
static const size_t DEFAULT_SOFT_PENDING_COMPACTION_GB = 64;
static const size_t DEFAULT_HARD_PENDING_COMPACTION_GB = 256;
static const int DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER = 20;
static const int DEFAULT_LEVEL0_STOP_WRITES_TRIGGER = 36;
static const size_t DEFAULT_MAX_TOTAL_WAL_SIZE_MB = 1024;
static const size_t DEFAULT_RATE_LIMIT_MB_PER_SEC = 0;

// ============================================================================
// Compression type parser
// ============================================================================

static CompressionType parse_compression_type(const char* value) {
    std::string s(value);
    if (s == "none")   return kNoCompression;
    if (s == "snappy") return kSnappyCompression;
    if (s == "zlib")   return kZlibCompression;
    if (s == "lz4")    return kLZ4Compression;
    if (s == "lz4hc")  return kLZ4HCCompression;
    if (s == "zstd")   return kZSTD;
    // Unknown value — keep current setting
    return kLZ4Compression;
}

// ============================================================================
// Shared rate limiter (disabled — API changed between RocksDB versions)
// ============================================================================

static std::shared_ptr<RateLimiter> get_shared_rate_limiter() {
    return nullptr;
}

// ============================================================================
// Shared block cache — process-global, created once
// ============================================================================

static std::shared_ptr<Cache> shared_cache = nullptr;
static std::mutex shared_cache_mutex;
static size_t shared_cache_capacity_mb = DEFAULT_BLOCK_CACHE_MB;

static std::shared_ptr<Cache> get_shared_block_cache() {
    std::lock_guard<std::mutex> lock(shared_cache_mutex);
    if (shared_cache == nullptr) {
        size_t cache_size_mb = shared_cache_capacity_mb;
        const char* env_cache = std::getenv("COZO_ROCKSDB_BLOCK_CACHE_MB");
        if (env_cache != nullptr) {
            cache_size_mb = std::strtoul(env_cache, nullptr, 10);
            if (cache_size_mb == 0) cache_size_mb = DEFAULT_BLOCK_CACHE_MB;
            shared_cache_capacity_mb = cache_size_mb;
        }
        shared_cache = NewLRUCache(cache_size_mb * 1024 * 1024);
    }
    return shared_cache;
}

void clear_shared_block_cache() {
    std::lock_guard<std::mutex> lock(shared_cache_mutex);
    if (shared_cache != nullptr) {
        shared_cache->EraseUnRefEntries();
    }
}

void set_shared_block_cache_capacity(size_t capacity_mb) {
    std::lock_guard<std::mutex> lock(shared_cache_mutex);
    shared_cache_capacity_mb = capacity_mb;
    if (shared_cache != nullptr) {
        shared_cache->SetCapacity(capacity_mb * 1024 * 1024);
    }
}

std::unique_ptr<BlockCacheStats> get_shared_block_cache_stats() {
    auto stats = std::make_unique<BlockCacheStats>();
    stats->capacity = 0;
    stats->usage = 0;
    stats->pinned_usage = 0;
    std::lock_guard<std::mutex> lock(shared_cache_mutex);
    if (shared_cache != nullptr) {
        stats->capacity = shared_cache->GetCapacity();
        stats->usage = shared_cache->GetUsage();
        stats->pinned_usage = shared_cache->GetPinnedUsage();
    }
    return stats;
}

// ============================================================================
// Baseline defaults — used when no OPTIONS file is present.
// Every value here can be overridden by an env var in open_db().
// ============================================================================

Options default_db_options() {
    Options options = Options();
    options.compression = kLZ4Compression;
    options.bottommost_compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.max_background_jobs = 6;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = kMinOverlappingRatio;
    options.compaction_readahead_size = 2 * 1024 * 1024;  // 2MB

    options.write_buffer_size = DEFAULT_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
    options.max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;
    options.db_write_buffer_size = DEFAULT_DB_WRITE_BUFFER_SIZE_MB * 1024 * 1024;

    options.soft_pending_compaction_bytes_limit = DEFAULT_SOFT_PENDING_COMPACTION_GB * 1024ULL * 1024ULL * 1024ULL;
    options.hard_pending_compaction_bytes_limit = DEFAULT_HARD_PENDING_COMPACTION_GB * 1024ULL * 1024ULL * 1024ULL;
    options.level0_slowdown_writes_trigger = DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
    options.level0_stop_writes_trigger = DEFAULT_LEVEL0_STOP_WRITES_TRIGGER;

    options.max_total_wal_size = DEFAULT_MAX_TOTAL_WAL_SIZE_MB * 1024 * 1024;
    options.wal_bytes_per_sync = 1048576;  // 1MB

    options.max_open_files = DEFAULT_MAX_OPEN_FILES;

    auto rate_limiter = get_shared_rate_limiter();
    if (rate_limiter) {
        options.rate_limiter = rate_limiter;
    }

    BlockBasedTableOptions table_options;
    table_options.block_cache = get_shared_block_cache();
    table_options.block_size = 32 * 1024;  // 32KB
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 6;
    table_options.optimize_filters_for_memory = true;

    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    return options;
}

// ============================================================================
// open_db — opens a RocksDB TransactionDB
//
// Precedence (highest wins):
//   1. Environment variables (COZO_ROCKSDB_*)
//   2. OPTIONS file (placed at <db_path>/options)
//   3. default_db_options() baseline
// ============================================================================

shared_ptr <RocksDbBridge> open_db(const DbOpts &opts, RocksDbStatus &status) {
    // --- Step 1: Baseline defaults ----------------------------------------
    auto options = default_db_options();

    // --- Step 2: Load OPTIONS file if present (overrides defaults) --------
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

        // If the OPTIONS file specifies a block cache capacity and no env var
        // override is set, resize the shared cache to match the OPTIONS file.
        // Then replace all loaded CF caches with the shared process-global cache
        // (needed for clear_block_cache/set_block_cache_capacity/get_block_cache_stats).
        const char* env_cache_check = std::getenv("COZO_ROCKSDB_BLOCK_CACHE_MB");
        if (env_cache_check == nullptr && !loaded_cf_descs.empty()) {
            auto* first_bbt =
                    loaded_cf_descs[0].options.table_factory->GetOptions<BlockBasedTableOptions>();
            if (first_bbt != nullptr && first_bbt->block_cache != nullptr) {
                size_t loaded_capacity = first_bbt->block_cache->GetCapacity();
                if (loaded_capacity > 0) {
                    set_shared_block_cache_capacity(loaded_capacity / (1024 * 1024));
                }
            }
        }
        for (size_t i = 0; i < loaded_cf_descs.size(); ++i) {
            auto* loaded_bbt_opt =
                    loaded_cf_descs[i].options.table_factory->GetOptions<BlockBasedTableOptions>();
            if (loaded_bbt_opt != nullptr) {
                loaded_bbt_opt->block_cache = get_shared_block_cache();
            }
        }

        options = Options(loaded_db_opt, loaded_cf_descs[0].options);
    }

    // --- Step 3: Functional settings from Rust builder --------------------
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

    // --- Step 4: Environment variable overrides ---------------------------
    // These have the HIGHEST precedence. They override both the OPTIONS file
    // and the baseline defaults. Each override is conditional — it only
    // applies when the env var is explicitly set.

    // -- 4a. File and thread limits --

    const char* env_max_open_files = std::getenv("COZO_ROCKSDB_MAX_OPEN_FILES");
    if (env_max_open_files != nullptr) {
        options.max_open_files = std::atoi(env_max_open_files);
    }

    const char* env_max_background_jobs = std::getenv("COZO_ROCKSDB_MAX_BACKGROUND_JOBS");
    if (env_max_background_jobs != nullptr) {
        int val = std::atoi(env_max_background_jobs);
        if (val > 0) {
            options.max_background_jobs = val;
        }
    }

    // -- 4b. Write buffer (memtable) settings --

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

    // -- 4c. Compaction backpressure --

    const char* env_soft_pending = std::getenv("COZO_ROCKSDB_SOFT_PENDING_COMPACTION_GB");
    if (env_soft_pending != nullptr) {
        size_t size_gb = std::strtoul(env_soft_pending, nullptr, 10);
        if (size_gb > 0) {
            options.soft_pending_compaction_bytes_limit = size_gb * 1024ULL * 1024ULL * 1024ULL;
        }
    }

    const char* env_hard_pending = std::getenv("COZO_ROCKSDB_HARD_PENDING_COMPACTION_GB");
    if (env_hard_pending != nullptr) {
        size_t size_gb = std::strtoul(env_hard_pending, nullptr, 10);
        if (size_gb > 0) {
            options.hard_pending_compaction_bytes_limit = size_gb * 1024ULL * 1024ULL * 1024ULL;
        }
    }

    // -- 4d. L0 compaction triggers --

    const char* env_l0_compaction_trigger = std::getenv("COZO_ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER");
    if (env_l0_compaction_trigger != nullptr) {
        int val = std::atoi(env_l0_compaction_trigger);
        if (val > 0) {
            options.level0_file_num_compaction_trigger = val;
        }
    }

    const char* env_l0_slowdown = std::getenv("COZO_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER");
    if (env_l0_slowdown != nullptr) {
        int val = std::atoi(env_l0_slowdown);
        if (val > 0) {
            options.level0_slowdown_writes_trigger = val;
        }
    }

    const char* env_l0_stop = std::getenv("COZO_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER");
    if (env_l0_stop != nullptr) {
        int val = std::atoi(env_l0_stop);
        if (val > 0) {
            options.level0_stop_writes_trigger = val;
        }
    }

    // -- 4e. Level sizing --

    const char* env_target_file_size_base = std::getenv("COZO_ROCKSDB_TARGET_FILE_SIZE_BASE_MB");
    if (env_target_file_size_base != nullptr) {
        size_t size_mb = std::strtoul(env_target_file_size_base, nullptr, 10);
        if (size_mb > 0) {
            options.target_file_size_base = size_mb * 1024 * 1024;
        }
    }

    const char* env_max_bytes_for_level_base = std::getenv("COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE_MB");
    if (env_max_bytes_for_level_base != nullptr) {
        size_t size_mb = std::strtoul(env_max_bytes_for_level_base, nullptr, 10);
        if (size_mb > 0) {
            options.max_bytes_for_level_base = size_mb * 1024 * 1024;
        }
    }

    // -- 4f. Compression --

    const char* env_compression = std::getenv("COZO_ROCKSDB_COMPRESSION_TYPE");
    if (env_compression != nullptr) {
        options.compression = parse_compression_type(env_compression);
    }

    const char* env_bottommost_compression = std::getenv("COZO_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE");
    if (env_bottommost_compression != nullptr) {
        options.bottommost_compression = parse_compression_type(env_bottommost_compression);
    }

    // -- 4g. WAL --

    const char* env_max_wal_size = std::getenv("COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE_MB");
    if (env_max_wal_size != nullptr) {
        size_t size_mb = std::strtoul(env_max_wal_size, nullptr, 10);
        if (size_mb > 0) {
            options.max_total_wal_size = size_mb * 1024 * 1024;
        }
    }

    // -- 4h. I/O tuning --

    const char* env_bytes_per_sync = std::getenv("COZO_ROCKSDB_BYTES_PER_SYNC");
    if (env_bytes_per_sync != nullptr) {
        size_t val = std::strtoul(env_bytes_per_sync, nullptr, 10);
        options.bytes_per_sync = val;
    }

    const char* env_wal_bytes_per_sync = std::getenv("COZO_ROCKSDB_WAL_BYTES_PER_SYNC");
    if (env_wal_bytes_per_sync != nullptr) {
        size_t val = std::strtoul(env_wal_bytes_per_sync, nullptr, 10);
        options.wal_bytes_per_sync = val;
    }

    const char* env_compaction_readahead = std::getenv("COZO_ROCKSDB_COMPACTION_READAHEAD_SIZE");
    if (env_compaction_readahead != nullptr) {
        size_t val = std::strtoul(env_compaction_readahead, nullptr, 10);
        options.compaction_readahead_size = val;
    }

    // --- Step 5: Blob files (from Rust builder) ---------------------------
    if (opts.enable_blob_files) {
        options.enable_blob_files = true;
        options.min_blob_size = opts.min_blob_size;
        options.blob_file_size = opts.blob_file_size;
        options.enable_blob_garbage_collection = opts.enable_blob_garbage_collection;
    }

    // --- Step 6: Bloom filter ---------------------------------------------
    // IMPORTANT: Preserve existing BlockBasedTableOptions (from OPTIONS file
    // or defaults). Only set the filter policy — do NOT rebuild from scratch.
    if (opts.use_bloom_filter) {
        auto* existing_bbt = options.table_factory->GetOptions<BlockBasedTableOptions>();
        BlockBasedTableOptions table_options;
        if (existing_bbt != nullptr) {
            // Copy all existing settings (block_size, partition_filters,
            // metadata_block_size, cache_index_and_filter_blocks, etc.)
            table_options = *existing_bbt;
        }
        // Ensure shared block cache
        table_options.block_cache = get_shared_block_cache();
        // Set bloom filter policy
        table_options.filter_policy.reset(NewBloomFilterPolicy(opts.bloom_filter_bits_per_key, false));
        table_options.whole_key_filtering = opts.bloom_filter_whole_key_filtering;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }

    // --- Step 7: Prefix extractors (from Rust builder) --------------------
    if (opts.use_capped_prefix_extractor) {
        options.prefix_extractor.reset(NewCappedPrefixTransform(opts.capped_prefix_extractor_len));
    }
    if (opts.use_fixed_prefix_extractor) {
        options.prefix_extractor.reset(NewFixedPrefixTransform(opts.fixed_prefix_extractor_len));
    }

    // --- Step 8: Table-level env var overrides ----------------------------
    // These come AFTER the bloom filter so they take final precedence over
    // everything (defaults, OPTIONS file, and bloom filter setup).
    const char* env_block_size = std::getenv("COZO_ROCKSDB_BLOCK_SIZE");
    if (env_block_size != nullptr) {
        size_t block_size = std::strtoul(env_block_size, nullptr, 10);
        if (block_size > 0) {
            auto* bbt = options.table_factory->GetOptions<BlockBasedTableOptions>();
            if (bbt != nullptr) {
                BlockBasedTableOptions new_bbt = *bbt;
                new_bbt.block_size = block_size;
                options.table_factory.reset(NewBlockBasedTableFactory(new_bbt));
            }
        }
    }

    // --- Step 9: Open the database ----------------------------------------
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
