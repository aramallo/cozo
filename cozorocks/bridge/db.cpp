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

// Default block cache size: 256MB (reduced from 2GB for better memory control)
// Can be overridden via COZO_ROCKSDB_BLOCK_CACHE_MB environment variable
static const size_t DEFAULT_BLOCK_CACHE_MB = 256;

// Default max open files (reduced from 5000 for better memory control)
// Can be overridden via COZO_ROCKSDB_MAX_OPEN_FILES environment variable
static const int DEFAULT_MAX_OPEN_FILES = 1000;

// Write buffer (memtable) settings - critical for controlling memory under write-heavy workloads
// Default write buffer size: 16MB per memtable (reduced from 32MB for better memory control)
// Can be overridden via COZO_ROCKSDB_WRITE_BUFFER_SIZE_MB environment variable
static const size_t DEFAULT_WRITE_BUFFER_SIZE_MB = 16;

// Maximum number of memtables (active + immutable) before stalling writes
// Can be overridden via COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER environment variable
static const int DEFAULT_MAX_WRITE_BUFFER_NUMBER = 3;

// Total memory budget for all memtables across the database (0 = unlimited)
// Reduced from 256MB to 128MB for better memory control
// Can be overridden via COZO_ROCKSDB_DB_WRITE_BUFFER_SIZE_MB environment variable
static const size_t DEFAULT_DB_WRITE_BUFFER_SIZE_MB = 128;

// Compaction backpressure settings - prevents runaway memory growth under heavy writes
// Soft limit: writes slow down when pending compaction bytes exceed this (default 64GB)
// Can be overridden via COZO_ROCKSDB_SOFT_PENDING_COMPACTION_GB environment variable
static const size_t DEFAULT_SOFT_PENDING_COMPACTION_GB = 64;

// Hard limit: writes stop when pending compaction bytes exceed this (default 256GB)
// Can be overridden via COZO_ROCKSDB_HARD_PENDING_COMPACTION_GB environment variable
static const size_t DEFAULT_HARD_PENDING_COMPACTION_GB = 256;

// L0 file count triggers - controls write stalls based on L0 file accumulation
// Slowdown trigger: start slowing writes (default 20 files)
static const int DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER = 20;
// Stop trigger: stop writes entirely (default 36 files)
static const int DEFAULT_LEVEL0_STOP_WRITES_TRIGGER = 36;

// WAL size limit - triggers memtable flush when total WAL size exceeds this (default 1GB)
// Can be overridden via COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE_MB environment variable
static const size_t DEFAULT_MAX_TOTAL_WAL_SIZE_MB = 1024;

// Rate limiter for compaction/flush I/O (0 = disabled, value in MB/s)
// Can be overridden via COZO_ROCKSDB_RATE_LIMIT_MB_PER_SEC environment variable
static const size_t DEFAULT_RATE_LIMIT_MB_PER_SEC = 0;

// Shared rate limiter - created once if enabled, used by all database instances
// Note: Rate limiter disabled - API has changed between RocksDB versions
static std::shared_ptr<RateLimiter> get_shared_rate_limiter() {
    return nullptr;
}

// Shared block cache - created once, used by all database instances
// This is a process-global cache that persists until explicitly cleared or reset
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

// Clear all entries from the shared block cache (releases memory but keeps cache structure)
void clear_shared_block_cache() {
    std::lock_guard<std::mutex> lock(shared_cache_mutex);
    if (shared_cache != nullptr) {
        // EraseUnRefEntries removes all entries not currently in use
        shared_cache->EraseUnRefEntries();
    }
}

// Set the capacity of the shared block cache in MB
// Setting to 0 effectively disables caching (but doesn't release the cache object)
void set_shared_block_cache_capacity(size_t capacity_mb) {
    std::lock_guard<std::mutex> lock(shared_cache_mutex);
    shared_cache_capacity_mb = capacity_mb;
    if (shared_cache != nullptr) {
        shared_cache->SetCapacity(capacity_mb * 1024 * 1024);
    }
}

// Get shared block cache statistics
// Returns: capacity, usage, pinned_usage (all in bytes)
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

Options default_db_options() {
    Options options = Options();
    // Use LZ4 for all levels - good balance of speed and compression for range queries
    options.compression = kLZ4Compression;
    options.bottommost_compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.max_background_jobs = 6;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = kMinOverlappingRatio;
    // Readahead for compaction - improves I/O efficiency on HDDs and network storage
    options.compaction_readahead_size = 2 * 1024 * 1024;  // 2MB

    // Write buffer settings for memory control
    options.write_buffer_size = DEFAULT_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
    options.max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;
    options.db_write_buffer_size = DEFAULT_DB_WRITE_BUFFER_SIZE_MB * 1024 * 1024;

    // Compaction backpressure - prevents runaway memory growth
    options.soft_pending_compaction_bytes_limit = DEFAULT_SOFT_PENDING_COMPACTION_GB * 1024ULL * 1024ULL * 1024ULL;
    options.hard_pending_compaction_bytes_limit = DEFAULT_HARD_PENDING_COMPACTION_GB * 1024ULL * 1024ULL * 1024ULL;
    options.level0_slowdown_writes_trigger = DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
    options.level0_stop_writes_trigger = DEFAULT_LEVEL0_STOP_WRITES_TRIGGER;

    // WAL size limit - triggers flush to prevent unbounded WAL growth
    options.max_total_wal_size = DEFAULT_MAX_TOTAL_WAL_SIZE_MB * 1024 * 1024;
    options.wal_bytes_per_sync = 1048576;  // 1MB - periodic WAL sync for durability

    // Rate limiter (if enabled via environment variable)
    auto rate_limiter = get_shared_rate_limiter();
    if (rate_limiter) {
        options.rate_limiter = rate_limiter;
    }

    BlockBasedTableOptions table_options;
    table_options.block_cache = get_shared_block_cache();
    table_options.block_size = 32 * 1024;  // 32KB - optimized for range queries
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 6;
    // Use Ribbon filters instead of Bloom - more memory efficient with similar performance
    table_options.optimize_filters_for_memory = true;

    auto table_factory = NewBlockBasedTableFactory(table_options);
    options.table_factory.reset(table_factory);

    return options;
}

ColumnFamilyOptions default_cf_options() {
    ColumnFamilyOptions options = ColumnFamilyOptions();
    // Use LZ4 for all levels - good balance of speed and compression for range queries
    options.compression = kLZ4Compression;
    options.bottommost_compression = kLZ4Compression;
    options.level_compaction_dynamic_level_bytes = true;
    options.compaction_pri = kMinOverlappingRatio;

    // Write buffer settings for memory control (per column family)
    options.write_buffer_size = DEFAULT_WRITE_BUFFER_SIZE_MB * 1024 * 1024;
    options.max_write_buffer_number = DEFAULT_MAX_WRITE_BUFFER_NUMBER;

    // Compaction backpressure (per column family)
    options.soft_pending_compaction_bytes_limit = DEFAULT_SOFT_PENDING_COMPACTION_GB * 1024ULL * 1024ULL * 1024ULL;
    options.hard_pending_compaction_bytes_limit = DEFAULT_HARD_PENDING_COMPACTION_GB * 1024ULL * 1024ULL * 1024ULL;
    options.level0_slowdown_writes_trigger = DEFAULT_LEVEL0_SLOWDOWN_WRITES_TRIGGER;
    options.level0_stop_writes_trigger = DEFAULT_LEVEL0_STOP_WRITES_TRIGGER;

    BlockBasedTableOptions table_options;
    table_options.block_cache = get_shared_block_cache();
    table_options.block_size = 32 * 1024;  // 32KB - optimized for range queries
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.format_version = 6;
    // Use Ribbon filters instead of Bloom - more memory efficient with similar performance
    table_options.optimize_filters_for_memory = true;

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

    // Compaction backpressure overrides
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

    // WAL size limit override
    const char* env_max_wal_size = std::getenv("COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE_MB");
    if (env_max_wal_size != nullptr) {
        size_t size_mb = std::strtoul(env_max_wal_size, nullptr, 10);
        if (size_mb > 0) {
            options.max_total_wal_size = size_mb * 1024 * 1024;
        }
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
        table_options.format_version = 6;
        table_options.optimize_filters_for_memory = true;
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
