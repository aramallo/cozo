/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! RocksDB storage backend using the official rust-rocksdb crate.
//!
//! This backend supports comprehensive configuration via environment variables.
//! All options are prefixed with `COZO_ROCKSDB_`.
//!
//! # Environment Variables
//!
//! ## General Options
//! - `COZO_ROCKSDB_CREATE_IF_MISSING` - Create database if it doesn't exist (default: true for new dbs)
//! - `COZO_ROCKSDB_CREATE_MISSING_COLUMN_FAMILIES` - Create missing column families (default: false)
//! - `COZO_ROCKSDB_ERROR_IF_EXISTS` - Error if database already exists (default: false)
//! - `COZO_ROCKSDB_PARANOID_CHECKS` - Enable aggressive data validation (default: false)
//! - `COZO_ROCKSDB_MAX_OPEN_FILES` - Maximum number of open files (default: -1, unlimited)
//! - `COZO_ROCKSDB_MAX_FILE_OPENING_THREADS` - Threads for opening files (default: 16)
//! - `COZO_ROCKSDB_NUM_LEVELS` - Number of LSM levels (default: 7)
//!
//! ## Parallelism & Background Jobs
//! - `COZO_ROCKSDB_INCREASE_PARALLELISM` - Set total background threads (default: num_cpus)
//! - `COZO_ROCKSDB_MAX_BACKGROUND_JOBS` - Maximum background jobs (default: 2)
//! - `COZO_ROCKSDB_MAX_SUBCOMPACTIONS` - Parallel compaction threads (default: 1)
//!
//! ## Write Buffer (Memtable)
//! - `COZO_ROCKSDB_WRITE_BUFFER_SIZE` - Size of single memtable in bytes (default: 64MB)
//! - `COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` - Maximum number of memtables (default: 2)
//! - `COZO_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE` - Minimum memtables to merge (default: 1)
//! - `COZO_ROCKSDB_DB_WRITE_BUFFER_SIZE` - Total write buffer size across column families (default: 0, disabled)
//!
//! ## Compaction
//! - `COZO_ROCKSDB_COMPACTION_STYLE` - Compaction style: level, universal, fifo (default: level)
//! - `COZO_ROCKSDB_DISABLE_AUTO_COMPACTIONS` - Disable automatic compaction (default: false)
//! - `COZO_ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER` - L0 files to trigger compaction (default: 4)
//! - `COZO_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER` - L0 files to slow down writes (default: 20)
//! - `COZO_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER` - L0 files to stop writes (default: 36)
//! - `COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE` - Max bytes for level 1 (default: 256MB)
//! - `COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_MULTIPLIER` - Level size multiplier (default: 10.0)
//! - `COZO_ROCKSDB_TARGET_FILE_SIZE_BASE` - Target file size for level 1 (default: 64MB)
//! - `COZO_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER` - File size multiplier per level (default: 1)
//! - `COZO_ROCKSDB_MAX_COMPACTION_BYTES` - Max bytes per compaction (default: 0, disabled)
//! - `COZO_ROCKSDB_COMPACTION_READAHEAD_SIZE` - Readahead for compaction (default: 0)
//! - `COZO_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES` - Dynamic level sizing (default: false)
//! - `COZO_ROCKSDB_PERIODIC_COMPACTION_SECONDS` - Periodic recompaction interval (default: 0, disabled)
//!
//! ## Compression
//! - `COZO_ROCKSDB_COMPRESSION_TYPE` - Compression: none, snappy, zlib, bz2, lz4, lz4hc, zstd (default: lz4)
//! - `COZO_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE` - Bottom level compression (default: zstd)
//! - `COZO_ROCKSDB_COMPRESSION_LEVEL` - Compression level (default: depends on algorithm)
//! - `COZO_ROCKSDB_ZSTD_MAX_TRAIN_BYTES` - Zstd dictionary training bytes (default: 0)
//!
//! ## Block-Based Table Options
//! - `COZO_ROCKSDB_BLOCK_SIZE` - Block size in bytes (default: 4KB)
//! - `COZO_ROCKSDB_BLOCK_CACHE_SIZE` - Block cache size in bytes (default: 8MB)
//! - `COZO_ROCKSDB_DISABLE_BLOCK_CACHE` - Disable block cache (default: false)
//! - `COZO_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY` - Bloom filter bits per key (default: 10.0)
//! - `COZO_ROCKSDB_BLOOM_FILTER_BLOCK_BASED` - Use block-based bloom filter (default: false)
//! - `COZO_ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS` - Cache index/filter in block cache (default: true)
//! - `COZO_ROCKSDB_PIN_L0_FILTER_AND_INDEX_BLOCKS` - Pin L0 index/filter in cache (default: true)
//! - `COZO_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS` - Optimize bloom for read hits (default: false)
//! - `COZO_ROCKSDB_WHOLE_KEY_FILTERING` - Enable whole key filtering (default: true)
//! - `COZO_ROCKSDB_FORMAT_VERSION` - Table format version (default: 5)
//!
//! ## Prefix Extractor
//! - `COZO_ROCKSDB_PREFIX_EXTRACTOR_LENGTH` - Fixed prefix length for prefix bloom (default: 9)
//!
//! ## Blob Storage
//! - `COZO_ROCKSDB_ENABLE_BLOB_FILES` - Enable blob storage (default: false)
//! - `COZO_ROCKSDB_MIN_BLOB_SIZE` - Minimum size to store as blob (default: 0)
//! - `COZO_ROCKSDB_BLOB_FILE_SIZE` - Target blob file size (default: 256MB)
//! - `COZO_ROCKSDB_BLOB_COMPRESSION_TYPE` - Blob compression type (default: none)
//! - `COZO_ROCKSDB_ENABLE_BLOB_GC` - Enable blob garbage collection (default: true)
//! - `COZO_ROCKSDB_BLOB_GC_AGE_CUTOFF` - Blob GC age cutoff (default: 0.25)
//! - `COZO_ROCKSDB_BLOB_GC_FORCE_THRESHOLD` - Blob GC force threshold (default: 1.0)
//!
//! ## WAL (Write-Ahead Log)
//! - `COZO_ROCKSDB_WAL_DIR` - WAL directory path (default: same as db)
//! - `COZO_ROCKSDB_WAL_TTL_SECONDS` - WAL file TTL (default: 0, disabled)
//! - `COZO_ROCKSDB_WAL_SIZE_LIMIT_MB` - WAL size limit (default: 0, disabled)
//! - `COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE` - Max total WAL size (default: 0, auto)
//! - `COZO_ROCKSDB_WAL_BYTES_PER_SYNC` - WAL sync granularity (default: 0)
//! - `COZO_ROCKSDB_MANUAL_WAL_FLUSH` - Manual WAL flushing (default: false)
//!
//! ## I/O Options
//! - `COZO_ROCKSDB_USE_FSYNC` - Use fsync instead of fdatasync (default: false)
//! - `COZO_ROCKSDB_USE_DIRECT_READS` - Direct I/O for reads (default: false)
//! - `COZO_ROCKSDB_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION` - Direct I/O for writes (default: false)
//! - `COZO_ROCKSDB_ALLOW_MMAP_READS` - Memory-map file reading (default: false)
//! - `COZO_ROCKSDB_ALLOW_MMAP_WRITES` - Memory-map file writing (default: false)
//! - `COZO_ROCKSDB_BYTES_PER_SYNC` - Data file sync granularity (default: 0)
//! - `COZO_ROCKSDB_WRITABLE_FILE_MAX_BUFFER_SIZE` - Write buffer size (default: 1MB)
//!
//! ## Concurrency
//! - `COZO_ROCKSDB_ALLOW_CONCURRENT_MEMTABLE_WRITE` - Parallel memtable writes (default: true)
//! - `COZO_ROCKSDB_ENABLE_WRITE_THREAD_ADAPTIVE_YIELD` - Write thread yielding (default: true)
//! - `COZO_ROCKSDB_ENABLE_PIPELINED_WRITE` - Pipelined write queues (default: false)
//! - `COZO_ROCKSDB_UNORDERED_WRITE` - Unordered write mode (default: false)
//!
//! ## Statistics & Logging
//! - `COZO_ROCKSDB_ENABLE_STATISTICS` - Enable statistics collection (default: false)
//! - `COZO_ROCKSDB_STATS_DUMP_PERIOD_SEC` - Stats dump frequency (default: 600)
//! - `COZO_ROCKSDB_LOG_LEVEL` - Log level: debug, info, warn, error, fatal, header (default: info)
//! - `COZO_ROCKSDB_MAX_LOG_FILE_SIZE` - Max log file size (default: 0, unlimited)
//! - `COZO_ROCKSDB_KEEP_LOG_FILE_NUM` - Log files to keep (default: 1000)
//!
//! ## Optimization Presets
//! - `COZO_ROCKSDB_OPTIMIZE_LEVEL_STYLE_COMPACTION` - Optimize for leveled compaction with memtable budget (bytes)
//! - `COZO_ROCKSDB_OPTIMIZE_UNIVERSAL_STYLE_COMPACTION` - Optimize for universal compaction with memtable budget (bytes)
//! - `COZO_ROCKSDB_OPTIMIZE_FOR_POINT_LOOKUP` - Optimize for point lookups with block cache size (bytes)
//! - `COZO_ROCKSDB_PREPARE_FOR_BULK_LOAD` - Prepare for bulk loading (default: false)

use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use log::info;
use miette::{miette, IntoDiagnostic, Result, WrapErr};

use rocksdb::{
    BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType,
    OptimisticTransactionDB, Options, SliceTransform, WriteBatchWithTransaction,
};

use crate::data::tuple::{check_key_for_validity, Tuple};
use crate::data::value::ValidityTs;
use crate::runtime::db::{BadDbInit, DbManifest};
use crate::runtime::relation::{decode_tuple_from_kv, extend_tuple_from_v};
use crate::storage::{Storage, StoreTx};
use crate::Db;

const KEY_PREFIX_LEN: usize = 9;
const CURRENT_STORAGE_VERSION: u64 = 3;

// =============================================================================
// Environment Variable Helpers
// =============================================================================

fn env_var<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_var_opt<T: std::str::FromStr>(name: &str) -> Option<T> {
    env::var(name).ok().and_then(|v| v.parse().ok())
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|v| v == "true" || v == "1" || v == "yes")
        .unwrap_or(default)
}

fn env_string(name: &str) -> Option<String> {
    env::var(name).ok().filter(|s| !s.is_empty())
}

fn parse_compression_type(s: &str) -> DBCompressionType {
    match s.to_lowercase().as_str() {
        "none" => DBCompressionType::None,
        "snappy" => DBCompressionType::Snappy,
        "zlib" => DBCompressionType::Zlib,
        "bz2" => DBCompressionType::Bz2,
        "lz4" => DBCompressionType::Lz4,
        "lz4hc" => DBCompressionType::Lz4hc,
        "zstd" => DBCompressionType::Zstd,
        _ => DBCompressionType::Lz4,
    }
}

fn parse_compaction_style(s: &str) -> DBCompactionStyle {
    match s.to_lowercase().as_str() {
        "level" => DBCompactionStyle::Level,
        "universal" => DBCompactionStyle::Universal,
        "fifo" => DBCompactionStyle::Fifo,
        _ => DBCompactionStyle::Level,
    }
}

// =============================================================================
// Configuration
// =============================================================================

/// Builds RocksDB Options from environment variables
fn build_options(is_new: bool) -> Options {
    let mut options = Options::default();

    // === General Options ===
    options.create_if_missing(env_bool("COZO_ROCKSDB_CREATE_IF_MISSING", is_new));
    options.create_missing_column_families(env_bool("COZO_ROCKSDB_CREATE_MISSING_COLUMN_FAMILIES", false));
    options.set_error_if_exists(env_bool("COZO_ROCKSDB_ERROR_IF_EXISTS", false));
    options.set_paranoid_checks(env_bool("COZO_ROCKSDB_PARANOID_CHECKS", false));

    if let Some(max_open_files) = env_var_opt::<i32>("COZO_ROCKSDB_MAX_OPEN_FILES") {
        options.set_max_open_files(max_open_files);
    }
    if let Some(threads) = env_var_opt::<i32>("COZO_ROCKSDB_MAX_FILE_OPENING_THREADS") {
        options.set_max_file_opening_threads(threads);
    }
    if let Some(levels) = env_var_opt::<i32>("COZO_ROCKSDB_NUM_LEVELS") {
        options.set_num_levels(levels);
    }

    // === Optimization Presets (apply before other settings so they can be overridden) ===
    if let Some(budget) = env_var_opt::<usize>("COZO_ROCKSDB_OPTIMIZE_LEVEL_STYLE_COMPACTION") {
        options.optimize_level_style_compaction(budget);
    }
    if let Some(budget) = env_var_opt::<usize>("COZO_ROCKSDB_OPTIMIZE_UNIVERSAL_STYLE_COMPACTION") {
        options.optimize_universal_style_compaction(budget);
    }
    if let Some(cache_size) = env_var_opt::<u64>("COZO_ROCKSDB_OPTIMIZE_FOR_POINT_LOOKUP") {
        options.optimize_for_point_lookup(cache_size);
    }
    if env_bool("COZO_ROCKSDB_PREPARE_FOR_BULK_LOAD", false) {
        options.prepare_for_bulk_load();
    }

    // === Parallelism & Background Jobs ===
    if let Some(parallelism) = env_var_opt::<i32>("COZO_ROCKSDB_INCREASE_PARALLELISM") {
        options.increase_parallelism(parallelism);
    }
    if let Some(jobs) = env_var_opt::<i32>("COZO_ROCKSDB_MAX_BACKGROUND_JOBS") {
        options.set_max_background_jobs(jobs);
    }
    if let Some(subcompactions) = env_var_opt::<u32>("COZO_ROCKSDB_MAX_SUBCOMPACTIONS") {
        options.set_max_subcompactions(subcompactions);
    }

    // === Write Buffer (Memtable) ===
    if let Some(size) = env_var_opt::<usize>("COZO_ROCKSDB_WRITE_BUFFER_SIZE") {
        options.set_write_buffer_size(size);
    }
    if let Some(num) = env_var_opt::<i32>("COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER") {
        options.set_max_write_buffer_number(num);
    }
    if let Some(num) = env_var_opt::<i32>("COZO_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE") {
        options.set_min_write_buffer_number_to_merge(num);
    }
    if let Some(size) = env_var_opt::<usize>("COZO_ROCKSDB_DB_WRITE_BUFFER_SIZE") {
        options.set_db_write_buffer_size(size);
    }

    // === Compaction ===
    if let Some(style) = env_string("COZO_ROCKSDB_COMPACTION_STYLE") {
        options.set_compaction_style(parse_compaction_style(&style));
    }
    options.set_disable_auto_compactions(env_bool("COZO_ROCKSDB_DISABLE_AUTO_COMPACTIONS", false));

    if let Some(trigger) = env_var_opt::<i32>("COZO_ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER") {
        options.set_level_zero_file_num_compaction_trigger(trigger);
    }
    if let Some(trigger) = env_var_opt::<i32>("COZO_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER") {
        options.set_level_zero_slowdown_writes_trigger(trigger);
    }
    if let Some(trigger) = env_var_opt::<i32>("COZO_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER") {
        options.set_level_zero_stop_writes_trigger(trigger);
    }
    if let Some(bytes) = env_var_opt::<u64>("COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE") {
        options.set_max_bytes_for_level_base(bytes);
    }
    if let Some(mult) = env_var_opt::<f64>("COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_MULTIPLIER") {
        options.set_max_bytes_for_level_multiplier(mult);
    }
    if let Some(size) = env_var_opt::<u64>("COZO_ROCKSDB_TARGET_FILE_SIZE_BASE") {
        options.set_target_file_size_base(size);
    }
    if let Some(mult) = env_var_opt::<i32>("COZO_ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER") {
        options.set_target_file_size_multiplier(mult);
    }
    if let Some(bytes) = env_var_opt::<u64>("COZO_ROCKSDB_MAX_COMPACTION_BYTES") {
        options.set_max_compaction_bytes(bytes);
    }
    if let Some(size) = env_var_opt::<usize>("COZO_ROCKSDB_COMPACTION_READAHEAD_SIZE") {
        options.set_compaction_readahead_size(size);
    }
    options.set_level_compaction_dynamic_level_bytes(
        env_bool("COZO_ROCKSDB_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES", false)
    );
    if let Some(secs) = env_var_opt::<u64>("COZO_ROCKSDB_PERIODIC_COMPACTION_SECONDS") {
        options.set_periodic_compaction_seconds(secs);
    }

    // === Compression ===
    if let Some(comp) = env_string("COZO_ROCKSDB_COMPRESSION_TYPE") {
        options.set_compression_type(parse_compression_type(&comp));
    }
    if let Some(comp) = env_string("COZO_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE") {
        options.set_bottommost_compression_type(parse_compression_type(&comp));
    }
    if let Some(bytes) = env_var_opt::<i32>("COZO_ROCKSDB_ZSTD_MAX_TRAIN_BYTES") {
        options.set_zstd_max_train_bytes(bytes);
    }

    // === Prefix Extractor ===
    let prefix_len = env_var("COZO_ROCKSDB_PREFIX_EXTRACTOR_LENGTH", KEY_PREFIX_LEN);
    options.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));

    // === Block-Based Table Options ===
    let mut block_opts = BlockBasedOptions::default();

    if let Some(size) = env_var_opt::<usize>("COZO_ROCKSDB_BLOCK_SIZE") {
        block_opts.set_block_size(size);
    }

    if env_bool("COZO_ROCKSDB_DISABLE_BLOCK_CACHE", false) {
        block_opts.disable_cache();
    } else if let Some(cache_size) = env_var_opt::<usize>("COZO_ROCKSDB_BLOCK_CACHE_SIZE") {
        let cache = Cache::new_lru_cache(cache_size);
        block_opts.set_block_cache(&cache);
    }

    let bloom_bits = env_var("COZO_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY", 10.0_f64);
    let bloom_block_based = env_bool("COZO_ROCKSDB_BLOOM_FILTER_BLOCK_BASED", false);
    block_opts.set_bloom_filter(bloom_bits, bloom_block_based);

    block_opts.set_cache_index_and_filter_blocks(
        env_bool("COZO_ROCKSDB_CACHE_INDEX_AND_FILTER_BLOCKS", true)
    );
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(
        env_bool("COZO_ROCKSDB_PIN_L0_FILTER_AND_INDEX_BLOCKS", true)
    );
    block_opts.set_whole_key_filtering(
        env_bool("COZO_ROCKSDB_WHOLE_KEY_FILTERING", true)
    );

    if let Some(version) = env_var_opt::<i32>("COZO_ROCKSDB_FORMAT_VERSION") {
        block_opts.set_format_version(version);
    }

    options.set_block_based_table_factory(&block_opts);
    options.set_optimize_filters_for_hits(
        env_bool("COZO_ROCKSDB_OPTIMIZE_FILTERS_FOR_HITS", false)
    );

    // === Blob Storage ===
    if env_bool("COZO_ROCKSDB_ENABLE_BLOB_FILES", false) {
        options.set_enable_blob_files(true);
        if let Some(size) = env_var_opt::<u64>("COZO_ROCKSDB_MIN_BLOB_SIZE") {
            options.set_min_blob_size(size);
        }
        if let Some(size) = env_var_opt::<u64>("COZO_ROCKSDB_BLOB_FILE_SIZE") {
            options.set_blob_file_size(size);
        }
        if let Some(comp) = env_string("COZO_ROCKSDB_BLOB_COMPRESSION_TYPE") {
            options.set_blob_compression_type(parse_compression_type(&comp));
        }
        options.set_enable_blob_gc(env_bool("COZO_ROCKSDB_ENABLE_BLOB_GC", true));
        if let Some(cutoff) = env_var_opt::<f64>("COZO_ROCKSDB_BLOB_GC_AGE_CUTOFF") {
            options.set_blob_gc_age_cutoff(cutoff);
        }
        if let Some(threshold) = env_var_opt::<f64>("COZO_ROCKSDB_BLOB_GC_FORCE_THRESHOLD") {
            options.set_blob_gc_force_threshold(threshold);
        }
    }

    // === WAL (Write-Ahead Log) ===
    if let Some(dir) = env_string("COZO_ROCKSDB_WAL_DIR") {
        options.set_wal_dir(&dir);
    }
    if let Some(ttl) = env_var_opt::<u64>("COZO_ROCKSDB_WAL_TTL_SECONDS") {
        options.set_wal_ttl_seconds(ttl);
    }
    if let Some(limit) = env_var_opt::<u64>("COZO_ROCKSDB_WAL_SIZE_LIMIT_MB") {
        options.set_wal_size_limit_mb(limit);
    }
    if let Some(size) = env_var_opt::<u64>("COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE") {
        options.set_max_total_wal_size(size);
    }
    if let Some(bytes) = env_var_opt::<u64>("COZO_ROCKSDB_WAL_BYTES_PER_SYNC") {
        options.set_wal_bytes_per_sync(bytes);
    }
    options.set_manual_wal_flush(env_bool("COZO_ROCKSDB_MANUAL_WAL_FLUSH", false));

    // === I/O Options ===
    options.set_use_fsync(env_bool("COZO_ROCKSDB_USE_FSYNC", false));
    options.set_use_direct_reads(env_bool("COZO_ROCKSDB_USE_DIRECT_READS", false));
    options.set_use_direct_io_for_flush_and_compaction(
        env_bool("COZO_ROCKSDB_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION", false)
    );
    options.set_allow_mmap_reads(env_bool("COZO_ROCKSDB_ALLOW_MMAP_READS", false));
    options.set_allow_mmap_writes(env_bool("COZO_ROCKSDB_ALLOW_MMAP_WRITES", false));
    if let Some(bytes) = env_var_opt::<u64>("COZO_ROCKSDB_BYTES_PER_SYNC") {
        options.set_bytes_per_sync(bytes);
    }
    if let Some(size) = env_var_opt::<u64>("COZO_ROCKSDB_WRITABLE_FILE_MAX_BUFFER_SIZE") {
        options.set_writable_file_max_buffer_size(size);
    }

    // === Concurrency ===
    options.set_allow_concurrent_memtable_write(
        env_bool("COZO_ROCKSDB_ALLOW_CONCURRENT_MEMTABLE_WRITE", true)
    );
    options.set_enable_write_thread_adaptive_yield(
        env_bool("COZO_ROCKSDB_ENABLE_WRITE_THREAD_ADAPTIVE_YIELD", true)
    );
    options.set_enable_pipelined_write(
        env_bool("COZO_ROCKSDB_ENABLE_PIPELINED_WRITE", false)
    );
    options.set_unordered_write(env_bool("COZO_ROCKSDB_UNORDERED_WRITE", false));

    // === Statistics & Logging ===
    if env_bool("COZO_ROCKSDB_ENABLE_STATISTICS", false) {
        options.enable_statistics();
    }
    if let Some(period) = env_var_opt::<u32>("COZO_ROCKSDB_STATS_DUMP_PERIOD_SEC") {
        options.set_stats_dump_period_sec(period);
    }
    if let Some(size) = env_var_opt::<usize>("COZO_ROCKSDB_MAX_LOG_FILE_SIZE") {
        options.set_max_log_file_size(size);
    }
    if let Some(num) = env_var_opt::<usize>("COZO_ROCKSDB_KEEP_LOG_FILE_NUM") {
        options.set_keep_log_file_num(num);
    }

    options
}

// =============================================================================
// Database Creation
// =============================================================================

/// Creates a RocksDB database object using the official rust-rocksdb crate.
///
/// This backend supports comprehensive configuration via environment variables.
/// See module documentation for the full list of supported options.
///
/// Supports concurrent readers and writers with optimistic transactions.
pub fn new_cozo_newrocksdb(path: impl AsRef<Path>) -> Result<Db<NewRocksDbStorage>> {
    fs::create_dir_all(&path).map_err(|err| {
        BadDbInit(format!(
            "cannot create directory {}: {}",
            path.as_ref().display(),
            err
        ))
    })?;
    let path_buf = path.as_ref().to_path_buf();

    let manifest_path = path_buf.join("manifest");
    let is_new = if manifest_path.exists() {
        let manifest_bytes = fs::read(&manifest_path)
            .into_diagnostic()
            .wrap_err("failed to read manifest")?;
        let existing: DbManifest = rmp_serde::from_slice(&manifest_bytes)
            .into_diagnostic()
            .wrap_err("failed to parse manifest")?;

        if existing.storage_version != CURRENT_STORAGE_VERSION {
            return Err(miette!(
                "Unsupported storage version {}",
                existing.storage_version
            ));
        }
        false
    } else {
        let manifest = DbManifest {
            storage_version: CURRENT_STORAGE_VERSION,
        };
        let manifest_bytes = rmp_serde::to_vec_named(&manifest)
            .into_diagnostic()
            .wrap_err("failed to serialize manifest")?;
        fs::write(&manifest_path, &manifest_bytes)
            .into_diagnostic()
            .wrap_err("failed to write manifest")?;
        true
    };

    let store_path = path_buf.join("data");
    let store_path_str = store_path.to_str().ok_or(miette!("bad path name"))?;

    // Build options from environment variables
    let options = build_options(is_new);

    info!(
        "Opening NewRocksDB at {} (is_new: {})",
        store_path_str, is_new
    );

    let db = OptimisticTransactionDB::open(&options, store_path_str)
        .into_diagnostic()
        .wrap_err("Failed to open RocksDB")?;

    let ret = Db::new(NewRocksDbStorage::new(db))?;
    ret.initialize()?;
    Ok(ret)
}

// =============================================================================
// Storage Implementation
// =============================================================================

/// RocksDB storage engine using the official rust-rocksdb crate
#[derive(Clone)]
pub struct NewRocksDbStorage {
    db: Arc<OptimisticTransactionDB>,
}

impl NewRocksDbStorage {
    pub(crate) fn new(db: OptimisticTransactionDB) -> Self {
        Self { db: Arc::new(db) }
    }

    /// Flush all memtables to disk
    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .into_diagnostic()
            .wrap_err("Failed to flush RocksDB")
    }

    /// Get RocksDB statistics (if enabled via COZO_ROCKSDB_ENABLE_STATISTICS)
    pub fn get_statistics(&self) -> Option<String> {
        self.db.property_value("rocksdb.stats").ok().flatten()
    }

    /// Get memory usage estimate
    pub fn get_memory_usage(&self) -> Result<u64> {
        self.db
            .property_int_value("rocksdb.estimate-table-readers-mem")
            .into_diagnostic()
            .wrap_err("Failed to get memory usage")?
            .ok_or_else(|| miette!("Memory property not available"))
    }
}

impl<'s> Storage<'s> for NewRocksDbStorage {
    type Tx = NewRocksDbTx<'s>;

    fn storage_kind(&self) -> &'static str {
        "newrocksdb"
    }

    fn transact(&'s self, _write: bool) -> Result<Self::Tx> {
        Ok(NewRocksDbTx {
            db_tx: Some(self.db.transaction()),
        })
    }

    fn range_compact(&self, lower: &[u8], upper: &[u8]) -> Result<()> {
        self.db.compact_range(Some(lower), Some(upper));
        Ok(())
    }

    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        let mut batch = WriteBatchWithTransaction::<true>::default();
        for result in data {
            let (key, val) = result?;
            batch.put(&key, &val);
        }
        self.db
            .write(batch)
            .into_diagnostic()
            .wrap_err_with(|| "Batch put failed")
    }
}

// =============================================================================
// Transaction Implementation
// =============================================================================

pub struct NewRocksDbTx<'a> {
    db_tx: Option<rocksdb::Transaction<'a, OptimisticTransactionDB>>,
}

unsafe impl<'a> Sync for NewRocksDbTx<'a> {}

impl<'s> StoreTx<'s> for NewRocksDbTx<'s> {
    fn get(&self, key: &[u8], _for_update: bool) -> Result<Option<Vec<u8>>> {
        let db_tx = self
            .db_tx
            .as_ref()
            .ok_or_else(|| miette!("Transaction already committed"))?;

        db_tx
            .get(key)
            .into_diagnostic()
            .wrap_err("failed to get value")
    }

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        let db_tx = self
            .db_tx
            .as_mut()
            .ok_or_else(|| miette!("Transaction already committed"))?;

        db_tx
            .put(key, val)
            .into_diagnostic()
            .wrap_err("failed to put value")
    }

    fn supports_par_put(&self) -> bool {
        true
    }

    #[inline]
    fn par_put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        match self.db_tx {
            Some(ref db_tx) => db_tx
                .put(key, val)
                .into_diagnostic()
                .wrap_err_with(|| "Parallel put failed"),
            None => Err(miette!("Transaction already committed")),
        }
    }

    #[inline]
    fn del(&mut self, key: &[u8]) -> Result<()> {
        match self.db_tx {
            Some(ref mut db_tx) => db_tx
                .delete(key)
                .into_diagnostic()
                .wrap_err_with(|| "Delete operation failed"),
            None => Err(miette!("Transaction already committed")),
        }
    }

    #[inline]
    fn par_del(&self, key: &[u8]) -> Result<()> {
        match self.db_tx {
            Some(ref db_tx) => db_tx
                .delete(key)
                .into_diagnostic()
                .wrap_err_with(|| "Parallel delete failed"),
            None => Err(miette!("Transaction already committed")),
        }
    }

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        match self.db_tx {
            Some(ref mut db_tx) => {
                let iter = db_tx.iterator(rocksdb::IteratorMode::From(
                    lower,
                    rocksdb::Direction::Forward,
                ));
                for item in iter {
                    let (k, _) = item
                        .into_diagnostic()
                        .wrap_err_with(|| "Error iterating during range delete")?;
                    if k >= upper.into() {
                        break;
                    }
                    db_tx
                        .delete(&k)
                        .into_diagnostic()
                        .wrap_err_with(|| "Error deleting during range delete")?;
                }
                Ok(())
            }
            None => Err(miette!("Transaction already committed")),
        }
    }

    #[inline]
    fn exists(&self, key: &[u8], _for_update: bool) -> Result<bool> {
        let db_tx = self
            .db_tx
            .as_ref()
            .ok_or(miette!("Transaction already committed"))?;
        db_tx
            .get(key)
            .into_diagnostic()
            .wrap_err("Error during exists check")
            .map(|opt| opt.is_some())
    }

    fn commit(&mut self) -> Result<()> {
        let db_tx = self.db_tx.take().expect("Transaction already committed");
        db_tx
            .commit()
            .into_diagnostic()
            .wrap_err_with(|| "Commit failed")
    }

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        match &self.db_tx {
            Some(db_tx) => Box::new(NewRocksDbIterator {
                inner: db_tx.iterator(rocksdb::IteratorMode::From(
                    lower,
                    rocksdb::Direction::Forward,
                )),
                upper_bound: upper.to_vec(),
            }),
            None => Box::new(std::iter::once(Err(miette!(
                "Transaction already committed"
            )))),
        }
    }

    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        match self.db_tx {
            Some(ref db_tx) => Box::new(NewRocksDbSkipIterator {
                inner: db_tx.iterator(rocksdb::IteratorMode::From(
                    lower,
                    rocksdb::Direction::Forward,
                )),
                upper_bound: upper.to_vec(),
                valid_at,
                next_bound: lower.to_vec(),
            }),
            None => Box::new(std::iter::once(Err(miette!(
                "Transaction already committed"
            )))),
        }
    }

    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        match self.db_tx {
            Some(ref db_tx) => {
                let iter = db_tx.iterator(rocksdb::IteratorMode::From(
                    lower,
                    rocksdb::Direction::Forward,
                ));
                Box::new(NewRocksDbIteratorRaw {
                    inner: iter,
                    upper_bound: upper.to_vec(),
                })
            }
            None => Box::new(std::iter::once(Err(miette!(
                "Transaction already committed"
            )))),
        }
    }

    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        let db_tx = self
            .db_tx
            .as_ref()
            .ok_or(miette!("Transaction already committed"))?;
        let iter = db_tx.iterator(rocksdb::IteratorMode::From(
            lower,
            rocksdb::Direction::Forward,
        ));
        let count = iter
            .take_while(|item| match item {
                Ok((k, _)) => k.as_ref() < upper,
                Err(_) => false,
            })
            .count();
        Ok(count)
    }

    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        match self.db_tx {
            Some(ref db_tx) => Box::new(db_tx.iterator(rocksdb::IteratorMode::Start).map(|item| {
                item.map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .into_diagnostic()
                    .wrap_err_with(|| "Error during total scan")
            })),
            None => Box::new(std::iter::once(Err(miette!(
                "Transaction already committed"
            )))),
        }
    }
}

// =============================================================================
// Iterators
// =============================================================================

pub(crate) struct NewRocksDbIterator<'a> {
    inner: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, OptimisticTransactionDB>>,
    upper_bound: Vec<u8>,
}

impl<'a> Iterator for NewRocksDbIterator<'a> {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(result) = self.inner.next() {
            match result {
                Ok((k, v)) => {
                    if k.as_ref() >= self.upper_bound.as_slice() {
                        return None;
                    }
                    return Some(Ok(decode_tuple_from_kv(&k, &v, None)));
                }
                Err(e) => return Some(Err(miette!("Iterator error: {}", e))),
            }
        }
        None
    }
}

pub(crate) struct NewRocksDbSkipIterator<'a> {
    inner: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, OptimisticTransactionDB>>,
    upper_bound: Vec<u8>,
    valid_at: ValidityTs,
    next_bound: Vec<u8>,
}

impl<'a> Iterator for NewRocksDbSkipIterator<'a> {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.inner.set_mode(rocksdb::IteratorMode::From(
                &self.next_bound,
                rocksdb::Direction::Forward,
            ));
            match self.inner.next() {
                None => return None,
                Some(Ok((k_slice, v_slice))) => {
                    if self.upper_bound.as_slice() <= k_slice.as_ref() {
                        return None;
                    }

                    let (ret, nxt_bound) =
                        check_key_for_validity(k_slice.as_ref(), self.valid_at, None);
                    self.next_bound = nxt_bound;
                    if let Some(mut tup) = ret {
                        extend_tuple_from_v(&mut tup, v_slice.as_ref());
                        return Some(Ok(tup));
                    }
                }
                Some(Err(e)) => return Some(Err(miette!("Iterator Error: {}", e))),
            }
        }
    }
}

pub(crate) struct NewRocksDbIteratorRaw<'a> {
    inner: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, OptimisticTransactionDB>>,
    upper_bound: Vec<u8>,
}

impl<'a> Iterator for NewRocksDbIteratorRaw<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((k, v))) => {
                if k.as_ref() >= self.upper_bound.as_slice() {
                    return None;
                }
                Some(Ok((k.to_vec(), v.to_vec())))
            }
            Some(Err(e)) => Some(Err(miette!("Iterator error: {}", e))),
            None => None,
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::value::{DataValue, Validity};
    use crate::runtime::db::ScriptMutability;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn setup_test_db() -> Result<(TempDir, Db<NewRocksDbStorage>)> {
        let temp_dir = TempDir::new().into_diagnostic()?;
        let db = new_cozo_newrocksdb(temp_dir.path())?;

        db.run_script(
            r#"
            {:create plain {k: Int => v}}
            {:create tt_test {k: Int, vld: Validity => v}}
            "#,
            Default::default(),
            ScriptMutability::Mutable,
        )?;

        Ok((temp_dir, db))
    }

    #[test]
    fn test_basic_operations() -> Result<()> {
        let (_temp_dir, db) = setup_test_db()?;

        let mut to_import = BTreeMap::new();
        to_import.insert(
            "plain".to_string(),
            crate::NamedRows {
                headers: vec!["k".to_string(), "v".to_string()],
                rows: (0..100)
                    .map(|i| vec![DataValue::from(i), DataValue::from(i * 2)])
                    .collect(),
                next: None,
            },
        );
        db.import_relations(to_import)?;

        let result = db.run_script(
            "?[v] := *plain{k: 5, v}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], DataValue::from(10));

        Ok(())
    }

    #[test]
    fn test_time_travel() -> Result<()> {
        let (_temp_dir, db) = setup_test_db()?;

        let mut to_import = BTreeMap::new();
        to_import.insert(
            "tt_test".to_string(),
            crate::NamedRows {
                headers: vec!["k".to_string(), "vld".to_string(), "v".to_string()],
                rows: vec![
                    vec![
                        DataValue::from(1),
                        DataValue::Validity(Validity::from((0, true))),
                        DataValue::from(100),
                    ],
                    vec![
                        DataValue::from(1),
                        DataValue::Validity(Validity::from((1, true))),
                        DataValue::from(200),
                    ],
                ],
                next: None,
            },
        );
        db.import_relations(to_import)?;

        let result = db.run_script(
            "?[v] := *tt_test{k: 1, v @ 0}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        assert_eq!(result.rows[0][0], DataValue::from(100));

        let result = db.run_script(
            "?[v] := *tt_test{k: 1, v @ 1}",
            Default::default(),
            ScriptMutability::Immutable,
        )?;
        assert_eq!(result.rows[0][0], DataValue::from(200));

        Ok(())
    }

    #[test]
    fn test_range_operations() -> Result<()> {
        let (_temp_dir, db) = setup_test_db()?;

        let mut to_import = BTreeMap::new();
        to_import.insert(
            "plain".to_string(),
            crate::NamedRows {
                headers: vec!["k".to_string(), "v".to_string()],
                rows: (0..10)
                    .map(|i| vec![DataValue::from(i), DataValue::from(i)])
                    .collect(),
                next: None,
            },
        );
        db.import_relations(to_import)?;

        let result = db.run_script(
            "?[k, v] := *plain{k, v}, k >= 3, k < 7",
            Default::default(),
            ScriptMutability::Immutable,
        )?;

        assert_eq!(result.rows.len(), 4);
        assert_eq!(result.rows[0][0], DataValue::from(3));
        assert_eq!(result.rows[3][0], DataValue::from(6));

        Ok(())
    }
}
