// Copyright 2022, The Cozo Project Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
// If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#ifndef COZOROCKS_DB_H
#define COZOROCKS_DB_H

#include <utility>

#include "iostream"
#include "common.h"
#include "tx.h"
#include "slice.h"

struct SnapshotBridge {
    const Snapshot *snapshot;
    DB *db;

    explicit SnapshotBridge(const Snapshot *snapshot_, DB *db_) : snapshot(snapshot_), db(db_) {}

    ~SnapshotBridge() {
        db->ReleaseSnapshot(snapshot);
//        printf("released snapshot\n");
    }
};

struct SstFileWriterBridge {
    SstFileWriter inner;

    SstFileWriterBridge(EnvOptions eopts, Options opts) : inner(eopts, opts) {
    }

    inline void finish(RocksDbStatus &status) {
        write_status(inner.Finish(), status);
    }

    inline void put(RustBytes key, RustBytes val, RocksDbStatus &status) {
        write_status(inner.Put(convert_slice(key), convert_slice(val)), status);
    }

};

static WriteOptions DEFAULT_WRITE_OPTIONS = WriteOptions();

struct RocksDbBridge {
    unique_ptr<TransactionDB> db;

    bool destroy_on_exit;
    string db_path;

    inline unique_ptr<SstFileWriterBridge> get_sst_writer(rust::Str path, RocksDbStatus &status) const {
        DB *db_ = get_base_db();
        auto cf = db->DefaultColumnFamily();
        Options options_ = db_->GetOptions(cf);
        auto sst_file_writer = std::make_unique<SstFileWriterBridge>(EnvOptions(), options_);
        string path_(path);

        write_status(sst_file_writer->inner.Open(path_), status);
        return sst_file_writer;
    }

    inline void ingest_sst(rust::Str path, RocksDbStatus &status) const {
        IngestExternalFileOptions ifo;
        DB *db_ = get_base_db();
        string path_(path);
        auto cf = db->DefaultColumnFamily();
        write_status(db_->IngestExternalFile(cf, {std::move(path_)}, ifo), status);
    }

    [[nodiscard]] inline const string &get_db_path() const {
        return db_path;
    }


    [[nodiscard]] inline unique_ptr<TxBridge> transact() const {
        auto ret = make_unique<TxBridge>(&*this->db, db->DefaultColumnFamily());
        return ret;
    }

    inline void del_range(RustBytes start, RustBytes end, RocksDbStatus &status) const {
        WriteBatch batch;
        auto cf = db->DefaultColumnFamily();
        auto s = batch.DeleteRange(cf, convert_slice(start), convert_slice(end));
        if (!s.ok()) {
            write_status(s, status);
            return;
        }
        WriteOptions w_opts;
        TransactionDBWriteOptimizations optimizations;
        optimizations.skip_concurrency_control = true;
        optimizations.skip_duplicate_key_check = true;
        auto s2 = db->Write(w_opts, optimizations, &batch);
        write_status(s2, status);
    }

    inline void put(RustBytes key, RustBytes val, RocksDbStatus &status) const {
        auto raw_db = this->get_base_db();
        auto s = raw_db->Put(DEFAULT_WRITE_OPTIONS, convert_slice(key), convert_slice(val));
        write_status(s, status);
    }

    void compact_range(RustBytes start, RustBytes end, RocksDbStatus &status) const {
        CompactRangeOptions options;
        auto cf = db->DefaultColumnFamily();
        auto start_s = convert_slice(start);
        auto end_s = convert_slice(end);
        auto s = db->CompactRange(options, cf, &start_s, &end_s);
        write_status(s, status);
    }

    // Flush all memtables to disk
    void flush(RocksDbStatus &status) const {
        FlushOptions flush_opts;
        flush_opts.wait = true;
        auto s = db->Flush(flush_opts);
        write_status(s, status);
    }

    // Get a RocksDB property value as string
    rust::String get_property(rust::Str property_name) const {
        std::string value;
        std::string prop_name(property_name);
        DB *db_ = get_base_db();
        if (db_->GetProperty(prop_name, &value)) {
            return rust::String(value);
        }
        return rust::String("");
    }

    // Get memory usage statistics as a formatted string
    // Returns: "memtable_size,block_cache_usage,block_cache_pinned,table_readers_mem"
    rust::String get_memory_stats() const {
        DB *db_ = get_base_db();
        std::string memtable_size, block_cache_usage, block_cache_pinned, table_readers_mem;

        db_->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_size);
        db_->GetProperty("rocksdb.block-cache-usage", &block_cache_usage);
        db_->GetProperty("rocksdb.block-cache-pinned-usage", &block_cache_pinned);
        db_->GetProperty("rocksdb.estimate-table-readers-mem", &table_readers_mem);

        std::string result = memtable_size + "," + block_cache_usage + "," +
                            block_cache_pinned + "," + table_readers_mem;
        return rust::String(result);
    }

    DB *get_base_db() const {
        return db->GetBaseDB();
    }

    ~RocksDbBridge();
};

shared_ptr<RocksDbBridge>
open_db(const DbOpts &opts, RocksDbStatus &status);

// Block cache statistics structure
struct BlockCacheStats {
    size_t capacity;
    size_t usage;
    size_t pinned_usage;

    // Getter methods for cxx bridge
    size_t get_capacity() const { return capacity; }
    size_t get_usage() const { return usage; }
    size_t get_pinned_usage() const { return pinned_usage; }
};

// Block cache control functions (process-global)
// Clear all entries from the shared block cache (releases memory)
void clear_shared_block_cache();

// Set the capacity of the shared block cache in MB
void set_shared_block_cache_capacity(size_t capacity_mb);

// Get block cache statistics
std::unique_ptr<BlockCacheStats> get_shared_block_cache_stats();

#endif //COZOROCKS_DB_H
