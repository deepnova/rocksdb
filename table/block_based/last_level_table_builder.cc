#include "table/block_based/last_level_table_builder.h"
#include "table/block_based/last_level_block_builder.h"
#include "file/parquet_file_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

LastLevelTableBuilder::LastLevelTableBuilder(
        const BlockBasedTableOptions& table_options,
        const TableBuilderOptions& tbo,
        AbstractWritableFileWriter* file) {

  BlockBasedTableOptions sanitized_table_options(table_options);
  if (sanitized_table_options.format_version == 0 &&
      sanitized_table_options.checksum != kCRC32c) {
    ROCKS_LOG_WARN(
        tbo.ioptions.logger,
        "Silently converting format_version to 1 because checksum is "
        "non-default");
    // silently convert format_version to 1 to keep consistent with current
    // behavior
    sanitized_table_options.format_version = 1;
  }

  rep_ = std::make_unique<Rep>(sanitized_table_options, tbo, file);

/*
  TEST_SYNC_POINT_CALLBACK(
      "LastLevelTableBuilder::LastLevelTableBuilder:PreSetupBaseCacheKey",
      const_cast<TableProperties*>(&rep_->props));

  BlockBasedTable::SetupBaseCacheKey(&rep_->props, tbo.db_session_id,
                                     tbo.cur_file_num, &rep_->base_cache_key);
  if (rep_->IsParallelCompressionEnabled()) {
    StartParallelCompression();
  }
*/

}

struct LastLevelTableBuilder::Rep {
  const ImmutableOptions ioptions;
  const MutableCFOptions moptions;
  const BlockBasedTableOptions table_options;
  ParquetFileWriter* file;
  //std::atomic<uint64_t> offset;
  //size_t alignment;
  LastLevelBlockBuilder data_block;
  Slice min_key_;
  Slice max_key_;
  // Buffers uncompressed data blocks to replay later. Needed when
  // compression dictionary is enabled so we can finalize the dictionary before
  // compressing any data blocks.
  //std::vector<std::string> data_block_buffers;
  //BlockBuilder range_del_block;

  //InternalKeySliceTransform internal_prefix_transform;
  //std::unique_ptr<IndexBuilder> index_builder;
  //PartitionedIndexBuilder* p_index_builder_ = nullptr;

  std::string last_key;
  const Slice* first_key_in_next_block = nullptr;
  //CompressionType compression_type;
  //uint64_t sample_for_compression;
  //std::atomic<uint64_t> compressible_input_data_bytes;
  //std::atomic<uint64_t> uncompressible_input_data_bytes;
  //std::atomic<uint64_t> sampled_input_data_bytes;
  //std::atomic<uint64_t> sampled_output_slow_data_bytes;
  //std::atomic<uint64_t> sampled_output_fast_data_bytes;
  //CompressionOptions compression_opts;
  //std::unique_ptr<CompressionDict> compression_dict;
  //std::vector<std::unique_ptr<CompressionContext>> compression_ctxs;
  //std::vector<std::unique_ptr<UncompressionContext>> verify_ctxs;
  //std::unique_ptr<UncompressionDict> verify_dict;

  //size_t data_begin_offset = 0;

  TableProperties props;

  // States of the builder.
  //
  // - `kBuffered`: This is the initial state where zero or more data blocks are
  //   accumulated uncompressed in-memory. From this state, call
  //   `EnterUnbuffered()` to finalize the compression dictionary if enabled,
  //   compress/write out any buffered blocks, and proceed to the `kUnbuffered`
  //   state.
  //
  // - `kUnbuffered`: This is the state when compression dictionary is finalized
  //   either because it wasn't enabled in the first place or it's been created
  //   from sampling previously buffered data. In this state, blocks are simply
  //   compressed/written out as they fill up. From this state, call `Finish()`
  //   to complete the file (write meta-blocks, etc.), or `Abandon()` to delete
  //   the partially created file.
  //
  // - `kClosed`: This indicates either `Finish()` or `Abandon()` has been
  //   called, so the table builder is no longer usable. We must be in this
  //   state by the time the destructor runs.
  enum class State {
    kBuffered,
    kUnbuffered,
    kClosed,
  };
  State state;
  // `kBuffered` state is allowed only as long as the buffering of uncompressed
  // data blocks (see `data_block_buffers`) does not exceed `buffer_limit`.
  //uint64_t buffer_limit;
  //std::shared_ptr<CacheReservationManager>
  //    compression_dict_buffer_cache_res_mgr;
  //const bool use_delta_encoding_for_index_values;
  //std::unique_ptr<FilterBlockBuilder> filter_builder;
  //OffsetableCacheKey base_cache_key;
  const TableFileCreationReason reason;

  //BlockHandle pending_handle;  // Handle to add to index block

  //std::string compressed_output;
  //std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  std::vector<std::unique_ptr<IntTblPropCollector>> table_properties_collectors;

  //std::unique_ptr<ParallelCompressionRep> pc_rep;

  //uint64_t get_offset() { return offset.load(std::memory_order_relaxed); }
  //void set_offset(uint64_t o) { offset.store(o, std::memory_order_relaxed); }
/*
  bool IsParallelCompressionEnabled() const {
    return compression_opts.parallel_threads > 1;
  }
*/
  Status GetStatus() {
    // We need to make modifications of status visible when status_ok is set
    // to false, and this is ensured by status_mutex, so no special memory
    // order for status_ok is required.
    if (status_ok.load(std::memory_order_relaxed)) {
      return Status::OK();
    } else {
      return CopyStatus();
    }
  }

  Status CopyStatus() {
    std::lock_guard<std::mutex> lock(status_mutex);
    return status;
  }

  IOStatus GetIOStatus() {
    // We need to make modifications of io_status visible when status_ok is set
    // to false, and this is ensured by io_status_mutex, so no special memory
    // order for io_status_ok is required.
    if (io_status_ok.load(std::memory_order_relaxed)) {
      return IOStatus::OK();
    } else {
      return CopyIOStatus();
    }
  }

  IOStatus CopyIOStatus() {
    std::lock_guard<std::mutex> lock(io_status_mutex);
    return io_status;
  }

  // Never erase an existing status that is not OK.
  void SetStatus(Status s) {
    if (!s.ok() && status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(status_mutex);
      status = s;
      status_ok.store(false, std::memory_order_relaxed);
    }
  }

  // Never erase an existing I/O status that is not OK.
  // Calling this will also SetStatus(ios)
  void SetIOStatus(IOStatus ios) {
    if (!ios.ok() && io_status_ok.load(std::memory_order_relaxed)) {
      // Locking is an overkill for non compression_opts.parallel_threads
      // case but since it's unlikely that s is not OK, we take this cost
      // to be simplicity.
      std::lock_guard<std::mutex> lock(io_status_mutex);
      io_status = ios;
      io_status_ok.store(false, std::memory_order_relaxed);
    }
    SetStatus(ios);
  }

  Rep(const BlockBasedTableOptions& table_opt, const TableBuilderOptions& tbo,
      AbstractWritableFileWriter* f)
      : ioptions(tbo.ioptions),
        moptions(tbo.moptions),
        table_options(table_opt),
        //internal_comparator(tbo.internal_comparator),
        //file(f),
        //offset(0),
        //alignment(table_options.block_align
        //              ? std::min(static_cast<size_t>(table_options.block_size),
        //                         kDefaultPageSize)
        //              : 0),
        data_block(),
        //range_del_block(1 /* block_restart_interval */),
        //internal_prefix_transform(tbo.moptions.prefix_extractor.get()),
        //compression_type(tbo.compression_type),
        //sample_for_compression(tbo.moptions.sample_for_compression),
        //compressible_input_data_bytes(0),
        //uncompressible_input_data_bytes(0),
        //sampled_input_data_bytes(0),
        //sampled_output_slow_data_bytes(0),
        //sampled_output_fast_data_bytes(0),
        //compression_opts(tbo.compression_opts),
        //compression_dict(),
        //compression_ctxs(tbo.compression_opts.parallel_threads),
        //verify_ctxs(tbo.compression_opts.parallel_threads),
        //verify_dict(),
        state(State::kUnbuffered),
        //use_delta_encoding_for_index_values(table_opt.format_version >= 4 &&
        //                                    !table_opt.block_align),
        reason(tbo.reason),
        //flush_block_policy(
        //    table_options.flush_block_policy_factory->NewFlushBlockPolicy(
        //        table_options, &data_block)),
        status_ok(true),
        io_status_ok(true) {

          file = static_cast<ParquetFileWriter*>(f);
          const avro::ValidSchema* schema = file->GetSchema();
          assert(schema != nullptr);
          data_block.SetSchema(schema);
/*
    if (tbo.target_file_size == 0) {
      buffer_limit = compression_opts.max_dict_buffer_bytes;
    } else if (compression_opts.max_dict_buffer_bytes == 0) {
      buffer_limit = tbo.target_file_size;
    } else {
      buffer_limit = std::min(tbo.target_file_size,
                              compression_opts.max_dict_buffer_bytes);
    }
    const auto compress_dict_build_buffer_charged =
        table_options.cache_usage_options.options_overrides
            .at(CacheEntryRole::kCompressionDictionaryBuildingBuffer)
            .charged;
    if (table_options.block_cache &&
        (compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kEnabled ||
         compress_dict_build_buffer_charged ==
             CacheEntryRoleOptions::Decision::kFallback)) {
      compression_dict_buffer_cache_res_mgr =
          std::make_shared<CacheReservationManagerImpl<
              CacheEntryRole::kCompressionDictionaryBuildingBuffer>>(
              table_options.block_cache);
    } else {
      compression_dict_buffer_cache_res_mgr = nullptr;
    }
*/
/*
    for (uint32_t i = 0; i < compression_opts.parallel_threads; i++) {
      compression_ctxs[i].reset(new CompressionContext(compression_type));
    }
*/
/*
    if (table_options.index_type ==
        BlockBasedTableOptions::kTwoLevelIndexSearch) {
      p_index_builder_ = PartitionedIndexBuilder::CreateIndexBuilder(
          &internal_comparator, use_delta_encoding_for_index_values,
          table_options);
      index_builder.reset(p_index_builder_);
    } else {
      index_builder.reset(IndexBuilder::CreateIndexBuilder(
          table_options.index_type, &internal_comparator,
          &this->internal_prefix_transform, use_delta_encoding_for_index_values,
          table_options));
    }
*/
/*
    if (ioptions.optimize_filters_for_hits && tbo.is_bottommost) {
      // Apply optimize_filters_for_hits setting here when applicable by
      // skipping filter generation
      filter_builder.reset();
    } else if (tbo.skip_filters) {
      // For SstFileWriter skip_filters
      filter_builder.reset();
    } else if (!table_options.filter_policy) {
      // Null filter_policy -> no filter
      filter_builder.reset();
    } else {
      FilterBuildingContext filter_context(table_options);

      filter_context.info_log = ioptions.logger;
      filter_context.column_family_name = tbo.column_family_name;
      filter_context.reason = reason;

      // Only populate other fields if known to be in LSM rather than
      // generating external SST file
      if (reason != TableFileCreationReason::kMisc) {
        filter_context.compaction_style = ioptions.compaction_style;
        filter_context.num_levels = ioptions.num_levels;
        filter_context.level_at_creation = tbo.level_at_creation;
        filter_context.is_bottommost = tbo.is_bottommost;
        assert(filter_context.level_at_creation < filter_context.num_levels);
      }

      filter_builder.reset(CreateFilterBlockBuilder(
          ioptions, moptions, filter_context,
          use_delta_encoding_for_index_values, p_index_builder_));
    }
*/
    assert(tbo.int_tbl_prop_collector_factories);
    for (auto& factory : *tbo.int_tbl_prop_collector_factories) {
      assert(factory);

      table_properties_collectors.emplace_back(
          factory->CreateIntTblPropCollector(tbo.column_family_id,
                                             tbo.level_at_creation));
    }
/*
    table_properties_collectors.emplace_back(
        new BlockBasedTablePropertiesCollector(
            table_options.index_type, table_options.whole_key_filtering,
            moptions.prefix_extractor != nullptr));
*/
    const Comparator* ucmp = tbo.internal_comparator.user_comparator();
    assert(ucmp);
    if (ucmp->timestamp_size() > 0) {
      table_properties_collectors.emplace_back(
          new TimestampTablePropertiesCollector(ucmp));
    }
/*
    if (table_options.verify_compression) {
      for (uint32_t i = 0; i < compression_opts.parallel_threads; i++) {
        verify_ctxs[i].reset(new UncompressionContext(compression_type));
      }
    }
*/
    // These are only needed for populating table properties
    props.column_family_id = tbo.column_family_id;
    props.column_family_name = tbo.column_family_name;
    props.oldest_key_time = tbo.oldest_key_time;
    props.file_creation_time = tbo.file_creation_time;
    props.orig_file_number = tbo.cur_file_num;
    props.db_id = tbo.db_id;
    props.db_session_id = tbo.db_session_id;
    props.db_host_id = ioptions.db_host_id;
    if (!ReifyDbHostIdProperty(ioptions.env, &props.db_host_id).ok()) {
      ROCKS_LOG_INFO(ioptions.logger, "db_host_id property will not be set");
    }
  }

  Rep(const Rep&) = delete;
  Rep& operator=(const Rep&) = delete;

 private:
  // Synchronize status & io_status accesses across threads from main thread,
  // compression thread and write thread in parallel compression.
  std::mutex status_mutex;
  std::atomic<bool> status_ok;
  Status status;
  std::mutex io_status_mutex;
  std::atomic<bool> io_status_ok;
  IOStatus io_status;
};


void LastLevelTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_.get();

  ValueType value_type = ExtractValueType(key);
  ROCKS_LOG_INFO(
    r->ioptions.logger,
    "[LastLevelTableBuilder::Add] value_type: %d\n",
    value_type);

  if (ExceptedValueType(value_type) == false) {
    ROCKS_LOG_ERROR(
      r->ioptions.logger,
      "[LastLevelTableBuilder::Add] unexcepted value_type: %d, make sure is there some error?\n",
      value_type);
    return;
  }

  //Slice user_key; //Tarim-TODO:

  if(r->data_block.hasRowGroup() == false){
    //Tarim-TODO: new row group
    //r->data_block.ResetRowGroup();
  }

  r->data_block.Add(key, value);

  //Tarim-TODO:
  //write row group: data_block
  //decode value to column
  //make sure file size
  //update min-max
}

Status LastLevelTableBuilder::Finish() {
        
  Rep* r = rep_.get();
  //Tarim-TODO:
  
  r->state = Rep::State::kClosed;
  return Status::OK();
}

void LastLevelTableBuilder::Abandon() {
  Rep* r = rep_.get();
  //Tarim-TODO:
  r->state = Rep::State::kClosed;
}

}

