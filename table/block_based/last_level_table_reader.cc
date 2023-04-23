
Status LastLevelTableReader::Open(
    const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const EnvOptions& env_options, const BlockBasedTableOptions& table_options,
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    const bool prefetch_index_and_filter_in_cache, const bool skip_filters,
    const int level, const bool immortal_table,
    const SequenceNumber largest_seqno, const bool force_direct_prefetch,
    TailPrefetchStats* tail_prefetch_stats,
    BlockCacheTracer* const block_cache_tracer,
    size_t max_file_size_for_l0_meta_pin, const std::string& cur_db_session_id,
    uint64_t cur_file_num, UniqueId64x2 expected_unique_id) {
  table_reader->reset();

  //Tarim-TODO: 
  //  * new parquet::ParquetFileReader() and Rep()
  //  * cache? OffsetableCacheKey? table_reader_cache_res_mgr/block_cache_tracer
  //  * parquet min-max, bloomfilter (in future)
  //  * Set up prefix extractor as needed (not support yet)

  Status s;

  // From read_options, retain deadline, io_timeout, and rate_limiter_priority.
  // In future, we may retain more
  // options. Specifically, we ignore verify_checksums and default to
  // checksum verification anyway when creating the index and filter
  // readers.
  ReadOptions ro;
  ro.deadline = read_options.deadline;
  ro.io_timeout = read_options.io_timeout;
  ro.rate_limiter_priority = read_options.rate_limiter_priority;

  Rep* rep = new BlockBasedTable::Rep(ioptions, env_options, table_options,
                                      internal_comparator, skip_filters,
                                      file_size, level, immortal_table);
  rep->file = std::move(file);
  
  // Read metaindex
  std::unique_ptr<BlockBasedTable> new_table(
      new BlockBasedTable(rep, block_cache_tracer));
  //std::unique_ptr<Block> metaindex;
  //std::unique_ptr<InternalIterator> metaindex_iter;
  //s = new_table->ReadMetaIndexBlock(ro, prefetch_buffer.get(), &metaindex,
  //                                  &metaindex_iter);
  //if (!s.ok()) {
  //  return s;
  //}

  // Populates table_properties and some fields that depend on it,
  // such as index_type.
  //s = new_table->ReadPropertiesBlock(ro, prefetch_buffer.get(),
  //                                   metaindex_iter.get(), largest_seqno);
  //if (!s.ok()) {
  //  return s;
  //}

  //Tarim-TODO: not sure what to do with it
  //if (s.ok() && table_reader_cache_res_mgr) {
  //  std::size_t mem_usage = new_table->ApproximateMemoryUsage();
  //  s = table_reader_cache_res_mgr->MakeCacheReservation(
  //      mem_usage, &(rep->table_reader_cache_res_handle));
  //  if (s.IsMemoryLimit()) {
  //    s = Status::MemoryLimit(
  //        "Can't allocate " +
  //        kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
  //            CacheEntryRole::kBlockBasedTableReader)] +
  //        " due to memory limit based on "
  //        "cache capacity for memory allocation");
  //  }
  //}

  //if (s.ok()) {
    *table_reader = std::move(new_table);
  //}
  return s;
}

InternalIterator* LastLevelTableReader::NewIterator(
    const ReadOptions& read_options, const SliceTransform* prefix_extractor,
    Arena* arena, bool skip_filters, TableReaderCaller caller,
    size_t compaction_readahead_size, bool allow_unprepared_value) {

  //Tarim-TODO: index
  
  bool need_upper_bound_check =
      read_options.auto_prefix_mode || PrefixExtractorChanged(prefix_extractor);

  if (arena == nullptr) {
    return new LastLevelTableIterator(
        this, read_options, rep_->internal_comparator, std::move(index_iter),
        !skip_filters && !read_options.total_order_seek &&
            prefix_extractor != nullptr,
        need_upper_bound_check, prefix_extractor, caller,
        compaction_readahead_size, allow_unprepared_value);
  } else {
    auto* mem = arena->AllocateAligned(sizeof(BlockBasedTableIterator));
    return new (mem) LastLevelTableIterator(
        this, read_options, rep_->internal_comparator, std::move(index_iter),
        !skip_filters && !read_options.total_order_seek &&
            prefix_extractor != nullptr,
        need_upper_bound_check, prefix_extractor, caller,
        compaction_readahead_size, allow_unprepared_value);
  }
}

uint64_t LastLevelTableReader::ApproximateOffsetOf(const Slice& key,
                                              TableReaderCaller caller) {
  uint64_t data_size = GetApproximateDataSize();
  if (UNLIKELY(data_size == 0)) {
    // Hmm. Let's just split in half to avoid skewing one way or another,
    // since we don't know whether we're operating on lower bound or
    // upper bound.
    return rep_->file_size / 2;
  }

  BlockCacheLookupContext context(caller);
  IndexBlockIter iiter_on_stack;
  ReadOptions ro;
  ro.total_order_seek = true;
  auto index_iter =
      NewIndexIterator(ro, /*disable_prefix_seek=*/true,
                       /*input_iter=*/&iiter_on_stack, /*get_context=*/nullptr,
                       /*lookup_context=*/&context);
  std::unique_ptr<InternalIteratorBase<IndexValue>> iiter_unique_ptr;
  if (index_iter != &iiter_on_stack) {
    iiter_unique_ptr.reset(index_iter);
  }

  index_iter->Seek(key);
  uint64_t offset;
  if (index_iter->status().ok()) {
    offset = ApproximateDataOffsetOf(*index_iter, data_size);
  } else {
    // Split in half to avoid skewing one way or another,
    // since we don't know whether we're operating on lower bound or
    // upper bound.
    return rep_->file_size / 2;
  }

  // Pro-rate file metadata (incl filters) size-proportionally across data
  // blocks.
  double size_ratio =
      static_cast<double>(offset) / static_cast<double>(data_size);
  return static_cast<uint64_t>(size_ratio *
                               static_cast<double>(rep_->file_size));
}
