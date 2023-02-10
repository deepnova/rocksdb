#pragma once

class LastLevelIterator final : public InternalIterator {
 public:
  // @param read_options Must outlive this iterator.
  LastLevelIterator(
      TableCache* table_cache, const ReadOptions& read_options,
      const FileOptions& file_options, const InternalKeyComparator& icomparator,
      const LevelFilesBrief* flevel,
      const std::shared_ptr<const SliceTransform>& prefix_extractor,
      bool should_sample, HistogramImpl* file_read_hist,
      TableReaderCaller caller, bool skip_filters, int level,
      RangeDelAggregator* range_del_agg,
      const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries =
          nullptr,
      bool allow_unprepared_value = false,
      TruncatedRangeDelIterator**** range_tombstone_iter_ptr_ = nullptr)
      : table_cache_(table_cache),
        read_options_(read_options),
        file_options_(file_options),
        icomparator_(icomparator),
        user_comparator_(icomparator.user_comparator()),
        flevel_(flevel),
        prefix_extractor_(prefix_extractor),
        file_read_hist_(file_read_hist),
        should_sample_(should_sample),
        caller_(caller),
        skip_filters_(skip_filters),
        allow_unprepared_value_(allow_unprepared_value),
        file_index_(flevel_->num_files),
        level_(level),
        range_del_agg_(range_del_agg),
        pinned_iters_mgr_(nullptr),
        compaction_boundaries_(compaction_boundaries),
        is_next_read_sequential_(false),
        range_tombstone_iter_(nullptr),
        to_return_sentinel_(false) {
    // Empty level is not supported.
    assert(flevel_ != nullptr && flevel_->num_files > 0);
    if (range_tombstone_iter_ptr_) {
      *range_tombstone_iter_ptr_ = &range_tombstone_iter_;
    }
  }

  ~LastLevelIterator() override { delete file_iter_.Set(nullptr); }

  // Seek to the first file with a key >= target.
  // If range_tombstone_iter_ is not nullptr, then we pretend that file
  // boundaries are fake keys (sentinel keys). These keys are used to keep range
  // tombstones alive even when all point keys in an SST file are exhausted.
  // These sentinel keys will be skipped in merging iterator.
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final override;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;

  // In addition to valid and invalid state (!file_iter.Valid() and
  // status.ok()), a third state of the iterator is when !file_iter_.Valid() and
  // to_return_sentinel_. This means we are at the end of a file, and a sentinel
  // key (the file boundary that we pretend as a key) is to be returned next.
  // file_iter_.Valid() and to_return_sentinel_ should not both be true.
  bool Valid() const override {
    assert(!(file_iter_.Valid() && to_return_sentinel_));
    return file_iter_.Valid() || to_return_sentinel_;
  }
  Slice key() const override {
    assert(Valid());
    if (to_return_sentinel_) {
      // Sentinel should be returned after file_iter_ reaches the end of the
      // file
      assert(!file_iter_.Valid());
      return sentinel_;
    }
    return file_iter_.key();
  }

  Slice value() const override {
    assert(Valid());
    assert(!to_return_sentinel_);
    return file_iter_.value();
  }

  Status status() const override {
    return file_iter_.iter() ? file_iter_.status() : Status::OK();
  }

  bool PrepareValue() override { return file_iter_.PrepareValue(); }

  inline bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return may_be_out_of_lower_bound_ && file_iter_.MayBeOutOfLowerBound();
  }

  inline IterBoundCheck UpperBoundCheckResult() override {
    if (Valid()) {
      return file_iter_.UpperBoundCheckResult();
    } else {
      return IterBoundCheck::kUnknown;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_.iter()) {
      file_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsKeyPinned();
  }

  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsValuePinned();
  }

  bool IsDeleteRangeSentinelKey() const override { return to_return_sentinel_; }

 private:
  // Return true if at least one invalid file is seen and skipped.
  bool SkipEmptyFileForward();
  void SkipEmptyFileBackward();
  void SetFileIterator(InternalIterator* iter);
  void InitFileIterator(size_t new_file_index);

  const Slice& file_smallest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].smallest_key;
  }

  const Slice& file_largest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].largest_key;
  }

  bool KeyReachedUpperBound(const Slice& internal_key) {
    return read_options_.iterate_upper_bound != nullptr &&
           user_comparator_.CompareWithoutTimestamp(
               ExtractUserKey(internal_key), /*a_has_ts=*/true,
               *read_options_.iterate_upper_bound, /*b_has_ts=*/false) >= 0;
  }

  void ClearRangeTombstoneIter() {
    if (range_tombstone_iter_ && *range_tombstone_iter_) {
      delete *range_tombstone_iter_;
      *range_tombstone_iter_ = nullptr;
    }
  }

  // Move file_iter_ to the file at file_index_.
  // range_tombstone_iter_ is updated with a range tombstone iterator
  // into the new file. Old range tombstone iterator is cleared.
  InternalIterator* NewFileIterator() {
    assert(file_index_ < flevel_->num_files);
    auto file_meta = flevel_->files[file_index_];
    if (should_sample_) {
      sample_file_read_inc(file_meta.file_metadata);
    }

    const InternalKey* smallest_compaction_key = nullptr;
    const InternalKey* largest_compaction_key = nullptr;
    if (compaction_boundaries_ != nullptr) {
      smallest_compaction_key = (*compaction_boundaries_)[file_index_].smallest;
      largest_compaction_key = (*compaction_boundaries_)[file_index_].largest;
    }
    CheckMayBeOutOfLowerBound();
    ClearRangeTombstoneIter();
    return table_cache_->NewIterator(
        read_options_, file_options_, icomparator_, *file_meta.file_metadata,
        range_del_agg_, prefix_extractor_,
        nullptr /* don't need reference to table */, file_read_hist_, caller_,
        /*arena=*/nullptr, skip_filters_, level_,
        /*max_file_size_for_l0_meta_pin=*/0, smallest_compaction_key,
        largest_compaction_key, allow_unprepared_value_, range_tombstone_iter_);
  }

  // Check if current file being fully within iterate_lower_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update may_be_out_of_lower_bound_ accordingly.
  void CheckMayBeOutOfLowerBound() {
    if (read_options_.iterate_lower_bound != nullptr &&
        file_index_ < flevel_->num_files) {
      may_be_out_of_lower_bound_ =
          user_comparator_.CompareWithoutTimestamp(
              ExtractUserKey(file_smallest_key(file_index_)), /*a_has_ts=*/true,
              *read_options_.iterate_lower_bound, /*b_has_ts=*/false) < 0;
    }
  }

  TableCache* table_cache_;
  const ReadOptions& read_options_;
  const FileOptions& file_options_;
  const InternalKeyComparator& icomparator_;
  const UserComparatorWrapper user_comparator_;
  const LevelFilesBrief* flevel_;
  mutable FileDescriptor current_value_;
  // `prefix_extractor_` may be non-null even for total order seek. Checking
  // this variable is not the right way to identify whether prefix iterator
  // is used.
  const std::shared_ptr<const SliceTransform>& prefix_extractor_;

  HistogramImpl* file_read_hist_;
  bool should_sample_;
  TableReaderCaller caller_;
  bool skip_filters_;
  bool allow_unprepared_value_;
  bool may_be_out_of_lower_bound_ = true;
  size_t file_index_;
  int level_;
  RangeDelAggregator* range_del_agg_;
  IteratorWrapper file_iter_;  // May be nullptr
  PinnedIteratorsManager* pinned_iters_mgr_;

  // To be propagated to RangeDelAggregator in order to safely truncate range
  // tombstones.
  const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries_;

  bool is_next_read_sequential_;

  // This is set when this level iterator is used under a merging iterator
  // that processes range tombstones. range_tombstone_iter_ points to where the
  // merging iterator stores the range tombstones iterator for this level. When
  // this level iterator moves to a new SST file, it updates the range
  // tombstones accordingly through this pointer. So the merging iterator always
  // has access to the current SST file's range tombstones.
  //
  // The level iterator treats file boundary as fake keys (sentinel keys) to
  // keep range tombstones alive if needed and make upper level, i.e. merging
  // iterator, aware of file changes (when level iterator moves to a new SST
  // file, there is some bookkeeping work that needs to be done at merging
  // iterator end).
  //
  // *range_tombstone_iter_ points to range tombstones of the current SST file
  TruncatedRangeDelIterator** range_tombstone_iter_;

  // Whether next/prev key is a sentinel key.
  bool to_return_sentinel_ = false;
  // The sentinel key to be returned
  Slice sentinel_;
  // Sets flags for if we should return the sentinel key next.
  // The condition for returning sentinel is reaching the end of current
  // file_iter_: !Valid() && status.().ok().
  void TrySetDeleteRangeSentinel(const Slice& boundary_key);
  void ClearSentinel() { to_return_sentinel_ = false; }

  // Set in Seek() when a prefix seek reaches end of the current file,
  // and the next file has a different prefix. SkipEmptyFileForward()
  // will not move to next file when this flag is set.
  bool prefix_exhausted_ = false;
};
