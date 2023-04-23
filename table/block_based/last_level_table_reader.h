
#pragma once
#include <memory>

#include "db/range_tombstone_fragmenter.h"
#if USE_COROUTINES
#include "folly/experimental/coro/Coroutine.h"
#include "folly/experimental/coro/Task.h"
#endif
#include "rocksdb/slice_transform.h"
#include "rocksdb/table_reader_caller.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {

class Iterator;
struct ParsedInternalKey;
class Slice;
class Arena;
struct ReadOptions;
struct TableProperties;
class GetContext;
class MultiGetContext;

class LastLevelTableReader {
 public:
  virtual ~LastLevelTableReader() {}

  static Status Open(
      const ReadOptions& ro, const ImmutableOptions& ioptions,
      const EnvOptions& env_options,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_key_comparator,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr =
          nullptr,
      const std::shared_ptr<const SliceTransform>& prefix_extractor = nullptr,
      bool prefetch_index_and_filter_in_cache = true, bool skip_filters = false,
      int level = -1, const bool immortal_table = false,
      const SequenceNumber largest_seqno = 0,
      bool force_direct_prefetch = false,
      TailPrefetchStats* tail_prefetch_stats = nullptr,
      BlockCacheTracer* const block_cache_tracer = nullptr,
      size_t max_file_size_for_l0_meta_pin = 0,
      const std::string& cur_db_session_id = "", uint64_t cur_file_num = 0,
      UniqueId64x2 expected_unique_id = {});

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // read_options: Must outlive the returned iterator.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        When destroying the iterator, the caller will not call "delete"
  //        but Iterator::~Iterator() directly. The destructor needs to destroy
  //        all the states but those allocated in arena.
  // skip_filters: disables checking the bloom filters even if they exist. This
  //               option is effective only for block-based table format.
  // compaction_readahead_size: its value will only be used if caller =
  // kCompaction
  virtual InternalIterator* NewIterator(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool skip_filters, TableReaderCaller caller,
      size_t compaction_readahead_size = 0,
      bool allow_unprepared_value = false);

  virtual FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& /*read_options*/) {
    return nullptr;
  }

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  // TODO(peterd): Since this function is only used for approximate size
  // from beginning of file, reduce code duplication by removing this
  // function and letting ApproximateSize take optional start and end, so
  // that absolute start and end can be specified and optimized without
  // key / index work.
  virtual uint64_t ApproximateOffsetOf(const Slice& key,
                                       TableReaderCaller caller);

  // Given start and end keys, return the approximate data size in the file
  // between the keys. The returned value is in terms of file bytes, and so
  // includes effects like compression of the underlying data and applicable
  // portions of metadata including filters and indexes. Nullptr for start or
  // end (or both) indicates absolute start or end of the table.
  virtual uint64_t ApproximateSize(const Slice& start, const Slice& end,
                                   TableReaderCaller caller);

  // Now try to return approximately 128 anchor keys.
  // The last one tends to be the largest key.
  virtual Status ApproximateKeyAnchors(const ReadOptions& /*read_options*/,
                                       std::vector<Anchor>& /*anchors*/) {
    return Status::NotSupported("ApproximateKeyAnchors() not supported.");
  }

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  virtual void SetupForCompaction();

  virtual std::shared_ptr<const TableProperties> GetTableProperties() const;

  // Prepare work that can be done before the real Get()
  virtual void Prepare(const Slice& /*target*/)

  // Report an approximation of how much memory has been used.
  virtual size_t ApproximateMemoryUsage() const;

  // Calls get_context->SaveValue() repeatedly, starting with
  // the entry found after a call to Seek(key), until it returns false.
  // May not make such a call if filter policy says that key is not present.
  //
  // get_context->MarkKeyMayExist needs to be called when it is configured to be
  // memory only and the key is not found in the block cache.
  //
  // readOptions is the options for the read
  // key is the key to search for
  // skip_filters: disables checking the bloom filters even if they exist. This
  //               option is effective only for block-based table format.
  virtual Status Get(const ReadOptions& readOptions, const Slice& key,
                     GetContext* get_context,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters = false);

  // Use bloom filters in the table file, if present, to filter out keys. The
  // mget_range will be updated to skip keys that get a negative result from
  // the filter lookup.
  virtual Status MultiGetFilter(const ReadOptions& /*readOptions*/,
                                const SliceTransform* /*prefix_extractor*/,
                                MultiGetContext::Range* /*mget_range*/) {
    return Status::NotSupported();
  }

  virtual void MultiGet(const ReadOptions& readOptions,
                        const MultiGetContext::Range* mget_range,
                        const SliceTransform* prefix_extractor,
                        bool skip_filters = false) {
    for (auto iter = mget_range->begin(); iter != mget_range->end(); ++iter) {
      *iter->s = Get(readOptions, iter->ikey, iter->get_context,
                     prefix_extractor, skip_filters);
    }
  }

#if USE_COROUTINES
  virtual folly::coro::Task<void> MultiGetCoroutine(
      const ReadOptions& readOptions, const MultiGetContext::Range* mget_range,
      const SliceTransform* prefix_extractor, bool skip_filters = false) {
    MultiGet(readOptions, mget_range, prefix_extractor, skip_filters);
    co_return;
  }
#endif  // USE_COROUTINES

  // Prefetch data corresponding to a give range of keys
  // Typically this functionality is required for table implementations that
  // persists the data on a non volatile storage medium like disk/SSD
  virtual Status Prefetch(const Slice* begin = nullptr,
                          const Slice* end = nullptr) {
    (void)begin;
    (void)end;
    // Default implementation is NOOP.
    // The child class should implement functionality when applicable
    return Status::OK();
  }

  // convert db file to a human readable form
  virtual Status DumpTable(WritableFile* /*out_file*/) {
    return Status::NotSupported("DumpTable() not supported");
  }

  // check whether there is corruption in this db file
  virtual Status VerifyChecksum(const ReadOptions& /*read_options*/,
                                TableReaderCaller /*caller*/) {
    return Status::NotSupported("VerifyChecksum() not supported");
  }

 protected:
  std::unique_ptr<Rep> rep_;
  explicit LastLevelTableReader(std::unique_ptr<Rep> rep, BlockCacheTracer* const block_cache_tracer)
      : rep_(rep), block_cache_tracer_(block_cache_tracer) {}
  // No copying allowed
  explicit LastLevelTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
}

struct LastLevelTableReader::Rep {
  Rep(const ImmutableOptions& _ioptions, const EnvOptions& _env_options,
      const BlockBasedTableOptions& _table_opt,
      const InternalKeyComparator& _internal_comparator, bool skip_filters,
      uint64_t _file_size, int _level, const bool _immortal_table)
      : ioptions(_ioptions),
        env_options(_env_options),
        table_options(_table_opt),
        filter_policy(skip_filters ? nullptr : _table_opt.filter_policy.get()),
        internal_comparator(_internal_comparator),
        filter_type(FilterType::kNoFilter),
        index_type(BlockBasedTableOptions::IndexType::kBinarySearch),
        whole_key_filtering(_table_opt.whole_key_filtering),
        prefix_filtering(true),
        global_seqno(kDisableGlobalSequenceNumber),
        file_size(_file_size),
        level(_level),
        immortal_table(_immortal_table) {}
  ~Rep() { status.PermitUncheckedError(); }
  const ImmutableOptions& ioptions;
  const EnvOptions& env_options;
  const BlockBasedTableOptions table_options;
  const FilterPolicy* const filter_policy;
  const InternalKeyComparator& internal_comparator;
  Status status;
  std::unique_ptr<RandomAccessFileReader> file;

  //OffsetableCacheKey base_cache_key;
  //PersistentCacheOptions persistent_cache_options;

  // Footer contains the fixed table information
  //Footer footer;

  //std::unique_ptr<IndexReader> index_reader;
  //std::unique_ptr<FilterBlockReader> filter;
  //std::unique_ptr<UncompressionDictReader> uncompression_dict_reader;

  //enum class FilterType {
  //  kNoFilter,
  //  kFullFilter,
  //  kPartitionedFilter,
  //};
  //FilterType filter_type;
  //BlockHandle filter_handle;
  //BlockHandle compression_dict_handle;

  std::shared_ptr<const TableProperties> table_properties; //Tarim-TODO: when created?
  //BlockBasedTableOptions::IndexType index_type;
  //bool whole_key_filtering;
  //bool prefix_filtering;
  //std::shared_ptr<const SliceTransform> table_prefix_extractor;

  //std::shared_ptr<FragmentedRangeTombstoneList> fragmented_range_dels;

  // If global_seqno is used, all Keys in this file will have the same
  // seqno with value `global_seqno`.
  //
  // A value of kDisableGlobalSequenceNumber means that this feature is disabled
  // and every key have it's own seqno.
  //SequenceNumber global_seqno;

  // Size of the table file on disk
  uint64_t file_size;

  // the level when the table is opened, could potentially change when trivial
  // move is involved
  int level;

  // If false, blocks in this file are definitely all uncompressed. Knowing this
  // before reading individual blocks enables certain optimizations.
  //bool blocks_maybe_compressed = true;

  // If true, data blocks in this file are definitely ZSTD compressed. If false
  // they might not be. When false we skip creating a ZSTD digested
  // uncompression dictionary. Even if we get a false negative, things should
  // still work, just not as quickly.
  //bool blocks_definitely_zstd_compressed = false;

  // These describe how index is encoded.
  //bool index_has_first_key = false;
  //bool index_key_includes_seq = true;
  //bool index_value_is_full = true;

  const bool immortal_table;

  std::unique_ptr<CacheReservationManager::CacheReservationHandle>
      table_reader_cache_res_handle = nullptr;

  //SequenceNumber get_global_seqno(BlockType block_type) const {
  //  return (block_type == BlockType::kFilterPartitionIndex ||
  //          block_type == BlockType::kCompressionDictionary)
  //             ? kDisableGlobalSequenceNumber
  //             : global_seqno;
  //}

  uint64_t cf_id_for_tracing() const {
    return table_properties
               ? table_properties->column_family_id
               : ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory::Context::
                     kUnknownColumnFamily;
  }

  Slice cf_name_for_tracing() const {
    return table_properties ? table_properties->column_family_name
                            : BlockCacheTraceHelper::kUnknownColumnFamilyName;
  }

  uint32_t level_for_tracing() const { return level >= 0 ? level : UINT32_MAX; }

  uint64_t sst_number_for_tracing() const {
    return file ? TableFileNameToNumber(file->file_name()) : UINT64_MAX;
  }

  //void CreateFilePrefetchBuffer(
  //    size_t readahead_size, size_t max_readahead_size,
  //    std::unique_ptr<FilePrefetchBuffer>* fpb, bool implicit_auto_readahead,
  //    uint64_t num_file_reads,
  //    uint64_t num_file_reads_for_auto_readahead) const {
  //  fpb->reset(new FilePrefetchBuffer(
  //      readahead_size, max_readahead_size,
  //      !ioptions.allow_mmap_reads /* enable */, false /* track_min_offset */,
  //      implicit_auto_readahead, num_file_reads,
  //      num_file_reads_for_auto_readahead, ioptions.fs.get(), ioptions.clock,
  //      ioptions.stats));
  //}

  void CreateFilePrefetchBufferIfNotExists(
      size_t readahead_size, size_t max_readahead_size,
      std::unique_ptr<FilePrefetchBuffer>* fpb, bool implicit_auto_readahead,
      uint64_t num_file_reads,
      uint64_t num_file_reads_for_auto_readahead) const {
    if (!(*fpb)) {
      CreateFilePrefetchBuffer(readahead_size, max_readahead_size, fpb,
                               implicit_auto_readahead, num_file_reads,
                               num_file_reads_for_auto_readahead);
    }
  }

  std::size_t ApproximateMemoryUsage() const {
    std::size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<LastLevelTableReader::Rep*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return usage;
  }
};

}  // namespace ROCKSDB_NAMESPACE

