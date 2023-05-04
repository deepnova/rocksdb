#pragma once

#include <array>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
//#include "table/meta_blocks.h"
#include "table/table_builder.h"
//#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

class LastLevelTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  LastLevelTableBuilder(const BlockBasedTableOptions& table_options,
                        const TableBuilderOptions& table_builder_options,
                        AbstractWritableFileWriter* file);

  // No copying allowed
  LastLevelTableBuilder(const LastLevelTableBuilder&) = delete;
  LastLevelTableBuilder& operator=(const LastLevelTableBuilder&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~LastLevelTableBuilder();

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override;

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  inline Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  inline void Abandon() override;

  // Number of calls to Add() so far.
  inline uint64_t NumEntries() const override;

  inline bool IsEmpty() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  inline uint64_t FileSize() const override;

  // Estimated size of the file generated so far. This is used when
  // FileSize() cannot estimate final SST size, e.g. parallel compression
  // is enabled.
  inline uint64_t EstimatedFileSize() const override;

  inline bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(
      const std::string& encoded_seqno_to_time_mapping,
      uint64_t oldest_ancestor_time) override;

 //private:

  //void WriteBlock(/*BlockBuilder* block, BlockHandle* handle,*/
  //                /*BlockType blocktype*/);

  //void Flush();

 private:
  struct Rep;
  std::unique_ptr<Rep> rep_;
};

inline bool ExceptedValueType(const ValueType value_type){
  return value_type == kTypeValue
         && value_type == kTypeMerge ;
}
  



}
