
#pragma once

#include "rocksdb/slice.h"
#include "rocksdb/table.h"
//#include "table/block_based/block_builder.h"

#include <avro/ValidSchema.hh>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/Stream.hh>


namespace ROCKSDB_NAMESPACE {

// parquet sample code
using parquet::ConvertedType;
using parquet::Repetition;
//using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

//Tarim: RowGroup of Parquet
class LastLevelBlockBuilder {
 public:
  LastLevelBlockBuilder(const LastLevelBlockBuilder&) = delete;
  void operator=(const LastLevelBlockBuilder&) = delete;

  explicit LastLevelBlockBuilder();

  // Reset the contents as if the LastLevelBlockBuilder was just constructed.
  void Reset();

  // Swap the contents in LastLevelBlockBuilder with buffer, then reset the LastLevelBlockBuilder.
  void SwapAndReset(std::string& buffer);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // DO NOT mix with AddWithLastKey() between Resets. For efficiency, use
  // AddWithLastKey() in contexts where previous added key is already known
  // and delta encoding might be used.
  void Add(const Slice& key, const Slice& value,
           const Slice* const delta_value = nullptr);

  // A faster version of Add() if the previous key is already known for all
  // Add()s.
  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // REQUIRES: if AddWithLastKey has been called since last Reset(), last_key
  // is the key from most recent AddWithLastKey. (For convenience, last_key
  // is ignored on first call after creation or Reset().)
  // DO NOT mix with Add() between Resets.
  void AddWithLastKey(const Slice& key, const Slice& value,
                      const Slice& last_key,
                      const Slice* const delta_value = nullptr);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  inline size_t CurrentSizeEstimate() const; /*{
    return estimate_ + (data_block_hash_index_builder_.Valid()
                            ? data_block_hash_index_builder_.EstimateSize()
                            : 0);
  }*/

  // Returns an estimated block size after appending key and value.
  size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return false; } //Tarim-TODO

  void SetSchema(const avro::ValidSchema *schema) {
    schema_ptr_ = schema;
  }
  //const avro::ValidSchema* GetSchema() { // needless
  //  return schema_ptr_;
  //}

  bool hasRowGroup(){
    return rg_writer_ != nullptr;
  }

  void ResetRowGroup(parquet::RowGroupWriter* rg_writer){
    assert(rg_writer != nullptr);
    rg_writer_ = rg_writer;
  }

 private:
  inline void AddWithLastKeyImpl(const Slice& key, const Slice& value,
                                 const Slice& last_key,
                                 const Slice* const delta_value,
                                 size_t buffer_size);

  //const int block_restart_interval_;
  // TODO(myabandeh): put it into a separate IndexBlockBuilder
  //const bool use_delta_encoding_;
  // Refer to BlockIter::DecodeCurrentValue for format of delta encoded values
  //const bool use_value_delta_encoding_;

  const avro::ValidSchema* schema_ptr_ = nullptr;
  parquet::RowGroupWriter* rg_writer_ = nullptr;
  //avro::GenericRecord record_; //Tarim-TODO
  Slice min_key_;
  Slice max_key_;

  //std::string buffer_;              // Destination buffer
  //std::vector<uint32_t> restarts_;  // Restart points
  size_t estimate_;
  int counter_;    // Number of entries emitted since restart
  bool finished_;  // Has Finish() been called?
  std::string last_key_;
  //DataBlockHashIndexBuilder data_block_hash_index_builder_;
#ifndef NDEBUG
  bool add_with_last_key_called_ = false;
#endif
};

} // namespace ROCKSDB_NAMESPACE
