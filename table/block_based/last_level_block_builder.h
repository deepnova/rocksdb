
#pragma once

#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/block_based/block_builder.h"

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>


namespace ROCKSDB_NAMESPACE {

// parquet sample code
using parquet::ConvertedType;
using parquet::Repetition;
//using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

constexpr int FIXED_LENGTH = 10;
//------------------

class LastLevelBlockBuilder : public BlockBuilder {
 public:
  LastLevelBlockBuilder(const LastLevelBlockBuilder&) = delete;
  void operator=(const LastLevelBlockBuilder&) = delete;

  explicit LastLevelBlockBuilder(int block_restart_interval,
                        bool use_delta_encoding = true,
                        bool use_value_delta_encoding = false,
                        BlockBasedTableOptions::DataBlockIndexType index_type =
                            BlockBasedTableOptions::kDataBlockBinarySearch,
                        double data_block_hash_table_util_ratio = 0.75);

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
  inline size_t CurrentSizeEstimate() const {
    return estimate_ + (data_block_hash_index_builder_.Valid()
                            ? data_block_hash_index_builder_.EstimateSize()
                            : 0);
  }

  // Returns an estimated block size after appending key and value.
  size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  inline void AddWithLastKeyImpl(const Slice& key, const Slice& value,
                                 const Slice& last_key,
                                 const Slice* const delta_value,
                                 size_t buffer_size);

  const int block_restart_interval_;
  // TODO(myabandeh): put it into a separate IndexBlockBuilder
  const bool use_delta_encoding_;
  // Refer to BlockIter::DecodeCurrentValue for format of delta encoded values
  const bool use_value_delta_encoding_;

  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  size_t estimate_;
  int counter_;    // Number of entries emitted since restart
  bool finished_;  // Has Finish() been called?
  std::string last_key_;
  DataBlockHashIndexBuilder data_block_hash_index_builder_;
#ifndef NDEBUG
  bool add_with_last_key_called_ = false;
#endif
};

// parquet sample code
static std::shared_ptr<GroupNode> SetupSchema() {
  parquet::schema::NodeVector fields;
  // Create a primitive node named 'boolean_field' with type:BOOLEAN,
  // repetition:REQUIRED
  fields.push_back(PrimitiveNode::Make("boolean_field", Repetition::REQUIRED,
                                       parquet::Type::BOOLEAN, ConvertedType::NONE));

  // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED,
  // logical type:TIME_MILLIS
  fields.push_back(PrimitiveNode::Make("int32_field", Repetition::REQUIRED, parquet::Type::INT32,
                                       ConvertedType::TIME_MILLIS));

  // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
  fields.push_back(PrimitiveNode::Make("int64_field", Repetition::REPEATED, parquet::Type::INT64,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("int96_field", Repetition::REQUIRED, parquet::Type::INT96,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("float_field", Repetition::REQUIRED, parquet::Type::FLOAT,
                                       ConvertedType::NONE));

  fields.push_back(PrimitiveNode::Make("double_field", Repetition::REQUIRED, parquet::Type::DOUBLE,
                                       ConvertedType::NONE));

  // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
  fields.push_back(PrimitiveNode::Make("ba_field", Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
                                       ConvertedType::NONE));

  // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY,
  // repetition:REQUIRED, field_length = FIXED_LENGTH
  fields.push_back(PrimitiveNode::Make("flba_field", Repetition::REQUIRED,
                                       parquet::Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
                                       FIXED_LENGTH));

  // Create a GroupNode named 'schema' using the primitive nodes defined above
  // This GroupNode is the root node of the schema tree
  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}
//------------------

} // namespace ROCKSDB_NAMESPACE
