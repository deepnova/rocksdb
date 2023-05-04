
#pragma once

#include "rocksdb/slice.h"
#include "rocksdb/table.h"
//#include "table/block_based/block_builder.h"

#include <avro/ValidSchema.hh>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
//#include <parquet/api/reader.h>
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

  explicit LastLevelBlockBuilder(Logger* logger);

  // Reset the contents as if the LastLevelBlockBuilder was just constructed.
  void Reset();

  // Swap the contents in LastLevelBlockBuilder with buffer, then reset the LastLevelBlockBuilder.
  //void SwapAndReset(std::string& buffer);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // DO NOT mix with AddWithLastKey() between Resets. For efficiency, use
  // AddWithLastKey() in contexts where previous added key is already known
  // and delta encoding might be used.
  void Add(const Slice& key, const Slice& value,
           const Slice* const delta_value = nullptr);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  inline void Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  inline size_t CurrentSizeEstimate() const {
    return (size_t)rg_writer_->total_bytes_written();
  }

  inline size_t CurrentCompressedSizeEstimate() const {
    return (size_t)rg_writer_->total_compressed_bytes_written();
  }

  inline int CurrentRows() const {
    return rg_writer_->num_rows();
  }

  // Returns an estimated block size after appending key and value.
  //size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return rg_writer_ == nullptr || rg_writer_->num_rows() == 0;
  }

  void SetSchema(const avro::ValidSchema *schema) { // call as early as possible
    assert(schema != nullptr);
    schema_ptr_ = schema;
    record_ = std::make_unique<avro::GenericRecord>(schema_ptr_->root());
  }

  bool HasRowGroup(){
    return rg_writer_ != nullptr;
  }

  void ResetRowGroup(parquet::RowGroupWriter* rg_writer){
    assert(rg_writer != nullptr);
    rg_writer_ = rg_writer;
  }

 private:
  Logger* logger_;
  const avro::ValidSchema* schema_ptr_ = nullptr;
  parquet::RowGroupWriter* rg_writer_ = nullptr;
  std::unique_ptr<avro::GenericRecord> record_; //Tarim-TODO
  avro::DecoderPtr decoder_;
};

} // namespace ROCKSDB_NAMESPACE
