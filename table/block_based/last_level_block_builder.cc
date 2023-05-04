#include "table/block_based/last_level_block_builder.h"

#include <assert.h>

#include <algorithm>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/block_based/data_block_footer.h"
#include "util/coding.h"
#include "logging/logging.h"

//#include "logging/logging.h"

// parquet sample code
#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>
//------------------

namespace ROCKSDB_NAMESPACE {

LastLevelBlockBuilder::LastLevelBlockBuilder(Logger* logger)
    : logger_(logger) {
  decoder_ = avro::binaryDecoder();
  //Tarim-TODO: Is there parqeut file index?
}

void LastLevelBlockBuilder::Add(const Slice& /*user_key*/, const Slice& value,
                       const Slice* const /*delta_value*/) {
  avro::InputStreamPtr is = avro::memoryInputStream((const uint8_t*)value.data(), value.size());
  decoder_->init(*is);

  //Tarim-TODO: not support schema evolution yet
  assert(record_->fieldCount() == (size_t)rg_writer_->num_columns());

  // only one record
  avro::GenericRecord& r = *record_;
  size_t c = r.schema()->leaves();
  for (size_t i = 0; i < c; ++i) {
    avro::decode(*decoder_, r.fieldAt(i));
    parquet::ColumnWriter* cw = rg_writer_->column(i);
    // types
    switch(r.fieldAt(i).type()){
      case avro::AVRO_STRING:
        //parquet::ByteArray s(r.fieldAt(i).value<std::string>());
        //static_cast<parquet::ByteArrayWriter*>(cw)->WriteBatch(1, nullptr, nullptr, &s);
        break;
      case avro::AVRO_INT:
        static_cast<parquet::Int32Writer*>(cw)->WriteBatch(1, nullptr, nullptr, &r.fieldAt(i).value<int32_t>());
        break;
      case avro::AVRO_LONG:
        static_cast<parquet::Int64Writer*>(cw)->WriteBatch(1, nullptr, nullptr, &r.fieldAt(i).value<int64_t>());
        break;
      case avro::AVRO_FLOAT:
        static_cast<parquet::FloatWriter*>(cw)->WriteBatch(1, nullptr, nullptr, &r.fieldAt(i).value<float>());
        break;
      case avro::AVRO_DOUBLE:
        static_cast<parquet::DoubleWriter*>(cw)->WriteBatch(1, nullptr, nullptr, &r.fieldAt(i).value<double>());
        break;
      case avro::AVRO_BOOL:
        static_cast<parquet::BoolWriter*>(cw)->WriteBatch(1, nullptr, nullptr, &r.fieldAt(i).value<bool>());
        break;
      case avro::AVRO_ENUM: 
        //cw->WriteBatch(1, nullptr, nullptr, &r.fieldAt(i).value<avro::GenericEnum>().value());
        //break;
      case avro::AVRO_BYTES:
      case avro::AVRO_RECORD:
      case avro::AVRO_ARRAY:
      case avro::AVRO_MAP:
      case avro::AVRO_FIXED:
      case avro::AVRO_SYMBOLIC:
      case avro::AVRO_UNION:
      case avro::AVRO_NULL:
      default:
        ROCKS_LOG_WARN(logger_, "not support avro type: %s\n", avro::toString(r.fieldAt(i).type()).c_str());
        break;
    }
  }
}

inline void LastLevelBlockBuilder::Reset() {
  rg_writer_->Close();
  rg_writer_ = nullptr; //Tarim-TODO: not sure it's OK?
}

void LastLevelBlockBuilder::Finish() {
  rg_writer_->Close();
  rg_writer_ = nullptr; //Tarim-TODO: not sure it's OK?
}

}  // namespace ROCKSDB_NAMESPACE

