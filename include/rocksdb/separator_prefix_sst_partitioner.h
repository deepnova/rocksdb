#pragma once

#include <memory>
#include <string>
#include <algorithm>

#include "rocksdb/sst_partitioner.h"
//#include "rocksdb/customizable.h"
//#include "rocksdb/rocksdb_namespace.h"
//#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class SstPartitionerSeparatorPrefix : public SstPartitioner {
 public:
  explicit SstPartitionerSeparatorPrefix(std::string separator, uint64_t max_size = 268435456 /* 256MB */) 
          : separator_(separator), max_size_(max_size) {}

  virtual ~SstPartitionerSeparatorPrefix() override {}

  const char* Name() const override { return "SstPartitionerSeparatorPrefix"; }

  PartitionerResult ShouldPartition(const PartitionerRequest& request) override { //Tarim-TODO: last two level
    if(request.current_output_file_size > max_size_) return kRequired;
    const static size_t MAX_BUF_SIZE = 128; 
    char last_key[MAX_BUF_SIZE] = {0}, current_key[MAX_BUF_SIZE] = {0};

    memcpy(last_key, request.prev_user_key->data(), std::min(request.prev_user_key->size(), MAX_BUF_SIZE));
    char *last_key_prefix = strtok(last_key, separator_.c_str());

    //Slice current_key(*request.current_user_key);
    memcpy(current_key, request.current_user_key->data(), std::min(request.current_user_key->size(), MAX_BUF_SIZE));
    char *current_key_prefix = strtok(current_key, separator_.c_str());
    printf("last key: %s, prefix: %s, current key: %s, prefix: %s.\n", 
                    request.prev_user_key->data(), last_key_prefix,
                    request.current_user_key->data(), current_key_prefix);

    return strcmp(last_key_prefix, current_key_prefix) == 0 ? kNotRequired 
                                                            : kRequired;
  }

  bool CanDoTrivialMove(const Slice& smallest_user_key,
                        const Slice& largest_user_key) override {
    return ShouldPartition(PartitionerRequest(smallest_user_key, largest_user_key,
                                            0)) == kNotRequired;
  }

 private:
  std::string separator_;
  uint64_t max_size_ = 0;
};

/*
 * Factory for prefix partitioner, prefix from start to 'separator' of key string.
 */
class SstPartitionerSeparatorPrefixFactory : public SstPartitionerFactory {
 public:
  explicit SstPartitionerSeparatorPrefixFactory(std::string separator) : separator_(separator) {}

  ~SstPartitionerSeparatorPrefixFactory() override {}

  static const char* kClassName() { return "SstPartitionerSeparatorPrefixFactory"; }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override{
    return std::unique_ptr<SstPartitioner>(new SstPartitionerSeparatorPrefix(separator_));
  }

 private:
  std::string separator_;
};

std::shared_ptr<SstPartitionerFactory> NewSstPartitionerSeparatorPrefixFactory(
    std::string separator) {
  return std::make_shared<SstPartitionerSeparatorPrefixFactory>(separator);
}

}
