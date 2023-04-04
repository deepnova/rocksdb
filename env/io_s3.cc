#include "env/io_s3.h"

namespace ROCKSDB_NAMESPACE {

/*
 * S3WritableFile
 *
 * Use S3 write to write data to a S3 object.
 */

S3WritableFile::S3WritableFile(const std::string& fname,
                               size_t logical_block_size,
                               const EnvOptions& options)
    : FSWritableFile(options),
      filename_(fname),
      logical_sector_size_(logical_block_size),
      filesize_(0) {
}

S3WritableFile::~S3WritableFile() {
  if (file_writer_ != nullptr) {
    IOStatus s = S3WritableFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}
}
