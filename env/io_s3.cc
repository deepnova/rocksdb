#include "env/io_s3.h"

namespace ROCKSDB_NAMESPACE {

/*
 * S3WritableFile
 *
 * Use S3 write to write data to a S3 object.
 */

S3WritableFile::S3WritableFile(const std::string& fname, int fd,
                               const EnvOptions& options)
    : FSWritableFile(options),
      filename_(fname),
      use_direct_io_(options.use_direct_writes),
      fd_(fd),
      filesize_(0),
      logical_sector_size_(logical_block_size) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  sync_file_range_supported_ = IsSyncFileRangeSupported(fd_);
#endif  // ROCKSDB_RANGESYNC_PRESENT
  assert(!options.use_mmap_writes);
}

S3WritableFile::~S3WritableFile() {
  if (fd_ >= 0) {
    IOStatus s = S3WritableFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}
}
