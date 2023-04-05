#pragma once

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
//#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <arrow/filesystem/s3fs.h>
//#include <arrow/filesystem/filesystem.h>
//#include <parquet/api/reader.h>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

using namespace arrow::fs;

//Tarim-TODO: not implement all API, call 'arrow::fs::parquet::ParquetFileWriter' directly
class S3WritableFile : public FSWritableFile {
 protected:
  const std::string filename_;
  std::shared_ptr<parquet::ParquetFileWriter> file_writer_;
  std::shared_ptr<::arrow::io::OutputStream> out_file_;
  //Tarim-TODO: schema
  std::shared_ptr<parquet::schema::GroupNode> schema_;
  size_t logical_sector_size_;
  uint64_t filesize_;

 public:
  explicit S3WritableFile(const std::string& fname,
                          size_t logical_block_size,
                          const EnvOptions& options);

  virtual ~S3WritableFile();

  // Need to implement this so the file is truncated correctly
  // with direct I/O
  virtual IOStatus Truncate(uint64_t size, const IOOptions& opts,
                            IODebugContext* dbg) override;
  virtual IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& opts,
                                    IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(
      const Slice& data, uint64_t offset, const IOOptions& opts,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  virtual IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  virtual bool IsSyncThreadSafe() const override;
  virtual bool use_direct_io() const override { return false; }
  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
  virtual uint64_t GetFileSize(const IOOptions& opts,
                               IODebugContext* dbg) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual IOStatus Allocate(uint64_t offset, uint64_t len,
                            const IOOptions& opts,
                            IODebugContext* dbg) override;
#endif
  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& opts,
                             IODebugContext* dbg) override;
#ifdef OS_LINUX
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif

  parquet::ParquetFileWriter* GetFileWriter() { return file_writer_.get(); }

  virtual IOStatus Open(const std::string& fname, arrow::fs::S3FileSystem *fs);

};

class S3SequentialFile : public FSSequentialFile {
 private:
  std::string filename_;
  //FILE* file_; //Tarim-TODO: S3 handler
  //int fd_;
  size_t logical_sector_size_;

 public:
  S3SequentialFile(const std::string& fname, FILE* file, int fd,
                      size_t logical_block_size, const EnvOptions& options);
  virtual ~S3SequentialFile();

  virtual IOStatus Read(size_t n, const IOOptions& opts, Slice* result,
                        char* scratch, IODebugContext* dbg) override;
  virtual IOStatus PositionedRead(uint64_t offset, size_t n,
                                  const IOOptions& opts, Slice* result,
                                  char* scratch, IODebugContext* dbg) override;
  virtual IOStatus Skip(uint64_t n) override;
  virtual IOStatus InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return false; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

class S3Directory : public FSDirectory {
 public:
  explicit S3Directory(const std::string& directory_name)
            :directory_name_(directory_name){}
  ~S3Directory(){}
  virtual IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override{
    return FsyncWithDirOptions(opts, dbg, DirFsyncOptions());
  }

  virtual IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override{
    return IOStatus::OK();
  }

  virtual IOStatus FsyncWithDirOptions(
      const IOOptions&, IODebugContext*,
      const DirFsyncOptions& /*dir_fsync_options*/) override{
    return IOStatus::OK();
  }

 private:
  const std::string directory_name_;
};

}
