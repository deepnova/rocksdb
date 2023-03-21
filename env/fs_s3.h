#pragma once

#include <arrow/io/file.h>
#include <arrow/util/logging.h>
//#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <arrow/filesystem/s3fs.h>
//#include <arrow/filesystem/filesystem.h>

#include "env/io_s3.h"

namespace ROCKSDB_NAMESPACE {

class ShortRetryStrategy : public arrow::fs::S3RetryStrategy {
 public:
  bool ShouldRetry(const AWSErrorDetail& error, int64_t attempted_retries) override {
    if (error.message.find(kFileExistsMessage) != error.message.npos) {
      // Minio returns "file exists" errors (when calling mkdir) as internal errors,
      // which would trigger spurious retries.
      return false;
    }
    return IsRetryable(error) && (attempted_retries * kRetryInterval < kMaxRetryDuration);
  }

  int64_t CalculateDelayBeforeNextRetry(const AWSErrorDetail& error,
                                        int64_t attempted_retries) override {
    return kRetryInterval;
  }

  bool IsRetryable(const AWSErrorDetail& error) const {
    return error.should_retry || error.exception_name == "XMinioServerNotInitialized";
  }

#ifdef _WIN32
  static constexpr const char* kFileExistsMessage = "file already exists";
#else
  static constexpr const char* kFileExistsMessage = "file exists";
#endif
};

//TODO: use arrow::fs::S3FileSystem directly?
//TODO: if need support HDFS..., add an extra layer abstraction.
class S3FileSystem : public FileSystem {
 public:
  S3FileSystem(const std::string endpoint,
               const std::string& access_key, 
               const std::string& secret_key)
          : endpoint_(endpoint),
            access_key_(access_key),
            secret_key_(secret_key) {
    S3GlobalOptions options;
    options.log_level = S3LogLevel::Warn; //TODO: configurable
    InitializeS3(options);
    S3Connect();
  }

  ~S3FileSystem() override {

  }

  static const char* kClassName() { return "S3FileSystem"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kDefaultName(); }

  bool IsInstanceOf(const std::string& name) const override {
    if (name == "s3") {
      return true;
    } else {
      return FileSystem::IsInstanceOf(name);
    }
  }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* /*dbg*/) override {
    result->reset();
    //TODO: not understand what it's for.
    return IOStatus::OK();
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* /*dbg*/) override {
    result->reset();
    IOStatus s = IOStatus::OK();
    //TODO: not understand what it's for.
    return s;
  }

  virtual IOStatus OpenWritableFile(const std::string& fname,
                                    const FileOptions& options, bool reopen,
                                    std::unique_ptr<FSWritableFile>* result,
                                    IODebugContext* /*dbg*/) {
    result->reset();
    IOStatus s;

    if(reopen == true){
      return IOStatus::NotSupported("S3 object not supported re-open.");
    }
    //TODO: schema
    
    auto&& res2 = fs_->OpenOutputStream(fname);
    std::cout << "S3 OpenOutputStream status: " << result.status() << std::endl;
    if(res2.status().code() != ::arrow::StatusCode::OK) {
        std::cerr << "S3 OpenOutputStream error, status: " << result.status() << std::endl;
        return IOStatus::IOError(result.status().message());
    }
    out_file_ = std::move(res2).ValueOrDie();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY); //TODO: configurable
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    
        parquet::ParquetFileWriter::Open(out_file_, schema_, props);
    result->reset(new S3WritableFile(fname, file_writer_));
    //    new PosixWritableFile(fname, fd,
    //                          GetLogicalBlockSizeForWriteIfNeeded(
    //                              no_mmap_writes_options, fname, fd),
    //                          no_mmap_writes_options));
    return s;
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    return OpenWritableFile(fname, options, false, result, dbg);
  }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    return OpenWritableFile(fname, options, true, result, dbg);
  }

  IOStatus ReuseWritableFile(const std::string& fname,
                             const std::string& old_fname,
                             const FileOptions& options,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* /*dbg*/) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus NewRandomRWFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* /*dbg*/) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& /*opts*/,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* /*dbg*/) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& opts,
                       std::vector<std::string>* result,
                       IODebugContext* /*dbg*/) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    IOStatus result;
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus CreateDir(const std::string& name, const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    //TODO: ???
    return IOStatus::NotSupported();
  }

  IOStatus CreateDirIfMissing(const std::string& name,
                              const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus DeleteDir(const std::string& name, const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& /*opts*/,
                       uint64_t* size, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*opts*/,
                                   uint64_t* file_mtime,
                                   IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus LinkFile(const std::string& src, const std::string& target,
                    const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus NumFileLinks(const std::string& fname, const IOOptions& /*opts*/,
                        uint64_t* count, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus AreFilesSame(const std::string& first, const std::string& second,
                        const IOOptions& /*opts*/, bool* res,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus LockFile(const std::string& fname, const IOOptions& /*opts*/,
                    FileLock** lock, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus UnlockFile(FileLock* lock, const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*opts*/, std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetTestDirectory(const IOOptions& /*opts*/, std::string* result,
                            IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus GetFreeSpace(const std::string& fname, const IOOptions& /*opts*/,
                        uint64_t* free_space,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& /*opts*/,
                       bool* is_dir, IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported();
  }

  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override {
    return IOStatus::NotSupported();
  }

  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override {
    return IOStatus::NotSupported();
  }

 private:
  IOStatus S3Connect()
  {
    options_.endpoint_override = endpoint;
    options_.scheme = "http";
    options_.ConfigureAccessKey(access_key, secret_key);
    if (!options_.retry_strategy) {
      options_.retry_strategy = std::make_shared<ShortRetryStrategy>();
    }
    auto&& result = S3FileSystem::Make(options_);
    std::cout << "S3FileSystem make status: " << result.status() << std::endl;
    if(result.status().code() != ::arrow::StatusCode::OK) {
        std::cerr << "S3FileSystem make error, status: " << result.status() << std::endl;
        return IOStatus::IOError();
    }
    fs_ = std::move(result).ValueOrDie();
    return IOStatus::OK();
  }

 private:
  std::shared_ptr<arrow::fs::S3FileSystem> fs_;
  std::shared_ptr<parquet::ParquetFileWriter> file_writer_;
  std::shared_ptr<::arrow::io::OutputStream> out_file_;
  S3Options options_;
  std::shared_ptr<GroupNode> schema_;
  std::string& endpoint_;
  const std::string& access_key_;
  const std::string& secret_key_;
};

}
