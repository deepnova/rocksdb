#pragma once

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

  int64_t CalculateDelayBeforeNextRetry(const AWSErrorDetail& /*error*/,
                                        int64_t /*attempted_retries*/) override {
    return kRetryInterval;
  }

  bool IsRetryable(const AWSErrorDetail& error) const {
    return error.should_retry || error.exception_name == "XMinioServerNotInitialized";
  }

 static constexpr int32_t kRetryInterval = 50;      /* milliseconds */
 static constexpr int32_t kMaxRetryDuration = 6000; /* milliseconds */

#ifdef _WIN32
  static constexpr const char* kFileExistsMessage = "file already exists";
#else
  static constexpr const char* kFileExistsMessage = "file exists";
#endif
};

//Tarim-TODO: use arrow::fs::S3FileSystem directly?
//Tarim-TODO: if need support HDFS..., add an extra layer abstraction.
class S3FileSystem : public FileSystem {
 public:
  explicit S3FileSystem(const S3Endpoint & s3_endpoint)
          : s3_endpoint_(s3_endpoint) {
    arrow::fs::S3GlobalOptions options;
    options.log_level = arrow::fs::S3LogLevel::Warn; //Tarim-TODO: configurable
    auto status = arrow::fs::InitializeS3(options);
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

  IOStatus NewSequentialFile(const std::string& /*fname*/,
                             const FileOptions& /*options*/,
                             std::unique_ptr<FSSequentialFile>* /*result*/,
                             IODebugContext* /*dbg*/) override {
    //result->reset();
    //Tarim-TODO: not understand what it's for.
    return IOStatus::NotSupported("S3 object not supported NewSequentialFile().");
  }

  IOStatus NewRandomAccessFile(const std::string& /*fname*/,
                               const FileOptions& /*options*/,
                               std::unique_ptr<FSRandomAccessFile>* /*result*/,
                               IODebugContext* /*dbg*/) override {
    //result->reset();
    return IOStatus::NotSupported("S3 object not supported NewRandomAccessFile().");
  }

  virtual IOStatus OpenWritableFile(const std::string& fname,
                                    const FileOptions& /*options*/, bool reopen,
                                    std::unique_ptr<FSWritableFile>* result,
                                    IODebugContext* /*dbg*/) {
    result->reset();
    IOStatus s;

    if(reopen == true){
      return IOStatus::NotSupported("S3 object not supported re-open.");
    }
    
    auto&& res2 = fs_->OpenOutputStream(fname);
    //std::cout << "S3 OpenOutputStream status: " << res2.status().code() << std::endl;
    if(res2.status().code() != ::arrow::StatusCode::OK) {
        //std::cerr << "S3 OpenOutputStream error, status: " << res2.status().code() << std::endl;
        return IOStatus::IOError(res2.status().message());
    }
/*
    out_file_ = std::move(res2).ValueOrDie();

    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::ACT_SNAPPY); //Tarim-TODO: configurable
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    
    parquet::ParquetFileWriter::Open(out_file_, schema_, props);
    result->reset(new S3WritableFile(fname, file_writer_));
    */
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

  IOStatus ReuseWritableFile(const std::string& /*fname*/,
                             const std::string& /*old_fname*/,
                             const FileOptions& /*options*/,
                             std::unique_ptr<FSWritableFile>* /*result*/,
                             IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported ReuseWritableFile().");
  }

  IOStatus NewRandomRWFile(const std::string& /*fname*/,
                           const FileOptions& /*options*/,
                           std::unique_ptr<FSRandomRWFile>* /*result*/,
                           IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported NewRandomRWFile().");
  }

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) override {
    return IOStatus::NotSupported("S3 object not supported NewMemoryMappedFileBuffer().");
  }

  IOStatus NewDirectory(const std::string& /*name*/,
                        const IOOptions& /*opts*/,
                        std::unique_ptr<FSDirectory>* /*result*/,
                        IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus FileExists(const std::string& /*fname*/,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus GetChildren(const std::string& /*dir*/,
                       const IOOptions& /*opts*/,
                       std::vector<std::string>* /*result*/,
                       IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus DeleteFile(const std::string& /*fname*/,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    IOStatus result;
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus CreateDir(const std::string& /*name*/,
                     const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus CreateDirIfMissing(const std::string& /*name*/,
                              const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus DeleteDir(const std::string& /*name*/,
                     const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus GetFileSize(const std::string& /*fname*/,
                       const IOOptions& /*opts*/,
                       uint64_t* /*size*/,
                       IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*opts*/,
                                   uint64_t* /*file_mtime*/,
                                   IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus RenameFile(const std::string& /*src*/,
                      const std::string& /*target*/,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported();
  }

  IOStatus LinkFile(const std::string& /*src*/,
                    const std::string& /*target*/,
                    const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported LinkFile().");
  }

  IOStatus NumFileLinks(const std::string& /*fname*/,
                        const IOOptions& /*opts*/,
                        uint64_t* /*count*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported NumFileLinks().");
  }

  IOStatus AreFilesSame(const std::string& /*first*/,
                        const std::string& /*second*/,
                        const IOOptions& /*opts*/,
                        bool* /*res*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported AreFilesSame().");
  }

  IOStatus LockFile(const std::string& /*fname*/,
                    const IOOptions& /*opts*/,
                    FileLock** /*lock*/,
                    IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3 object not supported LockFile().");
  }

  IOStatus UnlockFile(FileLock* /*lock*/,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3 object not supported UnlockFile().");
  }

  IOStatus GetAbsolutePath(const std::string& /*db_path*/,
                           const IOOptions& /*opts*/,
                           std::string* /*output_path*/,
                           IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3 object not supported GetAbsolutePath().");
  }

  IOStatus GetTestDirectory(const IOOptions& /*opts*/,
                            std::string* /*result*/,
                            IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported GetTestDirectory().");
  }

  IOStatus GetFreeSpace(const std::string& /*fname*/,
                        const IOOptions& /*opts*/,
                        uint64_t* /*free_space*/,
                        IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3 object not supported GetFreeSpace().");
  }

  IOStatus IsDirectory(const std::string& /*path*/,
                       const IOOptions& /*opts*/,
                       bool* /*is_dir*/,
                       IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3 object not supported IsDirectory().");
  }

  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& /*db_options*/) const override {
    FileOptions optimized = file_options;
    // Tarim-TODO: do nothing
    return optimized;
  }

  FileOptions OptimizeForManifestWrite(const FileOptions& file_options) const override {
    FileOptions optimized = file_options;
    // Tarim-TODO: do nothing
    return optimized;
  }

 private:
  IOStatus S3Connect()
  {
    //Tarim-TODO: reconnect if disconnected
    options_.endpoint_override = s3_endpoint_.endpoint;
    options_.scheme = "http";
    options_.ConfigureAccessKey(s3_endpoint_.access_key, s3_endpoint_.secret_key);
    if (!options_.retry_strategy) {
      options_.retry_strategy = std::make_shared<ShortRetryStrategy>();
    }
    auto&& result = arrow::fs::S3FileSystem::Make(options_);
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
  arrow::fs::S3Options options_;
  S3Endpoint s3_endpoint_;
};

}
