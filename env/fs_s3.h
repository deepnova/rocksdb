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
    return IOStatus::NotSupported("S3FileSystem not supported NewSequentialFile().");
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* /*dbg*/) override {
    result->reset();

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> result2 = fs_->OpenInputFile(fname);
    if(result2.status().code() != ::arrow::StatusCode::OK) {
      std::cerr << "S3RandomAccessFile open error, file: " << fname
                << ", status: " << result2.status().CodeAsString() << std::endl;
      return IOStatus::IOError(result2.status().message());
    }

    result->reset(new S3RandomAccessFile(fname, std::move(result2).ValueOrDie(), options));

    return IOStatus::OK();
  }

  virtual IOStatus OpenWritableFile(const std::string& fname,
                                    const FileOptions& options, bool reopen,
                                    std::unique_ptr<FSWritableFile>* result,
                                    IODebugContext* /*dbg*/) {
    result->reset();
    IOStatus s;

    if(reopen == true){
      return IOStatus::NotSupported("S3FileSystem not supported re-open.");
    }

    static size_t logical_sector_size = 64 * 1024; //Tarim-TODO: may not need
    result->reset(new S3WritableFile(fname, logical_sector_size, options));

    S3WritableFile* fp = static_cast<S3WritableFile*>(result->get());
    s = fp->Open(fname, fs_.get());
    
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
    return IOStatus::NotSupported("S3FileSystem not supported ReuseWritableFile().");
  }

  IOStatus NewRandomRWFile(const std::string& /*fname*/,
                           const FileOptions& /*options*/,
                           std::unique_ptr<FSRandomRWFile>* /*result*/,
                           IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported NewRandomRWFile().");
  }

  IOStatus NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported NewMemoryMappedFileBuffer().");
  }

  IOStatus NewDirectory(const std::string& /*name*/,
                        const IOOptions& /*opts*/,
                        std::unique_ptr<FSDirectory>* /*result*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported NewDirectory().");
  }

  IOStatus FileExists(const std::string& fname,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    arrow::Result<arrow::fs::FileInfo> result = fs_->GetFileInfo(fname); 
    if(result.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(result.status()));
    }
    if(result->type() == arrow::fs::FileType::NotFound){ //Tarim-TODO: is result.ok() == true in this case?
      return IOStatus::NotFound();
    }
    return IOStatus::OK();
  }

  IOStatus GetChildren(const std::string& /*dir*/,
                       const IOOptions& /*opts*/,
                       std::vector<std::string>* /*result*/,
                       IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3FileSystem not supported GetChildren().");
  }

  IOStatus DeleteFile(const std::string& fname,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    arrow::Status s = fs_->DeleteFile(fname);
    if(s.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(s));
    } 
    return IOStatus::OK();
  }

  IOStatus CreateDir(const std::string& name,
                     const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    arrow::Status s = fs_->CreateDir(name, true);
    if(s.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(s));
    } 
    return IOStatus::OK();
  }

  IOStatus CreateDirIfMissing(const std::string& name,
                              const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) override {
    arrow::Status s = fs_->CreateDir(name, true);
    if(s.ok() == false){
      //Tarim-TODO: success if there is 'directory exists' error.
      return IOStatus::IOError(Status::ArrowErrorStr(s));
    } 
    return IOStatus::OK();
  }

  IOStatus DeleteDir(const std::string& name,
                     const IOOptions& /*opts*/,
                     IODebugContext* /*dbg*/) override {
    arrow::Status s = fs_->DeleteDir(name);
    if(s.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(s));
    } 
    return IOStatus::OK();
  }

  IOStatus GetFileSize(const std::string& fname,
                       const IOOptions& /*opts*/,
                       uint64_t* size,
                       IODebugContext* /*dbg*/) override {
    arrow::Result<arrow::fs::FileInfo> result = fs_->GetFileInfo(fname); 
    if(result.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(result.status()));
    }
    *size = result->size();
    return IOStatus::OK();
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& /*opts*/,
                                   uint64_t* file_mtime, // seconds
                                   IODebugContext* /*dbg*/) override {
    arrow::Result<arrow::fs::FileInfo> result = fs_->GetFileInfo(fname); 
    if(result.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(result.status()));
    }
    arrow::fs::TimePoint mtime = result->mtime(); // nanoseconds
    *file_mtime = mtime.time_since_epoch().count() % static_cast<uint64_t>(1000000000);
    return IOStatus::OK();
  }

  IOStatus RenameFile(const std::string& src,
                      const std::string& target,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    arrow::Status s = fs_->Move(src, target);
    if(s.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(s));
    } 
    return IOStatus::OK();
  }

  IOStatus LinkFile(const std::string& /*src*/,
                    const std::string& /*target*/,
                    const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported LinkFile().");
  }

  IOStatus NumFileLinks(const std::string& /*fname*/,
                        const IOOptions& /*opts*/,
                        uint64_t* /*count*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported NumFileLinks().");
  }

  IOStatus AreFilesSame(const std::string& /*first*/,
                        const std::string& /*second*/,
                        const IOOptions& /*opts*/,
                        bool* /*res*/,
                        IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported AreFilesSame().");
  }

  IOStatus LockFile(const std::string& /*fname*/,
                    const IOOptions& /*opts*/,
                    FileLock** /*lock*/,
                    IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3FileSystem not supported LockFile().");
  }

  IOStatus UnlockFile(FileLock* /*lock*/,
                      const IOOptions& /*opts*/,
                      IODebugContext* /*dbg*/) override {
    //Tarim-TODO:
    return IOStatus::NotSupported("S3FileSystem not supported UnlockFile().");
  }

  IOStatus GetAbsolutePath(const std::string& db_path,
                           const IOOptions& /*opts*/,
                           std::string* output_path,
                           IODebugContext* /*dbg*/) override {
    //Tarim-TODO: if db_path[0] == '/' ?
    *output_path = db_path;
    return IOStatus::OK();
  }

  IOStatus GetTestDirectory(const IOOptions& /*opts*/,
                            std::string* /*result*/,
                            IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("S3FileSystem not supported GetTestDirectory().");
  }

  IOStatus GetFreeSpace(const std::string& /*fname*/,
                        const IOOptions& /*opts*/,
                        uint64_t* free_space,
                        IODebugContext* /*dbg*/) override {
    //Tarim-TODO: a rough value, 100GB 
    *free_space = 107374182400;
    return IOStatus::OK();
  }

  IOStatus IsDirectory(const std::string& path,
                       const IOOptions& /*opts*/,
                       bool* is_dir,
                       IODebugContext* /*dbg*/) override {
    arrow::Result<arrow::fs::FileInfo> result = fs_->GetFileInfo(path); 
    if(result.ok() == false){
      return IOStatus::IOError(Status::ArrowErrorStr(result.status()));
    }
    *is_dir = result->IsDirectory();
    return IOStatus::OK();
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
