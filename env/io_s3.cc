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

IOStatus S3WritableFile::Open(const std::string& fname, arrow::fs::S3FileSystem *fs) {

    auto&& res2 = fs->OpenOutputStream(fname);
    std::cout << "S3 OpenOutputStream status: " << res2.status().CodeAsString() << std::endl;
    if(res2.status().code() != ::arrow::StatusCode::OK) {
        std::cerr << "S3 OpenOutputStream error, status: " << res2.status().CodeAsString() << std::endl;
        return IOStatus::IOError(res2.status().message());
    }

    out_file_ = std::move(res2).ValueOrDie();
    
    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::ACT_SNAPPY); //Tarim-TODO: configurable
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    
    file_writer_ = parquet::ParquetFileWriter::Open(out_file_, schema_, props);

    return IOStatus::OK();
}

IOStatus S3WritableFile::Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  return IOStatus::NotSupported("S3WritableFile not supported Truncate().");
}

IOStatus S3WritableFile::Append(const Slice& data, const IOOptions& /*opts*/,
                                   IODebugContext* /*dbg*/) {
 //Tarim-TODO: incomplete
  size_t nbytes = data.size();
  filesize_ += nbytes;
  return IOStatus::OK();
}

IOStatus S3WritableFile::PositionedAppend(const Slice& /*data*/, uint64_t /*offset*/,
                                             const IOOptions& /*opts*/,
                                             IODebugContext* /*dbg*/) {
  return IOStatus::NotSupported("S3WritableFile not supported PositionedAppend().");
}

IOStatus S3WritableFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  //Tarim-TODO: need flush?
  file_writer_->Close();
  arrow::Status s = out_file_->Close();
  if(s.ok() == false){
    return IOStatus::IOError(Status::ArrowErrorStr(s));
  }
  return IOStatus::OK();
}

IOStatus S3WritableFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  arrow::Status s = out_file_->Flush();
  if(s.ok() == false){
    return IOStatus::IOError(Status::ArrowErrorStr(s));
  }
  return IOStatus::OK();
}

IOStatus S3WritableFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  return IOStatus::NotSupported("S3WritableFile not supported Sync().");
}

IOStatus S3WritableFile::Fsync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  return IOStatus::NotSupported("S3WritableFile not supported Fsync().");
}

}
