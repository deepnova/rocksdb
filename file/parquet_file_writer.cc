#include "file/parquet_file_writer.h"

#include "util/crc32c.h"
#include "monitoring/iostats_context_imp.h"

namespace ROCKSDB_NAMESPACE {

IOStatus ParquetFileWriter::Create(const std::shared_ptr<FileSystem>& fs,
                                    const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<ParquetFileWriter>* writer,
                                    IODebugContext* dbg) {
  if (file_opts.use_direct_writes &&
      0 == file_opts.writable_file_max_buffer_size) {
    return IOStatus::InvalidArgument(
        "Direct write requires writable_file_max_buffer_size > 0");
  }
  std::unique_ptr<FSWritableFile> file;
  IOStatus io_s = fs->NewWritableFile(fname, file_opts, &file, dbg);
  if (io_s.ok()) {
    writer->reset(new ParquetFileWriter(std::move(file), fname, file_opts));
  }
  return io_s;
}

IOStatus ParquetFileWriter::Append(const Slice& /*data*/, uint32_t /*crc32c_checksum*/,
                                    Env::IOPriority /*AppendRowGroup*/) {
  //use AppendRowGroup() instead.
  return IOStatus::NotSupported("Append()");
}

IOStatus ParquetFileWriter::Flush(Env::IOPriority /*op_rate_limiter_priority*/) {
  return IOStatus::OK();
}

IOStatus ParquetFileWriter::Close() {
  //Tarim-TODO:
  return IOStatus::OK();
}

IOStatus ParquetFileWriter::Sync(bool /*use_fsync*/) {
  // do nothing for S3
  return IOStatus::OK();
}

IOStatus ParquetFileWriter::SyncWithoutFlush(bool /*use_fsync*/) {
  // do nothing for S3
  return IOStatus::OK();
}

std::string ParquetFileWriter::GetFileChecksum() {
  //Tarim-TODO: There must be?
  return kUnknownFileChecksum;
}

const char* ParquetFileWriter::GetFileChecksumFuncName() const {
  //Tarim-TODO: There must be?
  return kUnknownFileChecksumFuncName;
}

IOStatus ParquetFileWriter::Pad(const size_t /*pad_bytes*/,
                                Env::IOPriority /*op_rate_limiter_priority*/) {
  return IOStatus::NotSupported("Pad()");
}

void ParquetFileWriter::UpdateFileChecksum(const Slice& /*data*/) {
  //Tarim-TODO: There must be?
}

// Currently, crc32c checksum is used to calculate the checksum value of the
// content in the input buffer for handoff. In the future, the checksum might be
// calculated from the existing crc32c checksums of the in WAl and Manifest
// records, or even SST file blocks.
// TODO: effectively use the existing checksum of the data being writing to
// generate the crc32c checksum instead of a raw calculation.
void WritableFileWriter::Crc32cHandoffChecksumCalculation(const char* /*data*/,
                                                          size_t /*size*/,
                                                          char* /*buf*/) {
  //Tarim-TODO: There must be?
}

}
