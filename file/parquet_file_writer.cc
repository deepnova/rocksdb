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

IOStatus ParquetFileWriter::Append(const Slice& data, uint32_t crc32c_checksum,
                                    Env::IOPriority op_rate_limiter_priority) {
  if (seen_error()) {
    return AssertFalseAndGetStatusForPrevError();
  }

  const char* src = data.data();
  size_t left = data.size();
  IOStatus s;
  pending_sync_ = true;

  TEST_KILL_RANDOM_WITH_WEIGHT("ParquetFileWriter::Append:0", REDUCE_ODDS2);

  // Calculate the checksum of appended data
  //UpdateFileChecksum(data);

  {
    IOOptions io_options;
    //io_options.rate_limiter_priority =
    //    ParquetFileWriter::DecideRateLimiterPriority(
    //        writable_file_->GetIOPriority(), op_rate_limiter_priority);
    IOSTATS_TIMER_GUARD(prepare_write_nanos);
    TEST_SYNC_POINT("ParquetFileWriter::Append:BeforePrepareWrite");
    writable_file_->PrepareWrite(static_cast<size_t>(GetFileSize()), left,
                                 io_options, nullptr);
  }

  // See whether we need to enlarge the buffer to avoid the flush
  if (buf_.Capacity() - buf_.CurrentSize() < left) {
    for (size_t cap = buf_.Capacity();
         cap < max_buffer_size_;  // There is still room to increase
         cap *= 2) {
      // See whether the next available size is large enough.
      // Buffer will never be increased to more than max_buffer_size_.
      size_t desired_capacity = std::min(cap * 2, max_buffer_size_);
      if (desired_capacity - buf_.CurrentSize() >= left ||
          (use_direct_io() && desired_capacity == max_buffer_size_)) {
        buf_.AllocateNewBuffer(desired_capacity, true);
        break;
      }
    }
  }

  // Flush only when buffered I/O
  if (!use_direct_io() && (buf_.Capacity() - buf_.CurrentSize()) < left) {
    if (buf_.CurrentSize() > 0) {
      s = Flush(op_rate_limiter_priority);
      if (!s.ok()) {
        set_seen_error();
        return s;
      }
    }
    assert(buf_.CurrentSize() == 0);
  }

  if (perform_data_verification_ && buffered_data_with_checksum_ &&
      crc32c_checksum != 0) {
    // Since we want to use the checksum of the input data, we cannot break it
    // into several pieces. We will only write them in the buffer when buffer
    // size is enough. Otherwise, we will directly write it down.
    if (use_direct_io() || (buf_.Capacity() - buf_.CurrentSize()) >= left) {
      if ((buf_.Capacity() - buf_.CurrentSize()) >= left) {
        size_t appended = buf_.Append(src, left);
        if (appended != left) {
          s = IOStatus::Corruption("Write buffer append failure");
        }
        buffered_data_crc32c_checksum_ = crc32c::Crc32cCombine(
            buffered_data_crc32c_checksum_, crc32c_checksum, appended);
      } else {
        while (left > 0) {
          size_t appended = buf_.Append(src, left);
          buffered_data_crc32c_checksum_ =
              crc32c::Extend(buffered_data_crc32c_checksum_, src, appended);
          left -= appended;
          src += appended;

          if (left > 0) {
            s = Flush(op_rate_limiter_priority);
            if (!s.ok()) {
              break;
            }
          }
        }
      }
    } else {
      assert(buf_.CurrentSize() == 0);
      buffered_data_crc32c_checksum_ = crc32c_checksum;
      //s = WriteBufferedWithChecksum(src, left, op_rate_limiter_priority);
    }
  } else {
    // In this case, either we do not need to do the data verification or
    // caller does not provide the checksum of the data (crc32c_checksum = 0).
    //
    // We never write directly to disk with direct I/O on.
    // or we simply use it for its original purpose to accumulate many small
    // chunks
    if (use_direct_io() || (buf_.Capacity() >= left)) {
      while (left > 0) {
        size_t appended = buf_.Append(src, left);
        if (perform_data_verification_ && buffered_data_with_checksum_) {
          buffered_data_crc32c_checksum_ =
              crc32c::Extend(buffered_data_crc32c_checksum_, src, appended);
        }
        left -= appended;
        src += appended;

        if (left > 0) {
          s = Flush(op_rate_limiter_priority);
          if (!s.ok()) {
            break;
          }
        }
      }
    } else {
      // Writing directly to file bypassing the buffer
      assert(buf_.CurrentSize() == 0);
      if (perform_data_verification_ && buffered_data_with_checksum_) {
        buffered_data_crc32c_checksum_ = crc32c::Value(src, left);
        //s = WriteBufferedWithChecksum(src, left, op_rate_limiter_priority);
      } else {
        //s = WriteBuffered(src, left, op_rate_limiter_priority);
      }
    }
  }

  TEST_KILL_RANDOM("ParquetFileWriter::Append:1");
  if (s.ok()) {
    uint64_t cur_size = filesize_.load(std::memory_order_acquire);
    filesize_.store(cur_size + data.size(), std::memory_order_release);
  } else {
    set_seen_error();
  }
  return s;
}

IOStatus ParquetFileWriter::Flush(Env::IOPriority op_rate_limiter_priority) {
  if(op_rate_limiter_priority == Env::IO_MID) return AssertFalseAndGetStatusForPrevError(); //TODO: for passing compile
  return IOStatus::OK();
}

IOStatus ParquetFileWriter::Close() {
  return IOStatus::OK();
}

IOStatus ParquetFileWriter::Sync(bool use_fsync) {
  if(use_fsync == false) return AssertFalseAndGetStatusForPrevError(); // for passing compile
  return IOStatus::OK();
}

IOStatus ParquetFileWriter::SyncWithoutFlush(bool use_fsync) {
  if(use_fsync == false) return AssertFalseAndGetStatusForPrevError(); // for passing compile
  return IOStatus::OK();
}

std::string ParquetFileWriter::GetFileChecksum() {
  if (checksum_generator_ != nullptr) {
    assert(checksum_finalized_);
    return checksum_generator_->GetChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* ParquetFileWriter::GetFileChecksumFuncName() const {
  if (checksum_generator_ != nullptr) {
    return checksum_generator_->Name();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}

IOStatus ParquetFileWriter::Pad(const size_t pad_bytes,
                                Env::IOPriority op_rate_limiter_priority) {
  IOStatus s;
  if(pad_bytes > 0) s = Flush(op_rate_limiter_priority); //TODO: for passing compile
  return s;
}

}
