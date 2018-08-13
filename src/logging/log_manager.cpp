/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"
#include "common/logger.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
  if (flush_thread_on == false) {
    ENABLE_LOGGING = true;
    flush_thread_on = true;
    flush_thread_ = new std::thread(&LogManager::FlushThread, this);
  }
}

void LogManager::FlushThread() {
  std::unique_lock<std::mutex> lock(latch_);
  while (flush_thread_on) {
    cv_.wait_for(lock, LOG_TIMEOUT);
    if (log_buffer_size_ != 0) {
      std::swap(flush_buffer_, log_buffer_);
      flush_buffer_size_ = log_buffer_size_;
      log_buffer_size_ = 0;
      disk_manager_->WriteLog(flush_buffer_, flush_buffer_size_);
      int32_t size = 0;
      while (size < flush_buffer_size_) {
        auto rec = reinterpret_cast<LogRecord *>(flush_buffer_ + size);
        persistent_lsn_ = rec->GetLSN();
        size += rec->GetSize();
      }
      assert(persistent_lsn_ != INVALID_LSN);
      flush_buffer_size_ = 0;
    }
    flush.notify_all();
  }
}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  if (flush_thread_on == true) {
    ENABLE_LOGGING = false;
    Flush();
    flush_thread_on = false;
    flush_thread_->join();
    delete flush_thread_;
  }
}

void LogManager::Flush() {
  FlushImpl();
  FlushImpl();
}

void LogManager::FlushImpl() {
  cv_.notify_all();
  std::unique_lock<std::mutex> lock(latch_);
  if (flush_thread_on) {
    flush.wait(lock);
  }
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  std::unique_lock<std::mutex> guard(append_latch_);
  log_record.lsn_ = next_lsn_++;
  if (log_record.GetSize() + log_buffer_size_ > LOG_BUFFER_SIZE) {
    Flush();
  }
  int pos = log_buffer_size_;
  memcpy(log_buffer_ + pos, &log_record, LogRecord::HEADER_SIZE);
  pos += LogRecord::HEADER_SIZE;

  if (log_record.log_record_type_ == LogRecordType::INSERT) {
    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE
      || log_record.log_record_type_ == LogRecordType::MARKDELETE
      || log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {
    memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
    memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
    pos += sizeof(RID);
    log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
    pos += log_record.old_tuple_.GetLength() + sizeof(int32_t);
    log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
    memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(log_record.prev_page_id_));
  } else {
    // nothing
  }
  log_buffer_size_ += log_record.GetSize();
  return log_record.lsn_;
}

} // namespace cmudb
