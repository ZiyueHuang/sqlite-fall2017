/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 *
 */
bool LogRecovery::DeserializeLogRecord(const char *data, int size,
                                       LogRecord &log_record) {
  if (size < 4) { return false; }
  LogRecord *ptr = reinterpret_cast<LogRecord *>(const_cast<char *>(data));
  log_record.size_ = ptr->GetSize();
  if (log_record.size_ > size || log_record.size_ <= 0) {
    return false;
  }
  log_record.lsn_ = ptr->GetLSN();
  log_record.txn_id_ = ptr->GetTxnId();
  log_record.prev_lsn_ = ptr->GetPrevLSN();
  log_record.log_record_type_ = ptr->GetLogRecordType();
  char *pos = const_cast<char *>(data) + LogRecord::HEADER_SIZE;
  if (log_record.log_record_type_ == LogRecordType::MARKDELETE
      || log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE
      || log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
    log_record.delete_rid_ = *reinterpret_cast<RID *>(pos);
    log_record.delete_tuple_.DeserializeFrom(pos + sizeof(RID));
  } else if (log_record.log_record_type_ == LogRecordType::INSERT) {
    log_record.insert_rid_ = *reinterpret_cast<RID *>(pos);
    log_record.insert_tuple_.DeserializeFrom(pos + sizeof(RID));
  } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
    log_record.update_rid_ = *reinterpret_cast<RID *>(pos);
    log_record.old_tuple_.DeserializeFrom(pos + sizeof(RID));
    log_record.new_tuple_.DeserializeFrom(pos + sizeof(RID) + log_record.old_tuple_.GetLength());
  } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
    log_record.prev_page_id_ = *reinterpret_cast<page_id_t *>(pos);
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  ENABLE_LOGGING = false;
  int fetchCnt = 0;
  auto retReadLog = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_);
  while (retReadLog) {
    int size = LOG_BUFFER_SIZE;
    LogRecord record;
    auto data = log_buffer_;
    while (size > 0) {
      auto retDeserial = DeserializeLogRecord(data, size, record);
      if (!retDeserial) {
        ENABLE_LOGGING = true;
        return;
      }

      auto type = record.log_record_type_;
      if (type == LogRecordType::BEGIN) {
        active_txn_[record.GetTxnId()] = record.GetLSN();
      } else if (type == LogRecordType::COMMIT || type == LogRecordType::ABORT) {
        active_txn_.erase(record.GetTxnId());
      } else {
        active_txn_[record.GetTxnId()] = record.GetLSN();
      }

      if (type == LogRecordType::NEWPAGE) {
        auto pageId = record.prev_page_id_;
        TablePage* page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(pageId));
        page->Init(pageId, PAGE_SIZE, INVALID_PAGE_ID, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(pageId, true);
      } else if (type == LogRecordType::UPDATE) {
        auto rid = record.update_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->UpdateTuple(record.new_tuple_, record.old_tuple_, rid,
          nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

      } else if (type == LogRecordType::INSERT) {
        auto rid = record.insert_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->InsertTuple(record.insert_tuple_, rid,
          nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (type == LogRecordType::MARKDELETE) {
        auto rid = record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (type == LogRecordType::ROLLBACKDELETE) {
        auto rid = record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->RollbackDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (type == LogRecordType::APPLYDELETE) {
        auto rid = record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->ApplyDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }

      lsn_mapping_[record.GetLSN()] = static_cast<int> (data - log_buffer_ + fetchCnt++ * LOG_BUFFER_SIZE);
      data += record.size_;
      size -= record.size_;
    }

    offset_ += LOG_BUFFER_SIZE;
    retReadLog = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_);
  }
  ENABLE_LOGGING = true;
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  ENABLE_LOGGING = false;
  for (auto item : active_txn_) {
    auto lastLsn = item.second;
    while (true) {
      auto offset = lsn_mapping_[lastLsn];
      auto retRead = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset);
      assert(retRead);
      LogRecord record;
      auto retDeserial = DeserializeLogRecord(log_buffer_, LOG_BUFFER_SIZE, record);
      assert(retDeserial);
      auto type = record.GetLogRecordType();
      if (type == LogRecordType::BEGIN) {
        break;
      }
      assert(type == LogRecordType::UPDATE || type == LogRecordType::INSERT || type == LogRecordType::MARKDELETE);
      if (type == LogRecordType::MARKDELETE) {
        auto rid = record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->RollbackDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (type == LogRecordType::INSERT) {
        auto rid = record.GetInsertRID();
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->ApplyDelete(rid, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (type == LogRecordType::UPDATE) {
        auto rid = record.update_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= record.GetLSN()) {
          continue;
        }
        TablePage *table_page = reinterpret_cast<TablePage *>(page);
        table_page->UpdateTuple(record.old_tuple_, record.new_tuple_, rid, nullptr, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      lastLsn = record.GetPrevLSN();
    }//end of while
  }
  ENABLE_LOGGING = false;
}

} // namespace cmudb
