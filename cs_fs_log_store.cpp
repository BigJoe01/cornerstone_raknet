/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  The ASF licenses
* this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include "cornerstone/cornerstone.h"

#define LOG_INDEX_FILE "store.idx"
#define LOG_DATA_FILE "store.dat"
#define LOG_START_INDEX_FILE "store.sti"
#define LOG_INDEX_FILE_BAK "store.idx.bak"
#define LOG_DATA_FILE_BAK "store.dat.bak"
#define LOG_START_INDEX_FILE_BAK "store.sti.bak"

#ifdef _WIN32
#include <Windows.h>
#define PATH_SEPARATOR '\\'
int truncate(const char* path, ulong new_size) {
    HANDLE file_handle = ::CreateFileA(path, GENERIC_WRITE, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (file_handle == INVALID_HANDLE_VALUE) {
        return -1;
    }

    LONG offset = new_size & 0xFFFFFFFF;
    LONG offset_high = (LONG)(new_size >> 32);
    LONG result = ::SetFilePointer(file_handle, offset, &offset_high, FILE_BEGIN);
    if (result == INVALID_SET_FILE_POINTER) {
        CloseHandle(file_handle);
        return -1;
    }

    if (!::SetEndOfFile(file_handle)) {
        CloseHandle(file_handle);
        return -1;
    }

    CloseHandle(file_handle);
    return 0;
}
#undef max
#undef min
#else
// for truncate function
#include <unistd.h>
#include <sys/types.h>
#define PATH_SEPARATOR '/'
#ifdef OS_FREEBSD
int truncate(const char* path, ulong new_size) {
    return ::truncate(path, (off_t)new_size);
}
#else
int truncate(const char* path, ulong new_size) {
    return ::truncate64(path, (off64_t)new_size);
}
#endif
#endif

using namespace cornerstone;
CPtr<CBuffer> zero_buf;
CPtr<CLogEntry> empty_entry(cs_new<CLogEntry>(0, zero_buf, ELogValueType::e_Application));

class cornerstone::CLogStoreBuffer {
public:
    typedef std::vector<CPtr<CLogEntry>>::const_iterator const_buf_itor;
    typedef std::vector<CPtr<CLogEntry>>::iterator buf_itor;
public:
    CLogStoreBuffer(ulong start_idx, int32 max_size)
        : buf_(), lock_(), start_idx_(start_idx), max_size_(max_size) {
    }

    ulong last_idx() {
        recur_lock(lock_);
        return start_idx_ + buf_.size();
    }

    ulong first_idx() {
        recur_lock(lock_);
        return start_idx_;
    }

    CPtr<CLogEntry> last_entry() {
        recur_lock(lock_);
        if (buf_.size() > 0) {
            return buf_[buf_.size() - 1];
        }

        return CPtr<CLogEntry>();
    }

    CPtr<CLogEntry> operator[](ulong idx) {
        recur_lock(lock_);
        size_t idx_within_buf = static_cast<size_t>(idx - start_idx_);
        if (idx_within_buf >= buf_.size() || idx < start_idx_) {
            return CPtr<CLogEntry>();
        }

        return buf_[idx_within_buf];
    }

    // [start, end), returns the start_idx_;
    ulong fill(ulong start, ulong end, std::vector<CPtr<CLogEntry>>& result) {
        recur_lock(lock_);
        if (end < start_idx_) {
            return start_idx_;
        }

        int offset = static_cast<int>(start - start_idx_);
        if (offset > 0) {
            for (int i = 0; i < static_cast<int>(end - start); ++i, ++offset) {
                result.push_back(buf_[offset]);
            }
        }
        else {
            offset *= -1;
            for (int i = 0; i < offset; ++i) {
                result.push_back(CPtr<CLogEntry>()); // make room for items that doesn't found in the buffer
            }

            for (int i = 0; i < static_cast<int>(end - start_idx_); ++i, ++offset) {
                result.push_back(buf_[i]);
            }
        }

        return start_idx_;
    }

    CPtr<CBuffer> pack(ulong start, int32 cnt) {
        recur_lock(lock_);
        size_t offset = static_cast<size_t>(start - start_idx_);
        size_t good_cnt = static_cast<size_t>((size_t)cnt > buf_.size() - offset ? buf_.size() - offset : cnt);
        std::vector<CPtr<CBuffer>> buffers;
        size_t size = 0;
        for (size_t i = offset; i < offset + good_cnt; ++offset) {
            CPtr<CBuffer> buf = buf_[i]->Serialize();
            size += buf->size();
        }

        CPtr<CBuffer> buf = CBuffer::alloc(size);
        for (size_t i = 0; i < buffers.size(); ++i) {
            buf->Put(*buffers[i]);
        }

        buf->Pos(0);
        return buf;
    }

    ulong get_term(ulong index) {
        recur_lock(lock_);
        if (index < start_idx_ || index >= start_idx_ + buf_.size()) {
            return 0;
        }

        return buf_[static_cast<int>(index - start_idx_)]->GetTerm();
    }

    // trimming the buffer [start, end)
    void trim(ulong start) {
        recur_lock(lock_);
        if (start < start_idx_) {
            return;
        }

        size_t index = static_cast<size_t>(start - start_idx_);
        if (index < buf_.size()) {
            buf_.erase(buf_.begin() + index, buf_.end());
        }
    }

    void append(CPtr<CLogEntry>& entry) {
        recur_lock(lock_);
        buf_.push_back(entry);
        if ((size_t)max_size_ < buf_.size()) {
            buf_.erase(buf_.begin());
            start_idx_ += 1;
        }
    }

    void reset(ulong start_idx) {
        recur_lock(lock_);
        buf_.clear();
        start_idx_ = start_idx;
    }
private:
    std::vector<CPtr<CLogEntry>> buf_;
    std::recursive_mutex lock_;
    ulong start_idx_;
    int32 max_size_;
};

CFileSystemLogStore::~CFileSystemLogStore() {
    recur_lock(store_lock_);
    if (idx_file_) {
        idx_file_.close();
    }

    if (data_file_) {
        data_file_.close();
    }

    if (start_idx_file_) {
        start_idx_file_.close();
    }

    if (buf_ != nilptr) {
        delete buf_;
    }
}

CFileSystemLogStore::CFileSystemLogStore(const std::string& log_folder, int buf_size)
    : idx_file_(), 
    data_file_(), 
    start_idx_file_(), 
    entries_in_store_(0), 
    start_idx_(1), 
    log_folder_(log_folder), 
    store_lock_(), 
    buf_(nilptr), 
    buf_size_(buf_size < 0 ? std::numeric_limits<int>::max() : buf_size) {
    if (log_folder_.length() > 0 && log_folder_[log_folder_.length() - 1] != PATH_SEPARATOR) {
        log_folder_.push_back(PATH_SEPARATOR);
    }

    idx_file_.open(log_folder_ + LOG_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!idx_file_) {
        // create the file and reopen
        idx_file_.open(log_folder_ + LOG_INDEX_FILE, std::fstream::out);
        idx_file_.close();
        idx_file_.open(log_folder_ + LOG_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    data_file_.open(log_folder_ + LOG_DATA_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!data_file_) {
        // create the file and reopen
        data_file_.open(log_folder_ + LOG_DATA_FILE, std::fstream::out);
        data_file_.close();
        data_file_.open(log_folder_ + LOG_DATA_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    start_idx_file_.open(log_folder_ + LOG_START_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!start_idx_file_) {
        start_idx_file_.open(log_folder_ + LOG_START_INDEX_FILE, std::fstream::out);
        start_idx_file_.close();
        start_idx_file_.open(log_folder_ + LOG_START_INDEX_FILE, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    if (!idx_file_ || !data_file_ || !start_idx_file_) {
        throw std::runtime_error("fail to create store files");
    }

    if (start_idx_file_.tellp() > 0) {
        start_idx_file_.seekg(0, std::fstream::beg);
        CPtr<CBuffer> idx_buf(CBuffer::alloc(sz_ulong));
        start_idx_file_ >> *idx_buf;
        start_idx_ = idx_buf->GetULong();
        entries_in_store_ = idx_file_.tellp() / sz_ulong;
    }
    else {
        start_idx_ = 1;
        CPtr<CBuffer> idx_buf(CBuffer::alloc(sz_ulong));
        idx_buf->Put(start_idx_);
        idx_buf->Pos(0);
        start_idx_file_.seekp(0, std::fstream::beg);
        start_idx_file_ << *idx_buf;
        entries_in_store_ = 0;
    }

    buf_ = new CLogStoreBuffer(entries_in_store_ > (size_t)buf_size_ ? entries_in_store_ - buf_size_ + start_idx_ : start_idx_, buf_size_);
    fill_buffer();
}

ulong CFileSystemLogStore::NextSlot() const {
    recur_lock(store_lock_);
    return start_idx_ + entries_in_store_;
}

ulong CFileSystemLogStore::StartIndex() const {
    recur_lock(store_lock_);
    return start_idx_;
}

CPtr<CLogEntry> CFileSystemLogStore::LastEntry() const {
    CPtr<CLogEntry> entry = buf_->last_entry();
    if (entry == nilptr) {
        return empty_entry;
    }

    return entry;
}

ulong CFileSystemLogStore::Append(CPtr<CLogEntry>& entry) {
    recur_lock(store_lock_);
    idx_file_.seekp(0, std::fstream::end);
    data_file_.seekp(0, std::fstream::end);
    CPtr<CBuffer> idx_buf(CBuffer::alloc(sz_ulong));
    idx_buf->Put(static_cast<ulong>(data_file_.tellp()));
    idx_buf->Pos(0);
    idx_file_ << *idx_buf;
    CPtr<CBuffer> entry_buf = entry->Serialize();
    data_file_ << *entry_buf;
    buf_->append(entry);
    entries_in_store_ += 1;
    idx_file_.flush();
    data_file_.flush();
    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }

    return start_idx_ + entries_in_store_ - 1;
}

void CFileSystemLogStore::WriteAt(ulong index, CPtr<CLogEntry>& entry) {
    recur_lock(store_lock_);
    if (index < start_idx_ || index > start_idx_ + entries_in_store_) {
        throw std::range_error("index out of range");
    }

    ulong local_idx = index - start_idx_ + 1; //start_idx is one based
    ulong idx_pos = (local_idx - 1) * sz_ulong;
    if (local_idx <= entries_in_store_) {
        idx_file_.seekg(idx_pos, std::fstream::beg);
        CPtr<CBuffer> buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *buf;
        data_file_.seekp(buf->GetULong(), std::fstream::beg);
    }
    else {
        data_file_.seekp(0, std::fstream::end);
    }

    idx_file_.seekp(idx_pos, std::fstream::beg);
    ulong data_pos = data_file_.tellp();
    CPtr<CBuffer> ibuf(std::move(CBuffer::alloc(sz_ulong)));
    ibuf->Put(data_pos);
    ibuf->Pos(0);
    idx_file_ << *ibuf;
    CPtr<CBuffer> ebuf = entry->Serialize();
    data_file_ << *ebuf;
    idx_file_.flush();
    data_file_.flush();

    // truncate the files if necessary
    ulong ndata_len = data_file_.tellp();
    ulong nidx_len = idx_file_.tellp();
    idx_file_.seekp(0, std::fstream::end);
    if (static_cast<ulong>(idx_file_.tellp()) > nidx_len) { // new index length is less than current file size, truncate it
        std::string idx_path = log_folder_ + LOG_INDEX_FILE;
        std::string data_path = log_folder_ + LOG_DATA_FILE;
        idx_file_.close();
        data_file_.close();
        if (truncate(idx_path.c_str(), nidx_len) < 0) {
            throw std::ios_base::failure("failed to truncate the index file");
        }

        if (truncate(data_path.c_str(), ndata_len) < 0) {
            throw std::ios_base::failure("failed to truncate the data file");
        }

        idx_file_.open(idx_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        data_file_.open(data_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    if (local_idx <= entries_in_store_) {
        buf_->trim(index);
    }

    buf_->append(entry);
    entries_in_store_ = local_idx;
    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }
}

CPtr<std::vector<CPtr<CLogEntry>>> CFileSystemLogStore::LogEntries(ulong start, ulong end) {
    ulong lstart(0), lend(0), good_end(0);
    {
        recur_lock(store_lock_);
        if (start < start_idx_) {
            throw std::range_error("index out of range");
        }

        if (start >= end || start >= (start_idx_ + entries_in_store_)) {
            return CPtr<std::vector<CPtr<CLogEntry>>>();
        }

        lstart = start - start_idx_;
        lend = end - start_idx_;
        lend = lend > entries_in_store_ ? entries_in_store_ : lend;
        good_end = lend + start_idx_;
    }

    if (lstart >= lend) {
        return CPtr<std::vector<CPtr<CLogEntry>>>();
    }

    CPtr<std::vector<CPtr<CLogEntry>>> results(cs_new<std::vector<CPtr<CLogEntry>>>());

    // fill with the buffer
    ulong buffer_first_idx = buf_->fill(start, good_end, *results);

    // Assumption: buffer.last_index() == entries_in_store_ + start_idx_
    // (Yes, for sure, we need to enforce this assumption to be true)
    if (start < buffer_first_idx) {
        // in this case, we need to read from store file
        recur_lock(store_lock_);
        if (!data_file_ || !idx_file_) {
            throw std::runtime_error("IO fails, data cannot be saved");
        }

        lend = buffer_first_idx - start_idx_;
        idx_file_.seekg(lstart * sz_ulong);
        CPtr<CBuffer> d_start_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *d_start_idx_buf;
        ulong data_start = d_start_idx_buf->GetULong();
        for (int i = 0; i < (int)(lend - lstart); ++i) {
            CPtr<CBuffer> d_end_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
            idx_file_ >> *d_end_idx_buf;
            ulong data_end = d_end_idx_buf->GetULong();
            int data_sz = (int)(data_end - data_start);
            data_file_.seekg(data_start);
            CPtr<CBuffer> entry_buf(std::move(CBuffer::alloc(data_sz)));
            data_file_ >> *entry_buf;
            (*results)[i] = CLogEntry::Deserialize(*entry_buf);
            data_start = data_end;
        }
    }

    return results;
}

CPtr<CLogEntry> CFileSystemLogStore::EntryAt(ulong index) {

    CPtr<CLogEntry> entry = (*buf_)[index];
    if (entry) {
        return entry;
    }

    {
        // since we don't hit the buffer, so this must not be the last entry 
        // (according to Assumption: buffer.last_index() == entries_in_store_ + start_idx_)
        recur_lock(store_lock_);
        if (index < start_idx_) {
            throw std::range_error("index out of range");
        }

        if (!data_file_ || !idx_file_) {
            throw std::runtime_error("IO fails, data cannot be saved");
        }

        if (index >= start_idx_ + entries_in_store_) {
            return CPtr<CLogEntry>();
        }

        ulong idx_pos = (index - start_idx_) * sz_ulong;
        idx_file_.seekg(idx_pos);
        CPtr<CBuffer> d_start_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *d_start_idx_buf;
        ulong d_pos = d_start_idx_buf->GetULong();
        CPtr<CBuffer> d_end_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *d_end_idx_buf;
        ulong end_d_pos = d_end_idx_buf->GetULong();
        data_file_.seekg(d_pos);
        CPtr<CBuffer> entry_buf(std::move(CBuffer::alloc((int)(end_d_pos - d_pos))));
        data_file_ >> *entry_buf;
        return CLogEntry::Deserialize(*entry_buf);
    }
}

ulong CFileSystemLogStore::TermAt(ulong index) {
    if (index >= buf_->first_idx() && index < buf_->last_idx()) {
        return buf_->get_term(index);
    }

    {
        recur_lock(store_lock_);
        if (index < start_idx_) {
            throw std::range_error("index out of range");
        }

        if (!data_file_ || !idx_file_) {
            throw std::runtime_error("IO fails, data cannot be saved");
        }

        idx_file_.seekg(static_cast<int>(index - start_idx_) * sz_ulong);
        CPtr<CBuffer> d_start_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *d_start_idx_buf;
        data_file_.seekg(d_start_idx_buf->GetULong());

        // IMPORTANT!! 
        // We hack the log_entry serialization details here
        CPtr<CBuffer> term_buf(std::move(CBuffer::alloc(sz_ulong)));
        data_file_ >> *term_buf;
        return term_buf->GetULong();
    }
}

CPtr<CBuffer> CFileSystemLogStore::Pack(ulong index, int32 cnt) {
    recur_lock(store_lock_);
    if (index < start_idx_) {
        throw std::range_error("index out of range");
    }

    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }

    ulong offset = index - start_idx_;
    if (offset >= entries_in_store_) {
        return CPtr<CBuffer>();
    }

    ulong end_offset = std::min(offset + cnt, entries_in_store_);
    idx_file_.seekg(static_cast<int>(offset) * sz_ulong);
    bool read_to_end = end_offset == entries_in_store_;
    CPtr<CBuffer> idx_buf(std::move(CBuffer::alloc(static_cast<int>(end_offset - offset) * sz_ulong)));
    idx_file_ >> *idx_buf;
    ulong end_of_data(0);
    if (read_to_end) {
        data_file_.seekg(0, std::fstream::end);
        end_of_data = data_file_.tellg();
    }
    else {
        CPtr<CBuffer> end_d_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *end_d_idx_buf;
        end_of_data = end_d_idx_buf->GetULong();
    }

    idx_buf->Pos(0);
    ulong start_of_data = idx_buf->GetULong();
    idx_buf->Pos(0);
    data_file_.seekg(start_of_data);
    CPtr<CBuffer> data_buf(std::move(CBuffer::alloc(static_cast<int>(end_of_data - start_of_data))));
    data_file_ >> *data_buf;
    CPtr<CBuffer> result = CBuffer::alloc(2 * sz_int + idx_buf->size() + data_buf->size());
    result->Put(static_cast<int32>(idx_buf->size()));
    result->Put(static_cast<int32>(data_buf->size()));
    result->Put(*idx_buf);
    result->Put(*data_buf);
    result->Pos(0);
    return result;
}

void CFileSystemLogStore::ApplyPack(ulong index, CBuffer& pack) {
    recur_lock(store_lock_);
    int32 idx_len = pack.GetInt();
    int32 data_len = pack.GetInt();
    ulong local_idx = index - start_idx_;
    if (local_idx == entries_in_store_) {
        idx_file_.seekp(0, std::fstream::end);
        data_file_.seekp(0, std::fstream::end);
    }
    else {
        ulong idx_pos = local_idx * sz_ulong;
        idx_file_.seekg(idx_pos);
        CPtr<CBuffer> data_pos_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_ >> *data_pos_buf;
        idx_file_.seekp(idx_pos);
        data_file_.seekp(data_pos_buf->GetULong());
    }

    idx_file_.write(reinterpret_cast<const char*>(pack.Data()), idx_len);
    data_file_.write(reinterpret_cast<const char*>(pack.Data() + idx_len), data_len);
    idx_file_.flush();
    data_file_.flush();
    ulong idx_pos = idx_file_.tellp();
    ulong data_pos = data_file_.tellp();
    idx_file_.seekp(0, std::fstream::end);
    data_file_.seekp(0, std::fstream::end);
    if (idx_pos < static_cast<ulong>(idx_file_.tellp())) {
        std::string idx_path = log_folder_ + LOG_INDEX_FILE;
        std::string data_path = log_folder_ + LOG_DATA_FILE;
        idx_file_.close();
        data_file_.close();
        if (truncate(idx_path.c_str(), idx_pos) < 0) {
            throw std::ios_base::failure("failed to truncate the index file");
        }

        if (truncate(data_path.c_str(), data_pos) < 0) {
            throw std::ios_base::failure("failed to truncate the data file");
        }

        idx_file_.open(idx_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        data_file_.open(data_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    }

    if (!data_file_ || !idx_file_) {
        throw std::runtime_error("IO fails, data cannot be saved");
    }

    entries_in_store_ = local_idx + idx_len / sz_ulong;
    buf_->reset(entries_in_store_ > (size_t)buf_size_ ? entries_in_store_ + start_idx_ - buf_size_ : start_idx_);
    fill_buffer();
}

bool CFileSystemLogStore::Compact(ulong last_log_index) {
    recur_lock(store_lock_);
    if (last_log_index < start_idx_) {
        throw std::range_error("index out of range");
    }

    // backup the files
    idx_file_.seekg(0);
    data_file_.seekg(0);
    start_idx_file_.seekg(0);
    std::string idx_bak_path = log_folder_ + LOG_INDEX_FILE_BAK;
    std::string data_bak_path = log_folder_ + LOG_DATA_FILE_BAK;
    std::string start_idx_bak_path = log_folder_ + LOG_START_INDEX_FILE_BAK;
    std::string data_file_path = log_folder_ + LOG_DATA_FILE;
    std::string idx_file_path = log_folder_ + LOG_INDEX_FILE;
    std::string start_idx_file_path = log_folder_ + LOG_START_INDEX_FILE;
    std::remove(idx_bak_path.c_str());
    std::remove(data_bak_path.c_str());
    std::remove(start_idx_bak_path.c_str());

    std::fstream idx_bak_file, data_bak_file, start_idx_bak_file;
    idx_bak_file.open(idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    if (!idx_bak_file) {
        idx_bak_file.open(idx_bak_path, std::fstream::out);
        idx_bak_file.close();
        idx_bak_file.open(idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    }

    data_bak_file.open(data_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    if (!data_bak_file) {
        data_bak_file.open(data_bak_path, std::fstream::out);
        data_bak_file.close();
        data_bak_file.open(data_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    }

    start_idx_bak_file.open(start_idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    if (!start_idx_bak_file) {
        start_idx_bak_file.open(start_idx_bak_path, std::fstream::out);
        start_idx_bak_file.close();
        start_idx_bak_file.open(start_idx_bak_path, std::fstream::in | std::fstream::out | std::fstream::binary);
    }

    if (!idx_bak_file || !data_bak_file || !start_idx_bak_file) return false; //we cannot proceed as backup files are bad

    idx_file_.seekg(0);
    data_file_.seekg(0);
    start_idx_file_.seekg(0);
    idx_bak_file << idx_file_.rdbuf();
    data_bak_file << data_file_.rdbuf();
    start_idx_bak_file << start_idx_file_.rdbuf();
    idx_bak_file.flush();
    data_bak_file.flush();
    start_idx_bak_file.flush();

    do {
        if (last_log_index >= start_idx_ + entries_in_store_ - 1) {
            // need to clear all entries in this store and update the start index
            idx_file_.close();
            data_file_.close();
            if (std::remove(idx_file_path.c_str()) != 0) break;
            if (std::remove(data_file_path.c_str()) != 0) break;
            idx_file_.open(idx_file_path, std::fstream::out);
            idx_file_.close();
            idx_file_.open(idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
            data_file_.open(data_file_path, std::fstream::out);
            data_file_.close();
            data_file_.open(data_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
            if (!idx_file_ || !data_file_) break;

            // save the logstore state
            start_idx_file_.seekp(0);
            CPtr<CBuffer> start_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
            start_idx_buf->Put(last_log_index + 1);
            start_idx_buf->Pos(0);
            start_idx_file_ << *start_idx_buf;
            start_idx_ = last_log_index + 1;
            entries_in_store_ = 0;
            buf_->reset(start_idx_);

            // close all backup files
            idx_bak_file.close();
            data_bak_file.close();
            start_idx_bak_file.close();
            return true;
        }

        // else, we need to compact partial of the logs
        ulong local_last_idx = last_log_index - start_idx_;
        ulong idx_pos = (local_last_idx + 1) * sz_ulong;
        CPtr<CBuffer> data_pos_buf(std::move(CBuffer::alloc(sz_ulong)));
        idx_file_.seekg(idx_pos);
        idx_file_ >> *data_pos_buf;
        idx_file_.seekp(0, std::fstream::end);
        data_file_.seekp(0, std::fstream::end);
        ulong data_pos = data_pos_buf->GetULong();
        ulong idx_len = static_cast<ulong>(idx_file_.tellp()) - idx_pos;
        ulong data_len = static_cast<ulong>(data_file_.tellp()) - data_pos;

        // compact the data file
        data_bak_file.seekg(data_pos);
        data_file_.seekp(0);
        data_file_ << data_bak_file.rdbuf();

        // truncate the data file
        data_file_.close();
        if(0 != truncate(data_file_path.c_str(), data_len)) break;
        data_file_.open(data_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        if (!data_file_) break;

        // compact the index file
        idx_bak_file.seekg(idx_pos);
        idx_file_.seekp(0);
        for (ulong i = 0; i < idx_len / sz_ulong; ++i) {
            data_pos_buf->Pos(0);
            idx_bak_file >> *data_pos_buf;
            ulong new_pos = data_pos_buf->GetULong() - data_pos;
            data_pos_buf->Pos(0);
            data_pos_buf->Put(new_pos);
            data_pos_buf->Pos(0);
            idx_file_ << *data_pos_buf;
        }

        // truncate the index file
        idx_file_.close();
        if(0 != truncate(idx_file_path.c_str(), idx_len)) break;
        idx_file_.open(idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
        if (!idx_file_) break;

        // close all backup files
        idx_bak_file.close();
        data_bak_file.close();
        start_idx_bak_file.close();

        start_idx_file_.seekp(0);
        CPtr<CBuffer> start_idx_buf(std::move(CBuffer::alloc(sz_ulong)));
        start_idx_buf->Put(last_log_index + 1);
        start_idx_buf->Pos(0);
        start_idx_file_ << *start_idx_buf;
        start_idx_file_.flush();
        entries_in_store_ -= (last_log_index - start_idx_ + 1);
        start_idx_ = last_log_index + 1;
        buf_->reset(entries_in_store_ > (size_t)buf_size_ ? entries_in_store_ + start_idx_ - buf_size_ : start_idx_);
        fill_buffer();
        return true;
    } while (false);

    // restore the state due to errors
    if (idx_file_) idx_file_.close();
    if (data_file_) data_file_.close();
    if (start_idx_file_) start_idx_file_.close();
    std::remove(idx_file_path.c_str());
    std::remove(data_file_path.c_str());
    std::remove(start_idx_file_path.c_str());
    idx_bak_file.seekg(0);
    data_bak_file.seekg(0);
    start_idx_bak_file.seekg(0);
    data_file_.open(data_bak_path, std::fstream::out);
    data_file_.close();
    data_file_.open(data_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    idx_file_.open(idx_file_path, std::fstream::out);
    idx_file_.close();
    idx_file_.open(idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    start_idx_file_.open(start_idx_file_path, std::fstream::out);
    start_idx_file_.close();
    start_idx_file_.open(start_idx_file_path, std::fstream::in | std::fstream::out | std::fstream::binary | std::fstream::ate);
    if (!idx_file_ || !data_file_ || !start_idx_file_) {
        throw std::runtime_error("IO error, fails to restore files back.");
    }

    data_file_ << data_bak_file.rdbuf();
    idx_file_ << idx_bak_file.rdbuf();
    start_idx_file_ << start_idx_bak_file.rdbuf();
    data_file_.flush();
    idx_file_.flush();
    start_idx_file_.flush();

    // close all backup files
    idx_bak_file.close();
    data_bak_file.close();
    start_idx_bak_file.close();
    return false;
}

void CFileSystemLogStore::close() {
    idx_file_.close();
    data_file_.close();
    start_idx_file_.close();
}

void CFileSystemLogStore::fill_buffer() {
    ulong first_idx = buf_->first_idx();
    idx_file_.seekg(0, std::fstream::end);
    data_file_.seekg(0, std::fstream::end);
    if (idx_file_.tellg() > 0) {
        ulong idx_file_len = idx_file_.tellg();
        ulong data_file_len = data_file_.tellg();
        ulong idx_pos = (first_idx - start_idx_) * sz_ulong;
        idx_file_.seekg(idx_pos);
        CPtr<CBuffer> idx_buf(std::move(CBuffer::alloc(static_cast<size_t>(idx_file_len - idx_pos))));
        idx_file_ >> *idx_buf;
        ulong data_start = idx_buf->GetULong();
        data_file_.seekg(data_start);
        while (idx_buf->size() > idx_buf->Pos()) {
            ulong data_end = idx_buf->GetULong();
            CPtr<CBuffer> buf = CBuffer::alloc(static_cast<size_t>(data_end - data_start));
            data_file_ >> *buf;
	    CPtr<CLogEntry> entry = CLogEntry::Deserialize(*buf);
            buf_->append(entry);
            data_start = data_end;
        }

        CPtr<CBuffer> last_buf = CBuffer::alloc(static_cast<size_t>(data_file_len - data_start));
        data_file_ >> *last_buf;
	CPtr<CLogEntry> entry = CLogEntry::Deserialize(*last_buf);
        buf_->append(entry);
    }
}
