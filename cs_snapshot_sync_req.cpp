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

using namespace cornerstone;

CPtr<CSnapshotSyncRequest> CSnapshotSyncRequest::Deserialize(CBuffer& buf) {
    CPtr<CSnapshot> snp(CSnapshot::Deserialize(buf));
    ulong offset = buf.GetULong();
    bool done = buf.GetByte() == 1;
    byte* src = buf.Data();
    CPtr<CBuffer> b;
    if (buf.Pos() < buf.size()) {
        size_t sz = buf.size() - buf.Pos();
        b = CBuffer::alloc(sz);
        ::memcpy(b->Data(), src, sz);
    }
    else {
        b = CBuffer::alloc(0);
    }

    return cs_new<CSnapshotSyncRequest>(snp, offset, b, done);
}

CPtr<CBuffer> CSnapshotSyncRequest::Serialize() {
    CPtr<CBuffer> snp_buf = m_Snapshot->Serialize();
    CPtr<CBuffer> buf = CBuffer::alloc(snp_buf->size() + sz_ulong + sz_byte + (m_pData->size() - m_pData->Pos()));
    buf->Put(*snp_buf);
    buf->Put(m_Offset);
    buf->Put(m_bDone ? (byte)1 : (byte)0);
    buf->Put(*m_pData);
    buf->Pos(0);
    return buf;
}