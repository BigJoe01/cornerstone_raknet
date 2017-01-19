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

#ifndef _SNAPSHOT_SYNC_REQ_HXX_
#define _SNAPSHOT_SYNC_REQ_HXX_

namespace cornerstone {
    class CSnapshotSyncRequest {
    public:
        CSnapshotSyncRequest(const CPtr<CSnapshot>& s, ulong offset, const CPtr<CBuffer>& buf, bool done)
            : m_Snapshot(s), m_Offset(offset), m_pData(buf), m_bDone(done) {}

    __nocopy__(CSnapshotSyncRequest)
    public:
        static CPtr<CSnapshotSyncRequest> Deserialize(CBuffer& buf);

        CSnapshot& GetSnapshot() const {
            return *m_Snapshot;
        }

        ulong GetOffset() const { return m_Offset; }

        CBuffer& GetData() const { return *m_pData; }

        bool IsDone() const { return m_bDone; }

        CPtr<CBuffer> Serialize();
    private:
        CPtr<CSnapshot> m_Snapshot;
        ulong m_Offset;
        CPtr<CBuffer> m_pData;
        bool m_bDone;
    };
}

#endif //_SNAPSHOT_SYNC_REQ_HXX_
