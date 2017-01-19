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

#ifndef _SNAPSHOT_SYNC_CTX_HXX_
#define _SNAPSHOT_SYNC_CTX_HXX_

namespace cornerstone {
    struct CSnapshotSyncContext {
    public:
        CSnapshotSyncContext(const CPtr<CSnapshot>& s, ulong offset = 0L)
            : m_Snapshot(s), m_ulOffset(offset) {}

    __nocopy__(CSnapshotSyncContext)
    
    public:
        const CPtr<CSnapshot>& GetSnapshot() const {
            return m_Snapshot;
        }

        ulong GetOffset() const {
            return m_ulOffset;
        }

        void SetOffset(ulong offset) {
            m_ulOffset = offset;
        }
    public:
        CPtr<CSnapshot> m_Snapshot;
        ulong m_ulOffset;
    };
}

#endif //_SNAPSHOT_SYNC_CTX_HXX_
