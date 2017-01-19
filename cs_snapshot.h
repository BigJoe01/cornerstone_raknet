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

#ifndef _SNAPSHOT_HXX_
#define _SNAPSHOT_HXX_

namespace cornerstone {
    class CSnapshot {
    public:
        CSnapshot(ulong last_log_idx, ulong last_log_term, const CPtr<CClusterConfig>& last_config, ulong size = 0)
            : m_ulLastLogIndex(last_log_idx), m_ulLastLogTerm(last_log_term), m_ulSize(size), m_LastConfig(last_config){}

        __nocopy__(CSnapshot)

    public:
        ulong GetLastLogIndex() const {
            return m_ulLastLogIndex;
        }

        ulong GetLastLogTerm() const {
            return m_ulLastLogTerm;
        }

        ulong Size() const {
            return m_ulSize;
        }

        const CPtr<CClusterConfig>& GetLastConfig() const {
            return m_LastConfig;
        }

        static CPtr<CSnapshot> Deserialize(CBuffer& buf);

        CPtr<CBuffer> Serialize();

    private:
        ulong m_ulLastLogIndex;
        ulong m_ulLastLogTerm;
        ulong m_ulSize;
        CPtr<CClusterConfig> m_LastConfig;
    };
}

#endif