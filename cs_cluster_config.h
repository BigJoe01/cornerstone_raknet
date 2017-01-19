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

#ifndef _CLUSTER_CONFIG_HXX_
#define _CLUSTER_CONFIG_HXX_

namespace cornerstone {
    class CClusterConfig {
    public:
        CClusterConfig(ulong log_idx = 0L, ulong prev_log_idx = 0L)
            : m_LogIndex(log_idx), m_PrevLogIndex(prev_log_idx), m_Servers() {}

        ~CClusterConfig() {
        }

        __nocopy__(CClusterConfig)
    public:
        typedef std::list<CPtr<CServerConfig>>::iterator srv_itor;
        typedef std::list<CPtr<CServerConfig>>::const_iterator const_srv_itor;

        static CPtr<CClusterConfig> Deserialize(CBuffer& buf);

        ulong GetLogIndex() const {
            return m_LogIndex;
        }

        void SetLogIndex(ulong log_idx) {
            m_PrevLogIndex = m_LogIndex;
            m_LogIndex = log_idx;
        }

        ulong GetPrevLogIndex() const {
            return m_PrevLogIndex;
        }

        std::list<CPtr<CServerConfig>>& GetServers() {
            return m_Servers;
        }

        CPtr<CServerConfig>  GetServer(int id) const {
            for (const_srv_itor it = m_Servers.begin(); it != m_Servers.end(); ++it) {
                if ((*it)->GetId() == id) {
                    return *it;
                }
            }

            return CPtr<CServerConfig>();
        }

        CPtr<CBuffer> Serialize();
    private:
        ulong m_LogIndex;
        ulong m_PrevLogIndex;
        std::list<CPtr<CServerConfig>> m_Servers;
    };
}

#endif //_CLUSTER_CONFIG_HXX_