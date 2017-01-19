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

CPtr<CBuffer> CClusterConfig::Serialize() {
    size_t sz = 2 * sz_ulong + sz_int;
    std::vector<CPtr<CBuffer>> srv_buffs;
    for (CClusterConfig::const_srv_itor it = m_Servers.begin(); it != m_Servers.end(); ++it) {
        CPtr<CBuffer> buf = (*it)->Serialize();
        srv_buffs.push_back(buf);
        sz += buf->size();
    }

    CPtr<CBuffer> result = CBuffer::alloc(sz);
    result->Put(m_LogIndex);
    result->Put(m_PrevLogIndex);
    result->Put((int32)m_Servers.size());
    for (size_t i = 0; i < srv_buffs.size(); ++i) {
        result->Put(*srv_buffs[i]);
    }

    result->Pos(0);
    return result;
}

CPtr<CClusterConfig> CClusterConfig::Deserialize(CBuffer& buf) {
    ulong log_idx = buf.GetULong();
    ulong prev_log_idx = buf.GetULong();
    int32 cnt = buf.GetInt();
    CPtr<CClusterConfig> conf = cs_new<CClusterConfig>(log_idx, prev_log_idx);
    while (cnt -- > 0) {
        conf->GetServers().push_back(CServerConfig::Deserialize(buf));
    }

    return conf;
}