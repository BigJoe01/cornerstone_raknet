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

CPtr<CSnapshot> CSnapshot::Deserialize(CBuffer& buf) {
    ulong last_log_idx = buf.GetULong();
    ulong last_log_term = buf.GetULong();
    ulong size = buf.GetULong();
    CPtr<CClusterConfig> conf(CClusterConfig::Deserialize(buf));
    return cs_new<CSnapshot>(last_log_idx, last_log_term, conf, size);
}

CPtr<CBuffer> CSnapshot::Serialize() {
    CPtr<CBuffer> conf_buf = m_LastConfig->Serialize();
    CPtr<CBuffer> buf = CBuffer::alloc(conf_buf->size() + sz_ulong * 3);
    buf->Put(m_ulLastLogIndex);
    buf->Put(m_ulLastLogTerm);
    buf->Put(m_ulSize);
    buf->Put(*conf_buf);
    buf->Pos(0);
    return buf;
}