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

CPtr<CServerConfig> CServerConfig::Deserialize(CBuffer& buf) {
    int32 id = buf.GetInt();
    const char* endpoint = buf.GetStr();
    return cs_new<CServerConfig>(id, endpoint);
}

CPtr<CBuffer> CServerConfig::Serialize() const{
    CPtr<CBuffer> buf = CBuffer::alloc(sz_int + m_strEndpoint.length() + 1);
    buf->Put(m_iId);
    buf->Put(m_strEndpoint);
    buf->Pos(0);
    return buf;
}

int32 CServerConfig::GetId() const
{
	return m_iId;
}

const std::string& CServerConfig::GetEndpoint() const
{
	return m_strEndpoint;
}