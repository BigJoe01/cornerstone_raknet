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

#ifndef _RPC_EXCEPTION_HXX_
#define _RPC_EXCEPTION_HXX_

namespace cornerstone {
    class CRpcException : public std::exception {
    public:
        CRpcException(const std::string& err, CPtr<CRequestMessage> req)
            : m_Request(req), m_sError(err.c_str()) {}

        __nocopy__(CRpcException)
    public:
        CPtr<CRequestMessage> Request() const { return m_Request; }

        virtual const char* what() const throw() __override__ {
            return m_sError;
        }
    private:
        CPtr<CRequestMessage> m_Request;
        const char* m_sError;
    };
}

#endif //_RPC_EXCEPTION_HXX_
