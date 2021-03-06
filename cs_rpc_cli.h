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

#ifndef _RPC_CLI_HXX_
#define _RPC_CLI_HXX_

namespace cornerstone {
    typedef CAsyncResult<CPtr<CResponseMsg>, CPtr<CRpcException>> rpc_result;
    typedef rpc_result::handler_type rpc_handler;

    class CRpcClient {
    __interface_body__(CRpcClient)
    public:
        virtual void Send(CPtr<CRequestMessage>& req, rpc_handler& when_done) = 0;
    };
}

#endif //_RPC_CLI_HXX_
