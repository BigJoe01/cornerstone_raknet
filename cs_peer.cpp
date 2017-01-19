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

void peer::send_req(CPtr<CRequestMessage>& req, rpc_handler& handler) {
    CPtr<rpc_result> pending = cs_new<rpc_result>(handler);
    rpc_handler h = (rpc_handler)std::bind(&peer::handle_rpc_result, this, req, pending, std::placeholders::_1, std::placeholders::_2);
    rpc_->Send(req, h);
}

void peer::handle_rpc_result(CPtr<CRequestMessage>& req, CPtr<rpc_result>& pending_result, CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err) {
    if (err == nilptr) {
        if (req->GetType() == EMsgType::append_entries_request ||
            req->GetType() == EMsgType::install_snapshot_request) {
            set_free();
        }

        resume_hb_speed();
        CPtr<CRpcException> no_except;
        pending_result->SetResult(resp, no_except);
    }
    else {
        if (req->GetType() == EMsgType::append_entries_request ||
            req->GetType() == EMsgType::install_snapshot_request) {
            set_free();
        }

        slow_down_hb();
        CPtr<CResponseMsg> no_resp;
        pending_result->SetResult(no_resp, err);
    }
}