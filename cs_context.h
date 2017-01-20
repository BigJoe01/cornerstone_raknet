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

#ifndef _CONTEXT_HXX_
#define _CONTEXT_HXX_

namespace cornerstone {
    struct SRaftContext {
    public:
        SRaftContext(CStateManager& mgr, CStateMachine& m, CRpcListener& listener, CLogger& l, CRpcClientFactory& cli_factory, CDelayedTaskScheduler& scheduler, CRaftParams* params = nilptr)
            : m_StateManager(mgr), m_StateMachine(m), m_RpcListener(listener), m_Logger(l), m_RpcCliFactory(cli_factory), m_Scheduler(scheduler), m_Params(params == nilptr ? new CRaftParams : params) {}

    __nocopy__(SRaftContext)
    public:
        CStateManager& m_StateManager;
        CStateMachine& m_StateMachine;
        CRpcListener& m_RpcListener;
        CLogger& m_Logger;
        CRpcClientFactory& m_RpcCliFactory;
        CDelayedTaskScheduler& m_Scheduler;
        std::unique_ptr<CRaftParams> m_Params;
    };
}

#endif //_CONTEXT_HXX_
