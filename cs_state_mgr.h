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

#ifndef _STATE_MGR_HXX_
#define _STATE_MGR_HXX_

namespace cornerstone {
    class CStateManager {
    __interface_body__(CStateManager)

    public:
        virtual CPtr<CClusterConfig> LoadConfig() = 0;
        virtual void SaveConfig(const CClusterConfig& config) = 0;
        virtual void SaveState(const CServerState& state) = 0;
        virtual CPtr<CServerState> ReadState() = 0;
        virtual CPtr<CLogStore> LoadLogStore() = 0;
        virtual int32 ServerId() = 0;
        virtual void SystemExit(const int exit_code) = 0;
    };
}

#endif //_STATE_MGR_HXX_