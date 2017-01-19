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

#ifndef _DELAYED_TASK_SCHEDULER_HXX_
#define _DELAYED_TASK_SCHEDULER_HXX_

namespace cornerstone {
    class CDelayedTaskScheduler {
    __interface_body__(CDelayedTaskScheduler)

    public:
        virtual void Schedule(CPtr<CDelayedTask>& pTask, int32 iMs) = 0;

        void Cancel(CPtr<CDelayedTask>& task) {
            OnCancel(task);
            task->Cancel();
        }

    private:
        virtual void OnCancel(CPtr<CDelayedTask>& pTask) = 0;
    };
}

#endif //_DELAYED_TASK_SCHEDULER_HXX_
