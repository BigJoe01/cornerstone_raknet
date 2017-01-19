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

#ifndef _DELAYED_TASK_HXX_
#define _DELAYED_TASK_HXX_

namespace cornerstone {
    class CDelayedTask {
    public:
        CDelayedTask()
            : cancelled_(false), impl_ctx_(nilptr), impl_ctx_del_() {}
        virtual ~CDelayedTask() {
            if (impl_ctx_ != nilptr) {
                if (impl_ctx_del_) {
                    impl_ctx_del_(impl_ctx_);
                }
            }
        }

    __nocopy__(CDelayedTask)
    public:
        void Execute() {
            if (!cancelled_.load()) {
                exec();
            }
        }

        void Cancel() {
            cancelled_.store(true);
        }

        void Reset() {
            cancelled_.store(false);
        }

        void* GetContext() const {
            return impl_ctx_;
        }

        void SetContext(void* ctx, std::function<void(void*)> del) {
            impl_ctx_ = ctx;
            impl_ctx_del_ = del;
        }

    protected:
        virtual void exec() = 0;

    private:
        std::atomic<bool> cancelled_;
        void* impl_ctx_;
        std::function<void(void*)> impl_ctx_del_;
    };
}

#endif